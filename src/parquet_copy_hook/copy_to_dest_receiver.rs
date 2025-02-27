use std::ffi::{c_char, CStr, CString};

use pg_sys::{
    get_typlenbyval, slot_getallattrs, toast_raw_datum_size, AllocSetContextCreateExtended,
    AsPgCStr, BlessTupleDesc, CommandDest, CurrentMemoryContext, Datum, DestReceiver,
    HeapTupleData, List, MemoryContext, MemoryContextAllocZero, MemoryContextDelete,
    MemoryContextReset, TupleDesc, TupleTableSlot, ALLOCSET_DEFAULT_INITSIZE,
    ALLOCSET_DEFAULT_MAXSIZE, ALLOCSET_DEFAULT_MINSIZE, VARHDRSZ,
};
use pgrx::{prelude::*, FromDatum, PgList, PgMemoryContexts, PgTupleDesc};

use crate::arrow_parquet::{
    compression::{PgParquetCompression, INVALID_COMPRESSION_LEVEL},
    parquet_writer::{ParquetWriterContext, DEFAULT_ROW_GROUP_SIZE, DEFAULT_ROW_GROUP_SIZE_BYTES},
    uri_utils::ParsedUriInfo,
};

#[repr(C)]
struct CopyToParquetOptions {
    pub row_group_size: i64,
    pub row_group_size_bytes: i64,
    pub compression: PgParquetCompression,
    pub compression_level: i32,
}

#[repr(C)]
struct CopyToParquetDestReceiver {
    dest: DestReceiver,
    natts: usize,
    tupledesc: TupleDesc,
    collected_tuples: *mut List,
    collected_tuple_count: i64,
    collected_tuple_size: i64,
    collected_tuple_column_sizes: *mut i64,
    uri: *const c_char,
    copy_options: CopyToParquetOptions,
    per_copy_context: MemoryContext,
    parquet_writer_context: *mut ParquetWriterContext,
}

impl CopyToParquetDestReceiver {
    fn collect_tuple(&mut self, tuple: PgHeapTuple<AllocatedByRust>, tuple_column_sizes: Vec<i32>) {
        let mut tuples = unsafe { PgList::from_pg(self.collected_tuples) };
        tuples.push(tuple.into_pg());
        self.collected_tuples = tuples.into_pg();

        let column_sizes = unsafe {
            std::slice::from_raw_parts_mut(self.collected_tuple_column_sizes, self.natts)
        };
        column_sizes
            .iter_mut()
            .zip(tuple_column_sizes.iter())
            .for_each(|(a, b)| *a += *b as i64);

        self.collected_tuple_size += tuple_column_sizes
            .iter()
            .map(|size| *size as i64)
            .sum::<i64>();

        self.collected_tuple_count += 1;
    }

    fn reset_collected_tuples(&mut self) {
        unsafe { MemoryContextReset(self.per_copy_context) };

        self.collected_tuple_count = 0;
        self.collected_tuple_size = 0;
        self.collected_tuples = PgList::<HeapTupleData>::new().into_pg();
        self.collected_tuple_column_sizes = unsafe {
            MemoryContextAllocZero(
                self.per_copy_context,
                std::mem::size_of::<i64>() * self.natts,
            ) as *mut i64
        };
    }

    fn collected_tuples_exceeds_row_group_size(&self) -> bool {
        self.collected_tuple_count >= self.copy_options.row_group_size
    }

    fn collected_tuples_exceeds_row_group_size_bytes(&self) -> bool {
        self.collected_tuple_size >= self.copy_options.row_group_size_bytes
    }

    fn collected_tuples_exceeds_max_col_size(&self, tuple_column_sizes: &[i32]) -> bool {
        const MAX_ARROW_ARRAY_SIZE: i64 = i32::MAX as _;

        let column_sizes =
            unsafe { std::slice::from_raw_parts(self.collected_tuple_column_sizes, self.natts) };

        column_sizes
            .iter()
            .zip(tuple_column_sizes)
            .map(|(a, b)| *a + *b as i64)
            .any(|size| size > MAX_ARROW_ARRAY_SIZE)
    }

    fn write_tuples_to_parquet(&mut self) {
        debug_assert!(!self.tupledesc.is_null());

        let tupledesc = unsafe { PgTupleDesc::from_pg_unchecked(self.tupledesc) };

        let tuples = unsafe { PgList::from_pg(self.collected_tuples) };
        let tuples = tuples
            .iter_ptr()
            .map(|tup_ptr: *mut HeapTupleData| unsafe {
                if tup_ptr.is_null() {
                    None
                } else {
                    let tup = PgHeapTuple::from_heap_tuple(tupledesc.clone(), tup_ptr).into_owned();
                    Some(tup)
                }
            })
            .collect::<Vec<_>>();

        let current_parquet_writer_context = unsafe {
            self.parquet_writer_context
                .as_mut()
                .expect("parquet writer context is not found")
        };
        current_parquet_writer_context.write_new_row_group(tuples);

        self.reset_collected_tuples();
    }

    fn cleanup(&mut self) {
        if !self.per_copy_context.is_null() {
            unsafe { MemoryContextDelete(self.per_copy_context) };

            self.per_copy_context = std::ptr::null_mut();
        }

        if !self.parquet_writer_context.is_null() {
            let parquet_writer_context = unsafe { Box::from_raw(self.parquet_writer_context) };

            self.parquet_writer_context = std::ptr::null_mut();

            drop(parquet_writer_context);
        }

        self.collected_tuple_count = 0;
    }
}

#[pg_guard]
extern "C" fn copy_startup(dest: *mut DestReceiver, _operation: i32, tupledesc: TupleDesc) {
    let parquet_dest = unsafe {
        (dest as *mut CopyToParquetDestReceiver)
            .as_mut()
            .expect("invalid parquet dest receiver ptr")
    };

    // bless tupledesc, otherwise lookup_row_tupledesc would fail for row types
    let tupledesc = unsafe { BlessTupleDesc(tupledesc) };

    // from_pg_unchecked makes sure tupledesc is not dropped since it is an external tupledesc
    let tupledesc = unsafe { PgTupleDesc::from_pg_unchecked(tupledesc) };

    // update the parquet dest receiver's missing fields
    parquet_dest.tupledesc = tupledesc.as_ptr();
    parquet_dest.collected_tuples = PgList::<HeapTupleData>::new().into_pg();
    parquet_dest.collected_tuple_column_sizes = unsafe {
        MemoryContextAllocZero(
            parquet_dest.per_copy_context,
            std::mem::size_of::<i64>() * tupledesc.len(),
        ) as *mut i64
    };
    parquet_dest.natts = tupledesc.len();

    let uri = unsafe { CStr::from_ptr(parquet_dest.uri) }
        .to_str()
        .expect("uri is not a valid C string");

    let uri_info = ParsedUriInfo::try_from(uri).unwrap_or_else(|e| {
        panic!("{}", e.to_string());
    });

    let compression = parquet_dest.copy_options.compression;

    let compression_level = parquet_dest.copy_options.compression_level;

    // leak the parquet writer context since it will be used during the COPY operation
    let parquet_writer_context =
        ParquetWriterContext::new(uri_info, compression, compression_level, &tupledesc);
    parquet_dest.parquet_writer_context = Box::into_raw(Box::new(parquet_writer_context));
}

#[pg_guard]
extern "C" fn copy_receive(slot: *mut TupleTableSlot, dest: *mut DestReceiver) -> bool {
    let parquet_dest = unsafe {
        (dest as *mut CopyToParquetDestReceiver)
            .as_mut()
            .expect("invalid parquet dest receiver ptr")
    };

    unsafe {
        let mut per_copy_ctx = PgMemoryContexts::For(parquet_dest.per_copy_context);

        per_copy_ctx.switch_to(|_context| {
            // extracts all attributes in statement "SELECT * FROM table"
            slot_getallattrs(slot);

            let natts = parquet_dest.natts;

            let datums = std::slice::from_raw_parts((*slot).tts_values, natts);

            let nulls = std::slice::from_raw_parts((*slot).tts_isnull, natts);

            let datums: Vec<Option<Datum>> = datums
                .iter()
                .zip(nulls)
                .map(|(datum, is_null)| if *is_null { None } else { Some(*datum) })
                .collect();

            let tupledesc = PgTupleDesc::from_pg_unchecked(parquet_dest.tupledesc);

            let column_sizes = tuple_column_sizes(&datums, &tupledesc);

            if parquet_dest.collected_tuples_exceeds_max_col_size(&column_sizes) {
                parquet_dest.write_tuples_to_parquet();
            }

            let heap_tuple = PgHeapTuple::from_datums(tupledesc, datums)
                .unwrap_or_else(|e| panic!("failed to create heap tuple from datums: {}", e));

            parquet_dest.collect_tuple(heap_tuple, column_sizes);

            if parquet_dest.collected_tuples_exceeds_row_group_size()
                || parquet_dest.collected_tuples_exceeds_row_group_size_bytes()
            {
                parquet_dest.write_tuples_to_parquet();
            }
        });
    };

    true
}

#[pg_guard]
extern "C" fn copy_shutdown(dest: *mut DestReceiver) {
    let parquet_dest = unsafe {
        (dest as *mut CopyToParquetDestReceiver)
            .as_mut()
            .expect("invalid parquet dest receiver ptr")
    };

    if parquet_dest.collected_tuple_count > 0 {
        parquet_dest.write_tuples_to_parquet();
    }

    parquet_dest.cleanup();
}

#[pg_guard]
extern "C" fn copy_destroy(_dest: *mut DestReceiver) {}

// create_copy_to_parquet_dest_receiver creates a new CopyToParquetDestReceiver that can be
// used as a destination receiver for COPY TO command. All arguments, except "uri", are optional
// and have default values if not provided.
#[pg_guard]
#[no_mangle]
pub extern "C" fn create_copy_to_parquet_dest_receiver(
    uri: *const c_char,
    row_group_size: *const i64,
    row_group_size_bytes: *const i64,
    compression: *const PgParquetCompression,
    compression_level: *const i32,
) -> *mut DestReceiver {
    let per_copy_context = unsafe {
        AllocSetContextCreateExtended(
            CurrentMemoryContext as _,
            "ParquetCopyDestReceiver".as_pg_cstr(),
            ALLOCSET_DEFAULT_MINSIZE as _,
            ALLOCSET_DEFAULT_INITSIZE as _,
            ALLOCSET_DEFAULT_MAXSIZE as _,
        )
    };

    let row_group_size = if row_group_size.is_null() {
        DEFAULT_ROW_GROUP_SIZE
    } else {
        unsafe { *row_group_size }
    };

    let row_group_size_bytes = if row_group_size_bytes.is_null() {
        DEFAULT_ROW_GROUP_SIZE_BYTES
    } else {
        unsafe { *row_group_size_bytes }
    };

    let compression = if compression.is_null() {
        PgParquetCompression::default()
    } else {
        unsafe { *compression }
    };

    let compression_level = if compression_level.is_null() {
        compression
            .default_compression_level()
            .unwrap_or(INVALID_COMPRESSION_LEVEL)
    } else {
        unsafe { *compression_level }
    };

    let mut parquet_dest =
        unsafe { PgBox::<CopyToParquetDestReceiver, AllocatedByPostgres>::alloc0() };

    parquet_dest.dest.receiveSlot = Some(copy_receive);
    parquet_dest.dest.rStartup = Some(copy_startup);
    parquet_dest.dest.rShutdown = Some(copy_shutdown);
    parquet_dest.dest.rDestroy = Some(copy_destroy);
    parquet_dest.dest.mydest = CommandDest::DestCopyOut;
    parquet_dest.uri = uri;
    parquet_dest.tupledesc = std::ptr::null_mut();
    parquet_dest.parquet_writer_context = std::ptr::null_mut();
    parquet_dest.natts = 0;
    parquet_dest.collected_tuple_count = 0;
    parquet_dest.collected_tuples = std::ptr::null_mut();
    parquet_dest.collected_tuple_column_sizes = std::ptr::null_mut();
    parquet_dest.copy_options.row_group_size = row_group_size;
    parquet_dest.copy_options.row_group_size_bytes = row_group_size_bytes;
    parquet_dest.copy_options.compression = compression;
    parquet_dest.copy_options.compression_level = compression_level;
    parquet_dest.per_copy_context = per_copy_context;

    unsafe { std::mem::transmute(parquet_dest) }
}

fn tuple_column_sizes(tuple_datums: &[Option<Datum>], tupledesc: &PgTupleDesc) -> Vec<i32> {
    let mut column_sizes = vec![];

    for (idx, column_datum) in tuple_datums.iter().enumerate() {
        if column_datum.is_none() {
            column_sizes.push(0);
            continue;
        }

        let column_datum = column_datum.as_ref().expect("column datum is None");

        let attribute = tupledesc.get(idx).expect("cannot get attribute");

        let typoid = attribute.type_oid();

        let mut typlen = -1_i16;
        let mut typbyval = false;
        unsafe { get_typlenbyval(typoid.value(), &mut typlen, &mut typbyval) };

        let column_size = if typlen == -1 {
            (unsafe { toast_raw_datum_size(*column_datum) }) as i32 - VARHDRSZ as i32
        } else if typlen == -2 {
            // cstring
            let cstring = unsafe {
                CString::from_datum(*column_datum, false).expect("cannot get cstring from datum")
            };
            cstring.as_bytes().len() as i32 + 1
        } else {
            // fixed size type
            typlen as i32
        };

        column_sizes.push(column_size);
    }

    column_sizes
}
