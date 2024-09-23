use pg_sys::{
    slot_getallattrs, AsPgCStr, BlessTupleDesc, CommandDest, CurrentMemoryContext, Datum,
    DestReceiver, HeapTupleData, List, MemoryContext, TupleDesc, TupleTableSlot,
};
use pgrx::{prelude::*, PgList, PgMemoryContexts, PgTupleDesc};

use crate::{
    arrow_parquet::{
        codec::ParquetCodecOption, parquet_writer::ParquetWriterContext,
        schema_visitor::parquet_schema_string_from_tupledesc, uri_utils::parse_uri,
    },
    parquet_copy_hook::copy_utils::tuple_column_sizes,
    pgrx_utils::collect_valid_attributes,
};

#[repr(C)]
struct CopyToParquetDestReceiver {
    dest: DestReceiver,
    uri: *mut i8,
    tupledesc: TupleDesc,
    tuple_count: i64,
    tuples: *mut List,
    natts: usize,
    column_sizes: *mut i64,
    codec: ParquetCodecOption,
    row_group_size: i64,
    per_copy_context: MemoryContext,
}

impl CopyToParquetDestReceiver {
    fn collect_tuple(&mut self, tuple: PgHeapTuple<AllocatedByRust>, tuple_column_sizes: Vec<i32>) {
        let mut tuples = unsafe { PgList::from_pg(self.tuples) };
        tuples.push(tuple.into_pg());
        self.tuples = tuples.into_pg();

        let column_sizes = unsafe { std::slice::from_raw_parts_mut(self.column_sizes, self.natts) };
        column_sizes
            .iter_mut()
            .zip(tuple_column_sizes.iter())
            .for_each(|(a, b)| *a += *b as i64);

        self.tuple_count += 1;
    }

    fn reset_tuples(&mut self) {
        unsafe { pg_sys::MemoryContextReset(self.per_copy_context) };

        self.tuple_count = 0;
        self.tuples = PgList::<HeapTupleData>::new().into_pg();
        self.column_sizes = unsafe {
            pg_sys::MemoryContextAllocZero(
                self.per_copy_context,
                std::mem::size_of::<i64>() * self.natts,
            ) as *mut i64
        };
    }

    fn exceeds_row_group_size(&self) -> bool {
        self.tuple_count >= self.row_group_size
    }

    fn exceeds_max_col_size(&self, tuple_column_sizes: &[i32]) -> bool {
        let column_sizes = unsafe { std::slice::from_raw_parts(self.column_sizes, self.natts) };
        column_sizes
            .iter()
            .zip(tuple_column_sizes)
            .map(|(a, b)| *a + *b as i64)
            .any(|size| size > i32::MAX as i64)
    }

    fn write_tuples_to_parquet(&mut self) {
        debug_assert!(!self.tupledesc.is_null());

        let tupledesc = unsafe { PgTupleDesc::from_pg(self.tupledesc) };

        let tuples = unsafe { PgList::from_pg(self.tuples) };
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

        pgrx::debug2!(
            "schema for tuples: {}",
            parquet_schema_string_from_tupledesc(&tupledesc)
        );

        let current_parquet_writer_context =
            peek_parquet_writer_context().expect("parquet writer context is not found");
        current_parquet_writer_context.write_new_row_group(tuples);

        self.reset_tuples();
    }

    fn cleanup(&mut self) {
        unsafe { pg_sys::MemoryContextDelete(self.per_copy_context) };
    }
}

// stack to store parquet writer contexts for COPY TO.
// This needs to be a stack since COPY command can be nested.
static mut PARQUET_WRITER_CONTEXT_STACK: Vec<ParquetWriterContext> = vec![];

pub(crate) fn peek_parquet_writer_context() -> Option<&'static mut ParquetWriterContext> {
    unsafe { PARQUET_WRITER_CONTEXT_STACK.last_mut() }
}

pub(crate) fn pop_parquet_writer_context(throw_error: bool) -> Option<ParquetWriterContext> {
    let mut current_parquet_writer_context = unsafe { PARQUET_WRITER_CONTEXT_STACK.pop() };

    if current_parquet_writer_context.is_none() {
        let level = if throw_error {
            PgLogLevel::ERROR
        } else {
            PgLogLevel::DEBUG2
        };

        ereport!(
            level,
            PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            "parquet writer context stack is already empty"
        );

        None
    } else {
        current_parquet_writer_context.take()
    }
}

pub(crate) fn push_parquet_writer_context(writer_ctx: ParquetWriterContext) {
    unsafe { PARQUET_WRITER_CONTEXT_STACK.push(writer_ctx) };
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
    let tupledesc = unsafe { PgTupleDesc::from_pg(tupledesc) };

    let include_generated_columns = true;
    let attributes = collect_valid_attributes(&tupledesc, include_generated_columns);

    // update the parquet dest receiver's missing fields
    parquet_dest.tupledesc = tupledesc.as_ptr();
    parquet_dest.tuples = PgList::<HeapTupleData>::new().into_pg();
    parquet_dest.column_sizes = unsafe {
        pg_sys::MemoryContextAllocZero(
            parquet_dest.per_copy_context,
            std::mem::size_of::<i64>() * attributes.len(),
        ) as *mut i64
    };
    parquet_dest.natts = attributes.len();

    let uri = unsafe { std::ffi::CStr::from_ptr(parquet_dest.uri) }
        .to_str()
        .expect("uri is not a valid C string");

    let uri = parse_uri(uri);

    let codec = parquet_dest.codec;

    // parquet writer context is used throughout the COPY TO operation.
    // This might be put into ParquetCopyDestReceiver, but it's hard to preserve repr(C).
    let parquet_writer_context = ParquetWriterContext::new(uri, codec, &tupledesc);
    push_parquet_writer_context(parquet_writer_context);
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

            let slot = PgBox::from_pg(slot);

            let natts = parquet_dest.natts;

            let datums = slot.tts_values;
            let datums = std::slice::from_raw_parts(datums, natts);

            let nulls = slot.tts_isnull;
            let nulls = std::slice::from_raw_parts(nulls, natts);

            let datums: Vec<Option<Datum>> = datums
                .iter()
                .zip(nulls)
                .map(|(datum, is_null)| if *is_null { None } else { Some(*datum) })
                .collect();

            let tupledesc = PgTupleDesc::from_pg(parquet_dest.tupledesc);

            let column_sizes = tuple_column_sizes(&datums, &tupledesc);

            if parquet_dest.exceeds_max_col_size(&column_sizes) {
                parquet_dest.write_tuples_to_parquet();
            }

            let heap_tuple = PgHeapTuple::from_datums(tupledesc, datums)
                .unwrap_or_else(|e| panic!("failed to create heap tuple from datums: {}", e));

            parquet_dest.collect_tuple(heap_tuple, column_sizes);

            if parquet_dest.exceeds_row_group_size() {
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

    if parquet_dest.tuple_count > 0 {
        parquet_dest.write_tuples_to_parquet();
    }

    parquet_dest.cleanup();

    let throw_error = true;
    let current_parquet_writer_context = pop_parquet_writer_context(throw_error);
    current_parquet_writer_context
        .expect("current parquet writer context is not found")
        .close();
}

#[pg_guard]
extern "C" fn copy_destroy(_dest: *mut DestReceiver) {}

#[pg_guard]
#[no_mangle]
pub extern "C" fn create_copy_to_parquet_dest_receiver(
    uri: *mut i8,
    row_group_size: i64,
    codec: ParquetCodecOption,
) -> *mut DestReceiver {
    let per_copy_context = unsafe {
        pg_sys::AllocSetContextCreateExtended(
            CurrentMemoryContext as _,
            "ParquetCopyDestReceiver".as_pg_cstr(),
            pg_sys::ALLOCSET_DEFAULT_MINSIZE as _,
            pg_sys::ALLOCSET_DEFAULT_INITSIZE as _,
            pg_sys::ALLOCSET_DEFAULT_MAXSIZE as _,
        )
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
    parquet_dest.natts = 0;
    parquet_dest.tuple_count = 0;
    parquet_dest.tuples = std::ptr::null_mut();
    parquet_dest.column_sizes = std::ptr::null_mut();
    parquet_dest.row_group_size = row_group_size;
    parquet_dest.codec = codec;
    parquet_dest.per_copy_context = per_copy_context;

    unsafe { std::mem::transmute(parquet_dest) }
}
