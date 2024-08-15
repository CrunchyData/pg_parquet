use pg_sys::{
    AsPgCStr, BlessTupleDesc, CommandDest_DestCopyOut, CurrentMemoryContext, Datum, DestReceiver,
    HeapTupleData, List, MemoryContext, TupleDesc, TupleTableSlot,
};
use pgrx::{prelude::*, PgList, PgMemoryContexts, PgTupleDesc};

use crate::{
    arrow_parquet::{
        codec::ParquetCodecOption, parquet_writer::ParquetWriterContext,
        schema_visitor::parquet_schema_string_from_tupledesc,
    },
    parquet_copy_hook::copy_utils::slot_getallattrs,
    pgrx_utils::collect_valid_attributes,
};

#[repr(C)]
struct CopyToParquetDestReceiver {
    dest: DestReceiver,
    filename: *mut i8,
    tupledesc: TupleDesc,
    natts: i32,
    tuple_count: i64,
    tuples: *mut List,
    row_group_size: i64,
    codec: ParquetCodecOption,
    per_copy_context: MemoryContext,
}

static mut PARQUET_WRITER_CONTEXT_STACK: Vec<ParquetWriterContext> = vec![];

pub(crate) fn peek_parquet_writer_context() -> Option<&'static mut ParquetWriterContext<'static>> {
    unsafe { PARQUET_WRITER_CONTEXT_STACK.last_mut() }
}

pub(crate) fn pop_parquet_writer_context(throw_error: bool) {
    let current_parquet_writer_context = unsafe { PARQUET_WRITER_CONTEXT_STACK.pop() };

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
    }

    current_parquet_writer_context.unwrap().close();
}

pub(crate) fn push_parquet_writer_context(writer_ctx: ParquetWriterContext<'static>) {
    unsafe { PARQUET_WRITER_CONTEXT_STACK.push(writer_ctx) };
}

fn collect_tuple(
    parquet_dest: &mut PgBox<CopyToParquetDestReceiver>,
    tuple: PgHeapTuple<AllocatedByRust>,
) {
    let mut tuples = unsafe { PgList::from_pg(parquet_dest.tuples) };
    tuples.push(tuple.into_pg());

    parquet_dest.tuples = tuples.into_pg();
    parquet_dest.tuple_count += 1;
}

fn reset_collected_tuples(parquet_dest: &mut PgBox<CopyToParquetDestReceiver>) {
    parquet_dest.tuple_count = 0;
    unsafe { pg_sys::list_free_deep(parquet_dest.tuples) };
    parquet_dest.tuples = PgList::<HeapTupleData>::new().into_pg();
}

fn copy_buffered_tuples(tupledesc: TupleDesc, tuples: *mut List) {
    let tupledesc = unsafe { PgTupleDesc::from_pg(tupledesc) };
    let tuples = unsafe { PgList::from_pg(tuples) };
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
        parquet_schema_string_from_tupledesc(tupledesc.clone())
    );

    let current_parquet_writer_context = peek_parquet_writer_context();

    if current_parquet_writer_context.is_none() {
        panic!("parquet writer context is not found");
    }

    let current_parquet_writer_context = current_parquet_writer_context.unwrap();
    current_parquet_writer_context.write_new_row_group(tuples);
}

#[pg_guard]
extern "C" fn copy_startup(dest: *mut DestReceiver, _operation: i32, tupledesc: TupleDesc) {
    let parquet_dest = dest as *mut CopyToParquetDestReceiver;
    let mut parquet_dest = unsafe { PgBox::from_pg(parquet_dest) };

    // bless tupledesc, otherwise lookup_row_tupledesc would fail for row types
    let tupledesc = unsafe { BlessTupleDesc(tupledesc) };
    let tupledesc = unsafe { PgTupleDesc::from_pg(tupledesc) };

    let filename = unsafe { std::ffi::CStr::from_ptr(parquet_dest.filename) }
        .to_str()
        .unwrap();

    let codec = parquet_dest.codec;

    // create parquet writer context and push it to the stack
    let parquet_writer_context =
        ParquetWriterContext::new(filename, codec, tupledesc.clone().to_owned());
    push_parquet_writer_context(parquet_writer_context);

    // count the number of attributes that are not dropped
    let include_generated_columns = true;
    let attributes = collect_valid_attributes(&tupledesc, include_generated_columns);
    let natts = attributes.len() as i32;

    parquet_dest.tupledesc = tupledesc.as_ptr();
    parquet_dest.natts = natts;
    parquet_dest.tuples = PgList::<HeapTupleData>::new().into_pg();
}

#[pg_guard]
extern "C" fn copy_receive(slot: *mut TupleTableSlot, dest: *mut DestReceiver) -> bool {
    let parquet_dest = dest as *mut CopyToParquetDestReceiver;
    let mut parquet_dest = unsafe { PgBox::from_pg(parquet_dest) };

    unsafe {
        let mut per_copy_ctx = PgMemoryContexts::For(parquet_dest.per_copy_context);

        per_copy_ctx.switch_to(|context| {
            let natts = parquet_dest.natts as usize;

            slot_getallattrs(slot);
            let slot = PgBox::from_pg(slot);

            let datums = slot.tts_values;
            let datums: Vec<Datum> = std::slice::from_raw_parts(datums, natts).to_vec();

            let nulls = slot.tts_isnull;
            let nulls: Vec<bool> = std::slice::from_raw_parts(nulls, natts).to_vec();

            let datums: Vec<Option<Datum>> = datums
                .into_iter()
                .zip(nulls)
                .map(|(datum, is_null)| if is_null { None } else { Some(datum) })
                .collect();

            let tupledesc = PgTupleDesc::from_pg(parquet_dest.tupledesc);

            let heap_tuple = PgHeapTuple::from_datums(tupledesc, datums).unwrap();

            collect_tuple(&mut parquet_dest, heap_tuple);

            if parquet_dest.tuple_count == parquet_dest.row_group_size {
                copy_buffered_tuples(parquet_dest.tupledesc, parquet_dest.tuples);
                reset_collected_tuples(&mut parquet_dest);
                context.reset()
            };
        });
    };

    true
}

#[pg_guard]
extern "C" fn copy_shutdown(dest: *mut DestReceiver) {
    let parquet_dest = dest as *mut CopyToParquetDestReceiver;
    let mut parquet_dest = unsafe { PgBox::from_pg(parquet_dest) };

    if parquet_dest.tuple_count > 0 {
        copy_buffered_tuples(parquet_dest.tupledesc, parquet_dest.tuples);
    }

    reset_collected_tuples(&mut parquet_dest);

    let throw_error = true;
    pop_parquet_writer_context(throw_error);

    let mut per_copy_ctx = PgMemoryContexts::For(parquet_dest.per_copy_context);
    unsafe { per_copy_ctx.reset() };
    unsafe { pg_sys::MemoryContextDelete(parquet_dest.per_copy_context) };
}

#[pg_guard]
extern "C" fn copy_destroy(_dest: *mut DestReceiver) {}

#[pg_guard]
#[no_mangle]
pub extern "C" fn create_copy_to_parquet_dest_receiver(
    filename: *mut i8,
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

    let mut parquet_dest = unsafe { PgBox::<CopyToParquetDestReceiver>::alloc0() };
    parquet_dest.dest.receiveSlot = Some(copy_receive);
    parquet_dest.dest.rStartup = Some(copy_startup);
    parquet_dest.dest.rShutdown = Some(copy_shutdown);
    parquet_dest.dest.rDestroy = Some(copy_destroy);
    parquet_dest.dest.mydest = CommandDest_DestCopyOut;
    parquet_dest.filename = filename;
    parquet_dest.tupledesc = std::ptr::null_mut();
    parquet_dest.natts = 0;
    parquet_dest.tuple_count = 0;
    parquet_dest.tuples = std::ptr::null_mut();
    parquet_dest.row_group_size = row_group_size;
    parquet_dest.codec = codec;
    parquet_dest.per_copy_context = per_copy_context;

    // it should be into_pg() (not as_ptr()) to prevent pfree of Rust allocated memory
    let dest: *mut DestReceiver = unsafe { std::mem::transmute(parquet_dest.into_pg()) };
    dest
}
