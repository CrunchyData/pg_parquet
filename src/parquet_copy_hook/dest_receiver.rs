use pg_sys::{
    pg_analyze_and_rewrite_fixedparams, pg_plan_query, AllocateFile, BlessTupleDesc,
    CommandDest_DestCopyOut, CommandTag_CMDTAG_COPY, CopyStmt, CreateNewPortal, Datum,
    DestReceiver, FreeFile, GetActiveSnapshot, HeapTupleData, List, NodeTag::T_CopyStmt,
    ParamListInfo, PlannedStmt, Portal, PortalDefineQuery, PortalDrop, QueryCompletion, RawStmt,
    Snapshot, TupleDesc, TupleTableSlot, CURSOR_OPT_PARALLEL_OK, PG_BINARY_W, _IO_FILE,
};
use pgrx::{is_a, prelude::*, PgList, PgTupleDesc};

use crate::arrow_parquet::{
    arrow_to_parquet_writer::write_tuples_to_parquet,
    schema_visitor::parquet_schema_string_from_tupledesc,
};

#[repr(C)]
struct ParquetCopyDestReceiver {
    dest: DestReceiver,
    filename: *mut i8,
    file: *mut _IO_FILE,
    tupledesc: TupleDesc,
    natts: i32,
    tuple_count: i64,
    tuples: *mut List,
}

fn collect_tuple(
    parquet_dest: &mut PgBox<ParquetCopyDestReceiver>,
    tuple: PgHeapTuple<AllocatedByRust>,
) {
    let mut tuples = unsafe { PgList::from_pg(parquet_dest.tuples) };
    tuples.push(tuple.into_pg());

    parquet_dest.tuples = tuples.into_pg();
    parquet_dest.tuple_count += 1;
}

fn reset_collected_tuples(parquet_dest: &mut PgBox<ParquetCopyDestReceiver>) {
    parquet_dest.tuple_count = 0;
    unsafe { pg_sys::list_free_deep(parquet_dest.tuples) };
    parquet_dest.tuples = PgList::<HeapTupleData>::new().into_pg();
}

fn copy_buffered_tuples(tupledesc: TupleDesc, tuples: *mut List, filename: *mut i8) {
    let tupledesc = unsafe { PgTupleDesc::from_pg(tupledesc) };
    let tuples = unsafe { PgList::from_pg(tuples) };
    let filename = unsafe { std::ffi::CStr::from_ptr(filename) }
        .to_str()
        .unwrap();

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

    let typoid = tupledesc.tdtypeid;
    let typmod = tupledesc.tdtypmod;

    pgrx::debug2!(
        "schema for tuples: {}",
        parquet_schema_string_from_tupledesc(tupledesc.clone())
    );

    write_tuples_to_parquet(filename, tuples, typoid, typmod);
}

#[pg_guard]
pub extern "C" fn copy_receive(slot: *mut TupleTableSlot, dest: *mut DestReceiver) -> bool {
    let parquet_dest = dest as *mut ParquetCopyDestReceiver;
    let mut parquet_dest = unsafe { PgBox::from_pg(parquet_dest) };

    let natts = parquet_dest.natts as usize;

    slot_getallattrs(slot);
    let slot = unsafe { PgBox::from_pg(slot) };

    let datums = slot.tts_values;
    let datums: Vec<Datum> = unsafe { std::slice::from_raw_parts(datums, natts).to_vec() };

    let nulls = slot.tts_isnull;
    let nulls: Vec<bool> = unsafe { std::slice::from_raw_parts(nulls, natts).to_vec() };

    let datums: Vec<Option<Datum>> = datums
        .into_iter()
        .zip(nulls.into_iter())
        .map(|(datum, is_null)| if is_null { None } else { Some(datum) })
        .collect();

    let tupledesc = unsafe { PgTupleDesc::from_pg(parquet_dest.tupledesc) };

    let heap_tuple = unsafe { PgHeapTuple::from_datums(tupledesc, datums).unwrap() };

    collect_tuple(&mut parquet_dest, heap_tuple);

    if parquet_dest.tuple_count == 100 {
        copy_buffered_tuples(
            parquet_dest.tupledesc,
            parquet_dest.tuples,
            parquet_dest.filename,
        );

        reset_collected_tuples(&mut parquet_dest);
    }

    true
}

#[pg_guard]
pub extern "C" fn copy_startup(dest: *mut DestReceiver, _operation: i32, tupledesc: TupleDesc) {
    let parquet_dest = dest as *mut ParquetCopyDestReceiver;
    let mut parquet_dest = unsafe { PgBox::from_pg(parquet_dest) };

    // bless tupledesc, otherwise lookup_row_tupledesc would fail for row types
    let tupledesc = unsafe { BlessTupleDesc(tupledesc) };
    let tupledesc = unsafe { PgTupleDesc::from_pg(tupledesc) };

    // count the number of attributes that are not dropped
    let mut natts = 0;
    for attr in tupledesc.iter() {
        if attr.is_dropped() {
            continue;
        }
        natts += 1;
    }

    // open the file
    let file = unsafe { AllocateFile(parquet_dest.filename, PG_BINARY_W.as_ptr() as _) };

    parquet_dest.file = file;
    parquet_dest.tupledesc = tupledesc.as_ptr();
    parquet_dest.natts = natts;
    parquet_dest.tuples = PgList::<HeapTupleData>::new().into_pg();
}

#[pg_guard]
pub extern "C" fn copy_shutdown(dest: *mut DestReceiver) {
    let parquet_dest = dest as *mut ParquetCopyDestReceiver;
    let mut parquet_dest = unsafe { PgBox::from_pg(parquet_dest) };

    if parquet_dest.tuple_count > 0 {
        copy_buffered_tuples(
            parquet_dest.tupledesc,
            parquet_dest.tuples,
            parquet_dest.filename,
        );
    }

    reset_collected_tuples(&mut parquet_dest);

    if !parquet_dest.file.is_null() {
        unsafe { FreeFile(parquet_dest.file) };
    }
}

#[pg_guard]
pub extern "C" fn copy_destroy(_dest: *mut DestReceiver) {
    ()
}

pub(crate) fn create_parquet_dest_receiver(filename: *mut i8) -> PgBox<DestReceiver> {
    let mut parquet_dest = unsafe { PgBox::<ParquetCopyDestReceiver>::alloc0() };
    parquet_dest.dest.receiveSlot = Some(copy_receive);
    parquet_dest.dest.rStartup = Some(copy_startup);
    parquet_dest.dest.rShutdown = Some(copy_shutdown);
    parquet_dest.dest.rDestroy = Some(copy_destroy);
    parquet_dest.dest.mydest = CommandDest_DestCopyOut;
    parquet_dest.filename = filename;
    parquet_dest.file = std::ptr::null_mut();
    parquet_dest.tupledesc = std::ptr::null_mut();
    parquet_dest.natts = 0;
    parquet_dest.tuple_count = 0;
    parquet_dest.tuples = std::ptr::null_mut();

    // it should be into_pg() (not as_ptr()) to prevent pfree of Rust allocated memory
    let dest: *mut DestReceiver = unsafe { std::mem::transmute(parquet_dest.into_pg()) };
    unsafe { PgBox::from_pg(dest) }
}

pub(crate) fn execute_query_with_dest_receiver(
    pstmt: PgBox<pg_sys::PlannedStmt>,
    query_string: &core::ffi::CStr,
    params: PgBox<pg_sys::ParamListInfoData>,
    query_env: PgBox<pg_sys::QueryEnvironment>,
    parquet_dest: PgBox<DestReceiver>,
) -> u64 {
    unsafe {
        assert!(is_a(pstmt.utilityStmt, T_CopyStmt));
        let copy_stmt = PgBox::<CopyStmt>::from_pg(pstmt.utilityStmt as _);

        // prepare raw query
        let mut raw_query = PgBox::<RawStmt>::alloc0();
        raw_query.stmt = copy_stmt.query;
        raw_query.stmt_location = pstmt.stmt_location;
        raw_query.stmt_len = pstmt.stmt_len;

        // analyze and rewrite raw query
        let rewritten_queries = pg_analyze_and_rewrite_fixedparams(
            raw_query.as_ptr(),
            query_string.as_ptr(),
            std::ptr::null_mut(),
            0,
            query_env.as_ptr(),
        );

        // plan rewritten query
        let query = PgList::from_pg(rewritten_queries).pop().unwrap();
        let plan = pg_plan_query(
            query,
            std::ptr::null(),
            CURSOR_OPT_PARALLEL_OK as _,
            params.as_ptr(),
        );

        // create portal
        let portal = CreateNewPortal();
        let mut portal = PgBox::from_pg(portal);
        portal.visible = false;

        // prepare portal
        let mut plans = PgList::<PlannedStmt>::new();
        plans.push(plan);

        PortalDefineQuery(
            portal.as_ptr(),
            std::ptr::null(),
            query_string.as_ptr(),
            CommandTag_CMDTAG_COPY,
            plans.as_ptr(),
            std::ptr::null_mut(),
        );

        // start portal
        PortalStart(portal.as_ptr(), params.as_ptr(), 0, GetActiveSnapshot());

        // run portal
        let mut completion_tag = QueryCompletion {
            commandTag: CommandTag_CMDTAG_COPY as _,
            nprocessed: 0,
        };

        PortalRun(
            portal.as_ptr(),
            i64::MAX,
            false,
            true,
            parquet_dest.as_ptr(),
            parquet_dest.as_ptr(),
            &mut completion_tag as _,
        );

        // drop portal
        PortalDrop(portal.as_ptr(), false);

        return completion_tag.nprocessed;
    };
}

// needed to declare these functions since they are not available in pg_sys yet
#[allow(improper_ctypes)]
extern "C" {
    fn PortalStart(
        portal: Portal,
        params: ParamListInfo,
        eflags: ::std::os::raw::c_int,
        snapshot: Snapshot,
    );
    fn PortalRun(
        portal: Portal,
        count: ::std::os::raw::c_long,
        is_top_level: bool,
        run_once: bool,
        dest: *mut DestReceiver,
        alt_dest: *mut DestReceiver,
        completion_tag: *mut QueryCompletion,
    );
}

/*
 * slot_getallattrs
 *		This function forces all the entries of the slot's Datum/isnull
 *		arrays to be valid.  The caller may then extract data directly
 *		from those arrays instead of using slot_getattr.
 */
fn slot_getallattrs(slot: *mut TupleTableSlot) {
    // copied from Postgres since this method was inlined in the original code
    // (not found in pg_sys)
    // handles select * from table
    unsafe {
        let slot = PgBox::from_pg(slot);
        let tts_tupledesc = PgBox::from_pg(slot.tts_tupleDescriptor);
        if (slot.tts_nvalid as i32) < tts_tupledesc.natts {
            pg_sys::slot_getsomeattrs_int(slot.as_ptr(), tts_tupledesc.natts);
        }
    };
}
