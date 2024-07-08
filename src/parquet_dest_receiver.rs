use pg_sys::{
    pg_analyze_and_rewrite_fixedparams, pg_plan_query, AllocateFile, BlessTupleDesc,
    CommandDest_DestCopyOut, CommandTag_CMDTAG_COPY, CopyStmt, CreateNewPortal, Datum,
    DestReceiver, FreeFile, GetActiveSnapshot, NodeTag::T_CopyStmt, ParamListInfo, PlannedStmt,
    Portal, PortalDefineQuery, PortalDrop, QueryCompletion, RawStmt, Snapshot, TupleDesc,
    TupleTableSlot, CURSOR_OPT_PARALLEL_OK, PG_BINARY_A, _IO_FILE,
};
use pgrx::{is_a, prelude::*, PgList, PgTupleDesc};

#[repr(C)]
struct ParquetCopyDestReceiver {
    dest: DestReceiver,
    filename: *mut i8,
    file: *mut _IO_FILE,
    tupledesc: TupleDesc,
    natts: i32,
}

pub extern "C" fn copy_receive(slot: *mut TupleTableSlot, dest: *mut DestReceiver) -> bool {
    let parquet_dest = dest as *mut ParquetCopyDestReceiver;
    let parquet_dest = unsafe { PgBox::from_pg(parquet_dest) };

    let natts = parquet_dest.natts as usize;

    let filename = unsafe { std::ffi::CStr::from_ptr(parquet_dest.filename) }
        .to_str()
        .unwrap();

    let slot = unsafe { PgBox::from_pg(slot) };
    let datums = slot.tts_values;
    let datums: Vec<Datum> = unsafe { std::slice::from_raw_parts(datums, natts).to_vec() };
    let datums: Vec<Option<Datum>> = datums.into_iter().map(|d| Some(d)).collect();
    let tupledesc = unsafe { PgTupleDesc::from_pg(parquet_dest.tupledesc) };

    let heap_tuple = unsafe { PgHeapTuple::from_datums(tupledesc, datums).unwrap() };
    unsafe {
        pgrx::direct_function_call::<Datum>(
            crate::pgparquet::serialize,
            &[heap_tuple.into_datum(), filename.into_datum()],
        )
        .unwrap();
    }

    true
}

pub extern "C" fn copy_startup(dest: *mut DestReceiver, _operation: i32, tupledesc: TupleDesc) {
    let parquet_dest = dest as *mut ParquetCopyDestReceiver;
    let mut parquet_dest = unsafe { PgBox::from_pg(parquet_dest) };

    // bless tupledesc, otherwise lookup_row_tupledesc would fail for row types
    let tupledesc = unsafe { PgTupleDesc::from_pg(tupledesc) };
    unsafe { BlessTupleDesc(tupledesc.as_ptr()) };

    // count the number of attributes that are not dropped
    let mut natts = 0;
    for attr in tupledesc.iter() {
        if attr.is_dropped() {
            continue;
        }
        natts += 1;
    }

    // open the file
    let file = unsafe { AllocateFile(parquet_dest.filename, PG_BINARY_A.as_ptr() as _) };

    parquet_dest.file = file;
    parquet_dest.tupledesc = tupledesc.as_ptr();
    parquet_dest.natts = natts;
}

pub extern "C" fn copy_shutdown(dest: *mut DestReceiver) {
    let parquet_dest = dest as *mut ParquetCopyDestReceiver;
    let parquet_dest = unsafe { PgBox::from_pg(parquet_dest) };

    if !parquet_dest.file.is_null() {
        unsafe { FreeFile(parquet_dest.file) };
    }
}

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
