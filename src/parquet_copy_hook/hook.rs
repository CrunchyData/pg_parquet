use pg_sys::{
    defGetInt64, defGetString, CommandTag_CMDTAG_COPY, CopyStmt, DefElem, NodeTag::T_CopyStmt,
};
use pgrx::{is_a, prelude::*, register_hook, HookResult, PgHooks, PgList};

use crate::parquet_copy_hook::dest_receiver::{
    create_parquet_dest_receiver, execute_query_with_dest_receiver,
};

static mut COPY_TO_PARQUET_HOOK: CopyToParquetHook = CopyToParquetHook {};

#[allow(static_mut_refs)]
#[pg_guard]
pub extern "C" fn _PG_init() {
    unsafe { register_hook(&mut COPY_TO_PARQUET_HOOK) };
}

struct CopyToParquetHook {}

impl CopyToParquetHook {
    fn is_parquet_format(copy_stmt: PgBox<CopyStmt>) -> bool {
        let copy_options = unsafe { PgList::<DefElem>::from_pg(copy_stmt.options) };
        for option in copy_options.iter_ptr() {
            let option = unsafe { PgBox::<DefElem>::from_pg(option) };
            let key = unsafe { std::ffi::CStr::from_ptr(option.defname).to_str().unwrap() };
            if key != "format" {
                continue;
            }

            let format = unsafe { defGetString(option.as_ptr()) };
            let format = unsafe { std::ffi::CStr::from_ptr(format).to_str().unwrap() };
            if format == "parquet" {
                return true;
            } else {
                return false;
            }
        }

        return false;
    }

    fn is_parquet_file(copy_stmt: PgBox<CopyStmt>) -> bool {
        let filename = unsafe {
            std::ffi::CStr::from_ptr(copy_stmt.filename)
                .to_str()
                .unwrap()
        };
        filename.ends_with(".parquet")
    }

    fn is_copy_to_parquet_stmt(pstmt: PgBox<pg_sys::PlannedStmt>) -> bool {
        let is_copy_stmt = unsafe { is_a(pstmt.utilityStmt, T_CopyStmt) };
        if !is_copy_stmt {
            return false;
        }

        let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(pstmt.utilityStmt as _) };
        if copy_stmt.is_from {
            return false;
        }

        if copy_stmt.filename.is_null() {
            return false;
        }

        if Self::is_parquet_format(copy_stmt.clone()) {
            return true;
        }

        Self::is_parquet_file(copy_stmt)
    }

    fn copy_stmt_filename(pstmt: PgBox<pg_sys::PlannedStmt>) -> *mut i8 {
        assert!(unsafe { is_a(pstmt.utilityStmt, T_CopyStmt) });
        let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(pstmt.utilityStmt as _) };
        assert!(!copy_stmt.filename.is_null());
        copy_stmt.filename
    }

    fn copy_stmt_batch_size_option(pstmt: PgBox<pg_sys::PlannedStmt>) -> i64 {
        assert!(unsafe { is_a(pstmt.utilityStmt, T_CopyStmt) });

        const DEFAULT_BATCH_SIZE: i64 = 1000;

        let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(pstmt.utilityStmt as _) };

        let copy_options = unsafe { PgList::<DefElem>::from_pg(copy_stmt.options) };
        for option in copy_options.iter_ptr() {
            let option = unsafe { PgBox::<DefElem>::from_pg(option) };
            let key = unsafe { std::ffi::CStr::from_ptr(option.defname).to_str().unwrap() };
            if key != "batch_size" {
                continue;
            }

            let batch_size = unsafe { defGetInt64(option.as_ptr()) };
            return batch_size;
        }

        DEFAULT_BATCH_SIZE
    }
}

impl PgHooks for CopyToParquetHook {
    fn process_utility_hook(
        &mut self,
        pstmt: PgBox<pg_sys::PlannedStmt>,
        query_string: &core::ffi::CStr,
        read_only_tree: Option<bool>,
        context: pg_sys::ProcessUtilityContext,
        params: PgBox<pg_sys::ParamListInfoData>,
        query_env: PgBox<pg_sys::QueryEnvironment>,
        dest: PgBox<pg_sys::DestReceiver>,
        completion_tag: *mut pg_sys::QueryCompletion,
        prev_hook: fn(
            pstmt: PgBox<pg_sys::PlannedStmt>,
            query_string: &core::ffi::CStr,
            read_only_tree: Option<bool>,
            context: pg_sys::ProcessUtilityContext,
            params: PgBox<pg_sys::ParamListInfoData>,
            query_env: PgBox<pg_sys::QueryEnvironment>,
            dest: PgBox<pg_sys::DestReceiver>,
            completion_tag: *mut pg_sys::QueryCompletion,
        ) -> HookResult<()>,
    ) -> HookResult<()> {
        if Self::is_copy_to_parquet_stmt(pstmt.clone()) {
            let filename = Self::copy_stmt_filename(pstmt.clone());
            let batch_size = Self::copy_stmt_batch_size_option(pstmt.clone());

            let parquet_dest = create_parquet_dest_receiver(filename, batch_size);

            let nprocessed = execute_query_with_dest_receiver(
                pstmt.clone(),
                query_string,
                params,
                query_env,
                parquet_dest,
            );

            if !completion_tag.is_null() {
                (unsafe { &mut *completion_tag }).nprocessed = nprocessed;
                (unsafe { &mut *completion_tag }).commandTag = CommandTag_CMDTAG_COPY as _;
            }

            return HookResult::new(());
        }

        prev_hook(
            pstmt,
            query_string,
            read_only_tree,
            context,
            params,
            query_env,
            dest,
            completion_tag,
        )
    }
}
