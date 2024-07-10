use pg_sys::{CommandTag_CMDTAG_COPY, CopyStmt, NodeTag::T_CopyStmt};
use pgrx::{is_a, prelude::*, register_hook, HookResult, PgHooks};

use crate::parquet_copy_hook::dest_receiver::{
    create_parquet_dest_receiver, execute_query_with_dest_receiver,
};

static mut COPY_HOOK: CopyHook = CopyHook {};

#[allow(static_mut_refs)]
#[pg_guard]
pub extern "C" fn _PG_init() {
    unsafe { register_hook(&mut COPY_HOOK) };
}

struct CopyHook {}

impl CopyHook {
    fn is_copy_to_parquet(pstmt: PgBox<pg_sys::PlannedStmt>) -> bool {
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

        let filename = unsafe {
            std::ffi::CStr::from_ptr(copy_stmt.filename)
                .to_str()
                .unwrap()
        };
        filename.ends_with(".parquet")
    }

    fn copy_filename(pstmt: PgBox<pg_sys::PlannedStmt>) -> *mut i8 {
        assert!(unsafe { is_a(pstmt.utilityStmt, T_CopyStmt) });
        let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(pstmt.utilityStmt as _) };
        assert!(!copy_stmt.filename.is_null());
        copy_stmt.filename
    }
}

impl PgHooks for CopyHook {
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
        if Self::is_copy_to_parquet(pstmt.clone()) {
            let parquet_dest = create_parquet_dest_receiver(Self::copy_filename(pstmt.clone()));

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
