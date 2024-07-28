use pg_sys::CommandTag_CMDTAG_COPY;
use pgrx::{prelude::*, HookResult, PgHooks};

use crate::parquet_copy_hook::{
    copy_to_dest_receiver::create_copy_to_parquet_dest_receiver,
    copy_utils::{
        copy_stmt_filename, copy_stmt_row_group_size_option, is_copy_from_parquet_stmt,
        is_copy_to_parquet_stmt,
    },
};

use super::{
    copy_from::execute_copy_from, copy_to::execute_copy_to_with_dest_receiver,
    copy_utils::copy_stmt_codec,
};

pub(crate) static mut PARQUET_COPY_HOOK: ParquetCopyHook = ParquetCopyHook {};

pub(crate) struct ParquetCopyHook {}

impl ParquetCopyHook {}

impl PgHooks for ParquetCopyHook {
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
        if is_copy_to_parquet_stmt(&pstmt) {
            let filename = copy_stmt_filename(&pstmt);
            let row_group_size = copy_stmt_row_group_size_option(&pstmt);
            let codec = copy_stmt_codec(&pstmt);

            let parquet_dest =
                create_copy_to_parquet_dest_receiver(filename, row_group_size, codec);

            let nprocessed = execute_copy_to_with_dest_receiver(
                &pstmt,
                query_string,
                params,
                query_env,
                parquet_dest,
            );

            let mut completion_tag = unsafe { PgBox::from_pg(completion_tag) };
            if !completion_tag.is_null() {
                completion_tag.nprocessed = nprocessed;
                completion_tag.commandTag = CommandTag_CMDTAG_COPY;
            }

            return HookResult::new(());
        } else if is_copy_from_parquet_stmt(&pstmt) {
            let nprocessed = execute_copy_from(pstmt, query_string, query_env);

            let mut completion_tag = unsafe { PgBox::from_pg(completion_tag) };
            if !completion_tag.is_null() {
                completion_tag.nprocessed = nprocessed as _;
                completion_tag.commandTag = CommandTag_CMDTAG_COPY;
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
