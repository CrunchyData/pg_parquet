use std::ffi::CStr;

use pg_sys::{standard_ProcessUtility, CommandTag, ProcessUtility_hook, ProcessUtility_hook_type};
use pgrx::{prelude::*, GucSetting};

use crate::parquet_copy_hook::{
    copy_to_dest_receiver::create_copy_to_parquet_dest_receiver,
    copy_utils::{
        copy_stmt_filename, copy_stmt_row_group_size_option, is_copy_from_parquet_stmt,
        is_copy_to_parquet_stmt,
    },
};

use super::{
    copy_from::{execute_copy_from, pop_parquet_reader_context},
    copy_to::execute_copy_to_with_dest_receiver,
    copy_to_dest_receiver::pop_parquet_writer_context,
    copy_utils::{copy_stmt_codec, validate_copy_from_options, validate_copy_to_options},
};

pub(crate) static ENABLE_PARQUET_COPY_HOOK: GucSetting<bool> = GucSetting::<bool>::new(true);

static mut PREV_PROCESS_UTILITY_HOOK: ProcessUtility_hook_type = None;

#[pg_guard]
#[no_mangle]
pub(crate) extern "C" fn init_parquet_copy_hook() {
    unsafe {
        if ProcessUtility_hook.is_some() {
            PREV_PROCESS_UTILITY_HOOK = ProcessUtility_hook
        }

        ProcessUtility_hook = Some(parquet_copy_hook);
    }
}

#[pg_guard]
#[allow(clippy::too_many_arguments)]
extern "C" fn parquet_copy_hook(
    pstmt: *mut pg_sys::PlannedStmt,
    query_string: *const i8,
    read_only_tree: bool,
    context: u32,
    params: *mut pg_sys::ParamListInfoData,
    query_env: *mut pg_sys::QueryEnvironment,
    dest: *mut pg_sys::DestReceiver,
    completion_tag: *mut pg_sys::QueryCompletion,
) {
    let pstmt = unsafe { PgBox::from_pg(pstmt) };
    let query_string = unsafe { CStr::from_ptr(query_string) };
    let params = unsafe { PgBox::from_pg(params) };
    let query_env = unsafe { PgBox::from_pg(query_env) };

    if ENABLE_PARQUET_COPY_HOOK.get() && is_copy_to_parquet_stmt(&pstmt) {
        validate_copy_to_options(&pstmt);

        let filename = copy_stmt_filename(&pstmt);
        let row_group_size = copy_stmt_row_group_size_option(&pstmt);
        let codec = copy_stmt_codec(&pstmt);

        PgTryBuilder::new(|| {
            let parquet_dest =
                create_copy_to_parquet_dest_receiver(filename, row_group_size, codec);

            let parquet_dest = unsafe { PgBox::from_pg(parquet_dest) };

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
                completion_tag.commandTag = CommandTag::CMDTAG_COPY;
            }
        })
        .catch_others(|cause| {
            // make sure to pop the parquet writer context
            let throw_error = false;
            pop_parquet_writer_context(throw_error);

            cause.rethrow()
        })
        .execute();

        return;
    } else if ENABLE_PARQUET_COPY_HOOK.get() && is_copy_from_parquet_stmt(&pstmt) {
        validate_copy_from_options(&pstmt);

        PgTryBuilder::new(|| {
            let nprocessed = execute_copy_from(pstmt, query_string, query_env);

            let mut completion_tag = unsafe { PgBox::from_pg(completion_tag) };
            if !completion_tag.is_null() {
                completion_tag.nprocessed = nprocessed;
                completion_tag.commandTag = CommandTag::CMDTAG_COPY;
            }
        })
        .catch_others(|cause| {
            // make sure to pop the parquet reader context
            let throw_error = false;
            pop_parquet_reader_context(throw_error);

            cause.rethrow()
        })
        .execute();

        return;
    }

    unsafe {
        if let Some(prev_hook) = PREV_PROCESS_UTILITY_HOOK {
            prev_hook(
                pstmt.into_pg(),
                query_string.as_ptr(),
                read_only_tree,
                context,
                params.into_pg(),
                query_env.into_pg(),
                dest,
                completion_tag,
            )
        } else {
            standard_ProcessUtility(
                pstmt.into_pg(),
                query_string.as_ptr(),
                read_only_tree,
                context,
                params.into_pg(),
                query_env.into_pg(),
                dest,
                completion_tag,
            )
        }
    }
}
