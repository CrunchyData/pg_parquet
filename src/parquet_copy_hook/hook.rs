use std::ffi::{c_char, CStr};

use pg_sys::{
    nodeToString, standard_ProcessUtility, AsPgCStr, CommandTag, CreateStmt, DestReceiver,
    ParamListInfoData, PlannedStmt, ProcessUtility_hook, ProcessUtility_hook_type, QueryCompletion,
    QueryEnvironment,
};
use pgrx::{prelude::*, GucSetting};

use crate::{
    arrow_parquet::{
        compression::INVALID_COMPRESSION_LEVEL,
        uri_utils::{ensure_access_privilege_to_uri, uri_as_string},
    },
    create_copy_to_parquet_split_dest_receiver,
    parquet_copy_hook::copy_utils::{
        copy_stmt_is_std_inout, copy_stmt_program, copy_stmt_uri, copy_to_stmt_compression_level,
        copy_to_stmt_row_group_size, copy_to_stmt_row_group_size_bytes, is_copy_from_parquet_stmt,
        is_copy_to_parquet_stmt,
    },
};

use super::{
    copy_from::{execute_copy_from, pop_parquet_reader_context},
    copy_to::execute_copy_to_with_dest_receiver,
    copy_to_split_dest_receiver::free_copy_to_parquet_split_dest_receiver,
    copy_utils::{
        copy_to_stmt_compression, copy_to_stmt_field_ids, copy_to_stmt_file_size_bytes,
        copy_to_stmt_parquet_version, has_option, validate_copy_from_options,
        validate_copy_to_options,
    },
    create_table::{
        create_copy_from_parquet_stmt_for_table, create_stmt_get_uri,
        create_stmt_remove_parquet_options, infer_column_definitions,
        is_create_table_from_parquet_stmt, validate_create_table_from_parquet_stmt,
    },
};

pub(crate) static ENABLE_PARQUET_COPY_HOOK: GucSetting<bool> = GucSetting::<bool>::new(true);

static mut PREV_PROCESS_UTILITY_HOOK: ProcessUtility_hook_type = None;

#[pg_guard]
#[no_mangle]
pub(crate) extern "C-unwind" fn init_parquet_copy_hook() {
    #[allow(static_mut_refs)]
    unsafe {
        if ProcessUtility_hook.is_some() {
            PREV_PROCESS_UTILITY_HOOK = ProcessUtility_hook
        }

        ProcessUtility_hook = Some(parquet_copy_hook);
    }
}

fn process_copy_to_parquet(
    p_stmt: &PgBox<PlannedStmt>,
    query_string: &CStr,
    params: &PgBox<ParamListInfoData>,
    query_env: &PgBox<QueryEnvironment>,
) -> u64 {
    let uri_info = copy_stmt_uri(p_stmt).unwrap_or_else(|e| panic!("{}", e));

    let program = if let Some(program) = copy_stmt_program(p_stmt) {
        program.as_pg_cstr()
    } else {
        std::ptr::null_mut()
    };

    let is_std_inout = copy_stmt_is_std_inout(p_stmt);

    let copy_from = false;
    ensure_access_privilege_to_uri(&uri_info, copy_from);

    validate_copy_to_options(p_stmt, &uri_info);

    let file_size_bytes = copy_to_stmt_file_size_bytes(p_stmt);
    let field_ids = copy_to_stmt_field_ids(p_stmt);
    let row_group_size = copy_to_stmt_row_group_size(p_stmt);
    let row_group_size_bytes = copy_to_stmt_row_group_size_bytes(p_stmt);
    let compression = copy_to_stmt_compression(p_stmt, &uri_info);
    let compression_level = copy_to_stmt_compression_level(p_stmt, &uri_info);
    let parquet_version = copy_to_stmt_parquet_version(p_stmt);

    let parquet_split_dest = create_copy_to_parquet_split_dest_receiver(
        uri_as_string(&uri_info.uri).as_pg_cstr(),
        program,
        is_std_inout,
        &file_size_bytes,
        field_ids,
        &row_group_size,
        &row_group_size_bytes,
        &compression,
        &compression_level.unwrap_or(INVALID_COMPRESSION_LEVEL),
        &parquet_version,
    );

    let parquet_split_dest = unsafe { PgBox::from_pg(parquet_split_dest) };

    PgTryBuilder::new(|| {
        execute_copy_to_with_dest_receiver(
            p_stmt,
            query_string,
            params,
            query_env,
            &parquet_split_dest,
        )
    })
    .catch_others(|cause| cause.rethrow())
    .finally(|| {
        free_copy_to_parquet_split_dest_receiver(parquet_split_dest.as_ptr());
    })
    .execute()
}

fn process_copy_from_parquet(
    p_stmt: &PgBox<PlannedStmt>,
    query_string: &CStr,
    query_env: &PgBox<QueryEnvironment>,
) -> u64 {
    let uri_info = copy_stmt_uri(p_stmt).unwrap_or_else(|e| panic!("{}", e));

    let copy_from = true;
    ensure_access_privilege_to_uri(&uri_info, copy_from);

    validate_copy_from_options(p_stmt);

    PgTryBuilder::new(|| execute_copy_from(p_stmt, query_string, query_env, uri_info))
        .catch_others(|cause| {
            // make sure to pop the parquet reader context
            // In case we did not push the context, we should not throw an error while popping
            let throw_error = false;
            pop_parquet_reader_context(throw_error);

            cause.rethrow()
        })
        .execute()
}

#[allow(clippy::too_many_arguments)]
fn process_create_table_from_parquet(
    p_stmt: &mut PgBox<PlannedStmt>,
    query_string: &CStr,
    read_only_tree: bool,
    context: u32,
    params: *mut ParamListInfoData,
    query_env: *mut QueryEnvironment,
    dest: *mut DestReceiver,
    completion_tag: *mut QueryCompletion,
) {
    let mut create_stmt = unsafe { PgBox::<CreateStmt>::from_pg(p_stmt.utilityStmt as _) };

    validate_create_table_from_parquet_stmt(&create_stmt);

    let uri_info = create_stmt_get_uri(&create_stmt);

    let load_from = has_option(create_stmt.options, "load_from");

    let column_defs = infer_column_definitions(&create_stmt);
    create_stmt.tableElts = column_defs.into_pg();

    // remove pg_parquet specific options to make PG happy
    create_stmt_remove_parquet_options(&mut create_stmt);

    unsafe {
        ProcessUtility_hook.expect("ProcessUtility_hook is None")(
            p_stmt.as_ptr(),
            query_string.as_ptr(),
            read_only_tree,
            context,
            params,
            query_env,
            dest,
            completion_tag,
        );
    }

    if load_from {
        let copy_from_stmt =
            create_copy_from_parquet_stmt_for_table(create_stmt.relation, &uri_info);

        let mut planned_stmt = p_stmt.clone();
        planned_stmt.utilityStmt = copy_from_stmt.into_pg() as _;

        let query_string = unsafe { nodeToString(planned_stmt.utilityStmt as _) };
        let query_string = unsafe { CStr::from_ptr(query_string) };

        process_copy_from_parquet(&planned_stmt, query_string, unsafe {
            &PgBox::from_pg(query_env)
        });
    }
}

#[pg_guard]
#[allow(clippy::too_many_arguments)]
extern "C-unwind" fn parquet_copy_hook(
    p_stmt: *mut PlannedStmt,
    query_string: *const c_char,
    read_only_tree: bool,
    context: u32,
    params: *mut ParamListInfoData,
    query_env: *mut QueryEnvironment,
    dest: *mut DestReceiver,
    completion_tag: *mut QueryCompletion,
) {
    let mut p_stmt = unsafe { PgBox::from_pg(p_stmt) };
    let query_string = unsafe { CStr::from_ptr(query_string) };
    let params = unsafe { PgBox::from_pg(params) };
    let query_env = unsafe { PgBox::from_pg(query_env) };
    let mut completion_tag = unsafe { PgBox::from_pg(completion_tag) };

    if is_copy_to_parquet_stmt(&p_stmt) {
        let nprocessed = process_copy_to_parquet(&p_stmt, query_string, &params, &query_env);

        if !completion_tag.is_null() {
            completion_tag.nprocessed = nprocessed;
            completion_tag.commandTag = CommandTag::CMDTAG_COPY;
        }
        return;
    } else if is_copy_from_parquet_stmt(&p_stmt) {
        let nprocessed = process_copy_from_parquet(&p_stmt, query_string, &query_env);

        if !completion_tag.is_null() {
            completion_tag.nprocessed = nprocessed;
            completion_tag.commandTag = CommandTag::CMDTAG_COPY;
        }
        return;
    } else if ENABLE_PARQUET_COPY_HOOK.get() && is_create_table_from_parquet_stmt(&p_stmt) {
        process_create_table_from_parquet(
            &mut p_stmt,
            query_string,
            read_only_tree,
            context,
            params.as_ptr(),
            query_env.as_ptr(),
            dest,
            completion_tag.as_ptr(),
        );
        return;
    }

    unsafe {
        if let Some(prev_hook) = PREV_PROCESS_UTILITY_HOOK {
            prev_hook(
                p_stmt.into_pg(),
                query_string.as_ptr(),
                read_only_tree,
                context,
                params.into_pg(),
                query_env.into_pg(),
                dest,
                completion_tag.into_pg(),
            )
        } else {
            standard_ProcessUtility(
                p_stmt.into_pg(),
                query_string.as_ptr(),
                read_only_tree,
                context,
                params.into_pg(),
                query_env.into_pg(),
                dest,
                completion_tag.into_pg(),
            )
        }
    }
}
