use std::{
    ffi::{c_char, CStr},
    path::PathBuf,
    thread,
};

use pg_sys::{
    lappend, makeConst, pg_get_querydef, standard_ProcessUtility, AsPgCStr, CommandTag,
    DestReceiver, InvalidOid, Node, NodeTag, ParamListInfoData, PlannedStmt, ProcessUtility_hook,
    ProcessUtility_hook_type, Query, QueryCompletion, QueryEnvironment,
    SelfItemPointerAttributeNumber, SortGroupClause, TargetEntry, Var, INT8OID, TIDOID,
};
use pgrx::{
    pg_sys::{
        get_namespace_name, get_rel_name, get_rel_namespace, quote_qualified_identifier, Oid,
    },
    prelude::*,
    GucSetting,
};
use postgres::{Client, NoTls};

use crate::{
    arrow_parquet::{
        compression::INVALID_COMPRESSION_LEVEL,
        uri_utils::{ensure_access_privilege_to_uri, uri_as_string, ParsedUriInfo},
    },
    create_copy_to_parquet_split_dest_receiver,
    parquet_copy_hook::{
        copy_to::extract_query_from_copy_to,
        copy_to_split_dest_receiver::{CopyToParquetOptions, INVALID_FILE_SIZE_BYTES},
        copy_utils::{
            copy_stmt_has_relation, copy_stmt_relation_oid, copy_stmt_uri,
            copy_to_stmt_compression_level, copy_to_stmt_parallel_workers,
            copy_to_stmt_row_group_size, copy_to_stmt_row_group_size_bytes,
            is_copy_from_parquet_stmt, is_copy_to_parquet_stmt,
        },
    },
};

use super::{
    copy_from::{execute_copy_from, pop_parquet_reader_context},
    copy_to::execute_query_into_dest_receiver,
    copy_to_split_dest_receiver::free_copy_to_parquet_split_dest_receiver,
    copy_utils::{
        copy_to_stmt_compression, copy_to_stmt_field_ids, copy_to_stmt_file_size_bytes,
        copy_to_stmt_parquet_version, validate_copy_from_options, validate_copy_to_options,
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
    let parallel_workers = copy_to_stmt_parallel_workers(p_stmt);

    let copy_to_options = CopyToParquetOptions {
        file_size_bytes,
        field_ids,
        row_group_size,
        row_group_size_bytes,
        compression,
        compression_level: compression_level.unwrap_or(INVALID_COMPRESSION_LEVEL),
        parquet_version,
    };

    let parquet_split_dest = create_copy_to_parquet_split_dest_receiver(
        uri_as_string(&uri_info.uri).as_pg_cstr(),
        uri_info.stdio_tmp_fd.is_some(),
        copy_to_options,
    );

    let parquet_split_dest = unsafe { PgBox::from_pg(parquet_split_dest) };

    PgTryBuilder::new(|| {
        if parallel_workers > 1 && uri_info.stdio_tmp_fd.is_some() {
            panic!("parallel_workers are not allowed with program or stdout");
        }

        if parallel_workers > 1 && file_size_bytes != INVALID_FILE_SIZE_BYTES {
            panic!("parallel_workers are not allowed with file_size_bytes");
        }

        if parallel_workers > 1 && !copy_stmt_has_relation(p_stmt) {
            panic!("parallel_workers are only allowed with COPY <table> TO");
        }

        let query = extract_query_from_copy_to(p_stmt, query_string, query_env);

        if parallel_workers > 1 {
            let table_oid = copy_stmt_relation_oid(p_stmt);

            let total_rows = query_table_row_count(table_oid);

            let limit = total_rows / parallel_workers;
            let limit_tail = if total_rows % parallel_workers == 0 {
                limit
            } else {
                total_rows % parallel_workers
            };

            let mut offset = 0;

            let mut worker_handles = vec![];
            for worker_id in 1..=parallel_workers {
                let limit = if worker_id == parallel_workers {
                    limit_tail
                } else {
                    limit
                };

                let worker_query = add_query_ctid_offset_and_limit(&query, offset, limit);
                let worker_query = add_order_by_ctid(&worker_query, table_oid);

                let worker_query_string = unsafe { pg_get_querydef(worker_query.as_ptr(), false) };
                let worker_query_string = unsafe { CStr::from_ptr(worker_query_string) }
                    .to_str()
                    .expect("cannot convert query to string");

                let worker_query_string = wrap_query_string_into_copy_to(
                    worker_query_string,
                    copy_to_options,
                    worker_id,
                    &uri_info,
                );

                worker_handles.push(thread::spawn(move || {
                    run_query_on_worker(
                        &worker_query_string,
                        // todo: connstring
                        "host=localhost port=28817 user=abozkurt dbname=pg_parquet",
                    )
                }));

                offset += limit;
            }

            let mut worker_results = vec![];
            for worker_handle in worker_handles {
                let worker_result = worker_handle.join().unwrap();
                worker_results.push(worker_result);
            }

            for worker_result in worker_results {
                if let Err(e) = worker_result {
                    // todo: remove folder prefix when failure
                    panic!("{e:?}");
                }
            }

            return total_rows;
        }

        execute_query_into_dest_receiver(&query, query_string, params, &parquet_split_dest)
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

    PgTryBuilder::new(|| execute_copy_from(p_stmt, query_string, query_env, &uri_info))
        .catch_others(|cause| {
            // make sure to pop the parquet reader context
            // In case we did not push the context, we should not throw an error while popping
            let throw_error = false;
            pop_parquet_reader_context(throw_error);

            cause.rethrow()
        })
        .execute()
}

fn run_query_on_worker(query: &str, conn_str: &str) -> Result<(), postgres::Error> {
    let mut client =
        Client::connect(conn_str, NoTls).expect("pg_parquet copy to worker connection failed");

    client.batch_execute(query)
}

fn add_query_ctid_offset_and_limit(query: &PgBox<Query>, offset: u64, limit: u64) -> PgBox<Query> {
    unsafe {
        let mut query = query.clone();

        if !query.limitCount.is_null() {
            panic!("query should not have limit clause to run with parallel workers");
        }

        if !query.limitOffset.is_null() {
            panic!("query should not have offset clause to run with parallel workers");
        }

        // Build Const nodes for OFFSET and LIMIT
        let limit_const = makeConst(INT8OID, -1, InvalidOid, 8, limit.into(), false, true);
        let offset_const = makeConst(INT8OID, -1, InvalidOid, 8, offset.into(), false, true);

        // Set query.limitCount and query.limitOffset (these are Node* in C)
        query.limitCount = limit_const as *mut Node;
        query.limitOffset = offset_const as *mut Node;

        query
    }
}

fn add_order_by_ctid(query: &PgBox<Query>, table_oid: Oid) -> PgBox<Query> {
    unsafe {
        if !query.sortClause.is_null() {
            panic!("query should not have order by clause to run with parallel workers");
        }

        let mut query = query.clone();

        // Make a Var for ctid
        let mut var = PgBox::<Var>::alloc_node(NodeTag::T_Var).into_pg_boxed();

        var.varno = 1; // we are sure we have single relation
        var.varattno = SelfItemPointerAttributeNumber as _; // => ctid
        var.varattnosyn = SelfItemPointerAttributeNumber as _;
        var.vartype = TIDOID;
        var.vartypmod = -1;
        var.varcollid = InvalidOid;
        var.varlevelsup = 0;
        var.location = -1;
        var.varnullingrels = std::ptr::null_mut();

        // Make a junk TargetEntry for the ORDER BY expr
        let mut tle = PgBox::<TargetEntry>::alloc_node(NodeTag::T_TargetEntry).into_pg_boxed();
        tle.resorigtbl = table_oid;
        tle.resorigcol = var.varattno;
        tle.resno = var.varattno;
        tle.resname = std::ptr::null_mut();
        tle.expr = var.as_ptr() as _;
        tle.resjunk = true; // do NOT project it in output
        tle.ressortgroupref = 1;

        // Append junk tle to targetList
        query.targetList = lappend(query.targetList, tle.as_ptr() as _);

        // Build SortGroupClause
        let sortop = pg_sys::get_opfamily_member(
            pg_sys::OID_BTREE_FAM_OID,
            pg_sys::OIDOID,
            pg_sys::OIDOID,
            pg_sys::BTLessStrategyNumber as i16,
        );

        let mut sgc =
            PgBox::<SortGroupClause>::alloc_node(NodeTag::T_SortGroupClause).into_pg_boxed();
        sgc.tleSortGroupRef = tle.ressortgroupref;
        sgc.sortop = sortop;
        sgc.eqop = InvalidOid;
        sgc.nulls_first = false;
        sgc.hashable = false;

        // Append to sortClause
        query.sortClause = lappend(query.sortClause, sgc.as_ptr() as _);

        query
    }
}

fn wrap_query_string_into_copy_to(
    query_str: &str,
    options: CopyToParquetOptions,
    worker_id: u64,
    uri_info: &ParsedUriInfo,
) -> String {
    let base_uri = uri_as_string(&uri_info.uri);
    let uri = PathBuf::from(base_uri).join(format!("data_{worker_id}"));
    let uri = uri.to_str().expect("unexpected worker uri");

    let copy_options_str = copy_to_options_to_string(options);

    format!("COPY ({query_str}) TO '{uri}' WITH ({copy_options_str})")
}

fn copy_to_options_to_string(options: CopyToParquetOptions) -> String {
    let mut options_str = String::new();

    options_str.push_str("format parquet");

    let compression = options.compression;
    options_str.push_str(&format!(", compression '{compression}'"));

    if options.compression_level != INVALID_COMPRESSION_LEVEL {
        let compression_level = options.compression_level;
        options_str.push_str(&format!(", compression_level {compression_level}"));
    }

    if !options.field_ids.is_null() {
        let field_ids = unsafe {
            CStr::from_ptr(options.field_ids)
                .to_str()
                .expect("field_ids null")
        };
        options_str.push_str(&format!(", field_ids '{field_ids}'"));
    }

    let parquet_version = options.parquet_version;
    options_str.push_str(&format!(", parquet_version '{parquet_version}'"));

    let row_group_size = options.row_group_size;
    options_str.push_str(&format!(", row_group_size {row_group_size}"));

    let row_group_size_bytes = options.row_group_size_bytes;
    options_str.push_str(&format!(", row_group_size_bytes {row_group_size_bytes}"));

    options_str
}

fn query_table_row_count(table_oid: Oid) -> u64 {
    let schema_oid = unsafe { get_rel_namespace(table_oid) };

    let schema_name = unsafe { get_namespace_name(schema_oid) };

    let rel_name = unsafe { get_rel_name(table_oid) };

    let qualified_relname = unsafe { quote_qualified_identifier(schema_name, rel_name) };

    let qualified_relname = unsafe {
        CStr::from_ptr(qualified_relname)
            .to_str()
            .expect("qualified_relname NULL")
    };

    let count: i64 = Spi::get_one(&format!("SELECT COUNT(*) FROM {qualified_relname}"))
        .unwrap()
        .unwrap();

    count as _
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
    let p_stmt = unsafe { PgBox::from_pg(p_stmt) };
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
