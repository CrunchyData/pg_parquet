use std::{ffi::CStr, rc::Rc};

use pgrx::{
    ereport, is_a, pg_guard,
    pg_sys::{
        check_enable_rls, makeRangeVar, relation_close, relation_open, A_Star, BeginCopyTo,
        CheckEnableRlsResult, ColumnRef, CopyStmt, DoCopyTo, EndCopyTo, InvalidOid, NoLock,
        NodeTag::{self, T_CopyStmt},
        PlannedStmt, QueryEnvironment, RawStmt, ResTarget, SelectStmt,
    },
    void_mut_ptr, PgBox, PgList, PgLogLevel, PgRelation, PgSqlErrorCode,
};

use crate::{
    arrow_parquet::{
        compression::INVALID_COMPRESSION_LEVEL,
        field_ids::FieldIds,
        parquet_version::ParquetVersion,
        parquet_writer::{
            ParquetWriterContext, DEFAULT_ROW_GROUP_SIZE, DEFAULT_ROW_GROUP_SIZE_BYTES,
        },
        uri_utils::ParsedUriInfo,
    },
    parquet_copy_hook::{
        copy_to_stdout::copy_file_to_stdout,
        copy_utils::{
            copy_from_stmt_create_option_list, copy_stmt_attribute_list,
            copy_stmt_create_namespace_item, copy_stmt_create_parse_state, copy_stmt_has_relation,
            copy_stmt_lock_mode, copy_stmt_relation_oid, copy_to_stmt_compression,
            copy_to_stmt_compression_level, copy_to_stmt_field_ids, copy_to_stmt_file_size_bytes,
            copy_to_stmt_parquet_version, copy_to_stmt_row_group_size,
            copy_to_stmt_row_group_size_bytes, create_filtered_tupledesc_for_relation,
            create_filtered_tupledesc_for_target_list,
        },
        pg_compat::check_copy_table_permission,
    },
    PgParquetCompression,
};

#[derive(Clone)]
pub(crate) struct CopyToParquetOptions {
    pub(crate) file_size_bytes: Option<i64>,
    pub(crate) field_ids: FieldIds,
    pub(crate) row_group_size: i64,
    pub(crate) row_group_size_bytes: i64,
    pub(crate) compression: PgParquetCompression,
    pub(crate) compression_level: i32,
    pub(crate) parquet_version: ParquetVersion,
}

impl CopyToParquetOptions {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        file_size_bytes: Option<i64>,
        field_ids: Option<FieldIds>,
        row_group_size: Option<i64>,
        row_group_size_bytes: Option<i64>,
        compression: Option<PgParquetCompression>,
        compression_level: Option<i32>,
        parquet_version: Option<ParquetVersion>,
    ) -> Self {
        let field_ids = field_ids.unwrap_or_default();

        let row_group_size = row_group_size.unwrap_or(DEFAULT_ROW_GROUP_SIZE);

        let row_group_size_bytes = row_group_size_bytes.unwrap_or(DEFAULT_ROW_GROUP_SIZE_BYTES);

        let compression = compression.unwrap_or_default();

        let compression_level = compression_level.unwrap_or(
            compression
                .default_compression_level()
                .unwrap_or(INVALID_COMPRESSION_LEVEL),
        );

        let parquet_version = parquet_version.unwrap_or_default();

        Self {
            file_size_bytes,
            field_ids,
            row_group_size,
            row_group_size_bytes,
            compression,
            compression_level,
            parquet_version,
        }
    }

    pub(crate) fn has_file_size_bytes(&self) -> bool {
        self.file_size_bytes.is_some()
    }

    pub(crate) fn file_size_bytes(&self) -> u64 {
        self.file_size_bytes.expect("file_size_bytes is not set") as u64
    }
}

// stack to store parquet writer contexts for COPY TO.
// This needs to be a stack since COPY command can be nested.
static mut PARQUET_WRITER_CONTEXT_STACK: Vec<ParquetWriterContext> = vec![];

pub(crate) fn peek_parquet_writer_context() -> Option<&'static mut ParquetWriterContext<'static>> {
    #[allow(static_mut_refs)]
    unsafe {
        PARQUET_WRITER_CONTEXT_STACK.last_mut()
    }
}

pub(crate) fn pop_parquet_writer_context(throw_error: bool) {
    #[allow(static_mut_refs)]
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
    } else {
        current_parquet_writer_context.take();
    }
}

pub(crate) fn push_parquet_writer_context(writer_ctx: ParquetWriterContext<'static>) {
    #[allow(static_mut_refs)]
    unsafe {
        PARQUET_WRITER_CONTEXT_STACK.push(writer_ctx)
    };
}

// This function is called by the COPY TO command to write input buffer to the parquet file
// by the executor.
#[pg_guard]
extern "C-unwind" fn copy_buffer_to_parquet(inbuf: void_mut_ptr, len: i32) {
    let current_parquet_writer_context =
        peek_parquet_writer_context().expect("current parquet writer context is not found");

    unsafe {
        let inbuf = std::slice::from_raw_parts_mut(inbuf as *mut u8, len as usize);

        let mut old_ctx = current_parquet_writer_context
            .copy_per_tuple_memory_ctx
            .set_as_current();

        current_parquet_writer_context.collect_tuple_from_buffer(inbuf);

        old_ctx.set_as_current();

        current_parquet_writer_context
            .copy_per_tuple_memory_ctx
            .reset();
    }
}

// execute_copy_to_parquet is called by the ProcessUtility_hook to execute the COPY TO command.
// It reads data from copy buffer that is passed by the registered COPY TO callback function and
// writes the data to the parquet file.
//
// 1. Before writing the data,
//    - validates the COPY TO options,
//    - checks table permissions, (DoCopy in https://github.com/postgres/postgres/blob/master/src/backend/commands/copy.c#L81)
//    - checks row level security, (DoCopy in https://github.com/postgres/postgres/blob/master/src/backend/commands/copy.c#L81)
//    - creates a ParquetWriterContext that is used to write data to the parquet file.
// 2. Registers a callback function, which is called by the executor, to write data to
//    the input buffer parameter that is read by ParquetWriterContext.
// 3. Calls the executor's DoCopyTo function to write data to the parquet file.
pub(crate) fn execute_copy_to_parquet(
    p_stmt: &PgBox<PlannedStmt>,
    query_string: &CStr,
    query_env: &PgBox<QueryEnvironment>,
    uri_info: ParsedUriInfo,
) -> u64 {
    unsafe {
        debug_assert!(is_a(p_stmt.utilityStmt, T_CopyStmt));

        let file_size_bytes = copy_to_stmt_file_size_bytes(p_stmt);
        let field_ids = copy_to_stmt_field_ids(p_stmt);
        let row_group_size = copy_to_stmt_row_group_size(p_stmt);
        let row_group_size_bytes = copy_to_stmt_row_group_size_bytes(p_stmt);
        let compression = copy_to_stmt_compression(p_stmt, &uri_info);
        let compression_level = copy_to_stmt_compression_level(p_stmt);
        let parquet_version = copy_to_stmt_parquet_version(p_stmt);

        let copy_to_options = CopyToParquetOptions::new(
            file_size_bytes,
            field_ids,
            row_group_size,
            row_group_size_bytes,
            compression,
            compression_level,
            parquet_version,
        );

        let p_state = copy_stmt_create_parse_state(query_string, query_env);

        let mut relation = std::ptr::null_mut();

        let mut rel_oid = InvalidOid;

        let mut raw_stmt = PgBox::<RawStmt>::from_pg(std::ptr::null_mut());

        let tupledesc;

        if copy_stmt_has_relation(p_stmt) {
            rel_oid = copy_stmt_relation_oid(p_stmt);

            let lock_mode = copy_stmt_lock_mode(p_stmt);

            relation = relation_open(rel_oid, lock_mode);

            let pg_relation = PgRelation::from_pg(relation);

            let ns_item = copy_stmt_create_namespace_item(p_stmt, &p_state, &pg_relation);

            tupledesc = create_filtered_tupledesc_for_relation(p_stmt, &pg_relation);

            check_copy_table_permission(p_stmt, &p_state, &ns_item, &pg_relation);

            if copy_stmt_check_row_level_security_enabled(&pg_relation) {
                let copy_stmt = PgBox::<CopyStmt>::from_pg(p_stmt.utilityStmt as _);

                let select_stmt = convert_copy_to_relation_to_select_stmt(&copy_stmt, &pg_relation);

                raw_stmt = PgBox::<RawStmt>::alloc_node(NodeTag::T_RawStmt).into_pg_boxed();
                raw_stmt.stmt_location = p_stmt.stmt_location;
                raw_stmt.stmt_len = p_stmt.stmt_len;
                raw_stmt.stmt = select_stmt.as_ptr() as _;

                // close relation and set it to null but keep the lock on it
                relation_close(relation, NoLock as _);
                relation = std::ptr::null_mut();
            }
        } else {
            let copy_stmt = PgBox::<CopyStmt>::from_pg(p_stmt.utilityStmt as _);

            raw_stmt = PgBox::<RawStmt>::alloc_node(NodeTag::T_RawStmt).into_pg_boxed();
            raw_stmt.stmt_location = p_stmt.stmt_location;
            raw_stmt.stmt_len = p_stmt.stmt_len;
            raw_stmt.stmt = copy_stmt.query;

            tupledesc =
                create_filtered_tupledesc_for_target_list(&raw_stmt, query_string, query_env);
        }

        let attribute_list = copy_stmt_attribute_list(p_stmt);

        let uri_info = Rc::new(uri_info);

        // parquet writer context is used throughout the COPY to operation.
        let parquet_writer_context =
            ParquetWriterContext::new(uri_info.clone(), copy_to_options, tupledesc.clone());
        push_parquet_writer_context(parquet_writer_context);

        // makes sure to set binary format
        let copy_options = copy_from_stmt_create_option_list(p_stmt);

        let copy_to_state = BeginCopyTo(
            p_state.as_ptr(),
            relation,
            raw_stmt.as_ptr(),
            rel_oid,
            std::ptr::null_mut(),
            false,
            Some(copy_buffer_to_parquet),
            attribute_list,
            copy_options.as_ptr(),
        );

        let nprocessed = DoCopyTo(copy_to_state);

        EndCopyTo(copy_to_state);

        if uri_info.stdio_tmp_fd.is_some() {
            copy_file_to_stdout(&uri_info, tupledesc.natts as _);
        }

        let throw_error = true;
        pop_parquet_writer_context(throw_error);

        if !relation.is_null() {
            relation_close(relation, NoLock as _);
        }

        nprocessed
    }
}

// convert_copy_to_relation_to_select_stmt is called by COPY TO operation when row level security is enabled
// for the relation. It converts the relation in COPY TO statement to a SELECT statement. This way, rls policies
// are applied during query processing.
pub(crate) fn convert_copy_to_relation_to_select_stmt(
    copy_stmt: &PgBox<CopyStmt>,
    relation: &PgRelation,
) -> PgBox<SelectStmt> {
    let mut target_list = PgList::new();

    if copy_stmt.attlist.is_null() {
        // SELECT * FROM relation
        let mut col_ref = unsafe { PgBox::<ColumnRef>::alloc_node(NodeTag::T_ColumnRef) };
        let a_star = unsafe { PgBox::<A_Star>::alloc_node(NodeTag::T_A_Star) };

        let mut field_list = PgList::new();
        field_list.push(a_star.into_pg());

        col_ref.fields = field_list.into_pg();
        col_ref.location = -1;

        let mut target = unsafe { PgBox::<ResTarget>::alloc_node(NodeTag::T_ResTarget) };
        target.name = std::ptr::null_mut();
        target.indirection = std::ptr::null_mut();
        target.val = col_ref.into_pg() as _;
        target.location = -1;

        target_list.push(target.into_pg());
    } else {
        // SELECT a,b,... FROM relation
        let attribute_name_list =
            unsafe { PgList::<pgrx::pg_sys::String>::from_pg(copy_stmt.attlist) };
        for attribute_name in attribute_name_list.iter_ptr() {
            let mut col_ref = unsafe { PgBox::<ColumnRef>::alloc_node(NodeTag::T_ColumnRef) };

            let mut field_list = PgList::new();
            field_list.push(attribute_name);

            col_ref.fields = field_list.into_pg();
            col_ref.location = -1;

            let mut target = unsafe { PgBox::<ResTarget>::alloc_node(NodeTag::T_ResTarget) };
            target.name = std::ptr::null_mut();
            target.indirection = std::ptr::null_mut();
            target.val = col_ref.into_pg() as _;
            target.location = -1;

            target_list.push(target.into_pg());
        }
    }

    let from = unsafe {
        makeRangeVar(
            relation.namespace().as_ptr() as _,
            relation.name().as_ptr() as _,
            -1,
        )
    };
    let mut from = unsafe { PgBox::from_pg(from) };
    from.inh = false;

    let mut select_stmt = unsafe { PgBox::<SelectStmt>::alloc_node(NodeTag::T_SelectStmt) };

    select_stmt.targetList = target_list.into_pg();

    let mut from_list = PgList::new();
    from_list.push(from.into_pg());
    select_stmt.fromClause = from_list.into_pg();

    select_stmt.into_pg_boxed()
}

// copy_stmt_check_row_level_security_enabled checks if row level security is enabled for the relation
// in COPY operation.
pub(crate) fn copy_stmt_check_row_level_security_enabled(relation: &PgRelation) -> bool {
    unsafe {
        check_enable_rls(relation.oid(), InvalidOid, false)
            == CheckEnableRlsResult::RLS_ENABLED as i32
    }
}
