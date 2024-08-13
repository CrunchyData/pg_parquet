use std::cell::RefCell;

use pgrx::{
    pg_guard,
    pg_sys::{
        self, canonicalize_qual, coerce_to_boolean, eval_const_expressions, make_ands_explicit,
        make_parsestate, AccessShareLock, AsPgCStr, CopyStmt, CreateTemplateTupleDesc, List,
        ParseExprKind_EXPR_KIND_COPY_WHERE, ParseNamespaceItem, ParseState, RowExclusiveLock,
        TupleDescInitEntry,
    },
    void_mut_ptr, PgBox, PgList, PgRelation, PgTupleDesc,
};

use crate::{
    arrow_parquet::parquet_reader::ParquetReaderContext,
    parquet_copy_hook::copy_utils::{
        addNSItemToQuery, addRangeTableEntryForRelation, add_binary_format_option,
        assign_expr_collations, copy_lock_mode, copy_relation_oid, copy_stmt_filename,
        is_copy_from_parquet_stmt, transformExpr, BeginCopyFrom, CopyFrom, CopyFromState,
        EndCopyFrom,
    },
};

static mut PARQUET_READER_CONTEXT: RefCell<Option<ParquetReaderContext>> = RefCell::new(None);

#[pg_guard]
extern "C" fn copy_received_parquet_data_to_buffer(
    outbuf: void_mut_ptr,
    _minread: i32,
    maxread: i32,
) -> i32 {
    let mut parquet_reader = unsafe { PARQUET_READER_CONTEXT.borrow_mut() };
    let parquet_reader = parquet_reader.as_mut().unwrap();

    let mut bytes_in_buffer = parquet_reader.bytes_in_buffer();

    if bytes_in_buffer == 0 {
        parquet_reader.reset_buffer();

        if !parquet_reader.read_parquet() {
            return 0;
        }

        bytes_in_buffer = parquet_reader.bytes_in_buffer();
    }

    let bytes_to_copy = std::cmp::min(maxread as usize, bytes_in_buffer);
    parquet_reader.copy_to_outbuf(bytes_to_copy, outbuf);

    bytes_to_copy as _
}

pub(crate) fn execute_copy_from(
    pstmt: PgBox<pg_sys::PlannedStmt>,
    query_string: &core::ffi::CStr,
    query_env: PgBox<pg_sys::QueryEnvironment>,
) -> i64 {
    let rel_oid = copy_relation_oid(&pstmt);
    let lock_mode = copy_lock_mode(&pstmt);
    let relation = unsafe { PgRelation::with_lock(rel_oid, lock_mode) };

    let filename = copy_stmt_filename(&pstmt);
    let pstate = create_parse_state(query_string, &query_env);

    let nsitem = copy_ns_item(&pstate, &pstmt, &relation);

    let where_clause = copy_from_where_clause(&pstmt);
    let mut where_clause = unsafe { PgBox::from_pg(where_clause) };
    if !where_clause.is_null() {
        where_clause = copy_from_transform_where_clause(&pstate, &nsitem, &where_clause);
    }

    let attlist = copy_attlist(&pstmt);
    let attnamelist = copy_attnames(&pstmt);

    let tupledesc = filter_tupledesc_for_relation(&relation, attnamelist);

    unsafe {
        let filename = std::ffi::CStr::from_ptr(filename)
            .to_str()
            .unwrap()
            .to_string();

        let parquet_reader_context = ParquetReaderContext::new(filename, tupledesc.as_ptr());
        PARQUET_READER_CONTEXT = RefCell::new(Some(parquet_reader_context));

        let copy_options = add_binary_format_option(&pstmt);

        let copy_from_state: CopyFromState = BeginCopyFrom(
            pstate.as_ptr(),
            relation.as_ptr(),
            where_clause.as_ptr(),
            std::ptr::null(),
            false,
            Some(copy_received_parquet_data_to_buffer),
            attlist,
            copy_options.as_ptr(),
        );

        let nprocessed = CopyFrom(copy_from_state);

        EndCopyFrom(copy_from_state);

        PARQUET_READER_CONTEXT = RefCell::new(None);

        nprocessed
    }
}

fn copy_from_where_clause(pstmt: &PgBox<pg_sys::PlannedStmt>) -> *mut pg_sys::Node {
    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(pstmt.utilityStmt as _) };
    copy_stmt.whereClause
}

fn copy_from_transform_where_clause(
    pstate: &PgBox<pg_sys::ParseState>,
    nsitem: &PgBox<ParseNamespaceItem>,
    where_clause: &PgBox<pg_sys::Node>,
) -> PgBox<pg_sys::Node> {
    unsafe { addNSItemToQuery(pstate.as_ptr(), nsitem.as_ptr(), false, true, true) };

    let where_clause = unsafe {
        transformExpr(
            pstate.as_ptr(),
            where_clause.as_ptr(),
            ParseExprKind_EXPR_KIND_COPY_WHERE,
        )
    };

    let construct = std::ffi::CString::new("WHERE").unwrap();
    let where_clause =
        unsafe { coerce_to_boolean(pstate.as_ptr(), where_clause, construct.as_ptr()) };

    let where_clause = unsafe { assign_expr_collations(pstate.as_ptr(), where_clause) };

    let where_clause = unsafe { eval_const_expressions(std::ptr::null_mut(), where_clause) };

    let expr = unsafe { canonicalize_qual(where_clause as _, false) };

    let mut expr_list = PgList::new();
    expr_list.push(expr);

    let expr = unsafe { make_ands_explicit(expr_list.as_ptr()) };

    unsafe { PgBox::from_pg(expr as _) }
}

fn copy_attlist(pstmt: &PgBox<pg_sys::PlannedStmt>) -> *mut List {
    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(pstmt.utilityStmt as _) };
    copy_stmt.attlist
}

fn copy_attnames(pstmt: &PgBox<pg_sys::PlannedStmt>) -> Vec<String> {
    let attnamelist = copy_attlist(pstmt);
    unsafe {
        PgList::from_pg(attnamelist)
            .iter_ptr()
            .map(|ptr| {
                { std::ffi::CStr::from_ptr(*ptr) }
                    .to_str()
                    .unwrap()
                    .to_string()
            })
            .collect::<Vec<_>>()
    }
}

fn create_parse_state(
    query_string: &core::ffi::CStr,
    query_env: &PgBox<pg_sys::QueryEnvironment>,
) -> PgBox<ParseState> {
    /* construct a parse state similar to standard_ProcessUtility */
    let pstate: *mut ParseState = unsafe { make_parsestate(std::ptr::null_mut()) };
    let mut pstate = unsafe { PgBox::from_pg(pstate) };
    pstate.p_sourcetext = query_string.as_ptr();
    pstate.p_queryEnv = query_env.as_ptr();
    pstate
}

fn copy_ns_item(
    pstate: &PgBox<ParseState>,
    pstmt: &PgBox<pg_sys::PlannedStmt>,
    relation: &PgRelation,
) -> PgBox<ParseNamespaceItem> {
    let is_copy_from = is_copy_from_parquet_stmt(pstmt);

    let nsitem = unsafe {
        addRangeTableEntryForRelation(
            pstate.as_ptr(),
            relation.as_ptr(),
            if is_copy_from {
                RowExclusiveLock as _
            } else {
                AccessShareLock as _
            },
            std::ptr::null_mut(),
            false,
            false,
        )
    };
    unsafe { PgBox::from_pg(nsitem) }
}

fn filter_tupledesc_for_relation(relation: &PgRelation, attnamelist: Vec<String>) -> PgTupleDesc {
    let table_tupledesc = relation.tuple_desc();

    if attnamelist.is_empty() {
        return table_tupledesc;
    }

    let tupledesc = unsafe { CreateTemplateTupleDesc(attnamelist.len() as i32) };
    let tupledesc = unsafe { PgTupleDesc::from_pg(tupledesc) };

    let mut attribute_number = 1;

    for attname in attnamelist.iter() {
        for table_att in table_tupledesc.iter() {
            if table_att.is_dropped() {
                continue;
            }

            let att_typoid = table_att.type_oid().value();
            let att_typmod = table_att.type_mod();
            let att_ndims = table_att.attndims;

            if table_att.name() == attname {
                unsafe {
                    TupleDescInitEntry(
                        tupledesc.as_ptr(),
                        attribute_number,
                        attname.as_pg_cstr() as _,
                        att_typoid,
                        att_typmod,
                        att_ndims as _,
                    )
                };

                attribute_number += 1;

                break;
            }
        }
    }

    tupledesc
}
