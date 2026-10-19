//! Running a `COPY ... FROM '<uri>'` by making Postgres consume the rows.
//!
//! The same trick as [`crate::copy_to`], from the other end: the base does not insert anything.
//! It opens the file, registers a callback that produces the binary `COPY` stream
//! ([`crate::batch_reader`]), and calls core's `CopyFrom`. Core then does the work it already
//! does — defaults, generated columns, `WHERE`, constraints, triggers, partition routing,
//! `ON ERROR`, `FREEZE`, progress reporting — against a source it believes is a file.
//!
//! What is left here is what core's `DoCopy` does before that: a parse state with the relation
//! in its range table, the `WHERE` clause transformed, the permission check, and the tuple
//! descriptor filtered down to the columns the statement listed. It is lifted from core's own
//! `COPY FROM` path so that a uri behaves like a path right down to the error messages.
//!
//! Nothing in this file is format-specific, and nothing in it touches a credential.

use std::ffi::CStr;

use pgrx::pg_sys::{
    addNSItemToQuery, assign_expr_collations, canonicalize_qual, check_enable_rls,
    coerce_to_boolean, eval_const_expressions, make_ands_implicit, transformExpr, AsPgCStr,
    BeginCopyFrom, CheckEnableRlsResult, CopyFrom, CopyStmt, EndCopyFrom, InvalidOid, Node, Oid,
    ParseExprKind, ParseNamespaceItem, ParseState, PlannedStmt, QueryEnvironment,
};
use pgrx::{prelude::*, void_mut_ptr, PgRelation};

use crate::batch_reader::CopyFromStream;
use crate::copy_plan::CopyFromPlan;
use crate::copy_stmt;
use crate::io::Place;
use crate::parallel_copy_from;
use crate::pg_compat::check_copy_table_permission;

// One stream per statement, and a stack because a COPY FROM can run inside a trigger fired by
// another one. The callback core invokes has no argument to carry state in, so the top of this
// is how it finds the stream it belongs to.
static mut COPY_FROM_STREAM_STACK: Vec<CopyFromStream> = Vec::new();

fn peek_stream() -> Option<&'static mut CopyFromStream> {
    #[allow(static_mut_refs)]
    unsafe {
        COPY_FROM_STREAM_STACK.last_mut()
    }
}

fn push_stream(stream: CopyFromStream) {
    #[allow(static_mut_refs)]
    unsafe {
        COPY_FROM_STREAM_STACK.push(stream)
    };
}

/// Drops the statement's stream. Called on the error path too, where the stack may already be
/// empty — a failure inside `CopyFromStream::open` happens before the push — so an empty stack
/// is not itself a problem to report.
pub(crate) fn pop_stream() {
    #[allow(static_mut_refs)]
    unsafe {
        COPY_FROM_STREAM_STACK.pop()
    };
}

/// Core's input callback: fill `outbuf` with up to `maxread` bytes of the binary copy stream,
/// and return 0 to mean end of input.
#[pg_guard]
extern "C-unwind" fn copy_arrow_data_to_buffer(
    outbuf: void_mut_ptr,
    _minread: i32,
    maxread: i32,
) -> i32 {
    let stream = peek_stream().unwrap_or_else(|| error!("there is no COPY FROM stream to read"));

    let mut bytes_size = stream.not_yet_copied_bytes();

    if bytes_size == 0 {
        // everything encoded so far has been taken, so the buffer can be reused rather than
        // grown for the rest of the file
        stream.reset_buffer();

        if !stream.read_next() {
            return 0;
        }

        bytes_size = stream.not_yet_copied_bytes();
    }

    let bytes_size_to_copy = std::cmp::min(maxread as usize, bytes_size);

    stream.copy_to_outbuf(bytes_size_to_copy, outbuf);

    bytes_size_to_copy as _
}

/// Carries out the statement, and returns how many rows core inserted.
pub(crate) fn execute_copy_from(
    p_stmt: &PgBox<PlannedStmt>,
    query_string: &CStr,
    query_env: &PgBox<QueryEnvironment>,
    plan: CopyFromPlan,
) -> u64 {
    let relation_oid = copy_stmt::relation_oid(p_stmt);

    ensure_no_row_level_security(relation_oid);

    let relation = unsafe { PgRelation::with_lock(relation_oid, copy_stmt::lock_mode(p_stmt)) };

    let p_state = copy_stmt::create_parse_state(query_string, query_env);

    let ns_item = copy_stmt::create_namespace_item(p_stmt, &p_state, &relation);

    let mut where_clause = where_clause(p_stmt);

    if !where_clause.is_null() {
        where_clause = transform_where_clause(&p_state, &ns_item, &relation, where_clause);
    }

    check_copy_table_permission(p_stmt, &p_state, &ns_item, &relation);

    let attribute_list = copy_stmt::attribute_list(p_stmt);

    let tupledesc = copy_stmt::create_filtered_tupledesc_for_relation(p_stmt, &relation);

    // a `FROM STDIN` or a `FROM PROGRAM` has to be read in full before the provider opens it:
    // both are byte streams, and a format that reads a footer first cannot be read from one.
    // The column count is what the client's CopyInResponse declares.
    plan.endpoint.fill(&plan.uri, tupledesc.len() as i16);

    // a file several processes can divide is decoded by workers and only inserted here, which
    // is a decision about this one statement: anything the workers cannot be given — a stream, a
    // format that reads no ranges, a table whose writes are not legal in parallel mode — is read
    // in this process as it always was
    let parallel = parallel_copy_from::try_parallel_copy_from(p_stmt, &plan, &relation);

    // opened before anything is inserted, so that an unreadable file or a schema that does not
    // match the table fails the statement rather than half-filling it
    if !parallel {
        push_stream(CopyFromStream::open(
            plan.provider,
            &Place::new(&plan.endpoint, &plan.uri),
            &plan.options,
            plan.match_by,
            &tupledesc,
        ));
    }

    let data_source_cb: pg_sys::copy_data_source_cb = if parallel {
        Some(parallel_copy_from::copy_parallel_arrow_data_to_buffer)
    } else {
        Some(copy_arrow_data_to_buffer)
    };

    unsafe {
        // the one option this rewrites: core reads our callback's bytes, which are the binary
        // format whatever the file was
        let copy_options = copy_stmt::binary_option_list(p_stmt);

        let copy_from_state = BeginCopyFrom(
            p_state.as_ptr(),
            relation.as_ptr(),
            where_clause,
            std::ptr::null(),
            false,
            data_source_cb,
            attribute_list,
            copy_options.as_ptr(),
        );

        let nprocessed = CopyFrom(copy_from_state);

        EndCopyFrom(copy_from_state);

        nprocessed
    }
}

fn where_clause(p_stmt: &PgBox<PlannedStmt>) -> *mut Node {
    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(p_stmt.utilityStmt as _) };

    copy_stmt.whereClause
}

/// The `WHERE` clause as the executor needs it, by the same steps core's `DoCopy` takes.
fn transform_where_clause(
    p_state: &PgBox<ParseState>,
    ns_item: &PgBox<ParseNamespaceItem>,
    relation: &PgRelation,
    where_clause: *mut Node,
) -> *mut Node {
    unsafe { addNSItemToQuery(p_state.as_ptr(), ns_item.as_ptr(), false, true, true) };

    let where_clause = unsafe {
        transformExpr(
            p_state.as_ptr(),
            where_clause,
            ParseExprKind::EXPR_KIND_COPY_WHERE,
        )
    };

    let where_clause =
        unsafe { coerce_to_boolean(p_state.as_ptr(), where_clause, "WHERE".as_pg_cstr()) };

    unsafe { assign_expr_collations(p_state.as_ptr(), where_clause) };

    #[cfg(not(pre_pg19))]
    ensure_no_system_column_in_where_clause(relation, where_clause);

    // the relation is only needed by the PG19 check above
    #[cfg(pre_pg19)]
    let _ = relation;

    let where_clause = unsafe { eval_const_expressions(std::ptr::null_mut(), where_clause) };

    let where_clause = unsafe { canonicalize_qual(where_clause as _, false) };

    unsafe { make_ands_implicit(where_clause as _) as _ }
}

/// A system column in a `COPY FROM WHERE` is not filled in when the filter runs, so PG19
/// started refusing one instead of reading whatever was in the slot.
#[cfg(not(pre_pg19))]
fn ensure_no_system_column_in_where_clause(relation: &PgRelation, where_clause: *mut Node) {
    use pgrx::pg_sys::{
        bms_del_member, bms_next_member, get_attname, pull_varattnos,
        FirstLowInvalidHeapAttributeNumber,
    };

    let mut expr_attrs = std::ptr::null_mut();
    unsafe { pull_varattnos(where_clause, 1, &mut expr_attrs) };

    // a whole-row reference stands for the user columns, so it is not a system column
    let whole_row_attno = -FirstLowInvalidHeapAttributeNumber;
    unsafe { expr_attrs = bms_del_member(expr_attrs, whole_row_attno) };

    let mut member = -1;

    loop {
        member = unsafe { bms_next_member(expr_attrs, member) };

        if member < 0 {
            break;
        }

        let attno = member + FirstLowInvalidHeapAttributeNumber;

        if attno >= 0 {
            continue;
        }

        let attname = unsafe { get_attname(relation.oid(), attno as _, false) };
        let attname = unsafe { CStr::from_ptr(attname) }
            .to_str()
            .expect("an attribute name is valid utf-8");

        ereport!(
            PgLogLevel::ERROR,
            PgSqlErrorCode::ERRCODE_INVALID_COLUMN_REFERENCE,
            "system columns are not supported in COPY FROM WHERE conditions",
            format!("Column \"{attname}\" is a system column."),
        );
    }
}

/// Row-level security is refused rather than ignored, as core refuses it: the policies would
/// have to be enforced per row and `COPY FROM` has no `INSERT` to attach them to.
fn ensure_no_row_level_security(relation_oid: Oid) {
    if unsafe { check_enable_rls(relation_oid, InvalidOid, false) }
        == CheckEnableRlsResult::RLS_ENABLED as i32
    {
        ereport!(
            PgLogLevel::ERROR,
            PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
            "COPY FROM not supported with row-level security",
            "Use INSERT statements instead."
        );
    }
}
