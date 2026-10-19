//! Running a `COPY ... TO '<uri>'` by making Postgres produce the rows.
//!
//! The base never reads a table itself. What it does is turn the statement into the `SELECT`
//! that core COPY would have run, plan it, and run it in a portal whose `DestReceiver` is ours
//! — so the executor does all of the real work (permissions, snapshots, partitions, row
//! security, parallel scans) and this extension only ever sees finished tuples.
//!
//! Which means there is nothing format-specific in this file at all, and nothing object-store
//! specific either. It is the same for parquet, Arrow IPC and avro, which is exactly why it
//! belongs here rather than in a provider.

use std::ffi::{c_char, CStr};

use pgrx::pg_sys::{
    makeRangeVar, A_Star, ColumnRef, CommandTag, CopyStmt, CreateNewPortal, DestReceiver,
    GetActiveSnapshot, Node, NodeTag, ParamListInfoData, PlannedStmt, PortalDefineQuery,
    PortalDrop, PortalRun, PortalStart, QueryCompletion, QueryEnvironment, RawStmt, ResTarget,
    SelectStmt, CURSOR_OPT_PARALLEL_OK, RELKIND_FOREIGN_TABLE, RELKIND_MATVIEW,
    RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, RELKIND_SEQUENCE, RELKIND_VIEW,
};
use pgrx::{is_a, prelude::*, AllocatedByRust, PgList, PgRelation};

use crate::copy_stmt;
use crate::pg_compat::{pg_analyze_and_rewrite, pg_plan_query};

/// Runs the statement with `dest` as its destination, and returns how many rows it produced.
///
/// The row count comes from the portal rather than from the receiver, because the portal's is
/// the one `COPY` reports and the one a client compares against.
pub(crate) fn execute_with_dest_receiver(
    p_stmt: &PgBox<PlannedStmt>,
    query_string: &CStr,
    params: &PgBox<ParamListInfoData>,
    query_env: &PgBox<QueryEnvironment>,
    dest: &PgBox<DestReceiver>,
) -> u64 {
    unsafe {
        debug_assert!(is_a(p_stmt.utilityStmt, NodeTag::T_CopyStmt));

        let stmt = PgBox::<CopyStmt>::from_pg(p_stmt.utilityStmt as _);

        let mut relation = PgRelation::from_pg(std::ptr::null_mut());

        if copy_stmt::has_relation(p_stmt) {
            // locked here and held for the whole statement: the generated SELECT resolves the
            // name again, and this lock is what guarantees it resolves to the same relation
            relation = PgRelation::with_lock(
                copy_stmt::relation_oid(p_stmt),
                copy_stmt::lock_mode(p_stmt),
            );

            ensure_table_kind(&relation);
        }

        let raw_query = raw_stmt_for(p_stmt, &stmt, &relation);

        let rewritten = pg_analyze_and_rewrite(
            raw_query.as_ptr(),
            query_string.as_ptr(),
            query_env.as_ptr(),
        );

        let query = PgList::from_pg(rewritten)
            .pop()
            .expect("analyzing a SELECT yields a query");

        let plan = pg_plan_query(
            query,
            std::ptr::null(),
            CURSOR_OPT_PARALLEL_OK as _,
            params.as_ptr(),
        );

        let mut portal = PgBox::from_pg(CreateNewPortal());

        // not a cursor anybody can see: this portal is an implementation detail of the
        // statement, and showing it in pg_cursors would be showing a cursor nobody opened
        portal.visible = false;

        let mut plans = PgList::<PlannedStmt>::new();
        plans.push(plan);

        PortalDefineQuery(
            portal.as_ptr(),
            std::ptr::null(),
            query_string.as_ptr(),
            CommandTag::CMDTAG_COPY,
            plans.as_ptr(),
            std::ptr::null_mut(),
        );

        PortalStart(portal.as_ptr(), params.as_ptr(), 0, GetActiveSnapshot());

        let mut completion_tag = QueryCompletion {
            commandTag: CommandTag::CMDTAG_COPY,
            nprocessed: 0,
        };

        // i64::MAX rows, in one go: a COPY has no client fetching in batches, so there is no
        // reason to suspend the portal
        PortalRun(
            portal.as_ptr(),
            i64::MAX,
            false,
            #[cfg(pre_pg18)]
            true,
            dest.as_ptr(),
            dest.as_ptr(),
            &mut completion_tag as _,
        );

        PortalDrop(portal.as_ptr(), false);

        completion_tag.nprocessed
    }
}

/// The raw statement to analyze: the query the user wrote, or the `SELECT` that a named table
/// stands for.
fn raw_stmt_for(
    p_stmt: &PgBox<PlannedStmt>,
    stmt: &PgBox<CopyStmt>,
    relation: &PgRelation,
) -> PgBox<RawStmt, AllocatedByRust> {
    let mut raw_query = unsafe { PgBox::<RawStmt>::alloc_node(NodeTag::T_RawStmt) };

    // carried over so that an error inside the query underlines the right part of the original
    // statement text rather than the whole thing
    raw_query.stmt_location = p_stmt.stmt_location;
    raw_query.stmt_len = p_stmt.stmt_len;

    raw_query.stmt = if relation.is_null() {
        stmt.query
    } else {
        select_stmt_for(stmt, relation).into_pg() as _
    };

    raw_query
}

/// `SELECT * FROM t`, or `SELECT a, b FROM t` when the statement listed columns.
fn select_stmt_for(stmt: &PgBox<CopyStmt>, relation: &PgRelation) -> PgBox<SelectStmt> {
    let mut target_list = PgList::new();

    if stmt.attlist.is_null() {
        let mut col_ref = unsafe { PgBox::<ColumnRef>::alloc_node(NodeTag::T_ColumnRef) };
        let a_star = unsafe { PgBox::<A_Star>::alloc_node(NodeTag::T_A_Star) };

        let mut field_list = PgList::new();
        field_list.push(a_star.into_pg());

        col_ref.fields = field_list.into_pg();
        col_ref.location = -1;

        target_list.push(res_target(col_ref.into_pg() as _));
    } else {
        let attribute_names = unsafe { PgList::<Node>::from_pg(stmt.attlist) };

        for attribute_name in attribute_names.iter_ptr() {
            let mut col_ref = unsafe { PgBox::<ColumnRef>::alloc_node(NodeTag::T_ColumnRef) };

            let mut field_list = PgList::new();
            field_list.push(attribute_name);

            col_ref.fields = field_list.into_pg();
            col_ref.location = -1;

            target_list.push(res_target(col_ref.into_pg() as _));
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

    // PG19 allows COPY TO from a partitioned table, which means the generated SELECT has to
    // recurse into the partitions. Any other relation kind has no children.
    from.inh = (unsafe { *relation.rd_rel }).relkind == RELKIND_PARTITIONED_TABLE as c_char;

    let mut select_stmt = unsafe { PgBox::<SelectStmt>::alloc_node(NodeTag::T_SelectStmt) };

    select_stmt.targetList = target_list.into_pg();

    let mut from_list = PgList::new();
    from_list.push(from.into_pg());
    select_stmt.fromClause = from_list.into_pg();

    select_stmt.into_pg_boxed()
}

fn res_target(val: *mut Node) -> *mut ResTarget {
    let mut target = unsafe { PgBox::<ResTarget>::alloc_node(NodeTag::T_ResTarget) };
    target.name = std::ptr::null_mut();
    target.indirection = std::ptr::null_mut();
    target.val = val;
    target.location = -1;

    target.into_pg()
}

/// Refuses the relation kinds core COPY refuses, with the messages core uses.
///
/// Matching core here is the point rather than an aesthetic preference: a user who learns that
/// `COPY a_view TO` needs the `COPY (SELECT ...) TO` form should learn it the same way whether
/// the destination was a path or a uri.
fn ensure_table_kind(relation: &PgRelation) {
    let relation_kind = unsafe { *relation.rd_rel }.relkind;

    if relation_kind == RELKIND_RELATION as c_char {
        return;
    }

    if relation_kind == RELKIND_VIEW as c_char {
        ereport!(
            PgLogLevel::ERROR,
            PgSqlErrorCode::ERRCODE_WRONG_OBJECT_TYPE,
            format!("cannot copy from view \"{}\"", relation.name()),
            "Try the COPY (SELECT ...) TO variant.",
        );
    } else if relation_kind == RELKIND_MATVIEW as c_char {
        // PG18 reads a populated materialized view directly; before that, core refused one
        #[cfg(not(pre_pg18))]
        if !unsafe { *relation.rd_rel }.relispopulated {
            ereport!(
                PgLogLevel::ERROR,
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                format!(
                    "cannot copy from unpopulated materialized view \"{}\"",
                    relation.name()
                ),
                "Use the REFRESH MATERIALIZED VIEW command.",
            );
        }

        #[cfg(pre_pg18)]
        ereport!(
            PgLogLevel::ERROR,
            PgSqlErrorCode::ERRCODE_WRONG_OBJECT_TYPE,
            format!("cannot copy from materialized view \"{}\"", relation.name()),
            "Try the COPY (SELECT ...) TO variant.",
        );
    } else if relation_kind == RELKIND_FOREIGN_TABLE as c_char {
        ereport!(
            PgLogLevel::ERROR,
            PgSqlErrorCode::ERRCODE_WRONG_OBJECT_TYPE,
            format!("cannot copy from foreign table \"{}\"", relation.name()),
            "Try the COPY (SELECT ...) TO variant.",
        );
    } else if relation_kind == RELKIND_SEQUENCE as c_char {
        ereport!(
            PgLogLevel::ERROR,
            PgSqlErrorCode::ERRCODE_WRONG_OBJECT_TYPE,
            format!("cannot copy from sequence \"{}\"", relation.name()),
            "Try the COPY (SELECT ...) TO variant.",
        );
    } else if relation_kind == RELKIND_PARTITIONED_TABLE as c_char {
        #[cfg(not(pre_pg19))]
        ensure_no_foreign_partition(relation);

        #[cfg(pre_pg19)]
        ereport!(
            PgLogLevel::ERROR,
            PgSqlErrorCode::ERRCODE_WRONG_OBJECT_TYPE,
            format!("cannot copy from partitioned table \"{}\"", relation.name()),
            "Try the COPY (SELECT ...) TO variant.",
        );
    } else {
        ereport!(
            PgLogLevel::ERROR,
            PgSqlErrorCode::ERRCODE_WRONG_OBJECT_TYPE,
            format!(
                "cannot copy from non-table relation \"{}\"",
                relation.name()
            ),
            "Try the COPY (SELECT ...) TO variant.",
        );
    }
}

/// A partitioned table with a foreign partition is refused whole, as core refuses it: reading
/// one would need the FDW to support it, and a partial COPY would be worse than none.
#[cfg(not(pre_pg19))]
fn ensure_no_foreign_partition(relation: &PgRelation) {
    use pgrx::pg_sys::{find_all_inheritors, get_rel_name, get_rel_relkind, AccessShareLock};

    let partitions =
        unsafe { find_all_inheritors(relation.oid(), AccessShareLock as _, std::ptr::null_mut()) };
    let partitions = unsafe { PgList::<std::ffi::c_void>::from_pg(partitions) };

    for partition in partitions.iter_oid() {
        if unsafe { get_rel_relkind(partition) } == RELKIND_FOREIGN_TABLE as c_char {
            let partition_name = unsafe { get_rel_name(partition) };
            let partition_name = unsafe { CStr::from_ptr(partition_name) }
                .to_str()
                .expect("a relation name is valid utf-8");

            ereport!(
                PgLogLevel::ERROR,
                PgSqlErrorCode::ERRCODE_WRONG_OBJECT_TYPE,
                format!("cannot copy from foreign table \"{partition_name}\""),
                format!(
                    "Partition \"{}\" is a foreign table in partitioned table \"{}\".",
                    partition_name,
                    relation.name()
                ),
            );
        }
    }
}
