use pgrx::{
    is_a,
    pg_sys::{
        self, makeRangeVar, pg_analyze_and_rewrite_fixedparams, pg_plan_query, CommandTag,
        CopyStmt, CreateNewPortal, DestReceiver, GetActiveSnapshot, NodeTag::T_CopyStmt,
        PlannedStmt, PortalDefineQuery, PortalDrop, PortalRun, PortalStart, QueryCompletion,
        RawStmt, CURSOR_OPT_PARALLEL_OK,
    },
    AllocatedByRust, PgBox, PgList, PgRelation,
};

use crate::parquet_copy_hook::copy_utils::{copy_has_relation, copy_lock_mode, copy_relation_oid};

pub(crate) fn execute_copy_to_with_dest_receiver(
    pstmt: &PgBox<pg_sys::PlannedStmt>,
    query_string: &core::ffi::CStr,
    params: PgBox<pg_sys::ParamListInfoData>,
    query_env: PgBox<pg_sys::QueryEnvironment>,
    parquet_dest: PgBox<DestReceiver>,
) -> u64 {
    unsafe {
        debug_assert!(is_a(pstmt.utilityStmt, T_CopyStmt));
        let copy_stmt = PgBox::<CopyStmt>::from_pg(pstmt.utilityStmt as _);

        let mut relation = PgRelation::from_pg(std::ptr::null_mut());

        if copy_has_relation(pstmt) {
            let rel_oid = copy_relation_oid(pstmt);
            let lock_mode = copy_lock_mode(pstmt);
            relation = PgRelation::with_lock(rel_oid, lock_mode);
        }

        // prepare raw query
        let raw_query = prepare_copy_to_raw_stmt(pstmt, &copy_stmt, &relation);

        // analyze and rewrite raw query
        let rewritten_queries = pg_analyze_and_rewrite_fixedparams(
            raw_query.as_ptr(),
            query_string.as_ptr(),
            std::ptr::null_mut(),
            0,
            query_env.as_ptr(),
        );

        // plan rewritten query
        let query = PgList::from_pg(rewritten_queries)
            .pop()
            .expect("rewritten query is empty");

        let plan = pg_plan_query(
            query,
            std::ptr::null(),
            CURSOR_OPT_PARALLEL_OK as _,
            params.as_ptr(),
        );

        // create portal
        let portal = CreateNewPortal();
        let mut portal = PgBox::from_pg(portal);
        portal.visible = false;

        // prepare portal
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

        // start portal
        PortalStart(portal.as_ptr(), params.as_ptr(), 0, GetActiveSnapshot());

        // run portal
        let mut completion_tag = QueryCompletion {
            commandTag: CommandTag::CMDTAG_COPY,
            nprocessed: 0,
        };

        PortalRun(
            portal.as_ptr(),
            i64::MAX,
            false,
            true,
            parquet_dest.as_ptr(),
            parquet_dest.as_ptr(),
            &mut completion_tag as _,
        );

        // drop portal
        PortalDrop(portal.as_ptr(), false);

        completion_tag.nprocessed
    }
}

fn prepare_copy_to_raw_stmt(
    pstmt: &PgBox<pg_sys::PlannedStmt>,
    copy_stmt: &PgBox<CopyStmt>,
    relation: &PgRelation,
) -> PgBox<RawStmt, AllocatedByRust> {
    let mut raw_query = unsafe { PgBox::<pg_sys::RawStmt>::alloc_node(pg_sys::NodeTag::T_RawStmt) };
    raw_query.stmt_location = pstmt.stmt_location;
    raw_query.stmt_len = pstmt.stmt_len;

    if relation.is_null() {
        raw_query.stmt = copy_stmt.query;
    } else {
        // convert relation to query
        let mut target_list = PgList::new();

        if copy_stmt.attlist.is_null() {
            let mut col_ref =
                unsafe { PgBox::<pg_sys::ColumnRef>::alloc_node(pg_sys::NodeTag::T_ColumnRef) };
            let a_star = unsafe { PgBox::<pg_sys::A_Star>::alloc_node(pg_sys::NodeTag::T_A_Star) };

            let mut field_list = PgList::new();
            field_list.push(a_star.into_pg());

            col_ref.fields = field_list.into_pg();
            col_ref.location = -1;

            let mut target =
                unsafe { PgBox::<pg_sys::ResTarget>::alloc_node(pg_sys::NodeTag::T_ResTarget) };
            target.name = std::ptr::null_mut();
            target.indirection = std::ptr::null_mut();
            target.val = col_ref.into_pg() as _;
            target.location = -1;

            target_list.push(target.into_pg());
        } else {
            let attlist = unsafe { PgList::<*mut i8>::from_pg(copy_stmt.attlist) };
            for attname in attlist.iter_ptr() {
                let mut col_ref =
                    unsafe { PgBox::<pg_sys::ColumnRef>::alloc_node(pg_sys::NodeTag::T_ColumnRef) };

                let mut field_list = PgList::new();
                field_list.push(unsafe { *attname });

                col_ref.fields = field_list.into_pg();
                col_ref.location = -1;

                let mut target =
                    unsafe { PgBox::<pg_sys::ResTarget>::alloc_node(pg_sys::NodeTag::T_ResTarget) };
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

        let mut select_stmt =
            unsafe { PgBox::<pg_sys::SelectStmt>::alloc_node(pg_sys::NodeTag::T_SelectStmt) };

        select_stmt.targetList = target_list.into_pg();

        let mut from_list = PgList::new();
        from_list.push(from.into_pg());
        select_stmt.fromClause = from_list.into_pg();

        raw_query.stmt = select_stmt.into_pg() as _;
    }

    raw_query
}
