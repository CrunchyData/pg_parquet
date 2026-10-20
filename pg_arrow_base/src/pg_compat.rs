//! The handful of core functions whose signature moved between supported versions.
//!
//! Everything here is a one-for-one wrapper, deliberately: a `#[cfg]` in the middle of the
//! COPY path is a thing to re-read on every release, while a `#[cfg]` around a call that has
//! one job is a thing to check once per version bump.

#![allow(non_snake_case)]

use std::ffi::{c_char, CStr};

use pgrx::pg_sys::{
    bms_add_member, set_config_option, AsPgCStr, AtEOXact_GUC, CopyGetAttnums, CopyStmt,
    FirstLowInvalidHeapAttributeNumber, GucAction::GUC_ACTION_SAVE, GucContext::PGC_SUSET,
    GucSource::PGC_S_SESSION, List, Node, ParamListInfo, ParseNamespaceItem, ParseState,
    PlannedStmt, Query, QueryEnvironment, RawStmt, ACL_INSERT, ACL_SELECT,
};
use pgrx::{PgBox, PgList, PgRelation};

/// Postgres 15 renamed `EmitWarningsOnPlaceholders` to `MarkGUCPrefixReserved`. The older one
/// only warns about the placeholders under our prefix that were already set when the library
/// loaded; the newer one also rejects a later `SET`, which is the only defence against
/// `arrow.enabl_copy_hooks = on` doing nothing at all.
pub(crate) fn MarkGUCPrefixReserved(guc_prefix: &str) {
    #[cfg(pre_pg15)]
    unsafe {
        pgrx::pg_sys::EmitWarningsOnPlaceholders(guc_prefix.as_pg_cstr())
    }

    #[cfg(not(pre_pg15))]
    unsafe {
        pgrx::pg_sys::MarkGUCPrefixReserved(guc_prefix.as_pg_cstr())
    }
}

/// Postgres 15 split `pg_analyze_and_rewrite` into a fixed-params and a param-hook form. The
/// query here never has parameters to resolve — it is a `SELECT` this extension just built —
/// so the fixed-params one is the equivalent of the old call.
pub(crate) fn pg_analyze_and_rewrite(
    raw_stmt: *mut RawStmt,
    query_string: *const c_char,
    query_env: *mut QueryEnvironment,
) -> *mut List {
    #[cfg(pre_pg15)]
    unsafe {
        pgrx::pg_sys::pg_analyze_and_rewrite(
            raw_stmt,
            query_string,
            std::ptr::null_mut(),
            0,
            query_env,
        )
    }

    #[cfg(not(pre_pg15))]
    unsafe {
        pgrx::pg_sys::pg_analyze_and_rewrite_fixedparams(
            raw_stmt,
            query_string,
            std::ptr::null_mut(),
            0,
            query_env,
        )
    }
}

/// Postgres 19 added an `ExplainState` argument, which is only non-null when a plan is being
/// built for `EXPLAIN`. This one never is.
pub(crate) fn pg_plan_query(
    query: *mut Query,
    query_string: *const c_char,
    cursor_options: i32,
    bound_params: ParamListInfo,
) -> *mut PlannedStmt {
    #[cfg(pre_pg19)]
    unsafe {
        pgrx::pg_sys::pg_plan_query(query, query_string, cursor_options, bound_params)
    }

    #[cfg(not(pre_pg19))]
    unsafe {
        pgrx::pg_sys::pg_plan_query(
            query,
            query_string,
            cursor_options,
            bound_params,
            std::ptr::null_mut(),
        )
    }
}

/// Postgres 15 gave `shm_mq_sendv` a `force_flush` argument. Flushing every message is what a
/// parallel `COPY FROM` worker wants on every version: the leader inserts the rows in file
/// order, so a message held back is a leader waiting for it.
pub(crate) fn shm_mq_sendv(
    handle: *mut pgrx::pg_sys::shm_mq_handle,
    iov: &mut [pgrx::pg_sys::shm_mq_iovec],
) -> pgrx::pg_sys::shm_mq_result::Type {
    let nowait = false;

    #[cfg(pre_pg15)]
    unsafe {
        pgrx::pg_sys::shm_mq_sendv(handle, iov.as_mut_ptr(), iov.len() as _, nowait)
    }

    #[cfg(not(pre_pg15))]
    unsafe {
        let force_flush = true;

        pgrx::pg_sys::shm_mq_sendv(
            handle,
            iov.as_mut_ptr(),
            iov.len() as _,
            nowait,
            force_flush,
        )
    }
}

/// Postgres 15 replaced the `Value` node with per-type nodes, so a string constant in a parse
/// tree — a column name in a `COPY`'s column list, here — is read out of a different struct.
pub(crate) fn strVal(val: *mut Node) -> String {
    #[cfg(pre_pg15)]
    let string = unsafe { (*(val as *mut pgrx::pg_sys::Value)).val.str_ };

    #[cfg(not(pre_pg15))]
    let string = unsafe { (*(val as *mut pgrx::pg_sys::String)).sval };

    unsafe { CStr::from_ptr(string) }
        .to_str()
        .expect("a name in a parse tree is valid utf-8")
        .to_string()
}

/// The privilege check core's `DoCopy` makes, which has to be made here because the statement
/// never reaches `DoCopy`: insert or select on the table, and the same on each column the
/// statement named.
///
/// Postgres 16 moved the required permissions off the range table entry and into an
/// `RTEPermissionInfo`, so the two versions set the same fields on different structs.
#[cfg(not(pre_pg16))]
pub(crate) fn check_copy_table_permission(
    p_stmt: &PgBox<PlannedStmt>,
    p_state: &PgBox<ParseState>,
    ns_item: &PgBox<ParseNamespaceItem>,
    relation: &PgRelation,
) {
    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(p_stmt.utilityStmt as _) };

    let mut perminfo =
        unsafe { PgBox::<pgrx::pg_sys::RTEPermissionInfo>::from_pg(ns_item.p_perminfo) };

    perminfo.requiredPerms = if copy_stmt.is_from {
        ACL_INSERT as _
    } else {
        ACL_SELECT as _
    };

    for attnum in copy_attnums(&copy_stmt, relation).iter_int() {
        let attno = attnum - FirstLowInvalidHeapAttributeNumber;

        unsafe {
            if copy_stmt.is_from {
                perminfo.insertedCols = bms_add_member(perminfo.insertedCols, attno);
            } else {
                perminfo.selectedCols = bms_add_member(perminfo.selectedCols, attno);
            }
        }
    }

    let mut perm_infos = PgList::<pgrx::pg_sys::RTEPermissionInfo>::new();
    perm_infos.push(perminfo.as_ptr());

    let guc_level = unsafe { disable_pgaudit() };

    unsafe { pgrx::pg_sys::ExecCheckPermissions(p_state.p_rtable, perm_infos.as_ptr(), true) };

    unsafe { reset_pgaudit(guc_level) };
}

#[cfg(pre_pg16)]
pub(crate) fn check_copy_table_permission(
    p_stmt: &PgBox<PlannedStmt>,
    p_state: &PgBox<ParseState>,
    ns_item: &PgBox<ParseNamespaceItem>,
    relation: &PgRelation,
) {
    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(p_stmt.utilityStmt as _) };

    let mut rte = unsafe { PgBox::<pgrx::pg_sys::RangeTblEntry>::from_pg(ns_item.p_rte) };

    rte.requiredPerms = if copy_stmt.is_from {
        ACL_INSERT as _
    } else {
        ACL_SELECT as _
    };

    for attnum in copy_attnums(&copy_stmt, relation).iter_int() {
        let attno = attnum - FirstLowInvalidHeapAttributeNumber;

        unsafe {
            if copy_stmt.is_from {
                rte.insertedCols = bms_add_member(rte.insertedCols, attno);
            } else {
                rte.selectedCols = bms_add_member(rte.selectedCols, attno);
            }
        }
    }

    let guc_level = unsafe { disable_pgaudit() };

    unsafe { pgrx::pg_sys::ExecCheckRTPerms(p_state.p_rtable, true) };

    unsafe { reset_pgaudit(guc_level) };
}

/// The attribute numbers the statement touches: the ones it listed, or all of the relation's.
fn copy_attnums(copy_stmt: &PgBox<CopyStmt>, relation: &PgRelation) -> PgList<i16> {
    let tupledesc = relation.tuple_desc();

    let attnums =
        unsafe { CopyGetAttnums(tupledesc.as_ptr(), relation.as_ptr(), copy_stmt.attlist) };

    unsafe { PgList::<i16>::from_pg(attnums) }
}

// pgaudit expects its own ProcessUtility hook to have run before the executor's permission
// check hook fires, and ours does not call it, so pgaudit crashes. Turning it off for the
// duration of the check is the workaround upstream suggests.
// See https://github.com/pgaudit/pgaudit/issues/212
unsafe fn disable_pgaudit() -> i32 {
    let guc_level = pgrx::pg_sys::NewGUCNestLevel();

    set_config_option(
        "pgaudit.log".as_pg_cstr(),
        "none".as_pg_cstr(),
        PGC_SUSET,
        PGC_S_SESSION,
        GUC_ACTION_SAVE,
        true,
        0,
        false,
    );

    guc_level
}

unsafe fn reset_pgaudit(guc_level: i32) {
    AtEOXact_GUC(true, guc_level);
}
