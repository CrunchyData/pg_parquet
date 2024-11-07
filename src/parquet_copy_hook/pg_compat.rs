use std::ffi::{c_char, CStr};

use pgrx::{
    datum::TimeWithTimeZone,
    direct_function_call,
    pg_sys::{
        copy_data_source_cb, AsPgCStr, List, Node, ParseState, QueryEnvironment, RawStmt, Relation,
    },
    IntoDatum,
};

#[cfg(not(feature = "pg13"))]
use pgrx::AnyNumeric;

pub(crate) fn pg_analyze_and_rewrite(
    raw_stmt: *mut RawStmt,
    query_string: *const c_char,
    query_env: *mut QueryEnvironment,
) -> *mut List {
    #[cfg(any(feature = "pg13", feature = "pg14"))]
    unsafe {
        pgrx::pg_sys::pg_analyze_and_rewrite(
            raw_stmt,
            query_string,
            std::ptr::null_mut(),
            0,
            query_env,
        )
    }

    #[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
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

#[allow(non_snake_case)]
pub(crate) fn strVal(val: *mut Node) -> String {
    #[cfg(any(feature = "pg13", feature = "pg14"))]
    unsafe {
        let val = (*(val as *mut pgrx::pg_sys::Value)).val.str_;

        CStr::from_ptr(val)
            .to_str()
            .expect("invalid string")
            .to_string()
    }

    #[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
    unsafe {
        let val = (*(val as *mut pgrx::pg_sys::String)).sval;

        CStr::from_ptr(val)
            .to_str()
            .expect("invalid string")
            .to_string()
    }
}

#[cfg(feature = "pg13")]
#[allow(non_snake_case)]
pub(crate) fn BeginCopyFrom(
    pstate: *mut ParseState,
    relation: Relation,
    _where_clause: *mut Node,
    data_source_cb: copy_data_source_cb,
    attribute_list: *mut List,
    copy_options: *mut List,
) -> *mut pgrx::pg_sys::CopyStateData {
    unsafe {
        pgrx::pg_sys::BeginCopyFrom(
            pstate,
            relation,
            std::ptr::null(),
            false,
            data_source_cb,
            attribute_list,
            copy_options,
        )
    }
}

#[cfg(any(feature = "pg14", feature = "pg15", feature = "pg16", feature = "pg17"))]
#[allow(non_snake_case)]
pub(crate) fn BeginCopyFrom(
    pstate: *mut ParseState,
    relation: Relation,
    _where_clause: *mut Node,
    data_source_cb: copy_data_source_cb,
    attribute_list: *mut List,
    copy_options: *mut List,
) -> *mut pgrx::pg_sys::CopyFromStateData {
    unsafe {
        pgrx::pg_sys::BeginCopyFrom(
            pstate,
            relation,
            _where_clause,
            std::ptr::null(),
            false,
            data_source_cb,
            attribute_list,
            copy_options,
        )
    }
}

pub(crate) fn extract_timezone_from_timetz(timetz: TimeWithTimeZone) -> f64 {
    #[cfg(feature = "pg13")]
    {
        let timezone_as_secs: f64 = unsafe {
            direct_function_call(
                pgrx::pg_sys::timetz_part,
                &["timezone".into_datum(), timetz.into_datum()],
            )
        }
        .expect("cannot extract timezone from timetz");

        timezone_as_secs
    }

    #[cfg(any(feature = "pg14", feature = "pg15", feature = "pg16", feature = "pg17"))]
    {
        let timezone_as_secs: AnyNumeric = unsafe {
            direct_function_call(
                pgrx::pg_sys::extract_timetz,
                &["timezone".into_datum(), timetz.into_datum()],
            )
        }
        .expect("cannot extract timezone from timetz");

        let timezone_as_secs: f64 = timezone_as_secs
            .try_into()
            .unwrap_or_else(|e| panic!("{}", e));

        timezone_as_secs
    }
}

#[allow(non_snake_case)]
pub(crate) fn MarkGUCPrefixReserved(guc_prefix: &str) {
    #[cfg(any(feature = "pg13", feature = "pg14"))]
    unsafe {
        pgrx::pg_sys::EmitWarningsOnPlaceholders(guc_prefix.as_pg_cstr())
    }

    #[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
    unsafe {
        pgrx::pg_sys::MarkGUCPrefixReserved(guc_prefix.as_pg_cstr())
    }
}
