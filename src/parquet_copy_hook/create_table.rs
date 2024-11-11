use std::ffi::CStr;

use pgrx::{
    is_a,
    pg_sys::{
        defGetString, makeDefElem, makeString, AsPgCStr, ColumnDef, CopyStmt, CreateStmt, DefElem,
        NodeTag::{T_CopyStmt, T_CreateStmt},
        PlannedStmt, RangeVar,
    },
    PgBox, PgList,
};

use crate::arrow_parquet::{
    schema::infer_schema::infer_columns_from_uri,
    uri_utils::{ensure_access_privilege_to_uri, uri_as_string, ParsedUriInfo},
};

use super::copy_utils::{get_option, has_option};

pub(crate) fn is_create_table_from_parquet_stmt(p_stmt: &PgBox<PlannedStmt>) -> bool {
    let is_create_stmt = unsafe { is_a(p_stmt.utilityStmt, T_CreateStmt) };

    if !is_create_stmt {
        return false;
    }

    let create_stmt = unsafe { PgBox::<CreateStmt>::from_pg(p_stmt.utilityStmt as _) };

    has_option(create_stmt.options, "load_from")
        || has_option(create_stmt.options, "definition_from")
}

pub(crate) fn validate_create_table_from_parquet_stmt(create_stmt: &PgBox<CreateStmt>) {
    let definition_from = has_option(create_stmt.options, "definition_from");
    let load_from = has_option(create_stmt.options, "load_from");

    if load_from && definition_from {
        panic!("cannot specify both 'load_from' and 'definition_from' options");
    }

    if !create_stmt.partbound.is_null() {
        panic!("cannot create a partition table from a parquet file");
    }

    if !create_stmt.tableElts.is_null() {
        panic!("cannot create a table from a parquet file when column definitions are provided");
    }
}

pub(crate) fn infer_column_definitions(create_stmt: &PgBox<CreateStmt>) -> PgList<ColumnDef> {
    let definition_from = has_option(create_stmt.options, "definition_from");
    let load_from = has_option(create_stmt.options, "load_from");

    if load_from && definition_from {
        panic!("cannot specify both 'load_from' and 'definition_from' options");
    }

    if !create_stmt.partbound.is_null() {
        panic!("cannot infer column definitions for a partition table");
    }

    if !create_stmt.tableElts.is_null() {
        panic!("cannot infer column definitions when column definitions are provided");
    }

    let uri_info = create_stmt_get_uri(create_stmt);

    let copy_from = true;
    ensure_access_privilege_to_uri(&uri_info, copy_from);

    infer_columns_from_uri(&uri_info)
}

pub(crate) fn create_stmt_get_uri(create_stmt: &PgBox<CreateStmt>) -> ParsedUriInfo {
    let load_from_uri = create_stmt_get_load_from_uri(create_stmt);
    let definition_from_uri = create_stmt_get_definition_from_uri(create_stmt);

    if let Some(uri_info) = load_from_uri {
        uri_info
    } else if let Some(uri_info) = definition_from_uri {
        uri_info
    } else {
        panic!("either 'load_from' or 'definition_from' option must be specified");
    }
}

fn create_stmt_get_load_from_uri(create_stmt: &PgBox<CreateStmt>) -> Option<ParsedUriInfo> {
    let load_from_option = get_option(create_stmt.options, "load_from");

    if load_from_option.is_null() {
        return None;
    }

    let uri = unsafe { defGetString(load_from_option.as_ptr()) };

    let uri = unsafe {
        CStr::from_ptr(uri)
            .to_str()
            .expect("load_from option is not a valid CString")
    };

    Some(ParsedUriInfo::try_from(uri).expect("invalid uri"))
}

fn create_stmt_get_definition_from_uri(create_stmt: &PgBox<CreateStmt>) -> Option<ParsedUriInfo> {
    let definition_from_option = get_option(create_stmt.options, "definition_from");

    if definition_from_option.is_null() {
        return None;
    }

    let uri = unsafe { defGetString(definition_from_option.as_ptr()) };

    let uri = unsafe {
        CStr::from_ptr(uri)
            .to_str()
            .expect("definition_from option is not a valid CString")
    };

    Some(ParsedUriInfo::try_from(uri).expect("invalid uri"))
}

pub(crate) fn create_stmt_remove_parquet_options(create_stmt: &mut PgBox<CreateStmt>) {
    let options = unsafe { PgList::<DefElem>::from_pg(create_stmt.options) };

    let mut new_options = PgList::<DefElem>::new();

    for option in options.iter_ptr() {
        let option = unsafe { PgBox::<DefElem>::from_pg(option) };

        let option_name = unsafe {
            CStr::from_ptr(option.defname)
                .to_str()
                .expect("option name is not a valid CString")
        };

        if option_name == "load_from" || option_name == "definition_from" {
            continue;
        }

        new_options.push(option.as_ptr());
    }

    create_stmt.options = new_options.into_pg();
}

pub(crate) fn create_copy_from_parquet_stmt_for_table(
    table: *mut RangeVar,
    uri_info: &ParsedUriInfo,
) -> PgBox<CopyStmt> {
    let mut copy_from_stmt = unsafe { PgBox::<CopyStmt>::alloc_node(T_CopyStmt) };
    copy_from_stmt.relation = table;
    copy_from_stmt.is_from = true;
    copy_from_stmt.filename = uri_as_string(&uri_info.uri).as_pg_cstr();

    let format_option_name = "format".as_pg_cstr();
    let format_option_val = unsafe { makeString("parquet".as_pg_cstr()) } as _;
    let format_option = unsafe { makeDefElem(format_option_name, format_option_val, -1) };

    let mut new_copy_options = PgList::<DefElem>::new();
    new_copy_options.push(format_option);

    copy_from_stmt.options = new_copy_options.into_pg();

    copy_from_stmt.into_pg_boxed()
}
