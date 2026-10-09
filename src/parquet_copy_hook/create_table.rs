use std::ffi::CStr;

use pgrx::{
    is_a,
    pg_sys::{
        defGetString, get_relname_relid, makeDefElem, makeString, AsPgCStr, ColumnDef, CopyStmt,
        CreateStmt, DefElem, InvalidOid,
        NodeTag::{T_CopyStmt, T_CreateStmt},
        PlannedStmt, RangeVar, RangeVarGetCreationNamespace,
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

    if !create_stmt.inhRelations.is_null() {
        // the inherited columns would be merged with the inferred ones,
        // which makes the column order of the table unpredictable
        panic!("cannot create a table from a parquet file when the table inherits columns");
    }
}

pub(crate) fn infer_column_definitions(uri_info: &ParsedUriInfo) -> PgList<ColumnDef> {
    let copy_from = true;
    ensure_access_privilege_to_uri(uri_info, copy_from);

    infer_columns_from_uri(uri_info)
}

// table_already_exists returns true if the table that would be created already exists.
// Postgres turns "CREATE TABLE IF NOT EXISTS" into a no-op in that case, so neither the
// column inference nor the data load should happen.
pub(crate) fn table_already_exists(table: *mut RangeVar) -> bool {
    // same lookup as Postgres does for "IF NOT EXISTS" at DefineRelation()
    let namespace_oid = unsafe { RangeVarGetCreationNamespace(table) };

    let relid = unsafe { get_relname_relid((*table).relname, namespace_oid) };

    relid != InvalidOid
}

pub(crate) fn create_stmt_get_uri(create_stmt: &PgBox<CreateStmt>) -> ParsedUriInfo {
    for option_name in ["load_from", "definition_from"] {
        let option = get_option(create_stmt.options, option_name);

        if option.is_null() {
            continue;
        }

        let uri = unsafe { defGetString(option.as_ptr()) };

        let uri = unsafe {
            CStr::from_ptr(uri)
                .to_str()
                .unwrap_or_else(|_| panic!("{} option is not a valid CString", option_name))
        };

        return ParsedUriInfo::try_from(uri).expect("invalid uri");
    }

    panic!("either 'load_from' or 'definition_from' option must be specified");
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
