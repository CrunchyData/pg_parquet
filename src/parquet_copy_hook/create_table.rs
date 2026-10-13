use std::ffi::CStr;

use pgrx::{
    ereport, is_a,
    pg_sys::{
        defGetString, get_relname_relid, makeDefElem, makeString, AsPgCStr, ColumnDef, CopyStmt,
        CreateStmt, DefElem, GetDatabaseEncodingName, InvalidOid,
        NodeTag::{T_CopyStmt, T_CreateStmt},
        Oid, PlannedStmt, PreventCommandIfParallelMode, PreventCommandIfReadOnly, RangeVar,
        RangeVarGetCreationNamespace, RelationClose, RelationIdGetRelation, SubTransactionId,
    },
    PgBox, PgList, PgLogLevel, PgSqlErrorCode,
};

use crate::{
    arrow_parquet::{
        schema::infer_schema::infer_columns_from_uri,
        uri_utils::{ensure_access_privilege_to_uri, uri_as_string, ParsedUriInfo},
    },
    pgrx_utils::extension_exists,
};

use super::copy_utils::{count_option, get_option, has_option};

pub(crate) fn is_create_table_from_parquet_stmt(p_stmt: &PgBox<PlannedStmt>) -> bool {
    let is_create_stmt = unsafe { is_a(p_stmt.utilityStmt, T_CreateStmt) };

    if !is_create_stmt {
        return false;
    }

    let create_stmt = unsafe { PgBox::<CreateStmt>::from_pg(p_stmt.utilityStmt as _) };

    if !has_option(create_stmt.options, "load_from")
        && !has_option(create_stmt.options, "definition_from")
    {
        return false;
    }

    // pg_parquet is loaded via shared_preload_libraries, so the hook also runs in the databases
    // where the extension was never created. The inference needs the types that the extension
    // installs, so leave the statement to Postgres there, as the COPY hook does.
    if !extension_exists("pg_parquet") {
        ereport!(
            PgLogLevel::WARNING,
            PgSqlErrorCode::ERRCODE_WARNING,
            "pg_parquet can handle this CREATE TABLE command but is not enabled",
            "Run CREATE EXTENSION pg_parquet; to enable the pg_parquet extension.",
        );

        return false;
    }

    true
}

// error_if_create_table_not_allowed gives the error that Postgres gives for a CREATE TABLE
// that it cannot execute at all. Postgres checks it when it executes the statement, which is
// after the column inference read the file and created the types that the file needs.
pub(crate) fn error_if_create_table_not_allowed() {
    let command_name = "CREATE TABLE".as_pg_cstr();

    unsafe {
        PreventCommandIfReadOnly(command_name);
        PreventCommandIfParallelMode(command_name);
    }
}

pub(crate) fn validate_create_table_from_parquet_stmt(create_stmt: &PgBox<CreateStmt>) {
    let definition_from = has_option(create_stmt.options, "definition_from");
    let load_from = has_option(create_stmt.options, "load_from");

    if load_from && definition_from {
        panic!("cannot specify both 'load_from' and 'definition_from' options");
    }

    // Postgres rejects a duplicate reloption, but it never sees the parquet ones
    for option_name in ["load_from", "definition_from"] {
        if count_option(create_stmt.options, option_name) > 1 {
            panic!("parameter \"{option_name}\" specified more than once");
        }
    }

    if !create_stmt.partbound.is_null() {
        panic!("cannot create a partition table from a parquet file");
    }

    if !create_stmt.tableElts.is_null() {
        panic!("cannot create a table from a parquet file when column definitions are provided");
    }

    if !create_stmt.ofTypename.is_null() {
        // the inferred columns would be added to the ones that come from the composite type,
        // which makes the table inconsistent with its type e.g. pg_dump would not dump them
        panic!("cannot create a table from a parquet file when the table is of a composite type");
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
    table_relid(table) != InvalidOid
}

// table_created_in_this_transaction returns true if the relation that holds the table's name
// was created by the current transaction. A concurrent session can commit a table with the
// same name after we checked that it does not exist, in which case Postgres skips the creation
// with "IF NOT EXISTS" and the parquet file must not be loaded into that session's table.
pub(crate) fn table_created_in_this_transaction(table: *mut RangeVar) -> bool {
    // Postgres defines InvalidSubTransactionId as 0, which pgrx does not generate
    const INVALID_SUB_TRANSACTION_ID: SubTransactionId = 0;

    let relid = table_relid(table);

    if relid == InvalidOid {
        return false;
    }

    unsafe {
        let relation = RelationIdGetRelation(relid);

        if relation.is_null() {
            return false;
        }

        let created_in_this_transaction = (*relation).rd_createSubid != INVALID_SUB_TRANSACTION_ID;

        RelationClose(relation);

        created_in_this_transaction
    }
}

fn table_relid(table: *mut RangeVar) -> Oid {
    // same lookup as Postgres does for "IF NOT EXISTS" at DefineRelation()
    let namespace_oid = unsafe { RangeVarGetCreationNamespace(table) };

    unsafe { get_relname_relid((*table).relname, namespace_oid) }
}

// uri_not_in_utf8_error errors out if the option is not valid utf-8. The option arrives in the
// encoding of the database while a uri has to be utf-8, and nothing converts between the two,
// so a uri that is not ascii is only usable in a utf-8 database.
fn uri_not_in_utf8_error(option_name: &str) -> ! {
    let encoding_name = unsafe { CStr::from_ptr(GetDatabaseEncodingName()) }
        .to_str()
        .expect("database encoding name is not valid UTF-8");

    ereport!(
        ERROR,
        PgSqlErrorCode::ERRCODE_UNTRANSLATABLE_CHARACTER,
        format!("the \"{option_name}\" option is not valid UTF-8"),
        format!(
            "A uri with non-ascii characters cannot be used \
             in a database with encoding \"{encoding_name}\"."
        ),
    );
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
                .unwrap_or_else(|_| uri_not_in_utf8_error(option_name))
        };

        return ParsedUriInfo::try_from(uri).unwrap_or_else(|e| panic!("{}", e.to_string()));
    }

    panic!("either 'load_from' or 'definition_from' option must be specified");
}

pub(crate) fn create_stmt_remove_parquet_options(create_stmt: &mut PgBox<CreateStmt>) {
    let options = unsafe { PgList::<DefElem>::from_pg(create_stmt.options) };

    let mut new_options = PgList::<DefElem>::new();

    for option in options.iter_ptr() {
        let option = unsafe { PgBox::<DefElem>::from_pg(option) };

        // a namespaced option is not ours, so Postgres keeps rejecting it
        if !option.defnamespace.is_null() {
            new_options.push(option.as_ptr());
            continue;
        }

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
