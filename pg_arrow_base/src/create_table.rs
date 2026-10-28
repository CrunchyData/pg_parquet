//! `CREATE TABLE t FROM '<uri>'`: the columns a file implies, and optionally its rows.
//!
//! Spelled as two reloptions, as `pg_parquet` spells them:
//!
//! ```sql
//! -- columns and rows
//! CREATE TABLE orders () WITH (load_from = 's3://bucket/orders.parquet');
//! -- columns only
//! CREATE TABLE orders () WITH (definition_from = 's3://bucket/orders.parquet');
//! ```
//!
//! The statement is rewritten rather than carried out here: the file's schema becomes a column
//! list, that goes into the `CreateStmt`, and the next `ProcessUtility` hook creates the table
//! as though the columns had been written out. A `load_from` then runs the `COPY FROM` the user
//! would have written next. Postgres therefore owns the table creation in both cases, which is
//! what keeps `IF NOT EXISTS`, event triggers, tablespaces and ownership out of this file.
//!
//! The inference itself — a struct to a composite type, a map to a `crunchy_map`, an unsigned
//! integer to the next wider signed one — is `pg_arrow_convert::infer`, because it is the type
//! mapping run backwards and has nothing to do with any format.

use std::ffi::CStr;
use std::sync::Arc;

use format_registry::options::FormatOptions;
use format_registry::provider::FormatProvider;
use pg_arrow_convert::infer::infer_columns_from_schema;
use pg_arrow_convert::match_by::MatchBy;
use pg_arrow_convert::pgrx_utils::extension_exists;
use pgrx::pg_sys::{
    copyObjectImpl, defGetString, get_relname_relid, makeDefElem, makeString, nodeToString,
    AsPgCStr, ColumnDef, CopyStmt, CreateStmt, DefElem, GetDatabaseEncodingName, InvalidOid,
    NodeTag, Oid, PlannedStmt, PreventCommandIfParallelMode, PreventCommandIfReadOnly,
    QueryCompletion, QueryEnvironment, RangeVar, RangeVarGetCreationNamespace, RelationClose,
    RelationIdGetRelation, SubTransactionId,
};
use pgrx::prelude::*;
use pgrx::{is_a, PgList, PgLogLevel, PgSqlErrorCode};

use crate::copy_plan::CopyFromPlan;
use crate::endpoint::Endpoint;
use crate::{batch_reader, copy_hook, copy_stmt, object_store, providers};

/// The two reloptions that make a `CREATE TABLE` ours, plus the one that says in what format to
/// read the uri. Removed from the statement before Postgres sees it, which is also why
/// `count_option` has to check for duplicates here: Postgres never gets the chance.
const CREATE_TABLE_OPTIONS: &[&str] = &["load_from", "definition_from", "format"];

/// A `CREATE TABLE` that names a uri.
pub(crate) struct ArrowCreateStmt {
    pub(crate) uri: String,
    /// Whether the file's rows are loaded as well as its columns: `load_from` rather than
    /// `definition_from`.
    pub(crate) load: bool,
}

fn create_stmt(p_stmt: &PgBox<PlannedStmt>) -> Option<PgBox<CreateStmt>> {
    if p_stmt.utilityStmt.is_null() {
        return None;
    }

    if unsafe { !is_a(p_stmt.utilityStmt, NodeTag::T_CreateStmt) } {
        return None;
    }

    Some(unsafe { PgBox::<CreateStmt>::from_pg(p_stmt.utilityStmt as _) })
}

/// Whether a statement is a `CREATE TABLE` that names a uri.
///
/// Nothing here refuses a statement, for the same reason [`copy_stmt::recognize`] refuses
/// nothing: one that is not ours has to be handed on untouched. The warning below and the
/// `load_from` without a value are the two things it can report.
pub(crate) fn recognize(p_stmt: &PgBox<PlannedStmt>) -> Option<ArrowCreateStmt> {
    let create_stmt = create_stmt(p_stmt)?;

    let load = copy_stmt::count_option(create_stmt.options, "load_from") > 0;
    let definition = copy_stmt::count_option(create_stmt.options, "definition_from") > 0;

    if !load && !definition {
        return None;
    }

    // the library is preloaded, so this hook also runs in the databases where the extension was
    // never created. The inference creates types in a schema the extension owns, so there the
    // statement is Postgres' — which will report the unrecognized reloption itself
    if !extension_exists("pg_arrow_base") {
        ereport!(
            PgLogLevel::WARNING,
            PgSqlErrorCode::ERRCODE_WARNING,
            "pg_arrow_base can handle this CREATE TABLE command but is not enabled",
            "Run CREATE EXTENSION pg_arrow_base; to enable it."
        );

        return None;
    }

    Some(ArrowCreateStmt {
        uri: uri_option(&create_stmt),
        load,
    })
}

/// Everything about the statement that can be decided before the file is read.
fn validate(create_stmt: &PgBox<CreateStmt>, uri: &str) -> Arc<dyn FormatProvider> {
    let refuse = |what: &str| {
        ereport!(
            PgLogLevel::ERROR,
            PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
            format!("cannot create a table from a file {what}"),
            "Create the table with explicit column definitions and then COPY FROM the file."
        );
    };

    if copy_stmt::count_option(create_stmt.options, "load_from") > 0
        && copy_stmt::count_option(create_stmt.options, "definition_from") > 0
    {
        ereport!(
            PgLogLevel::ERROR,
            PgSqlErrorCode::ERRCODE_SYNTAX_ERROR,
            "cannot specify both \"load_from\" and \"definition_from\"",
            "load_from reads the file's rows as well as its columns; definition_from reads only \
             its columns."
        );
    }

    for option_name in CREATE_TABLE_OPTIONS {
        if copy_stmt::count_option(create_stmt.options, option_name) > 1 {
            ereport!(
                PgLogLevel::ERROR,
                PgSqlErrorCode::ERRCODE_SYNTAX_ERROR,
                format!("parameter \"{option_name}\" specified more than once")
            );
        }
    }

    if !create_stmt.partbound.is_null() {
        refuse("as a partition");
    }

    if !create_stmt.tableElts.is_null() {
        refuse("when column definitions are provided");
    }

    if !create_stmt.ofTypename.is_null() {
        // the inferred columns would be added to the ones the composite type brings, which
        // leaves the table inconsistent with its type -- pg_dump would not dump them
        refuse("when the table is of a composite type");
    }

    if !create_stmt.inhRelations.is_null() {
        // the inherited columns would be merged with the inferred ones, which makes the
        // column order of the table unpredictable
        refuse("when the table inherits columns");
    }

    // the uri before the format, because the format is inferred *from* the uri when the
    // statement did not name one: a uri tier 1 cannot even parse should be reported as the uri
    // it is rather than as a file of no known format
    let copy_from = true;

    if !object_store::check_privilege(uri, copy_from) {
        ereport!(
            PgLogLevel::ERROR,
            PgSqlErrorCode::ERRCODE_INSUFFICIENT_PRIVILEGE,
            format!("permission denied to read {uri}"),
            "Remote uris need object_store_read; a path on the server needs \
             pg_read_server_files."
        );
    }

    let format = copy_stmt::get_option(create_stmt.options, "format");

    providers::resolve(format.as_deref(), uri)
}

/// Carries out a recognized `CREATE TABLE ... FROM '<uri>'`.
///
/// The tree is this session's to rewrite: the caller copies it first when Postgres still owns
/// it, because a cached plan — a plpgsql function's, say — that came back with our columns
/// already in it would fail the next time it ran.
#[allow(clippy::too_many_arguments)]
pub(crate) fn execute(
    p_stmt: &mut PgBox<PlannedStmt>,
    query_string: &CStr,
    context: u32,
    params: *mut pgrx::pg_sys::ParamListInfoData,
    query_env: *mut QueryEnvironment,
    dest: *mut pgrx::pg_sys::DestReceiver,
    completion_tag: *mut QueryCompletion,
    stmt: ArrowCreateStmt,
) {
    // the error Postgres gives for a CREATE TABLE it cannot run at all. It checks this when it
    // executes the statement, which is after the inference has read a file and created types
    let command_name = "CREATE TABLE".as_pg_cstr();

    unsafe {
        PreventCommandIfReadOnly(command_name);
        PreventCommandIfParallelMode(command_name);
    }

    let mut create_stmt = unsafe { PgBox::<CreateStmt>::from_pg(p_stmt.utilityStmt as _) };

    let provider = validate(&create_stmt, &stmt.uri);

    remove_our_options(&mut create_stmt);

    // Postgres turns `CREATE TABLE IF NOT EXISTS` into a no-op when the table is there, so
    // neither the inference nor the load may happen in that case
    let skip_creation = create_stmt.if_not_exists && table_already_exists(create_stmt.relation);

    if !skip_creation {
        let file_schema = batch_reader::file_schema(&provider, &stmt.uri, &FormatOptions::new());

        let column_defs: PgList<ColumnDef> = infer_columns_from_schema(&file_schema);

        create_stmt.tableElts = column_defs.into_pg();
    }

    // the tree is already our own copy, so the next hook is welcome to reuse it
    let read_only_tree = false;

    copy_hook::call_next_process_utility(
        p_stmt.as_ptr(),
        query_string.as_ptr(),
        read_only_tree,
        context,
        params,
        query_env,
        dest,
        completion_tag,
    );

    // Postgres skipped the creation if a concurrent session committed a table of the same name
    // while the columns were being inferred, and the file must not be loaded into that
    // session's table. No table under the name at all means the one just created is gone --
    // an event trigger dropped or renamed it -- and the load then reports the missing table,
    // which is what it does without `IF NOT EXISTS` rather than silently loading nothing
    let created_by_us = !create_stmt.if_not_exists
        || table_created_in_this_transaction(create_stmt.relation)
        || !table_already_exists(create_stmt.relation);

    if stmt.load && !skip_creation && created_by_us {
        load_into(p_stmt, create_stmt.relation, provider, &stmt.uri, query_env);
    }
}

/// Runs the `COPY FROM` the user would have written next.
///
/// Built as a `CopyStmt` and driven through [`crate::copy_from`] rather than as its own path:
/// the tuple descriptor, the permission check and the binary stream are all the same work, and
/// a second way of doing it would be a second place for it to be wrong.
fn load_into(
    p_stmt: &PgBox<PlannedStmt>,
    table: *mut RangeVar,
    provider: Arc<dyn FormatProvider>,
    uri: &str,
    query_env: *mut QueryEnvironment,
) {
    let mut copy_stmt = unsafe { PgBox::<CopyStmt>::alloc_node(NodeTag::T_CopyStmt) };

    copy_stmt.relation = table;
    copy_stmt.is_from = true;
    copy_stmt.filename = uri.as_pg_cstr();

    let mut options = PgList::<DefElem>::new();

    let format = unsafe {
        makeDefElem(
            "format".as_pg_cstr(),
            makeString(provider.name().as_pg_cstr()) as _,
            -1,
        )
    };

    options.push(format);

    copy_stmt.options = options.into_pg();

    // a planned statement of its own, so that nothing of the CREATE TABLE's tree is left
    // pointing at a COPY afterwards
    let mut planned =
        unsafe { PgBox::<PlannedStmt>::from_pg(copyObjectImpl(p_stmt.as_ptr() as _) as _) };

    planned.utilityStmt = copy_stmt.into_pg() as _;

    // the statement text an error inside the COPY points at: there is none the user wrote, so
    // the deparsed node is the honest answer
    let query_string = unsafe { nodeToString(planned.utilityStmt as _) };
    let query_string = unsafe { CStr::from_ptr(query_string) };

    let query_env = unsafe { PgBox::from_pg(query_env) };

    let plan = CopyFromPlan {
        uri: uri.to_string(),
        // `load_from` is a uri option and has no other spelling: there is no `CREATE TABLE ...
        // FROM STDIN` to serve
        endpoint: Endpoint::Uri(uri.to_string()),
        provider,
        options: FormatOptions::new(),
        // the table was just created from this file's own schema, so the columns are in the
        // file's order by construction
        match_by: MatchBy::Position,
    };

    copy_hook::process_copy_from(&planned, query_string, &query_env, plan);
}

/// The uri the statement named, as `load_from` or `definition_from`.
fn uri_option(create_stmt: &PgBox<CreateStmt>) -> String {
    let options = unsafe { PgList::<DefElem>::from_pg(create_stmt.options) };

    for option in options.iter_ptr() {
        let option = unsafe { PgBox::<DefElem>::from_pg(option) };

        let Some(name) = copy_stmt::option_name(&option) else {
            continue;
        };

        if name != "load_from" && name != "definition_from" {
            continue;
        }

        let uri = unsafe { defGetString(option.as_ptr()) };

        return unsafe { CStr::from_ptr(uri) }
            .to_str()
            .unwrap_or_else(|_| uri_not_in_utf8_error(&name))
            .to_string();
    }

    unreachable!("a recognized statement has one of the two options");
}

/// The option arrives in the database's encoding while a uri has to be utf-8, and nothing
/// converts between the two, so a uri that is not ascii is only usable in a utf-8 database.
fn uri_not_in_utf8_error(option_name: &str) -> ! {
    let encoding_name = unsafe { CStr::from_ptr(GetDatabaseEncodingName()) }
        .to_str()
        .expect("a database encoding name is valid utf-8");

    ereport!(
        PgLogLevel::ERROR,
        PgSqlErrorCode::ERRCODE_UNTRANSLATABLE_CHARACTER,
        format!("the \"{option_name}\" option is not valid UTF-8"),
        format!(
            "A uri with non-ascii characters cannot be used in a database with encoding \
             \"{encoding_name}\"."
        )
    );

    unreachable!("ereport at ERROR does not return");
}

/// Takes our reloptions out of the statement, so that what reaches Postgres is a `CREATE TABLE`
/// it recognizes.
fn remove_our_options(create_stmt: &mut PgBox<CreateStmt>) {
    let options = unsafe { PgList::<DefElem>::from_pg(create_stmt.options) };

    let mut kept = PgList::<DefElem>::new();

    for option in options.iter_ptr() {
        let option_box = unsafe { PgBox::<DefElem>::from_pg(option) };

        // a namespaced option is not ours, so Postgres goes on rejecting or using it
        let is_ours = copy_stmt::option_name(&option_box)
            .is_some_and(|name| CREATE_TABLE_OPTIONS.contains(&name.as_str()));

        if !is_ours {
            kept.push(option);
        }
    }

    create_stmt.options = kept.into_pg();
}

/// Whether the table that would be created is already there. `CREATE TABLE IF NOT EXISTS` is a
/// no-op in that case, so neither the inference nor the load should happen.
fn table_already_exists(table: *mut RangeVar) -> bool {
    table_relid(table) != InvalidOid
}

/// Whether the relation holding the table's name was created by this transaction.
///
/// A concurrent session can commit a table of the same name after the check above found none,
/// in which case Postgres skipped the creation and the file must not go into that session's
/// table.
fn table_created_in_this_transaction(table: *mut RangeVar) -> bool {
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
    // the same lookup Postgres does for `IF NOT EXISTS` in DefineRelation
    let namespace_oid = unsafe { RangeVarGetCreationNamespace(table) };

    unsafe { get_relname_relid((*table).relname, namespace_oid) }
}
