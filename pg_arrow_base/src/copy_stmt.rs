//! Reading a `COPY` statement: is this one of ours, where does it point, and which of its
//! options belong to the format rather than to `COPY` itself.
//!
//! Kept apart from the hook because this half is pure inspection — no I/O, no dispatch, no
//! reporting — which is what makes it testable and what makes the hook short enough to read.

use std::ffi::CStr;

use format_registry::options::FormatOptions;
use format_registry::registry;
use pgrx::pg_sys::{
    addRangeTableEntryForRelation, defGetString, get_namespace_name, get_rel_namespace,
    makeDefElem, makeString, make_parsestate, quote_qualified_identifier, AccessShareLock,
    AsPgCStr, CopyStmt, CreateTemplateTupleDesc, DefElem, List, NoLock, Node, NodeTag, Oid,
    ParseNamespaceItem, ParseState, PlannedStmt, QueryEnvironment, RangeVar,
    RangeVarGetRelidExtended, RowExclusiveLock, TupleDescInitEntry,
};
use pgrx::{ereport, is_a, PgBox, PgList, PgLogLevel, PgRelation, PgSqlErrorCode, PgTupleDesc};

use crate::endpoint::Endpoint;
use crate::pg_compat;
use crate::providers;

/// Options `COPY` itself owns on at least one supported version. Everything else in
/// `WITH (...)` is handed to the format, so a provider can add an option without the base
/// needing to know about it — which is the whole point of the registry.
///
/// Erring towards a long list is the safe direction: a core option mistaken for a format
/// option would be rejected by the provider, and a sensible one at that, but a format option
/// mistaken for a core one would be silently dropped.
const CORE_COPY_OPTIONS: &[&str] = &[
    "format",
    "freeze",
    "delimiter",
    "null",
    "default",
    "header",
    "quote",
    "escape",
    "force_quote",
    "force_not_null",
    "force_null",
    "convert_selectively",
    "encoding",
    "on_error",
    "log_verbosity",
    "reject_limit",
];

/// The core options a statement in one of our formats may still carry.
///
/// `format` is what named us, and `freeze` is passed on to core because it is about how the
/// rows are written into the table rather than about how the bytes are read. Every other core
/// option describes a text or csv file — a delimiter, a quote, a header, what to do with a row
/// that will not parse — and a format that is neither has nothing to apply it to. Silently
/// ignoring one would be the dangerous reading: `on_error set_null` changes which rows arrive.
const HONOURED_CORE_COPY_OPTIONS: &[&str] = &["format", "freeze"];

/// Options the *base* owns: not `COPY`'s, and not a format's either, so they are kept out of
/// what a provider is asked to validate.
///
/// Both earn their place by being questions no format has a different answer to. `match_by`
/// decides whether a file's columns line up with a table's by position or by name, which is a
/// question about two schemas rather than about a file. `file_size_bytes` decides when a `COPY
/// TO` starts another file, which is a byte count.
const BASE_COPY_OPTIONS: &[&str] = &["match_by", "file_size_bytes"];

/// A `COPY` in a format this backend can serve, whatever end it names.
pub(crate) struct ArrowCopyStmt {
    pub(crate) endpoint: Endpoint,
    /// The format as the statement named it, if it named one. `None` means it was inferred,
    /// and the inference has already succeeded by the time this exists.
    pub(crate) format: Option<String>,
    pub(crate) options: FormatOptions,
    /// `match_by` as written, unparsed: recognizing a statement must not fail, and a bad value
    /// has to be reported by the validation that comes after.
    pub(crate) match_by: Option<String>,
    /// `file_size_bytes` as written, unparsed, for the same reason as `match_by`.
    pub(crate) file_size_bytes: Option<String>,
    /// The core `COPY` options written that this base cannot honour, reported by validation
    /// rather than here: recognizing a statement must not fail.
    pub(crate) unhonoured_core_options: Vec<String>,
    pub(crate) is_from: bool,
}

fn copy_stmt(p_stmt: &PgBox<PlannedStmt>) -> Option<PgBox<CopyStmt>> {
    if p_stmt.utilityStmt.is_null() {
        return None;
    }

    if unsafe { !is_a(p_stmt.utilityStmt, NodeTag::T_CopyStmt) } {
        return None;
    }

    Some(unsafe { PgBox::<CopyStmt>::from_pg(p_stmt.utilityStmt as _) })
}

/// A `DefElem`'s name, or `None` for a namespaced one: `toast.compression` belongs to that
/// namespace and not to us.
pub(crate) fn option_name(option: &PgBox<DefElem>) -> Option<String> {
    if !option.defnamespace.is_null() || option.defname.is_null() {
        return None;
    }

    unsafe { CStr::from_ptr(option.defname) }
        .to_str()
        .ok()
        .map(|name| name.to_ascii_lowercase())
}

fn option_value(option: &PgBox<DefElem>) -> Option<String> {
    if option.arg.is_null() {
        // `WITH (header)` -- a bare option is Postgres' spelling of true
        return Some("true".to_string());
    }

    let value = unsafe { defGetString(option.as_ptr()) };

    if value.is_null() {
        return None;
    }

    unsafe { CStr::from_ptr(value) }
        .to_str()
        .ok()
        .map(str::to_string)
}

pub(crate) fn get_option(options: *mut List, name: &str) -> Option<String> {
    let options = unsafe { PgList::<DefElem>::from_pg(options) };

    let mut found = None;

    for option in options.iter_ptr() {
        let option = unsafe { PgBox::<DefElem>::from_pg(option) };

        if option_name(&option).is_some_and(|option| option == name) {
            // keep going: a repeated option wins on the right, as it does in core COPY
            found = option_value(&option);
        }
    }

    found
}

/// How many times an option was written. Postgres rejects a duplicate reloption, but it never
/// sees the ones this extension takes out of the list.
pub(crate) fn count_option(options: *mut List, name: &str) -> usize {
    let options = unsafe { PgList::<DefElem>::from_pg(options) };

    options
        .iter_ptr()
        .filter(|option| {
            let option = unsafe { PgBox::<DefElem>::from_pg(*option) };

            option_name(&option).is_some_and(|option| option == name)
        })
        .count()
}

/// Everything in `WITH (...)` that is not a core `COPY` option, in the order it was written.
fn format_options(options: *mut List) -> FormatOptions {
    let options = unsafe { PgList::<DefElem>::from_pg(options) };

    let mut format_options = FormatOptions::new();

    for option in options.iter_ptr() {
        let option = unsafe { PgBox::<DefElem>::from_pg(option) };

        let Some(name) = option_name(&option) else {
            continue;
        };

        if CORE_COPY_OPTIONS.contains(&name.as_str()) || BASE_COPY_OPTIONS.contains(&name.as_str())
        {
            continue;
        }

        if let Some(value) = option_value(&option) {
            format_options.push(name, value);
        }
    }

    format_options
}

/// The core `COPY` options the statement wrote that this base cannot honour, in the order they
/// were written. Names only: the value never gets as far as being parsed.
fn unhonoured_core_options(options: *mut List) -> Vec<String> {
    let options = unsafe { PgList::<DefElem>::from_pg(options) };

    options
        .iter_ptr()
        .filter_map(|option| {
            let option = unsafe { PgBox::<DefElem>::from_pg(option) };

            option_name(&option)
        })
        .filter(|name| {
            CORE_COPY_OPTIONS.contains(&name.as_str())
                && !HONOURED_CORE_COPY_OPTIONS.contains(&name.as_str())
        })
        .collect()
}

/// Whether a statement is a `COPY` in a format this backend can serve, and if so, everything
/// about it the base needs.
///
/// Never errors. A hook has to be able to decide *not* to handle a statement, because the one
/// it declines is almost always somebody else's perfectly good `COPY ... FROM '/tmp/x.csv'`, and
/// it has to decide without a message of its own. The one side effect it allows itself is
/// loading the provider an extension names, because a provider is loaded on demand and an
/// unloaded one is indistinguishable from an absent one.
pub(crate) fn recognize(p_stmt: &PgBox<PlannedStmt>) -> Option<ArrowCopyStmt> {
    let copy_stmt = copy_stmt(p_stmt)?;

    let filename = (!copy_stmt.filename.is_null())
        .then(|| unsafe { CStr::from_ptr(copy_stmt.filename) }.to_str().ok())
        .flatten()
        .map(str::to_string);

    let endpoint = match (copy_stmt.is_program, filename) {
        // a program's command is in the same field a uri would be in
        (true, Some(program)) => Endpoint::Program(program),
        (true, None) => return None,
        (false, Some(uri)) => Endpoint::Uri(uri),
        // STDIN or STDOUT, which core serves perfectly well -- but not in a format core does
        // not have, so one of ours is still ours
        (false, None) => Endpoint::Stdio,
    };

    let format = get_option(copy_stmt.options, "format");

    match &format {
        // an explicitly named format that core does not have is ours even if no provider has
        // registered it, because declining would mean core reporting `COPY format "parquet"
        // not recognized` for a statement whose real problem is that pg_parquet is not
        // installed. Claiming it lets the dispatcher load the provider, and say so clearly if
        // there is none.
        Some(format) => {
            let format = format.to_ascii_lowercase();

            if matches!(format.as_str(), "csv" | "text" | "binary") {
                return None;
            }
        }
        // no format named: ours if the session named a default, else only if the uri's
        // extension says so unambiguously -- which also settles stdio and program, since
        // neither has a uri to assume a format for
        None if matches!(endpoint, Endpoint::Uri(_)) && crate::default_format().is_some() => {}
        None => {
            let path = endpoint.path_for_inference();

            if registry::lookup_by_path(path).is_none() {
                // nothing has claimed the extension, which in a session that has loaded no
                // provider is every statement. Declining here would hand `COPY t TO
                // '/tmp/x.parquet'` to core COPY, which writes *text* under that name, so the
                // installed providers get their chance to register first. A uri with no
                // extension at all is left to the scheme test below, and loads nothing.
                let claimed = registry::inferable_extension(path).is_some()
                    && providers::load_by_path(path).is_some();

                // a uri in a scheme only this stack can reach is ours even when nothing says
                // what format it holds: core COPY opens it as a local file name, and reports
                // `s3://bucket/data` as a relative path, when what is missing is the format
                if !claimed && !names_a_store(path) {
                    return None;
                }
            }
        }
    }

    Some(ArrowCopyStmt {
        endpoint,
        format,
        options: format_options(copy_stmt.options),
        match_by: get_option(copy_stmt.options, "match_by"),
        file_size_bytes: get_option(copy_stmt.options, "file_size_bytes"),
        unhonoured_core_options: unhonoured_core_options(copy_stmt.options),
        is_from: copy_stmt.is_from,
    })
}

/// Whether a uri names a store this stack can reach. A bare path never does: one with nothing
/// to infer a format from is core `COPY`'s, which is what writes a plain `.csv`.
///
/// Asked of tier 1 rather than decided here, so that the set of schemes lives in one place and
/// a uri in a scheme nobody reaches is still declined.
fn names_a_store(path: &str) -> bool {
    path.contains("://") && crate::object_store::recognizes(path)
}

/// Whether the statement names a table rather than wrapping a query.
///
/// `COPY t TO ...` has one; `COPY (SELECT ...) TO ...` does not, and the difference decides
/// both what gets locked and what `pg_stat_progress_copy` has to report against.
pub(crate) fn has_relation(p_stmt: &PgBox<PlannedStmt>) -> bool {
    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(p_stmt.utilityStmt as _) };

    !copy_stmt.relation.is_null()
}

/// The lock core COPY would take on the named relation, by direction.
pub(crate) fn lock_mode(p_stmt: &PgBox<PlannedStmt>) -> i32 {
    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(p_stmt.utilityStmt as _) };

    if copy_stmt.is_from {
        RowExclusiveLock as _
    } else {
        AccessShareLock as _
    }
}

/// The oid of the relation the statement names.
///
/// Resolved twice over, which looks redundant and is not: `RangeVarGetRelidExtended` with
/// `NoLock` finds the oid so that the schema can be named, and opening it by its qualified name
/// under a share lock is what makes the search path resolve to the same relation the planner
/// will later resolve to. Resolving once without a lock would leave a window in which the name
/// came to mean a different table.
pub(crate) fn relation_oid(p_stmt: &PgBox<PlannedStmt>) -> Oid {
    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(p_stmt.utilityStmt as _) };

    let range_var = copy_stmt.relation;

    let unlocked_oid =
        unsafe { RangeVarGetRelidExtended(range_var, NoLock as _, 0, None, std::ptr::null_mut()) };

    let schema_name = unsafe { get_namespace_name(get_rel_namespace(unlocked_oid)) };

    let range_var = unsafe { PgBox::<RangeVar>::from_pg(range_var) };

    let qualified_name = unsafe { quote_qualified_identifier(schema_name, range_var.relname) };

    let qualified_name = unsafe { CStr::from_ptr(qualified_name) }
        .to_str()
        .expect("a quoted qualified name is valid utf-8");

    let relation =
        PgRelation::open_with_name_and_share_lock(qualified_name).unwrap_or_else(|e| panic!("{e}"));

    relation.rd_id
}

/// The columns the statement listed, as core's `CopyGetAttnums` and `BeginCopyFrom` want them:
/// the `CopyStmt`'s own list, or null for all of them.
pub(crate) fn attribute_list(p_stmt: &PgBox<PlannedStmt>) -> *mut List {
    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(p_stmt.utilityStmt as _) };

    copy_stmt.attlist
}

/// A parse state shaped like the one `standard_ProcessUtility` would have built, which is what
/// makes an error inside a `WHERE` clause point at the right part of the statement text.
pub(crate) fn create_parse_state(
    query_string: &CStr,
    query_env: &PgBox<QueryEnvironment>,
) -> PgBox<ParseState> {
    let p_state = unsafe { make_parsestate(std::ptr::null_mut()) };
    let mut p_state = unsafe { PgBox::from_pg(p_state) };

    p_state.p_sourcetext = query_string.as_ptr();
    p_state.p_queryEnv = query_env.as_ptr();

    p_state
}

/// The relation in the parse state's range table, under the lock the direction implies. This is
/// the entry the permission check and the `WHERE` clause are both resolved against.
pub(crate) fn create_namespace_item(
    p_stmt: &PgBox<PlannedStmt>,
    p_state: &PgBox<ParseState>,
    relation: &PgRelation,
) -> PgBox<ParseNamespaceItem> {
    let ns_item = unsafe {
        addRangeTableEntryForRelation(
            p_state.as_ptr(),
            relation.as_ptr(),
            lock_mode(p_stmt),
            std::ptr::null_mut(),
            false,
            false,
        )
    };

    unsafe { PgBox::from_pg(ns_item) }
}

/// The options to pass core, which is the statement's own `freeze` and nothing else of it:
/// whatever the file is, what core reads from our callback is the binary copy format.
pub(crate) fn binary_option_list(p_stmt: &PgBox<PlannedStmt>) -> PgList<DefElem> {
    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(p_stmt.utilityStmt as _) };

    let mut options = PgList::<DefElem>::new();

    let format = unsafe {
        makeDefElem(
            "format".as_pg_cstr(),
            makeString("binary".as_pg_cstr()) as _,
            -1,
        )
    };

    options.push(format);

    // freeze is about how the rows are written, not about how they are read, so it is the one
    // core option that still means something here
    let statement_options = unsafe { PgList::<DefElem>::from_pg(copy_stmt.options) };

    for option in statement_options.iter_ptr() {
        let option_box = unsafe { PgBox::<DefElem>::from_pg(option) };

        if option_name(&option_box).is_some_and(|name| name == "freeze") {
            options.push(option);
        }
    }

    options
}

/// The tuple descriptor of the rows being read: the relation's, with the dropped attributes
/// removed and the rest restricted to the columns the statement listed, in the order it listed
/// them.
///
/// Both parts matter to the file side. A dropped column is not in the file, and a column list
/// is how a file with fewer columns than the table is read at all — so this, and not the
/// relation's descriptor, is what the Arrow schema is built from.
pub(crate) fn create_filtered_tupledesc_for_relation<'a>(
    p_stmt: &PgBox<PlannedStmt>,
    relation: &'a PgRelation,
) -> PgTupleDesc<'a> {
    create_filtered_tupledesc_for_attribute_names(&attribute_names(p_stmt), relation)
}

/// The same, for a caller that has the column names but not the statement they came from: a
/// parallel `COPY FROM` worker is handed the names through shared memory.
pub(crate) fn create_filtered_tupledesc_for_attribute_names<'a>(
    attribute_names: &[String],
    relation: &'a PgRelation,
) -> PgTupleDesc<'a> {
    let relation_tupledesc = relation.tuple_desc();

    if attribute_names.is_empty() {
        return relation_tupledesc;
    }

    let filtered_tupledesc = unsafe { CreateTemplateTupleDesc(attribute_names.len() as i32) };
    let filtered_tupledesc = unsafe { PgTupleDesc::from_pg(filtered_tupledesc) };

    for (attribute_number, attribute_name) in (1..).zip(attribute_names.iter()) {
        let found = relation_tupledesc
            .iter()
            .filter(|attribute| !attribute.is_dropped())
            .find(|attribute| attribute.name() == attribute_name);

        let Some(attribute) = found else {
            ereport!(
                PgLogLevel::ERROR,
                PgSqlErrorCode::ERRCODE_UNDEFINED_COLUMN,
                format!(
                    "column \"{attribute_name}\" of relation \"{}\" does not exist",
                    relation.name()
                )
            );

            unreachable!("ereport at ERROR does not return");
        };

        unsafe {
            TupleDescInitEntry(
                filtered_tupledesc.as_ptr(),
                attribute_number,
                attribute_name.as_pg_cstr() as _,
                attribute.type_oid().value(),
                attribute.type_mod(),
                attribute.attndims as _,
            )
        };
    }

    filtered_tupledesc
}

pub(crate) fn attribute_names(p_stmt: &PgBox<PlannedStmt>) -> Vec<String> {
    let names = unsafe { PgList::<Node>::from_pg(attribute_list(p_stmt)) };

    names.iter_ptr().map(pg_compat::strVal).collect()
}
