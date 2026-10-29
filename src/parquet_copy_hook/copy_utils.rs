use std::{ffi::CStr, str::FromStr};

use parquet::file::properties::DEFAULT_BLOOM_FILTER_FPP;
use pgrx::{
    ereport,
    ffi::c_char,
    is_a,
    pg_sys::{
        addRangeTableEntryForRelation, defGetInt32, defGetInt64, defGetNumeric, defGetString,
        get_namespace_name, get_rel_namespace, makeDefElem, makeString, make_parsestate,
        quote_qualified_identifier, AccessShareLock, AsPgCStr, CopyStmt, CreateTemplateTupleDesc,
        DefElem, List, NoLock, Node, NodeTag::T_CopyStmt, Oid, ParseNamespaceItem, ParseState,
        PlannedStmt, QueryEnvironment, RangeVar, RangeVarGetRelidExtended, RowExclusiveLock,
        TupleDescInitEntry,
    },
    PgBox, PgList, PgLogLevel, PgRelation, PgSqlErrorCode, PgTupleDesc,
};
use url::Url;

use crate::{
    arrow_parquet::{
        column_selection::{ColumnSelection, DEFAULT_BLOOM_FILTER, DEFAULT_DICTIONARY},
        compression::{all_supported_compressions, PgParquetCompression},
        field_ids,
        match_by::MatchBy,
        parquet_version::ParquetVersion,
        parquet_writer::{DEFAULT_ROW_GROUP_SIZE, DEFAULT_ROW_GROUP_SIZE_BYTES},
        uri_utils::ParsedUriInfo,
    },
    pgrx_utils::extension_exists,
};

use self::field_ids::FieldIds;

// A bloom filter is sized from the false positive probability it is asked for, and below this
// one even a single value needs more than the 128MiB that parquet allows a filter per column,
// so the filter written would be less precise than the one asked for.
const MIN_BLOOM_FILTER_FPP: f64 = 1e-62;

use super::{
    copy_to_split_dest_receiver::INVALID_FILE_SIZE_BYTES, hook::ENABLE_PARQUET_COPY_HOOK,
    pg_compat::strVal,
};

pub(crate) fn validate_copy_to_options(p_stmt: &PgBox<PlannedStmt>, uri_info: &ParsedUriInfo) {
    validate_copy_option_names(
        p_stmt,
        &[
            "format",
            "file_size_bytes",
            "field_ids",
            "row_group_size",
            "row_group_size_bytes",
            "compression",
            "compression_level",
            "parquet_version",
            "bloom_filter",
            "bloom_filter_fpp",
            "dictionary",
            "freeze",
        ],
    );

    let format_option = copy_stmt_get_option(p_stmt, "format");

    if !format_option.is_null() {
        let format = unsafe { defGetString(format_option.as_ptr()) };

        let format = unsafe {
            CStr::from_ptr(format)
                .to_str()
                .expect("format option is not a valid CString")
        };

        if format != "parquet" {
            panic!("{format} is not a valid format. Only parquet format is supported.");
        }
    }

    let file_size_bytes_option = copy_stmt_get_option(p_stmt, "file_size_bytes");

    if !file_size_bytes_option.is_null() {
        let file_size_bytes = unsafe { defGetString(file_size_bytes_option.as_ptr()) };

        let file_size_bytes = unsafe {
            CStr::from_ptr(file_size_bytes)
                .to_str()
                .expect("file_size_bytes option is not a valid CString")
        };

        parse_file_size(file_size_bytes)
            .unwrap_or_else(|e| panic!("file_size_bytes option is not valid: {e}"));

        // the size cuts the rows into the files of a folder, so the destination has to be a uri
        // the copy can create files under. Neither stdout nor a program is one.
        if copy_stmt_is_std_inout(p_stmt) {
            panic!("file_size_bytes is not supported for COPY TO STDOUT.");
        }

        if copy_stmt_program(p_stmt).is_some() {
            panic!("file_size_bytes is not supported for COPY TO PROGRAM.");
        }
    } else {
        // a copy without file_size_bytes writes the single object its uri names, so the uri
        // has to name one. The destination of a folder copy is the folder rather than an
        // object in it, and the whole of a store is a folder a copy can fill.
        uri_info
            .ensure_names_an_object()
            .unwrap_or_else(|e| panic!("{}", e));
    }

    let field_ids_option = copy_stmt_get_option(p_stmt, "field_ids");

    if !field_ids_option.is_null() {
        let field_ids = unsafe { defGetString(field_ids_option.as_ptr()) };

        let field_ids = unsafe {
            CStr::from_ptr(field_ids)
                .to_str()
                .expect("field_ids option is not a valid CString")
        };

        if let Err(e) = FieldIds::from_str(field_ids) {
            ereport!(
                pgrx::PgLogLevel::ERROR,
                pgrx::PgSqlErrorCode::ERRCODE_INVALID_JSON_TEXT,
                e,
                "Allowed options are: none, auto, or a JSON object with field names as keys and field ids as values.",
            );
        }
    }

    let row_group_size_option = copy_stmt_get_option(p_stmt, "row_group_size");

    if !row_group_size_option.is_null() {
        let row_group_size = unsafe { defGetInt64(row_group_size_option.as_ptr()) };

        if row_group_size <= 0 {
            panic!("row_group_size must be greater than 0");
        }
    }

    let row_group_size_bytes_option = copy_stmt_get_option(p_stmt, "row_group_size_bytes");

    if !row_group_size_bytes_option.is_null() {
        let row_group_size_bytes = unsafe { defGetInt64(row_group_size_bytes_option.as_ptr()) };

        if row_group_size_bytes <= 0 {
            panic!("row_group_size_bytes must be greater than 0");
        }
    }

    let compression_option = copy_stmt_get_option(p_stmt, "compression");

    if !compression_option.is_null() {
        let compression = unsafe { defGetString(compression_option.as_ptr()) };

        let compression = unsafe {
            CStr::from_ptr(compression)
                .to_str()
                .expect("compression option is not a valid CString")
        };

        if PgParquetCompression::from_str(compression).is_err() {
            panic!(
                "{} is not a valid compression format. Supported compression formats are {}",
                compression,
                all_supported_compressions()
                    .into_iter()
                    .map(|c| c.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
    }

    let compression_level_option = copy_stmt_get_option(p_stmt, "compression_level");

    if !compression_level_option.is_null() {
        let compression_level = unsafe { defGetInt32(compression_level_option.as_ptr()) };

        let compression = copy_to_stmt_compression(p_stmt, uri_info);

        compression.ensure_compression_level(compression_level);
    }

    let parquet_version_option = copy_stmt_get_option(p_stmt, "parquet_version");

    if !parquet_version_option.is_null() {
        let parquet_version = unsafe { defGetString(parquet_version_option.as_ptr()) };

        let parquet_version = unsafe {
            CStr::from_ptr(parquet_version)
                .to_str()
                .expect("parquet_version option is not a valid CString")
        };

        ParquetVersion::from_str(parquet_version).unwrap_or_else(|e| panic!("{}", e));
    }

    validate_column_selection_option(p_stmt, "bloom_filter");

    validate_column_selection_option(p_stmt, "dictionary");

    let bloom_filter_fpp_option = copy_stmt_get_option(p_stmt, "bloom_filter_fpp");

    if !bloom_filter_fpp_option.is_null() {
        let bloom_filter_fpp = unsafe { defGetNumeric(bloom_filter_fpp_option.as_ptr()) };

        if bloom_filter_fpp <= 0. || bloom_filter_fpp >= 1. {
            panic!("bloom_filter_fpp must be greater than 0 and less than 1");
        }

        if bloom_filter_fpp < MIN_BLOOM_FILTER_FPP {
            panic!(
                "bloom_filter_fpp must be greater than or equal to {MIN_BLOOM_FILTER_FPP:e}, \
                 below which a bloom filter does not fit the size limit of a column"
            );
        }
    }
}

fn validate_column_selection_option(p_stmt: &PgBox<PlannedStmt>, option_name: &str) {
    let column_selection_option = copy_stmt_get_option(p_stmt, option_name);

    if column_selection_option.is_null() {
        return;
    }

    let column_selection = unsafe { defGetString(column_selection_option.as_ptr()) };

    let column_selection = unsafe {
        CStr::from_ptr(column_selection)
            .to_str()
            .unwrap_or_else(|_| panic!("{option_name} option is not a valid CString"))
    };

    if let Err(e) = ColumnSelection::from_str(column_selection) {
        ereport!(
            pgrx::PgLogLevel::ERROR,
            pgrx::PgSqlErrorCode::ERRCODE_INVALID_JSON_TEXT,
            format!("invalid value for \"{option_name}\": {e}"),
        );
    }
}

pub(crate) fn validate_copy_from_options(p_stmt: &PgBox<PlannedStmt>) {
    validate_copy_option_names(p_stmt, &["format", "match_by", "freeze"]);

    let format_option = copy_stmt_get_option(p_stmt, "format");

    if !format_option.is_null() {
        let format = unsafe { defGetString(format_option.as_ptr()) };

        let format = unsafe {
            CStr::from_ptr(format)
                .to_str()
                .expect("format option is not a valid CString")
        };

        if format != "parquet" {
            panic!("{format} is not a valid format. Only parquet format is supported.");
        }
    }
}

fn validate_copy_option_names(p_stmt: &PgBox<PlannedStmt>, allowed_options: &[&str]) {
    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(p_stmt.utilityStmt as _) };

    let copy_options = unsafe { PgList::<DefElem>::from_pg(copy_stmt.options) };

    let copy_from_str = if copy_stmt.is_from { "from" } else { "to" };

    for option in copy_options.iter_ptr() {
        let option = unsafe { PgBox::<DefElem>::from_pg(option) };

        let option_name = unsafe {
            CStr::from_ptr(option.defname)
                .to_str()
                .expect("option name is not a valid CString")
        };

        if !allowed_options.contains(&option_name) {
            panic!(
                "{} is not a valid option for \"copy {} parquet\". Supported options are {}",
                option_name,
                copy_from_str,
                allowed_options.join(", ")
            );
        }
    }
}

pub(crate) fn copy_stmt_uri(p_stmt: &PgBox<PlannedStmt>) -> Result<ParsedUriInfo, String> {
    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(p_stmt.utilityStmt as _) };

    if copy_stmt.is_program {
        return Ok(ParsedUriInfo::for_program());
    }

    if copy_stmt.filename.is_null() {
        return Ok(ParsedUriInfo::for_std_inout());
    }

    let uri = unsafe {
        CStr::from_ptr(copy_stmt.filename)
            .to_str()
            .expect("uri option is not a valid CString")
    };

    ParsedUriInfo::try_from(uri)
}

pub(crate) fn copy_stmt_program(p_stmt: &PgBox<PlannedStmt>) -> Option<String> {
    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(p_stmt.utilityStmt as _) };

    if copy_stmt.is_program {
        let program = unsafe {
            CStr::from_ptr(copy_stmt.filename)
                .to_str()
                .expect("program option is not a valid CString")
        };

        Some(program.to_string())
    } else {
        None
    }
}

pub(crate) fn copy_stmt_is_std_inout(p_stmt: &PgBox<PlannedStmt>) -> bool {
    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(p_stmt.utilityStmt as _) };

    copy_stmt.filename.is_null() && !copy_stmt.is_program
}

pub(crate) fn copy_to_stmt_file_size_bytes(p_stmt: &PgBox<PlannedStmt>) -> i64 {
    let file_size_bytes_option = copy_stmt_get_option(p_stmt, "file_size_bytes");

    if file_size_bytes_option.is_null() {
        INVALID_FILE_SIZE_BYTES
    } else {
        let file_size_bytes = unsafe { defGetString(file_size_bytes_option.as_ptr()) };

        let file_size_bytes = unsafe {
            CStr::from_ptr(file_size_bytes)
                .to_str()
                .expect("file_size_bytes option is not a valid CString")
        };

        parse_file_size(file_size_bytes)
            .unwrap_or_else(|e| panic!("file_size_bytes option is not valid: {e}"))
    }
}

pub(crate) fn copy_to_stmt_field_ids(p_stmt: &PgBox<PlannedStmt>) -> *const c_char {
    let field_ids_option = copy_stmt_get_option(p_stmt, "field_ids");

    if field_ids_option.is_null() {
        FieldIds::default().to_string().as_pg_cstr()
    } else {
        unsafe { defGetString(field_ids_option.as_ptr()) }
    }
}

pub(crate) fn copy_to_stmt_row_group_size(p_stmt: &PgBox<PlannedStmt>) -> i64 {
    let row_group_size_option = copy_stmt_get_option(p_stmt, "row_group_size");

    if row_group_size_option.is_null() {
        DEFAULT_ROW_GROUP_SIZE
    } else {
        unsafe { defGetInt64(row_group_size_option.as_ptr()) }
    }
}

// the row group size the command asked for, which is None when it asked for none and
// DEFAULT_ROW_GROUP_SIZE is what the copy writes
pub(crate) fn copy_to_stmt_explicit_row_group_size(p_stmt: &PgBox<PlannedStmt>) -> Option<i64> {
    let row_group_size_option = copy_stmt_get_option(p_stmt, "row_group_size");

    if row_group_size_option.is_null() {
        None
    } else {
        Some(unsafe { defGetInt64(row_group_size_option.as_ptr()) })
    }
}

pub(crate) fn copy_to_stmt_row_group_size_bytes(p_stmt: &PgBox<PlannedStmt>) -> i64 {
    let row_group_size_bytes_option = copy_stmt_get_option(p_stmt, "row_group_size_bytes");

    if row_group_size_bytes_option.is_null() {
        DEFAULT_ROW_GROUP_SIZE_BYTES
    } else {
        unsafe { defGetInt64(row_group_size_bytes_option.as_ptr()) }
    }
}

pub(crate) fn copy_to_stmt_compression(
    p_stmt: &PgBox<PlannedStmt>,
    uri_info: &ParsedUriInfo,
) -> PgParquetCompression {
    let compression_option = copy_stmt_get_option(p_stmt, "compression");

    if compression_option.is_null() {
        PgParquetCompression::try_from(uri_info.uri.clone()).unwrap_or_default()
    } else {
        let compression = unsafe { defGetString(compression_option.as_ptr()) };

        let compression = unsafe {
            CStr::from_ptr(compression)
                .to_str()
                .expect("compression option is not a valid CString")
        };

        PgParquetCompression::from_str(compression).unwrap_or_else(|e| panic!("{}", e))
    }
}

pub(crate) fn copy_to_stmt_compression_level(
    p_stmt: &PgBox<PlannedStmt>,
    uri_info: &ParsedUriInfo,
) -> Option<i32> {
    let compression_level_option = copy_stmt_get_option(p_stmt, "compression_level");

    if compression_level_option.is_null() {
        let compression = copy_to_stmt_compression(p_stmt, uri_info);

        compression.default_compression_level()
    } else {
        Some(unsafe { defGetInt32(compression_level_option.as_ptr()) as _ })
    }
}

pub(crate) fn copy_to_stmt_parquet_version(p_stmt: &PgBox<PlannedStmt>) -> ParquetVersion {
    let parquet_version_option = copy_stmt_get_option(p_stmt, "parquet_version");

    if parquet_version_option.is_null() {
        ParquetVersion::default()
    } else {
        let parquet_version = unsafe { defGetString(parquet_version_option.as_ptr()) };
        let parquet_version = unsafe {
            CStr::from_ptr(parquet_version)
                .to_str()
                .expect("parquet_version option is not a valid CString")
        };

        ParquetVersion::from_str(parquet_version).unwrap_or_else(|e| panic!("{}", e))
    }
}

pub(crate) fn copy_to_stmt_bloom_filter(p_stmt: &PgBox<PlannedStmt>) -> *const c_char {
    copy_to_stmt_column_selection(p_stmt, "bloom_filter", DEFAULT_BLOOM_FILTER)
}

pub(crate) fn copy_to_stmt_dictionary(p_stmt: &PgBox<PlannedStmt>) -> *const c_char {
    copy_to_stmt_column_selection(p_stmt, "dictionary", DEFAULT_DICTIONARY)
}

fn copy_to_stmt_column_selection(
    p_stmt: &PgBox<PlannedStmt>,
    option_name: &str,
    default_column_selection: ColumnSelection,
) -> *const c_char {
    let column_selection_option = copy_stmt_get_option(p_stmt, option_name);

    if column_selection_option.is_null() {
        default_column_selection.to_string().as_pg_cstr()
    } else {
        unsafe { defGetString(column_selection_option.as_ptr()) }
    }
}

pub(crate) fn copy_to_stmt_bloom_filter_fpp(p_stmt: &PgBox<PlannedStmt>) -> f64 {
    let bloom_filter_fpp_option = copy_stmt_get_option(p_stmt, "bloom_filter_fpp");

    if bloom_filter_fpp_option.is_null() {
        DEFAULT_BLOOM_FILTER_FPP
    } else {
        unsafe { defGetNumeric(bloom_filter_fpp_option.as_ptr()) }
    }
}

pub(crate) fn copy_from_stmt_create_option_list(p_stmt: &PgBox<PlannedStmt>) -> PgList<DefElem> {
    let mut new_copy_options = PgList::<DefElem>::new();

    // ensure binary format
    let format_option_name = "format".as_pg_cstr();

    let format_option_val = unsafe { makeString("binary".as_pg_cstr()) } as _;

    let format_option = unsafe { makeDefElem(format_option_name, format_option_val, -1) };

    new_copy_options.push(format_option);

    // add freeze option if it is  present in original statement
    let freeze_option = copy_stmt_get_option(p_stmt, "freeze");

    if !freeze_option.is_null() {
        new_copy_options.push(freeze_option.as_ptr());
    }

    new_copy_options
}

pub(crate) fn copy_from_stmt_match_by(p_stmt: &PgBox<PlannedStmt>) -> MatchBy {
    let match_by_option = copy_stmt_get_option(p_stmt, "match_by");

    if match_by_option.is_null() {
        MatchBy::default()
    } else {
        let match_by = unsafe { defGetString(match_by_option.as_ptr()) };

        let match_by = unsafe {
            CStr::from_ptr(match_by)
                .to_str()
                .expect("match_by option is not a valid CString")
        };

        MatchBy::from_str(match_by).unwrap_or_else(|e| panic!("{}", e))
    }
}

fn copy_stmt_get_option(p_stmt: &PgBox<PlannedStmt>, option_name: &str) -> PgBox<DefElem> {
    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(p_stmt.utilityStmt as _) };
    get_option(copy_stmt.options, option_name)
}

pub(crate) fn has_option(options: *mut List, option_name: &str) -> bool {
    count_option(options, option_name) > 0
}

pub(crate) fn count_option(options: *mut List, option_name: &str) -> usize {
    let options = unsafe { PgList::<DefElem>::from_pg(options) };

    options
        .iter_ptr()
        .filter(|option| {
            let option = unsafe { PgBox::<DefElem>::from_pg(*option) };
            option_matches(&option, option_name)
        })
        .count()
}

// option_matches returns true if the option is the given one. A namespaced option,
// e.g. "toast.load_from", belongs to that namespace and not to pg_parquet.
fn option_matches(option: &PgBox<DefElem>, option_name: &str) -> bool {
    if !option.defnamespace.is_null() {
        return false;
    }

    let current_option_name = unsafe {
        CStr::from_ptr(option.defname)
            .to_str()
            .expect("option name is not a valid CString")
    };

    current_option_name == option_name
}

pub(crate) fn get_option(options: *mut List, option_name: &str) -> PgBox<DefElem> {
    let options = unsafe { PgList::<DefElem>::from_pg(options) };

    for current_option in options.iter_ptr() {
        let current_option = unsafe { PgBox::<DefElem>::from_pg(current_option) };

        if option_matches(&current_option, option_name) {
            return current_option;
        }
    }

    PgBox::null()
}

fn is_copy_parquet_stmt(p_stmt: &PgBox<PlannedStmt>, copy_from: bool) -> bool {
    // the GUC pg_parquet.enable_copy_hook must be set to true
    if !ENABLE_PARQUET_COPY_HOOK.get() {
        return false;
    }

    let is_copy_stmt = unsafe { is_a(p_stmt.utilityStmt, T_CopyStmt) };

    if !is_copy_stmt {
        return false;
    }

    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(p_stmt.utilityStmt as _) };

    if copy_from != copy_stmt.is_from {
        return false;
    }

    let uri_info = copy_stmt_uri(p_stmt);
    let has_parquet_format = has_parquet_format_option(p_stmt);

    match &uri_info {
        // only parquet format is supported
        Ok(uri_info) => {
            if !has_parquet_format && !is_parquet_uri(uri_info.uri.clone()) {
                return false;
            }
        }
        // a uri pg_parquet cannot handle is left to postgres, unless the copy asked for the
        // parquet format, which is what makes the uri pg_parquet's to complain about below
        Err(_) => {
            if !has_parquet_format {
                return false;
            }
        }
    }

    // extension checks are done via catalog (not yet searched via cache by postgres till pg18)
    // this is why we check them after the uri checks

    // crunchy_query_engine should not be created
    if extension_exists("crunchy_query_engine") {
        return false;
    }

    // pg_parquet should be created
    if !extension_exists("pg_parquet") {
        ereport!(
            PgLogLevel::WARNING,
            PgSqlErrorCode::ERRCODE_WARNING,
            "pg_parquet can handle this COPY command but is not enabled",
            "Run CREATE EXTENSION pg_parquet; to enable the pg_parquet extension.",
        );

        return false;
    }

    // the copy asked for the parquet format, so pg_parquet is the one that tells it what is
    // wrong with its uri. Postgres would only report that it has no format named parquet, which
    // says nothing about the uri the copy could not be run with.
    if let Err(uri_error) = uri_info {
        ereport!(
            PgLogLevel::ERROR,
            PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
            uri_error
        );
    }

    true
}

pub(crate) fn is_copy_to_parquet_stmt(p_stmt: &PgBox<PlannedStmt>) -> bool {
    let copy_from = false;
    is_copy_parquet_stmt(p_stmt, copy_from)
}

pub(crate) fn is_copy_from_parquet_stmt(p_stmt: &PgBox<PlannedStmt>) -> bool {
    let copy_from = true;
    is_copy_parquet_stmt(p_stmt, copy_from)
}

fn has_parquet_format_option(p_stmt: &PgBox<PlannedStmt>) -> bool {
    let format_option = copy_stmt_get_option(p_stmt, "format");

    if format_option.is_null() {
        return false;
    }

    let format = unsafe { defGetString(format_option.as_ptr()) };

    let format = unsafe {
        CStr::from_ptr(format)
            .to_str()
            .unwrap_or_else(|e| panic!("format option is not a valid CString: {e}"))
    };

    format == "parquet"
}

fn is_parquet_uri(uri: Url) -> bool {
    PgParquetCompression::try_from(uri).is_ok()
}

pub(crate) fn copy_stmt_has_relation(p_stmt: &PgBox<PlannedStmt>) -> bool {
    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(p_stmt.utilityStmt as _) };

    !copy_stmt.relation.is_null()
}

pub(crate) fn copy_stmt_lock_mode(p_stmt: &PgBox<PlannedStmt>) -> i32 {
    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(p_stmt.utilityStmt as _) };

    if copy_stmt.is_from {
        RowExclusiveLock as _
    } else {
        AccessShareLock as _
    }
}

pub(crate) fn copy_stmt_relation_oid(p_stmt: &PgBox<PlannedStmt>) -> Oid {
    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(p_stmt.utilityStmt as _) };

    let copy_stmt_range_var = copy_stmt.relation;

    let relation_oid = unsafe {
        RangeVarGetRelidExtended(
            copy_stmt_range_var,
            NoLock as _,
            0,
            None,
            std::ptr::null_mut(),
        )
    };

    let schema_oid = unsafe { get_rel_namespace(relation_oid) };

    let schema_name = unsafe { get_namespace_name(schema_oid) };

    let copy_stmt_range_var = unsafe { PgBox::<RangeVar>::from_pg(copy_stmt_range_var) };

    let qualified_relname =
        unsafe { quote_qualified_identifier(schema_name, copy_stmt_range_var.relname) };

    let qualified_relname = unsafe {
        CStr::from_ptr(qualified_relname)
            .to_str()
            .expect("qualified_relname is not a valid CString")
    };

    let copy_stmt_relation = PgRelation::open_with_name_and_share_lock(qualified_relname)
        .unwrap_or_else(|e| panic!("{}", e));

    copy_stmt_relation.rd_id
}

pub(crate) fn copy_stmt_create_parse_state(
    query_string: &CStr,
    query_env: &PgBox<QueryEnvironment>,
) -> PgBox<ParseState> {
    /* construct a parse state similar to standard_ProcessUtility */
    let p_state: *mut ParseState = unsafe { make_parsestate(std::ptr::null_mut()) };
    let mut p_state = unsafe { PgBox::from_pg(p_state) };
    p_state.p_sourcetext = query_string.as_ptr();
    p_state.p_queryEnv = query_env.as_ptr();
    p_state
}

pub(crate) fn copy_stmt_create_namespace_item(
    p_stmt: &PgBox<PlannedStmt>,
    p_state: &PgBox<ParseState>,
    relation: &PgRelation,
) -> PgBox<ParseNamespaceItem> {
    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(p_stmt.utilityStmt as _) };

    let ns_item = unsafe {
        addRangeTableEntryForRelation(
            p_state.as_ptr(),
            relation.as_ptr(),
            if copy_stmt.is_from {
                RowExclusiveLock
            } else {
                AccessShareLock
            } as _,
            std::ptr::null_mut(),
            false,
            false,
        )
    };
    unsafe { PgBox::from_pg(ns_item) }
}

pub(crate) fn copy_stmt_attribute_names(p_stmt: &PgBox<PlannedStmt>) -> Vec<String> {
    let attribute_name_list = copy_stmt_attribute_list(p_stmt);

    unsafe {
        PgList::from_pg(attribute_name_list)
            .iter_ptr()
            .map(|attribute_name: *mut Node| strVal(attribute_name))
            .collect::<Vec<_>>()
    }
}

pub(crate) fn copy_stmt_attribute_list(p_stmt: &PgBox<PlannedStmt>) -> *mut List {
    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(p_stmt.utilityStmt as _) };
    copy_stmt.attlist
}

// create_filtered_tupledesc_for_relation creates a new tuple descriptor for the COPY operation by
// removing dropped attributes and filtering the attributes based on the attribute name list.
pub(crate) fn create_filtered_tupledesc_for_relation<'a>(
    p_stmt: &PgBox<PlannedStmt>,
    relation: &'a PgRelation,
) -> PgTupleDesc<'a> {
    create_filtered_tupledesc_for_attribute_names(&copy_stmt_attribute_names(p_stmt), relation)
}

// create_filtered_tupledesc_for_attribute_names is create_filtered_tupledesc_for_relation for a
// caller that already has the attribute names, such as a parallel COPY FROM worker, which is
// handed the names rather than the statement they came from.
pub(crate) fn create_filtered_tupledesc_for_attribute_names<'a>(
    attribute_name_list: &[String],
    relation: &'a PgRelation,
) -> PgTupleDesc<'a> {
    let relation_tupledesc = relation.tuple_desc();

    if attribute_name_list.is_empty() {
        return relation_tupledesc;
    }

    let filtered_tupledesc = unsafe { CreateTemplateTupleDesc(attribute_name_list.len() as i32) };
    let filtered_tupledesc = unsafe { PgTupleDesc::from_pg(filtered_tupledesc) };

    let mut attribute_number = 1;

    for attribute_name in attribute_name_list.iter() {
        let mut found = false;

        for attribute in relation_tupledesc.iter() {
            if attribute.is_dropped() {
                continue;
            }

            let att_typoid = attribute.type_oid().value();
            let att_typmod = attribute.type_mod();
            let att_ndims = attribute.attndims;

            if attribute.name() == attribute_name {
                unsafe {
                    TupleDescInitEntry(
                        filtered_tupledesc.as_ptr(),
                        attribute_number,
                        attribute_name.as_pg_cstr() as _,
                        att_typoid,
                        att_typmod,
                        att_ndims as _,
                    )
                };

                attribute_number += 1;

                found = true;

                break;
            }
        }

        if !found {
            panic!(
                "column \"{}\" of relation \"{}\" does not exist",
                attribute_name,
                relation.name()
            );
        }
    }

    filtered_tupledesc
}

/// Parses a size string like "1MB", "512KB", or just "1000000" into a byte count.
/// Enforces a minimum of 1MB and a maximum of i64::MAX, which is what the copy holds the
/// size in.
fn parse_file_size(size_str: &str) -> Result<i64, String> {
    // Normalize casing and trim whitespace
    let size_str = size_str.trim().to_uppercase();

    // Find the first non-digit character
    let mut idx = 0;
    for c in size_str.chars() {
        if !c.is_ascii_digit() {
            break;
        }
        idx += 1;
    }

    // If there's no numeric portion, return an error
    if idx == 0 {
        return Err(format!("No numeric value found in '{size_str}'"));
    }

    // Split into numeric part and (optional) unit
    let num_part = &size_str[..idx];
    let unit_part = size_str[idx..].trim();

    // Convert the numeric portion. The size is a count of bytes the copy compares the size of
    // the file it is writing against, which it holds in an i64, so a count that does not fit
    // one is out of range rather than something to hand on.
    let bytes = num_part
        .parse::<i64>()
        .map_err(|_| error_file_size_out_of_range(&size_str))?;

    // Interpret the suffix, if present
    let multiplier: i64 = match unit_part {
        "" => 1, // no suffix: treat as bytes
        "KB" => 1_024,
        "MB" => 1_024 * 1_024,
        "GB" => 1_024 * 1_024 * 1_024,
        _ => {
            return Err(format!(
                "Unrecognized unit '{unit_part}'. Allowed units are KB, MB or GB."
            ))
        }
    };

    let bytes = bytes
        .checked_mul(multiplier)
        .ok_or_else(|| error_file_size_out_of_range(&size_str))?;

    // Enforce a minimum of 1MB
    if bytes < 1_024 * 1_024 {
        return Err(format!("Minimum allowed size is 1MB. Got {bytes} bytes."));
    }

    Ok(bytes)
}

fn error_file_size_out_of_range(size_str: &str) -> String {
    format!(
        "'{size_str}' is out of range. Maximum allowed size is {} bytes.",
        i64::MAX
    )
}
