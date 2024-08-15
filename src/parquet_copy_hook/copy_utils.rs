use std::str::FromStr;

use pgrx::{
    is_a,
    pg_sys::{
        self, defGetInt64, defGetString, AccessShareLock, Alias, CopyStmt, DefElem, DestReceiver,
        NodeTag::T_CopyStmt, Oid, ParamListInfo, ParseExprKind, ParseNamespaceItem, ParseState,
        Portal, QueryCompletion, Relation, RowExclusiveLock, Snapshot, TupleTableSlot,
    },
    void_mut_ptr, PgBox, PgList, PgRelation,
};

use crate::arrow_parquet::codec::{all_supported_codecs, FromPath, ParquetCodecOption};

// same as DuckDB
pub(crate) const DEFAULT_ROW_GROUP_SIZE: i64 = 122880;

pub(crate) fn is_parquet_format(copy_stmt: &PgBox<CopyStmt>) -> bool {
    let copy_options = unsafe { PgList::<DefElem>::from_pg(copy_stmt.options) };
    for option in copy_options.iter_ptr() {
        let option = unsafe { PgBox::<DefElem>::from_pg(option) };
        let key = unsafe { std::ffi::CStr::from_ptr(option.defname).to_str().unwrap() };
        if key != "format" {
            continue;
        }

        let format = unsafe { defGetString(option.as_ptr()) };
        let format = unsafe { std::ffi::CStr::from_ptr(format).to_str().unwrap() };
        return format == "parquet";
    }

    false
}

pub(crate) fn is_parquet_file(copy_stmt: &PgBox<CopyStmt>) -> bool {
    let filename = unsafe {
        std::ffi::CStr::from_ptr(copy_stmt.filename)
            .to_str()
            .unwrap()
    };
    <ParquetCodecOption as FromPath>::try_from_path(filename).is_ok()
}

pub(crate) fn validate_copy_to_options(pstmt: &PgBox<pg_sys::PlannedStmt>) {
    let allowed_options = ["format", "row_group_size", "codec"];

    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(pstmt.utilityStmt as _) };
    let copy_options = unsafe { PgList::<DefElem>::from_pg(copy_stmt.options) };

    for option in copy_options.iter_ptr() {
        let option = unsafe { PgBox::<DefElem>::from_pg(option) };
        let key = unsafe { std::ffi::CStr::from_ptr(option.defname).to_str().unwrap() };
        if !allowed_options.contains(&key) {
            panic!("{} is not a valid option for COPY TO PARQUET", key);
        }

        if key == "format" {
            let format = unsafe { defGetString(option.as_ptr()) };
            let format = unsafe { std::ffi::CStr::from_ptr(format).to_str().unwrap() };
            if format != "parquet" {
                panic!(
                    "{} is not a valid format. Only parquet format is supported.",
                    format
                );
            }
        }

        if key == "row_group_size" {
            let row_group_size = unsafe { defGetInt64(option.as_ptr()) };
            if row_group_size <= 0 {
                panic!("row_group_size must be greater than 0");
            }
        }

        if key == "codec" {
            let codec = unsafe { defGetString(option.as_ptr()) };
            let codec = unsafe { std::ffi::CStr::from_ptr(codec).to_str().unwrap() };
            if ParquetCodecOption::from_str(codec).is_err() {
                panic!(
                    "{} is not a valid codec. Supported codecs are {}",
                    codec,
                    all_supported_codecs()
                        .into_iter()
                        .map(|c| c.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                );
            }
        }
    }
}

pub(crate) fn validate_copy_from_options(pstmt: &PgBox<pg_sys::PlannedStmt>) {
    let allowed_options = ["format"];

    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(pstmt.utilityStmt as _) };
    let copy_options = unsafe { PgList::<DefElem>::from_pg(copy_stmt.options) };

    for option in copy_options.iter_ptr() {
        let option = unsafe { PgBox::<DefElem>::from_pg(option) };
        let key = unsafe { std::ffi::CStr::from_ptr(option.defname).to_str().unwrap() };
        if !allowed_options.contains(&key) {
            panic!("{} is not a valid option for COPY FROM PARQUET", key);
        }

        if key == "format" {
            let format = unsafe { defGetString(option.as_ptr()) };
            let format = unsafe { std::ffi::CStr::from_ptr(format).to_str().unwrap() };
            if format != "parquet" {
                panic!("{} is not a valid format for COPY FROM PARQUET", format);
            }
        }
    }
}

pub(crate) fn copy_stmt_filename(pstmt: &PgBox<pg_sys::PlannedStmt>) -> *mut i8 {
    debug_assert!(unsafe { is_a(pstmt.utilityStmt, T_CopyStmt) });
    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(pstmt.utilityStmt as _) };
    debug_assert!(!copy_stmt.filename.is_null());
    copy_stmt.filename
}

pub(crate) fn copy_stmt_row_group_size_option(pstmt: &PgBox<pg_sys::PlannedStmt>) -> i64 {
    debug_assert!(unsafe { is_a(pstmt.utilityStmt, T_CopyStmt) });

    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(pstmt.utilityStmt as _) };

    let copy_options = unsafe { PgList::<DefElem>::from_pg(copy_stmt.options) };
    for option in copy_options.iter_ptr() {
        let option = unsafe { PgBox::<DefElem>::from_pg(option) };
        let key = unsafe { std::ffi::CStr::from_ptr(option.defname).to_str().unwrap() };
        if key != "row_group_size" {
            continue;
        }

        let row_group_size = unsafe { defGetInt64(option.as_ptr()) };
        return row_group_size;
    }

    DEFAULT_ROW_GROUP_SIZE
}

pub(crate) fn copy_stmt_codec_option(
    pstmt: &PgBox<pg_sys::PlannedStmt>,
) -> Option<ParquetCodecOption> {
    debug_assert!(unsafe { is_a(pstmt.utilityStmt, T_CopyStmt) });

    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(pstmt.utilityStmt as _) };

    let copy_options = unsafe { PgList::<DefElem>::from_pg(copy_stmt.options) };
    for option in copy_options.iter_ptr() {
        let option = unsafe { PgBox::<DefElem>::from_pg(option) };
        let key = unsafe { std::ffi::CStr::from_ptr(option.defname).to_str().unwrap() };
        if key != "codec" {
            continue;
        }

        let codec = unsafe { defGetString(option.as_ptr()) };
        let codec = unsafe { std::ffi::CStr::from_ptr(codec).to_str().unwrap() };
        let codec = ParquetCodecOption::from_str(codec).unwrap();
        return Some(codec);
    }

    None
}

pub(crate) fn copy_stmt_codec(pstmt: &PgBox<pg_sys::PlannedStmt>) -> ParquetCodecOption {
    if let Some(codec) = copy_stmt_codec_option(pstmt) {
        codec
    } else {
        let copy_filename = copy_stmt_filename(pstmt);
        let copy_filename = unsafe { std::ffi::CStr::from_ptr(copy_filename).to_str().unwrap() };
        FromPath::try_from_path(copy_filename).unwrap_or(ParquetCodecOption::Uncompressed)
    }
}

pub(crate) fn add_binary_format_option(pstmt: &PgBox<pg_sys::PlannedStmt>) -> PgList<DefElem> {
    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(pstmt.utilityStmt as _) };

    let binary = std::ffi::CString::new("binary").unwrap();
    let binary = unsafe { pg_sys::makeString(binary.into_raw() as _) };

    let mut found_parquet_format = false;
    let mut copy_options = unsafe { PgList::<DefElem>::from_pg(copy_stmt.options) };
    for option in copy_options.iter_ptr() {
        let mut option = unsafe { PgBox::<DefElem>::from_pg(option) };
        let key = unsafe { std::ffi::CStr::from_ptr(option.defname).to_str().unwrap() };
        if key != "format" {
            continue;
        }

        let format = unsafe { defGetString(option.as_ptr()) };
        let format = unsafe { std::ffi::CStr::from_ptr(format).to_str().unwrap() };
        if format == "parquet" {
            option.arg = binary as _;
            found_parquet_format = true;
        }
    }

    if !found_parquet_format {
        let format = std::ffi::CString::new("format").unwrap();
        let format = unsafe { pg_sys::makeDefElem(format.into_raw() as _, binary as _, -1) };
        copy_options.push(format);
    }

    copy_options
}

pub(crate) fn is_copy_to_parquet_stmt(pstmt: &PgBox<pg_sys::PlannedStmt>) -> bool {
    let is_copy_stmt = unsafe { is_a(pstmt.utilityStmt, T_CopyStmt) };
    if !is_copy_stmt {
        return false;
    }

    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(pstmt.utilityStmt as _) };
    if copy_stmt.is_from {
        return false;
    }

    if copy_stmt.filename.is_null() {
        return false;
    }

    if is_parquet_format(&copy_stmt) {
        return true;
    }

    is_parquet_file(&copy_stmt)
}

pub(crate) fn is_copy_from_parquet_stmt(pstmt: &PgBox<pg_sys::PlannedStmt>) -> bool {
    let is_copy_stmt = unsafe { is_a(pstmt.utilityStmt, T_CopyStmt) };
    if !is_copy_stmt {
        return false;
    }

    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(pstmt.utilityStmt as _) };
    if !copy_stmt.is_from {
        return false;
    }

    if copy_stmt.filename.is_null() {
        return false;
    }

    if is_parquet_format(&copy_stmt) {
        return true;
    }

    is_parquet_file(&copy_stmt)
}

pub(crate) fn copy_has_relation(pstmt: &PgBox<pg_sys::PlannedStmt>) -> bool {
    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(pstmt.utilityStmt as _) };
    !copy_stmt.relation.is_null()
}

pub(crate) fn copy_lock_mode(pstmt: &PgBox<pg_sys::PlannedStmt>) -> i32 {
    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(pstmt.utilityStmt as _) };
    if copy_stmt.is_from {
        RowExclusiveLock as _
    } else {
        AccessShareLock as _
    }
}

pub(crate) fn copy_relation_oid(pstmt: &PgBox<pg_sys::PlannedStmt>) -> Oid {
    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(pstmt.utilityStmt as _) };
    let range_var = copy_stmt.relation;
    let range_var = unsafe { PgBox::<pg_sys::RangeVar>::from_pg(range_var) };
    let relname = unsafe {
        std::ffi::CStr::from_ptr(range_var.relname)
            .to_str()
            .unwrap()
    };
    let relation = PgRelation::open_with_name_and_share_lock(relname).unwrap();
    relation.rd_id
}

// needed to declare these functions since they are not available in pg_sys yet
#[repr(C)]
pub(crate) struct CopyFromStateData {
    _data: [u8; 0],
    _marker: core::marker::PhantomData<(*mut u8, core::marker::PhantomPinned)>,
}

pub(crate) type CopyFromState = *mut CopyFromStateData;

#[allow(improper_ctypes)]
extern "C" {
    pub(crate) fn PortalStart(
        portal: Portal,
        params: ParamListInfo,
        eflags: ::std::os::raw::c_int,
        snapshot: Snapshot,
    );
    pub(crate) fn PortalRun(
        portal: Portal,
        count: ::std::os::raw::c_long,
        is_top_level: bool,
        run_once: bool,
        dest: *mut DestReceiver,
        alt_dest: *mut DestReceiver,
        completion_tag: *mut QueryCompletion,
    );

    pub(crate) fn BeginCopyFrom(
        parse_state: *mut ParseState,
        rel: Relation,
        where_clause: *mut pg_sys::Node,
        filename: *const i8,
        is_program: bool,
        data_source_cb: Option<extern "C" fn(void_mut_ptr, i32, i32) -> i32>,
        attnamelist: *mut pg_sys::List,
        options: *mut pg_sys::List,
    ) -> CopyFromState;

    pub(crate) fn CopyFrom(cstate: CopyFromState) -> i64;

    pub(crate) fn EndCopyFrom(cstate: CopyFromState);

    pub(crate) fn addRangeTableEntryForRelation(
        pstate: *mut ParseState,
        rel: Relation,
        lockmode: i32,
        alias: *mut Alias,
        inh: bool,
        inFromCl: bool,
    ) -> *mut ParseNamespaceItem;

    pub(crate) fn addNSItemToQuery(
        pstate: *mut ParseState,
        nsitem: *mut ParseNamespaceItem,
        add_to_join_list: bool,
        add_to_rel_namespace: bool,
        add_to_var_namespace: bool,
    );

    pub(crate) fn transformExpr(
        pstate: *mut ParseState,
        expr: *mut pg_sys::Node,
        expr_kind: ParseExprKind,
    ) -> *mut pg_sys::Node;

    pub(crate) fn assign_expr_collations(
        pstate: *mut ParseState,
        expr: *mut pg_sys::Node,
    ) -> *mut pg_sys::Node;
}

/*
 * slot_getallattrs
 *		This function forces all the entries of the slot's Datum/isnull
 *		arrays to be valid.  The caller may then extract data directly
 *		from those arrays instead of using slot_getattr.
 */
pub(crate) fn slot_getallattrs(slot: *mut TupleTableSlot) {
    // copied from Postgres since this method was inlined in the original code
    // (not found in pg_sys)
    // handles select * from table
    unsafe {
        let slot = PgBox::from_pg(slot);
        let tts_tupledesc = PgBox::from_pg(slot.tts_tupleDescriptor);
        if (slot.tts_nvalid as i32) < tts_tupledesc.natts {
            pg_sys::slot_getsomeattrs_int(slot.as_ptr(), tts_tupledesc.natts);
        }
    };
}
