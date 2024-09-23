use std::str::FromStr;

use pgrx::{
    is_a,
    pg_sys::{
        self, defGetInt64, defGetString, get_typlenbyval, AccessShareLock, CopyStmt, DefElem,
        NodeTag::T_CopyStmt, Oid, RowExclusiveLock,
    },
    PgBox, PgList, PgRelation, PgTupleDesc,
};
use url::Url;

use crate::arrow_parquet::{
    codec::{all_supported_codecs, ParquetCodecOption},
    parquet_writer::DEFAULT_ROW_GROUP_SIZE,
    uri_utils::parse_uri,
};

// missing PG function at pgrx
extern "C" {
    fn toast_raw_datum_size(datum: pg_sys::Datum) -> usize;
}

pub(crate) fn is_parquet_format(copy_stmt: &PgBox<CopyStmt>) -> bool {
    let copy_options = unsafe { PgList::<DefElem>::from_pg(copy_stmt.options) };
    for option in copy_options.iter_ptr() {
        let option = unsafe { PgBox::<DefElem>::from_pg(option) };
        let key = unsafe {
            std::ffi::CStr::from_ptr(option.defname)
                .to_str()
                .unwrap_or_else(|e| panic!("copy option is not a valid CString: {}", e))
        };
        if key != "format" {
            continue;
        }

        let format = unsafe { defGetString(option.as_ptr()) };
        let format = unsafe {
            std::ffi::CStr::from_ptr(format)
                .to_str()
                .unwrap_or_else(|e| panic!("format option is not a valid CString: {}", e))
        };
        return format == "parquet";
    }

    false
}

pub(crate) fn is_parquet_uri(uri: Url) -> bool {
    ParquetCodecOption::try_from(uri).is_ok()
}

pub(crate) fn validate_copy_to_options(pstmt: &PgBox<pg_sys::PlannedStmt>) {
    let allowed_options = ["format", "freeze", "row_group_size", "compression"];

    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(pstmt.utilityStmt as _) };
    let copy_options = unsafe { PgList::<DefElem>::from_pg(copy_stmt.options) };

    for option in copy_options.iter_ptr() {
        let option = unsafe { PgBox::<DefElem>::from_pg(option) };
        let key = unsafe {
            std::ffi::CStr::from_ptr(option.defname)
                .to_str()
                .expect("copy option is not a valid CString")
        };

        if !allowed_options.contains(&key) {
            panic!("{} is not a valid option for COPY TO PARQUET", key);
        }

        if key == "format" {
            let format = unsafe { defGetString(option.as_ptr()) };
            let format = unsafe {
                std::ffi::CStr::from_ptr(format)
                    .to_str()
                    .expect("copy option is not a valid CString")
            };

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

        if key == "compression" {
            let codec = unsafe { defGetString(option.as_ptr()) };
            let codec = unsafe {
                std::ffi::CStr::from_ptr(codec)
                    .to_str()
                    .expect("codec option is not a valid CString")
            };

            if ParquetCodecOption::from_str(codec).is_err() {
                panic!(
                    "{} is not a valid compression format. Supported compression formats are {}",
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
    let allowed_options = ["format", "freeze"];

    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(pstmt.utilityStmt as _) };
    let copy_options = unsafe { PgList::<DefElem>::from_pg(copy_stmt.options) };

    for option in copy_options.iter_ptr() {
        let option = unsafe { PgBox::<DefElem>::from_pg(option) };
        let key = unsafe {
            std::ffi::CStr::from_ptr(option.defname)
                .to_str()
                .expect("copy option is not a valid CString")
        };

        if !allowed_options.contains(&key) {
            panic!("{} is not a valid option for COPY FROM PARQUET", key);
        }

        if key == "format" {
            let format = unsafe { defGetString(option.as_ptr()) };
            let format = unsafe {
                std::ffi::CStr::from_ptr(format)
                    .to_str()
                    .expect("format option is not a valid CString")
            };
            if format != "parquet" {
                panic!("{} is not a valid format for COPY FROM PARQUET", format);
            }
        }
    }
}

pub(crate) fn copy_stmt_uri(pstmt: &PgBox<pg_sys::PlannedStmt>) -> Option<Url> {
    debug_assert!(unsafe { is_a(pstmt.utilityStmt, T_CopyStmt) });
    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(pstmt.utilityStmt as _) };

    if copy_stmt.filename.is_null() {
        return None;
    }

    let uri = unsafe {
        std::ffi::CStr::from_ptr(copy_stmt.filename)
            .to_str()
            .expect("uri option is not a valid CString")
    };
    Some(parse_uri(uri))
}

pub(crate) fn copy_to_stmt_row_group_size_option(pstmt: &PgBox<pg_sys::PlannedStmt>) -> i64 {
    debug_assert!(unsafe { is_a(pstmt.utilityStmt, T_CopyStmt) });

    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(pstmt.utilityStmt as _) };

    let copy_options = unsafe { PgList::<DefElem>::from_pg(copy_stmt.options) };
    for option in copy_options.iter_ptr() {
        let option = unsafe { PgBox::<DefElem>::from_pg(option) };
        let key = unsafe {
            std::ffi::CStr::from_ptr(option.defname)
                .to_str()
                .expect("row_group_size option is not a valid CString")
        };

        if key != "row_group_size" {
            continue;
        }

        let row_group_size = unsafe { defGetInt64(option.as_ptr()) };
        return row_group_size;
    }

    DEFAULT_ROW_GROUP_SIZE
}

pub(crate) fn copy_to_stmt_codec_option(
    pstmt: &PgBox<pg_sys::PlannedStmt>,
) -> Option<ParquetCodecOption> {
    debug_assert!(unsafe { is_a(pstmt.utilityStmt, T_CopyStmt) });

    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(pstmt.utilityStmt as _) };

    let copy_options = unsafe { PgList::<DefElem>::from_pg(copy_stmt.options) };
    for option in copy_options.iter_ptr() {
        let option = unsafe { PgBox::<DefElem>::from_pg(option) };
        let key = unsafe {
            std::ffi::CStr::from_ptr(option.defname)
                .to_str()
                .expect("compression option is not a valid CString")
        };
        if key != "compression" {
            continue;
        }

        let codec = unsafe { defGetString(option.as_ptr()) };
        let codec = unsafe {
            std::ffi::CStr::from_ptr(codec)
                .to_str()
                .expect("compression option is not a valid CString")
        };
        let codec = ParquetCodecOption::from_str(codec).unwrap_or_else(|e| panic!("{}", e));
        return Some(codec);
    }

    None
}

pub(crate) fn copy_to_stmt_codec(
    pstmt: &PgBox<pg_sys::PlannedStmt>,
    uri: Url,
) -> ParquetCodecOption {
    if let Some(codec) = copy_to_stmt_codec_option(pstmt) {
        codec
    } else {
        ParquetCodecOption::try_from(uri).unwrap_or(ParquetCodecOption::Uncompressed)
    }
}

pub(crate) fn copy_from_stmt_add_or_update_binary_format_option(
    pstmt: &PgBox<pg_sys::PlannedStmt>,
) -> PgList<DefElem> {
    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(pstmt.utilityStmt as _) };

    let binary = std::ffi::CString::new("binary").expect("CString::new failed");
    let binary = unsafe { pg_sys::makeString(binary.into_raw() as _) };

    let mut copy_options = unsafe { PgList::<DefElem>::from_pg(copy_stmt.options) };

    let mut format = copy_from_format_option(pstmt);

    if format.is_null() {
        // add new format = binary option
        let format = std::ffi::CString::new("format").expect("CString::new failed");
        let format = unsafe { pg_sys::makeDefElem(format.into_raw() as _, binary as _, -1) };

        copy_options.push(format);
    } else {
        // update existing format option
        format.arg = binary as _;
    }

    copy_options
}

pub(crate) fn copy_from_format_option(pstmt: &PgBox<pg_sys::PlannedStmt>) -> PgBox<DefElem> {
    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(pstmt.utilityStmt as _) };

    let copy_options = unsafe { PgList::<DefElem>::from_pg(copy_stmt.options) };

    for option in copy_options.iter_ptr() {
        let option = unsafe { PgBox::<DefElem>::from_pg(option) };
        let key = unsafe {
            std::ffi::CStr::from_ptr(option.defname)
                .to_str()
                .expect("copy option is not a valid CString")
        };

        if key == "format" {
            return option;
        }
    }

    PgBox::null()
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

    let uri = copy_stmt_uri(pstmt).expect("uri is None");

    is_parquet_format(&copy_stmt) || is_parquet_uri(uri)
}

pub(crate) fn is_copy_from_stmt(pstmt: &PgBox<pg_sys::PlannedStmt>) -> bool {
    let is_copy_stmt = unsafe { is_a(pstmt.utilityStmt, T_CopyStmt) };
    if !is_copy_stmt {
        return false;
    }

    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(pstmt.utilityStmt as _) };
    copy_stmt.is_from
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

    let uri = copy_stmt_uri(pstmt).expect("uri is None");

    is_parquet_format(&copy_stmt) || is_parquet_uri(uri)
}

pub(crate) fn copy_stmt_has_relation(pstmt: &PgBox<pg_sys::PlannedStmt>) -> bool {
    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(pstmt.utilityStmt as _) };
    !copy_stmt.relation.is_null()
}

pub(crate) fn copy_stmt_lock_mode(pstmt: &PgBox<pg_sys::PlannedStmt>) -> i32 {
    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(pstmt.utilityStmt as _) };
    if copy_stmt.is_from {
        RowExclusiveLock as _
    } else {
        AccessShareLock as _
    }
}

pub(crate) fn copy_stmt_relation_oid(pstmt: &PgBox<pg_sys::PlannedStmt>) -> Oid {
    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(pstmt.utilityStmt as _) };
    let range_var = copy_stmt.relation;
    let range_var = unsafe { PgBox::<pg_sys::RangeVar>::from_pg(range_var) };
    let relname = unsafe {
        std::ffi::CStr::from_ptr(range_var.relname)
            .to_str()
            .expect("relation name is not a valid CString")
    };
    let relation =
        PgRelation::open_with_name_and_share_lock(relname).unwrap_or_else(|e| panic!("{}", e));
    relation.rd_id
}

pub(crate) fn tuple_column_sizes(
    tuple_datums: &[Option<pg_sys::Datum>],
    tupledesc: &PgTupleDesc,
) -> Vec<i32> {
    let mut column_sizes = vec![];

    for (idx, column_datum) in tuple_datums.iter().enumerate() {
        let att = tupledesc.get(idx).expect("cannot get attribute");
        let typoid = att.type_oid();

        let mut typlen = -1_i16;
        let mut typbyval = false;
        unsafe { get_typlenbyval(typoid.value(), &mut typlen, &mut typbyval) };

        let column_size = if let Some(column_datum) = column_datum {
            if typlen == -1 {
                // varlena type
                (unsafe { toast_raw_datum_size(*column_datum) }) as i32
            } else if typlen == -2 {
                // cstring
                let cstring = unsafe { pg_sys::DatumGetCString(*column_datum) };

                let cstring = unsafe {
                    std::ffi::CStr::from_ptr(cstring)
                        .to_str()
                        .expect("cstring is not a valid CString")
                };

                cstring.len() as i32 + 1
            } else {
                // fixed size type
                typlen as i32
            }
        } else {
            0
        };

        column_sizes.push(column_size);
    }

    column_sizes
}
