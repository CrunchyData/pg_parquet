use std::str::FromStr;

use pgrx::{
    is_a,
    pg_sys::{
        self, defGetInt64, defGetString, AccessShareLock, CopyStmt, DefElem, NodeTag::T_CopyStmt,
        Oid, RowExclusiveLock,
    },
    PgBox, PgList, PgRelation,
};

use crate::arrow_parquet::{
    codec::{all_supported_codecs, FromPath, ParquetCodecOption},
    parquet_writer::DEFAULT_ROW_GROUP_SIZE,
};

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

pub(crate) fn is_parquet_file(copy_stmt: &PgBox<CopyStmt>) -> bool {
    let filename = unsafe {
        std::ffi::CStr::from_ptr(copy_stmt.filename)
            .to_str()
            .expect("filename is not a valid C string")
    };
    <ParquetCodecOption as FromPath>::try_from_path(filename).is_ok()
}

pub(crate) fn validate_copy_to_options(pstmt: &PgBox<pg_sys::PlannedStmt>) {
    let allowed_options = ["format", "row_group_size", "compression"];

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
    let allowed_options = ["format"];

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

pub(crate) fn copy_stmt_codec_option(
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

pub(crate) fn copy_stmt_codec(pstmt: &PgBox<pg_sys::PlannedStmt>) -> ParquetCodecOption {
    if let Some(codec) = copy_stmt_codec_option(pstmt) {
        codec
    } else {
        let copy_filename = copy_stmt_filename(pstmt);
        let copy_filename = unsafe {
            std::ffi::CStr::from_ptr(copy_filename)
                .to_str()
                .expect("filename option is not a valid CString")
        };
        FromPath::try_from_path(copy_filename).unwrap_or(ParquetCodecOption::Uncompressed)
    }
}

pub(crate) fn add_binary_format_option(pstmt: &PgBox<pg_sys::PlannedStmt>) -> PgList<DefElem> {
    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(pstmt.utilityStmt as _) };

    let binary = std::ffi::CString::new("binary").expect("CString::new failed");
    let binary = unsafe { pg_sys::makeString(binary.into_raw() as _) };

    let mut found_parquet_format = false;
    let mut copy_options = unsafe { PgList::<DefElem>::from_pg(copy_stmt.options) };
    for option in copy_options.iter_ptr() {
        let mut option = unsafe { PgBox::<DefElem>::from_pg(option) };
        let key = unsafe {
            std::ffi::CStr::from_ptr(option.defname)
                .to_str()
                .expect("copy option is not a valid CString")
        };

        if key != "format" {
            continue;
        }

        let format = unsafe { defGetString(option.as_ptr()) };
        let format = unsafe {
            std::ffi::CStr::from_ptr(format)
                .to_str()
                .expect("format option is not a valid CString")
        };

        if format == "parquet" {
            option.arg = binary as _;
            found_parquet_format = true;
        }
    }

    if !found_parquet_format {
        let format = std::ffi::CString::new("format").expect("CString::new failed");
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

    is_parquet_format(&copy_stmt) || is_parquet_file(&copy_stmt)
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

    is_parquet_format(&copy_stmt) || is_parquet_file(&copy_stmt)
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
            .expect("relation name is not a valid CString")
    };
    let relation =
        PgRelation::open_with_name_and_share_lock(relname).unwrap_or_else(|e| panic!("{}", e));
    relation.rd_id
}