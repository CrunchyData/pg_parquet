use std::{fmt::Display, str::FromStr};

use parquet::basic::Compression;

#[repr(C)]
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum ParquetCodecOption {
    Uncompressed,
    Snappy,
    Gzip,
    Lz4,
    Lz4raw,
    Brotli,
    Zstd,
}

pub(crate) fn all_supported_codecs() -> Vec<ParquetCodecOption> {
    vec![
        ParquetCodecOption::Uncompressed,
        ParquetCodecOption::Snappy,
        ParquetCodecOption::Gzip,
        ParquetCodecOption::Lz4,
        ParquetCodecOption::Lz4raw,
        ParquetCodecOption::Brotli,
        ParquetCodecOption::Zstd,
    ]
}

impl From<ParquetCodecOption> for Compression {
    fn from(value: ParquetCodecOption) -> Self {
        match value {
            ParquetCodecOption::Uncompressed => Compression::UNCOMPRESSED,
            ParquetCodecOption::Snappy => Compression::SNAPPY,
            ParquetCodecOption::Gzip => Compression::GZIP(Default::default()),
            ParquetCodecOption::Lz4 => Compression::LZ4,
            ParquetCodecOption::Lz4raw => Compression::LZ4_RAW,
            ParquetCodecOption::Brotli => Compression::BROTLI(Default::default()),
            ParquetCodecOption::Zstd => Compression::ZSTD(Default::default()),
        }
    }
}

impl Display for ParquetCodecOption {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParquetCodecOption::Uncompressed => write!(f, "uncompressed"),
            ParquetCodecOption::Snappy => write!(f, "snappy"),
            ParquetCodecOption::Gzip => write!(f, "gzip"),
            ParquetCodecOption::Lz4 => write!(f, "lz4"),
            ParquetCodecOption::Lz4raw => write!(f, "lz4raw"),
            ParquetCodecOption::Brotli => write!(f, "brotli"),
            ParquetCodecOption::Zstd => write!(f, "zstd"),
        }
    }
}

impl FromStr for ParquetCodecOption {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_string();

        if s == ParquetCodecOption::Uncompressed.to_string() {
            Ok(ParquetCodecOption::Uncompressed)
        } else if s == ParquetCodecOption::Snappy.to_string() {
            Ok(ParquetCodecOption::Snappy)
        } else if s == ParquetCodecOption::Gzip.to_string() {
            Ok(ParquetCodecOption::Gzip)
        } else if s == ParquetCodecOption::Lz4.to_string() {
            Ok(ParquetCodecOption::Lz4)
        } else if s == ParquetCodecOption::Lz4raw.to_string() {
            Ok(ParquetCodecOption::Lz4raw)
        } else if s == ParquetCodecOption::Brotli.to_string() {
            Ok(ParquetCodecOption::Brotli)
        } else if s == ParquetCodecOption::Zstd.to_string() {
            Ok(ParquetCodecOption::Zstd)
        } else {
            Err(format!("uncregonized codec: {}", s))
        }
    }
}

pub(crate) trait FromPath {
    fn try_from_path(filename: &str) -> Result<Self, String>
    where
        Self: Sized;
}

impl FromPath for ParquetCodecOption {
    fn try_from_path(filename: &str) -> Result<Self, String> {
        if filename.ends_with(".parquet") {
            Ok(ParquetCodecOption::Uncompressed)
        } else if filename.ends_with(".parquet.snappy") {
            Ok(ParquetCodecOption::Snappy)
        } else if filename.ends_with(".parquet.gzip") {
            Ok(ParquetCodecOption::Gzip)
        } else if filename.ends_with(".parquet.lz4") {
            Ok(ParquetCodecOption::Lz4)
        } else if filename.ends_with(".parquet.lz4_raw") {
            Ok(ParquetCodecOption::Lz4raw)
        } else if filename.ends_with(".parquet.br") {
            Ok(ParquetCodecOption::Brotli)
        } else if filename.ends_with(".parquet.zstd") {
            Ok(ParquetCodecOption::Zstd)
        } else {
            Err("unrecognized parquet file extension".into())
        }
    }
}
