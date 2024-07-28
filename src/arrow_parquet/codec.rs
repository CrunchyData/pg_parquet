use std::str::FromStr;

use parquet::basic::Compression;

#[repr(C)]
#[derive(Debug, Copy, Clone, PartialEq)]
pub(crate) enum ParquetCodecOption {
    Uncompressed,
    Snappy,
    Gzip,
    Lz4,
    Lz4raw,
    Brotli,
    Zstd,
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

impl ToString for ParquetCodecOption {
    fn to_string(&self) -> String {
        match self {
            ParquetCodecOption::Uncompressed => "uncompressed".to_string(),
            ParquetCodecOption::Snappy => "snappy".to_string(),
            ParquetCodecOption::Gzip => "gzip".to_string(),
            ParquetCodecOption::Lz4 => "lz4".to_string(),
            ParquetCodecOption::Lz4raw => "lz4raw".to_string(),
            ParquetCodecOption::Brotli => "brotli".to_string(),
            ParquetCodecOption::Zstd => "zstd".to_string(),
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
