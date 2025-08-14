use std::{fmt::Display, str::FromStr};

use parquet::file::properties::WriterVersion;

#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) enum ParquetVersion {
    #[default]
    V1,
    V2,
}

impl FromStr for ParquetVersion {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "v1" => Ok(ParquetVersion::V1),
            "v2" => Ok(ParquetVersion::V2),
            _ => Err(format!(
                "unrecognized parquet version: {s}. v1 or v2 is supported.",
            )),
        }
    }
}

impl From<ParquetVersion> for WriterVersion {
    fn from(value: ParquetVersion) -> Self {
        match value {
            ParquetVersion::V1 => WriterVersion::PARQUET_1_0,
            ParquetVersion::V2 => WriterVersion::PARQUET_2_0,
        }
    }
}

impl Display for ParquetVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParquetVersion::V1 => write!(f, "v1"),
            ParquetVersion::V2 => write!(f, "v2"),
        }
    }
}
