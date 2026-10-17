//! The options a `COPY ... TO ... WITH (format 'parquet', ...)` takes, and the
//! `WriterProperties` they become.

use std::fmt::Display;
use std::str::FromStr;

use arrow_schema::Schema;
use format_registry::options::{FormatOptions, FILE_NAME_OPTION};
use format_registry::Result;
use parquet::basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel};
use parquet::file::properties::{
    EnabledStatistics, WriterProperties, WriterVersion, DEFAULT_BLOOM_FILTER_FPP,
};

use crate::column_selection::{
    leaf_column_paths, ColumnSelection, DEFAULT_BLOOM_FILTER, DEFAULT_DICTIONARY,
};
use crate::field_ids::FieldIds;

/// Parquet's own default row group size, and the byte budget that goes with it.
pub const DEFAULT_ROW_GROUP_SIZE: usize = 122880;
pub const DEFAULT_ROW_GROUP_SIZE_BYTES: usize = DEFAULT_ROW_GROUP_SIZE * 1024;

pub const COMPRESSIONS: &[&str] = &[
    "uncompressed",
    "snappy",
    "gzip",
    "lz4",
    "lz4_raw",
    "brotli",
    "zstd",
];

pub const PARQUET_VERSIONS: &[&str] = &["v1", "v2"];

/// The codecs parquet can be written with. Narrower than parquet's own list on purpose: these
/// are the ones arrow-rs can write and the common readers can read.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
pub enum ParquetCompression {
    Uncompressed,
    #[default]
    Snappy,
    Gzip,
    Lz4,
    Lz4Raw,
    Brotli,
    Zstd,
}

impl ParquetCompression {
    /// What the file name says it is, if it says anything. A plain `.parquet` means snappy,
    /// which is both parquet's convention and this provider's default.
    pub fn of_file_name(file_name: &str) -> Option<Self> {
        if file_name.ends_with(".parquet") || file_name.ends_with(".parquet.snappy") {
            Some(Self::Snappy)
        } else if file_name.ends_with(".parquet.gz") {
            Some(Self::Gzip)
        } else if file_name.ends_with(".parquet.lz4") {
            // the extension does not say which of the two lz4 codecs, and the plain one is
            // deprecated by the format, so readers need not support it — DuckDB does not
            Some(Self::Lz4Raw)
        } else if file_name.ends_with(".parquet.br") {
            Some(Self::Brotli)
        } else if file_name.ends_with(".parquet.zst") {
            Some(Self::Zstd)
        } else {
            None
        }
    }

    /// Whether this codec takes a level at all, which decides what a `compression_level` on a
    /// statement means and whether it is an error.
    fn default_level(&self) -> Option<i32> {
        match self {
            Self::Zstd => Some(ZstdLevel::default().compression_level()),
            Self::Gzip => Some(GzipLevel::default().compression_level() as i32),
            Self::Brotli => Some(BrotliLevel::default().compression_level() as i32),
            _ => None,
        }
    }

    /// The codec, at a level. Every range check is the codec's own rather than a copy of it
    /// kept in step here.
    fn codec(&self, level: Option<i32>) -> Result<Compression> {
        if level.is_some() && self.default_level().is_none() {
            return Err(format!(
                "compression level is not supported for \"{self}\" compression"
            ));
        }

        let level = level.or_else(|| self.default_level());

        let codec = match self {
            Self::Uncompressed => Compression::UNCOMPRESSED,
            Self::Snappy => Compression::SNAPPY,
            Self::Lz4 => Compression::LZ4,
            Self::Lz4Raw => Compression::LZ4_RAW,
            Self::Gzip => Compression::GZIP(
                GzipLevel::try_new(self.unsigned_level(level)?)
                    .map_err(|e| format!("invalid gzip compression level: {e}"))?,
            ),
            Self::Brotli => Compression::BROTLI(
                BrotliLevel::try_new(self.unsigned_level(level)?)
                    .map_err(|e| format!("invalid brotli compression level: {e}"))?,
            ),
            Self::Zstd => Compression::ZSTD(
                ZstdLevel::try_new(level.expect("zstd has a default level"))
                    .map_err(|e| format!("invalid zstd compression level: {e}"))?,
            ),
        };

        Ok(codec)
    }

    /// gzip and brotli count their levels unsigned, so a negative one is this crate's error to
    /// report rather than a `try_new` panic on the cast.
    fn unsigned_level(&self, level: Option<i32>) -> Result<u32> {
        let level = level.expect("this codec has a default level");

        u32::try_from(level)
            .map_err(|_| format!("compression level {level} is negative for \"{self}\""))
    }
}

impl Display for ParquetCompression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::Uncompressed => "uncompressed",
            Self::Snappy => "snappy",
            Self::Gzip => "gzip",
            Self::Lz4 => "lz4",
            Self::Lz4Raw => "lz4_raw",
            Self::Brotli => "brotli",
            Self::Zstd => "zstd",
        };

        write!(f, "{name}")
    }
}

impl FromStr for ParquetCompression {
    type Err = String;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "uncompressed" => Ok(Self::Uncompressed),
            "snappy" => Ok(Self::Snappy),
            "gzip" => Ok(Self::Gzip),
            "lz4" => Ok(Self::Lz4),
            "lz4_raw" => Ok(Self::Lz4Raw),
            "brotli" => Ok(Self::Brotli),
            "zstd" => Ok(Self::Zstd),
            other => Err(format!("unrecognized compression format: {other}")),
        }
    }
}

/// Which parquet writer version. V1 by default, because it is what every reader handles.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ParquetVersion {
    #[default]
    V1,
    V2,
}

impl FromStr for ParquetVersion {
    type Err = String;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "v1" => Ok(Self::V1),
            "v2" => Ok(Self::V2),
            other => Err(format!(
                "unrecognized parquet version: {other}. v1 or v2 is supported."
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

/// Every write option, parsed. Parsing is separate from use so that a bad option is reported
/// before a header has been written, which for a `COPY TO` means before the object exists.
#[derive(Debug, Clone)]
pub struct WriteOptions {
    pub compression: ParquetCompression,
    pub compression_level: Option<i32>,
    pub row_group_size: usize,
    pub row_group_size_bytes: usize,
    pub parquet_version: ParquetVersion,
    pub bloom_filter: ColumnSelection,
    pub bloom_filter_fpp: f64,
    pub dictionary: ColumnSelection,
    pub field_ids: FieldIds,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            compression: ParquetCompression::default(),
            compression_level: None,
            row_group_size: DEFAULT_ROW_GROUP_SIZE,
            row_group_size_bytes: DEFAULT_ROW_GROUP_SIZE_BYTES,
            parquet_version: ParquetVersion::default(),
            bloom_filter: DEFAULT_BLOOM_FILTER,
            bloom_filter_fpp: DEFAULT_BLOOM_FILTER_FPP,
            dictionary: DEFAULT_DICTIONARY,
            field_ids: FieldIds::default(),
        }
    }
}

impl WriteOptions {
    pub fn parse(options: &FormatOptions) -> Result<Self> {
        let default = Self::default();

        let compression = match options.get_enum("compression", COMPRESSIONS)? {
            Some(named) => named.parse()?,
            // no option, so the file name decides; a name that says nothing leaves snappy
            None => options
                .get(FILE_NAME_OPTION)
                .and_then(ParquetCompression::of_file_name)
                .unwrap_or(default.compression),
        };

        let parquet_version = match options.get_enum("parquet_version", PARQUET_VERSIONS)? {
            Some(named) => named.parse()?,
            None => default.parquet_version,
        };

        Ok(Self {
            compression,
            compression_level: compression_level(options)?,
            row_group_size: positive(options, "row_group_size")?.unwrap_or(default.row_group_size),
            row_group_size_bytes: positive(options, "row_group_size_bytes")?
                .unwrap_or(default.row_group_size_bytes),
            parquet_version,
            bloom_filter: selection(options, "bloom_filter")?.unwrap_or(default.bloom_filter),
            bloom_filter_fpp: fpp(options)?.unwrap_or(default.bloom_filter_fpp),
            dictionary: selection(options, "dictionary")?.unwrap_or(default.dictionary),
            field_ids: field_ids(options)?.unwrap_or(default.field_ids),
        })
    }

    /// The writer properties for a schema. Needs the schema because bloom filters and
    /// dictionaries are set per *leaf* column, and only the schema knows what the leaves of a
    /// struct or a map are.
    pub fn writer_properties(&self, schema: &Schema) -> Result<WriterProperties> {
        self.bloom_filter
            .validate_against_schema(schema, "bloom_filter")?;
        self.dictionary
            .validate_against_schema(schema, "dictionary")?;

        let mut builder = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_compression(self.compression.codec(self.compression_level)?)
            .set_max_row_group_row_count(Some(self.row_group_size))
            .set_max_row_group_bytes(Some(self.row_group_size_bytes))
            .set_writer_version(self.parquet_version.into())
            .set_created_by("pg_parquet".to_string());

        // per leaf column rather than for all of them at once, so that a selection can enable
        // these for some columns and not others
        for path in leaf_column_paths(schema)? {
            if let Some(enabled) = self.bloom_filter.enabled_for(&path) {
                builder = builder.set_column_bloom_filter_enabled(path.clone(), enabled);

                // setting the false positive probability enables the filter too, so it is only
                // set for the columns that are getting one
                if enabled {
                    builder =
                        builder.set_column_bloom_filter_fpp(path.clone(), self.bloom_filter_fpp);
                }
            }

            if let Some(enabled) = self.dictionary.enabled_for(&path) {
                builder = builder.set_column_dictionary_enabled(path, enabled);
            }
        }

        Ok(builder.build())
    }
}

/// `compression_level` parsed as a signed integer: zstd's levels include negative ones, so
/// rejecting `-1` here as "not a number" would be the wrong message for the wrong reason.
pub(crate) fn compression_level(options: &FormatOptions) -> Result<Option<i32>> {
    let Some(value) = options.get("compression_level") else {
        return Ok(None);
    };

    value
        .trim()
        .parse::<i32>()
        .map(Some)
        .map_err(|_| format!("compression_level must be an integer, not \"{value}\""))
}

pub(crate) fn positive(options: &FormatOptions, name: &str) -> Result<Option<usize>> {
    match options.get_usize(name)? {
        Some(0) => Err(format!("{name} must be greater than 0")),
        parsed => Ok(parsed),
    }
}

pub(crate) fn fpp(options: &FormatOptions) -> Result<Option<f64>> {
    let Some(value) = options.get("bloom_filter_fpp") else {
        return Ok(None);
    };

    let parsed = value
        .trim()
        .parse::<f64>()
        .map_err(|_| format!("bloom_filter_fpp must be a number, not \"{value}\""))?;

    if parsed <= 0.0 || parsed >= 1.0 {
        return Err(format!(
            "bloom_filter_fpp must be between 0 and 1, exclusive, not {parsed}"
        ));
    }

    Ok(Some(parsed))
}

pub(crate) fn selection(options: &FormatOptions, name: &str) -> Result<Option<ColumnSelection>> {
    // the parse does not know which of the two selection options it is parsing, and a message
    // that does not name the option is a message a statement with both cannot act on
    options
        .get(name)
        .map(|value| {
            value
                .parse()
                .map_err(|e| format!("invalid value for \"{name}\": {e}"))
        })
        .transpose()
}

pub(crate) fn field_ids(options: &FormatOptions) -> Result<Option<FieldIds>> {
    options
        .get("field_ids")
        .map(|value| value.parse())
        .transpose()
}
