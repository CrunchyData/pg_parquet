//! The [`FormatProvider`] implementation.

use std::ops::Range;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatchReader;
use bytes::Bytes;
use format_registry::options::FormatOptions;
use format_registry::provider::StreamingSink;
use format_registry::provider::{ByteSink, ByteSource, FileInfo, FormatProvider, RecordBatchSink};
use format_registry::Result;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::ChunkReader;

use crate::field_ids::stamp_field_ids;
use crate::options::{WriteOptions, COMPRESSIONS, PARQUET_VERSIONS};
use crate::schema_hint::schema_hint;
use crate::source::SourceChunkReader;
use crate::writer::{BufferedParquetSink, StreamingParquetSink};

/// The name in `COPY ... WITH (format 'parquet')`.
pub const FORMAT_NAME: &str = "parquet";

/// Only `.parquet`. A `.parquet.zst` ends in `zst`, so it is not inferred by extension and
/// needs `format 'parquet'` on the statement — the same as in `pg_parquet` 0.6, where the
/// compressed extensions decided a *codec* for a file already known to be parquet rather than
/// deciding the format.
pub const FILE_EXTENSIONS: &[&str] = &["parquet"];

/// What one split is called here, and the unit a parallel `COPY FROM` hands out.
pub const SPLIT_LABEL: &str = "row group";

/// Every option this provider takes, for the message a typo gets.
pub const OPTIONS: &[&str] = &[
    "compression",
    "compression_level",
    "row_group_size",
    "row_group_size_bytes",
    "parquet_version",
    "bloom_filter",
    "bloom_filter_fpp",
    "dictionary",
    "field_ids",
];

/// The parquet provider. Stateless: every call is a function of the bytes or the source it is
/// given.
#[derive(Debug, Default, Clone, Copy)]
pub struct Parquet;

/// A whole-file read, for the three questions the base asks about a file it already holds.
///
/// `Bytes::copy_from_slice` because `&[u8]` is not a `ChunkReader` and parquet's readers need
/// one. Only the footer is parsed out of the copy, and the base is holding the file anyway on
/// this path; a `COPY` goes through [`Parquet::open_ranged_reader`] and copies nothing.
fn metadata(data: &[u8]) -> Result<ParquetMetaData> {
    read_metadata(&Bytes::copy_from_slice(data))
}

fn read_metadata<R: ChunkReader>(reader: &R) -> Result<ParquetMetaData> {
    parquet::file::metadata::ParquetMetaDataReader::new()
        .parse_and_finish(reader)
        .map_err(|e| format!("not a readable parquet file: {e}"))
}

/// How many rows a reader should decode at a time.
///
/// An Arrow array addresses its values with `i32` offsets, so a batch whose byte-array column
/// holds more than 2 GB cannot be built at all — upstream fails it with "index overflow
/// decoding byte array". A file with such a column is therefore read a row at a time, which is
/// slow and is the only thing that works; everything else is read in the default batches.
fn reader_batch_size(metadata: &ParquetMetaData) -> Option<usize> {
    const MAX_ARROW_ARRAY_SIZE: i64 = i32::MAX as i64;

    let mut column_sizes = metadata.row_groups().iter().flat_map(|row_group| {
        row_group.columns().iter().map(|column| {
            // the unencoded size when the footer records it, and the compressed-pages figure as
            // the only other thing there is to go on
            column
                .unencoded_byte_array_data_bytes()
                .unwrap_or(column.uncompressed_size())
        })
    });

    column_sizes
        .any(|column_size| column_size > MAX_ARROW_ARRAY_SIZE)
        .then_some(1)
}

/// A reader over a file, built with the schema the file should be *read* with rather than
/// always the one it declares — see [`crate::schema_hint`] for the two cases where those
/// differ. A file with neither pays only the metadata load, which the reader does anyway.
fn builder<R: ChunkReader + 'static>(reader: R) -> Result<ParquetRecordBatchReaderBuilder<R>> {
    let metadata = ArrowReaderMetadata::load(&reader, ArrowReaderOptions::new())
        .map_err(|e| format!("not a readable parquet file: {e}"))?;

    let hint = schema_hint(metadata.schema(), metadata.metadata().file_metadata());

    let metadata = match hint {
        // the hint is a correction, not a cast: if upstream will not read the file with it, the
        // file's own schema is still the right thing to read with
        Some(schema) => ArrowReaderMetadata::try_new(
            metadata.metadata().clone(),
            ArrowReaderOptions::new().with_schema(schema),
        )
        .unwrap_or(metadata),
        None => metadata,
    };

    Ok(ParquetRecordBatchReaderBuilder::new_with_metadata(
        reader, metadata,
    ))
}

/// The writer properties and the schema that go with a set of write options. Shared by the two
/// writer entry points, since the only difference between them is where the bytes go.
fn writer_setup(
    schema: SchemaRef,
    options: &FormatOptions,
) -> Result<(SchemaRef, parquet::file::properties::WriterProperties)> {
    let write_options = WriteOptions::parse(options)?;

    // the ids go into the schema the file is written with, so this happens before the
    // properties are built: a per-column property is keyed by a path through this schema
    let schema: SchemaRef = Arc::new(stamp_field_ids(&schema, &write_options.field_ids)?);
    let properties = write_options.writer_properties(&schema)?;

    Ok((schema, properties))
}

/// What a parquet footer says about its file. Taken out of the trait method because the footer
/// reaches this crate two ways — out of a buffer the base holds, and through a `ByteSource` — and
/// the answer must not depend on which.
fn file_info(metadata: &ParquetMetaData) -> FileInfo {
    let file_metadata = metadata.file_metadata();

    // the codec is per column chunk, so "the file's compression" is only well defined when
    // every chunk agrees; a file written by something that mixed them reports none
    let mut compressions = metadata
        .row_groups()
        .iter()
        .flat_map(|row_group| row_group.columns())
        .map(|column| column.compression().to_string());

    let first = compressions.next();
    let compression = match first {
        Some(ref codec) if compressions.all(|other| &other == codec) => first,
        _ => None,
    };

    FileInfo {
        num_rows: Some(file_metadata.num_rows()),
        num_splits: Some(metadata.num_row_groups() as i64),
        split_label: Some(SPLIT_LABEL.to_string()),
        compression,
        format_version: Some(format!("v{}", file_metadata.version())),
    }
}

impl FormatProvider for Parquet {
    fn name(&self) -> String {
        FORMAT_NAME.to_string()
    }

    fn file_extensions(&self) -> Vec<String> {
        FILE_EXTENSIONS.iter().map(|e| e.to_string()).collect()
    }

    fn validate_option(&self, name: &str, value: &str) -> Result<()> {
        // the base validates every option of a statement before it knows whether it is reading
        // or writing, so a write option on a COPY FROM is accepted here and then ignored,
        // exactly as an unused option is in core COPY
        let mut options = FormatOptions::new();
        options.push(name, value);

        match name.to_ascii_lowercase().as_str() {
            "compression" => options.get_enum("compression", COMPRESSIONS).map(|_| ()),
            "compression_level" => crate::options::compression_level(&options).map(|_| ()),
            "row_group_size" => crate::options::positive(&options, "row_group_size").map(|_| ()),
            "row_group_size_bytes" => {
                crate::options::positive(&options, "row_group_size_bytes").map(|_| ())
            }
            "parquet_version" => options
                .get_enum("parquet_version", PARQUET_VERSIONS)
                .map(|_| ()),
            "bloom_filter" => crate::options::selection(&options, "bloom_filter").map(|_| ()),
            "bloom_filter_fpp" => crate::options::fpp(&options).map(|_| ()),
            "dictionary" => crate::options::selection(&options, "dictionary").map(|_| ()),
            // the names in the mapping are checked against the schema when there is one, which
            // is at open_writer; here only the JSON is
            "field_ids" => crate::options::field_ids(&options).map(|_| ()),
            _ => Err(format!(
                "the parquet format has no option \"{name}\"; it takes {}",
                OPTIONS.join(", ")
            )),
        }
    }

    fn schema(&self, data: &[u8]) -> Result<SchemaRef> {
        Ok(builder(Bytes::copy_from_slice(data))?.schema().clone())
    }

    /// The file's key/value metadata, in the order the footer has it.
    ///
    /// A value is `None` for a key stored without one, which parquet permits and which is why
    /// the base's signature has an `Option` at all.
    fn kv_metadata(&self, data: &[u8]) -> Result<Vec<(String, Option<String>)>> {
        let metadata = metadata(data)?;

        Ok(metadata
            .file_metadata()
            .key_value_metadata()
            .map(|pairs| {
                pairs
                    .iter()
                    .map(|pair| (pair.key.clone(), pair.value.clone()))
                    .collect()
            })
            .unwrap_or_default())
    }

    /// Everything here is in the footer, so none of it is `None`: a parquet file knows its row
    /// count, its row groups and what it was compressed with.
    fn file_info(&self, data: &[u8]) -> Result<FileInfo> {
        Ok(file_info(&metadata(data)?))
    }

    /// The same footer, fetched through the base's source instead: a `COPY FROM` never holds
    /// the file, and this is where its row count comes from.
    fn ranged_file_info(&self, source: Arc<dyn ByteSource>) -> Result<FileInfo> {
        let reader = SourceChunkReader::new(source)?;

        Ok(file_info(&read_metadata(&reader)?))
    }


    fn open_reader(
        &self,
        data: Bytes,
        _options: &FormatOptions,
    ) -> Result<Box<dyn RecordBatchReader + Send>> {
        // `Bytes` rather than a copy: the reader holds it for as long as it needs, and a clone
        // of one is a refcount bump
        let mut builder = builder(data)?;

        if let Some(batch_size) = reader_batch_size(builder.metadata()) {
            builder = builder.with_batch_size(batch_size);
        }

        Ok(Box::new(builder.build().map_err(|e| {
            format!("could not read the parquet file: {e}")
        })?))
    }

    /// The whole reason provider ABI v2 exists. The footer is read first, then the row groups
    /// that are wanted, so a `COPY FROM` of a file larger than this backend's memory is a
    /// sequence of range requests rather than a download.
    fn can_read_ranges(&self) -> bool {
        true
    }

    fn open_ranged_reader(
        &self,
        source: Arc<dyn ByteSource>,
        _options: &FormatOptions,
        splits: Option<Range<i64>>,
    ) -> Result<Box<dyn RecordBatchReader + Send>> {
        let reader = SourceChunkReader::new(source)?;
        let mut builder = builder(reader)?;

        if let Some(splits) = splits {
            let row_groups = builder.metadata().num_row_groups() as i64;

            if splits.start < 0 || splits.end > row_groups {
                return Err(format!(
                    "row groups {}..{} were asked for from a file that has {row_groups}",
                    splits.start, splits.end
                ));
            }

            builder = builder.with_row_groups(
                (splits.start..splits.end)
                    .map(|split| split as usize)
                    .collect(),
            );
        }

        if let Some(batch_size) = reader_batch_size(builder.metadata()) {
            builder = builder.with_batch_size(batch_size);
        }

        Ok(Box::new(builder.build().map_err(|e| {
            format!("could not read the parquet file: {e}")
        })?))
    }

    /// Parquet writes its footer last and everything before it once, so a `COPY TO` of any size
    /// goes straight into the base's upload.
    fn can_stream_write(&self) -> bool {
        true
    }

    fn open_writer(
        &self,
        schema: SchemaRef,
        options: &FormatOptions,
    ) -> Result<Box<dyn RecordBatchSink>> {
        let (schema, properties) = writer_setup(schema, options)?;

        Ok(Box::new(BufferedParquetSink::new(schema, properties)?))
    }

    fn open_streaming_writer(
        &self,
        schema: SchemaRef,
        options: &FormatOptions,
        sink: Box<dyn ByteSink>,
    ) -> Result<Box<dyn StreamingSink>> {
        let (schema, properties) = writer_setup(schema, options)?;

        Ok(Box::new(StreamingParquetSink::new(
            schema, properties, sink,
        )?))
    }
}
