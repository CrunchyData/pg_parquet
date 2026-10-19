//! The trait a file format implements.

use std::ops::Range;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatchReader;
use bytes::Bytes;

use crate::options::FormatOptions;
use crate::Result;

/// Refuses a schema that cannot cross the C Data Interface, naming what is wrong with it.
///
/// A name in an `FFI_ArrowSchema` is a C string, so a nul byte in one is not exportable — and
/// arrow's export reports it as a producer error from inside the stream, by which time nothing
/// on the consumer side knows which field it was. A provider calls this before it exports, so
/// that a file with such a name is a message about the file rather than about the boundary.
pub fn check_exportable_schema(schema: &arrow::datatypes::Schema) -> Result<()> {
    fn check(fields: &arrow::datatypes::Fields) -> Result<()> {
        for field in fields {
            if field.name().contains('\0') {
                return Err(format!(
                    "field name \"{}\" contains a nul byte",
                    field.name().replace('\0', "\\0")
                ));
            }

            if let arrow::datatypes::DataType::Struct(fields) = field.data_type() {
                check(fields)?;
            }
        }

        Ok(())
    }

    check(schema.fields())
}

/// What `arrow.file_info()` reports. Every field is optional because a format is allowed not to
/// know: Arrow IPC does not record a row count in its footer without reading the blocks, and
/// compression is per-buffer there rather than per-file.
#[derive(Debug, Clone, Default)]
pub struct FileInfo {
    pub num_rows: Option<i64>,
    /// How many independently readable chunks the file has. Row groups in parquet, record
    /// batches in Arrow IPC, blocks in avro. This is the unit a parallel `COPY FROM` would hand
    /// one per worker, which is why it is in the common surface rather than in each format's.
    pub num_splits: Option<i64>,
    /// What to call one of those chunks in a message, singular and lower case: `row group`,
    /// `record batch`, `block`.
    pub split_label: Option<String>,
    pub compression: Option<String>,
    pub format_version: Option<String>,
}

/// Where a writer's batches go. `finish` consumes the sink, because for most formats the footer
/// can only be written once and the bytes are only valid after it is.
pub trait RecordBatchSink {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()>;

    /// Finishes the file and returns it whole.
    ///
    /// Whole-buffer, so the output sits in the backend's memory before the base uploads it.
    /// Fine for a small file and wrong for a multi-gigabyte `COPY TO`, which is what
    /// [`FormatProvider::open_streaming_writer`] is for.
    fn finish(self: Box<Self>) -> Result<Vec<u8>>;
}

/// Where a *streaming* writer's bytes go: the base's upload, one chunk at a time.
///
/// A provider never constructs one of these. It is handed one at `open_streaming_writer` and
/// writes the file into it as it goes, which is the whole difference from `RecordBatchSink`:
/// no finished buffer is ever materialized on either side of the boundary.
///
/// An `Err` means the upload failed. The provider's job on one is to stop and propagate it,
/// not to retry: the base has the real message and will report that one rather than whatever
/// the provider says about it.
pub trait ByteSink {
    fn write(&mut self, data: &[u8]) -> Result<()>;
}

/// A streaming writer: batches in, bytes out through the [`ByteSink`] it was opened with.
///
/// `finish` writes the footer through the sink and returns nothing, since by then the file is
/// already most of the way to the object store.
pub trait StreamingSink {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()>;

    /// How many bytes of the file exist so far, if the writer keeps count.
    ///
    /// The base otherwise counts what has reached its [`ByteSink`], which lags by whatever the
    /// provider's own writer is buffering — enough for a small `COPY TO` to report zero bytes
    /// from start to finish. A provider that knows the real figure reports it here.
    fn bytes_written(&self) -> Option<i64> {
        None
    }

    fn finish(self: Box<Self>) -> Result<()>;
}

/// Random access to a file the base holds, for a format whose reader does not want the whole
/// thing. Parquet is the reason this exists: it reads a footer, decides which row groups it
/// needs, and then fetches those.
///
/// `get_ranges` is plural because the fetches are concurrent inside tier 1, and calling a
/// singular form in a loop would serialize a read that upstream has spent years making
/// parallel. A range is `(offset, length)`, and one that runs past the end of the file is an
/// error rather than a short read, which is what the stores underneath do.
///
/// The buffers it returns are only guaranteed valid until the next call on the same source.
/// A reader that holds bytes for longer — and an Arrow decoder does — copies them, which the
/// `Bytes` here already are: cheap to clone, owned by the caller.
pub trait ByteSource: Send + Sync {
    fn size(&self) -> Result<i64>;

    fn get_ranges(&self, ranges: &[(i64, i64)]) -> Result<Vec<Bytes>>;
}

/// A file format, as tier 2 needs to see it.
///
/// Implementors get bytes and give back Arrow. Note what is *not* here: no uri, no object store,
/// no `pg_sys`, no tuple descriptor, no type mapping. A provider that finds itself wanting one
/// of those is a provider that is about to break the layering — the thing it wants almost
/// certainly belongs in `pg_arrow_base`, where it can be written once for every format.
pub trait FormatProvider: Send + Sync {
    /// The name in `COPY ... WITH (format 'x')`, lower case. Must be unique in a cluster.
    fn name(&self) -> String;

    /// File extensions this format claims, without the dot, lower case. Used only to infer a
    /// format when `COPY` did not name one; an inferred format is never preferred over an
    /// explicit one.
    fn file_extensions(&self) -> Vec<String>;

    /// Whether this format understands an option, before any I/O happens. Called once per
    /// option so that the message can name the offending one, and called even for a statement
    /// that turns out to read nothing.
    fn validate_option(&self, name: &str, value: &str) -> Result<()>;

    /// The Arrow schema of a file, including its field-level metadata.
    fn schema(&self, data: &[u8]) -> Result<SchemaRef>;

    /// The file-level key/value metadata. A value is `None` for a key stored with no value,
    /// which parquet permits and which is distinct from an empty string.
    fn kv_metadata(&self, data: &[u8]) -> Result<Vec<(String, Option<String>)>>;

    fn file_info(&self, data: &[u8]) -> Result<FileInfo>;

    /// The same, for a file the base is *not* holding: one read through a [`ByteSource`].
    ///
    /// A format that reads in ranges is never downloaded whole, so this is the only way the row
    /// count of such a file can be known — and the base needs it to report a `bytes_total` in
    /// `pg_stat_progress_copy`. The default is "nothing known", which is also what a provider
    /// should return rather than fetching the whole file to answer.
    fn ranged_file_info(&self, _source: Arc<dyn ByteSource>) -> Result<FileInfo> {
        Ok(FileInfo::default())
    }

    /// Batches out of a file. `Bytes` rather than `&[u8]` so that a reader may hold a cheap
    /// clone of the buffer for as long as it needs rather than copying out of it.
    fn open_reader(
        &self,
        data: Bytes,
        options: &FormatOptions,
    ) -> Result<Box<dyn RecordBatchReader + Send>>;

    fn open_writer(
        &self,
        schema: SchemaRef,
        options: &FormatOptions,
    ) -> Result<Box<dyn RecordBatchSink>>;

    /// Whether this format can read from a [`ByteSource`] rather than from a whole buffer.
    /// Asked before any I/O, because the answer decides whether the base fetches the file at
    /// all.
    fn can_read_ranges(&self) -> bool {
        false
    }

    /// Batches out of a file the provider fetches in pieces, optionally restricted to a range
    /// of splits — row groups in parquet, record batches in Arrow IPC — which is what hands one
    /// worker of a parallel `COPY FROM` its share.
    ///
    /// `splits` is a half-open `[begin, end)` in split numbers. `None` means the whole file.
    /// A provider that reports `can_read_ranges` must honour it; one that cannot restrict
    /// reads must report `false` and be read whole, because silently reading everything in
    /// every worker would multiply the rows.
    fn open_ranged_reader(
        &self,
        _source: Arc<dyn ByteSource>,
        _options: &FormatOptions,
        _splits: Option<Range<i64>>,
    ) -> Result<Box<dyn RecordBatchReader + Send>> {
        Err(format!(
            "{} cannot read a file in ranges; it has to be read whole",
            self.name()
        ))
    }

    /// Whether this format can write incrementally into a [`ByteSink`]. A format whose
    /// container needs the finished size up front cannot, and says so here rather than
    /// failing partway through a `COPY TO`.
    fn can_stream_write(&self) -> bool {
        false
    }

    fn open_streaming_writer(
        &self,
        _schema: SchemaRef,
        _options: &FormatOptions,
        _sink: Box<dyn ByteSink>,
    ) -> Result<Box<dyn StreamingSink>> {
        Err(format!(
            "{} cannot write a file incrementally; it has to be buffered whole",
            self.name()
        ))
    }
}
