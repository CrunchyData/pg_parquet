//! The [`FormatProvider`] implementation.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::ops::Range;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::ipc::reader::{FileReader, StreamReader};
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use bytes::Bytes;
use format_registry::options::FormatOptions;
use format_registry::provider::{
    ByteSink, ByteSource, FileInfo, FormatProvider, RecordBatchSink, StreamingSink,
};
use format_registry::Result;

use crate::source::SourceReader;
use crate::writer::{write_options, IpcSink, StreamingIpcSink};

/// The name in `COPY ... WITH (format 'arrow')`.
pub const FORMAT_NAME: &str = "arrow";

/// Extensions this format claims when a statement names no format. `.arrow` and `.feather` are
/// the file variant, `.arrows` the stream; all four infer *this provider*, which then reads
/// whichever variant the bytes turn out to be.
pub const FILE_EXTENSIONS: &[&str] = &["arrow", "arrows", "ipc", "feather"];

/// What one split is called here. A parquet row group's counterpart.
pub const SPLIT_LABEL: &str = "record batch";

/// Every option this provider takes, for the message a typo gets.
pub const OPTIONS: &[&str] = &["variant", "compression", "compression_level"];

pub(crate) const VARIANTS: &[&str] = &["file", "stream"];
pub(crate) const COMPRESSIONS: &[&str] = &["none", "lz4", "zstd"];

/// The six bytes an IPC *file* starts with. The stream variant starts with a message instead,
/// so this is the whole of variant detection on read.
const FILE_MAGIC: &[u8] = b"ARROW1";

/// Which of the two IPC containers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Variant {
    /// The random-access format: a magic header, the messages, a footer, the magic again. Also
    /// known as Feather v2.
    File,
    /// The streaming format: the messages and nothing else. No footer, so nothing can be known
    /// about the file without reading it.
    Stream,
}

impl Variant {
    /// What the bytes say they are. Everything that is not a file is treated as a stream and
    /// allowed to fail in the stream reader, which gives a better message than guessing here
    /// would: "not a readable Arrow IPC stream" plus arrow's own reason.
    pub fn of(data: &[u8]) -> Self {
        if data.starts_with(FILE_MAGIC) {
            Self::File
        } else {
            Self::Stream
        }
    }

    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Stream => "stream",
        }
    }
}

/// The Arrow IPC provider. Stateless: every call is a function of the bytes it is given.
#[derive(Debug, Default, Clone, Copy)]
pub struct ArrowIpc;

fn file_reader(data: &[u8]) -> Result<FileReader<Cursor<&[u8]>>> {
    FileReader::try_new(Cursor::new(data), None)
        .map_err(|e| format!("not a readable Arrow IPC file: {e}"))
}

fn stream_reader(data: &[u8]) -> Result<StreamReader<Cursor<&[u8]>>> {
    StreamReader::try_new(Cursor::new(data), None)
        .map_err(|e| format!("not a readable Arrow IPC stream: {e}"))
}

/// Which container a file the base holds is, out of its first six bytes rather than its whole
/// length.
fn source_variant(source: &Arc<dyn ByteSource>) -> Result<Variant> {
    let size = source.size()?.min(FILE_MAGIC.len() as i64);

    if size <= 0 {
        return Ok(Variant::Stream);
    }

    let magic = source.get_ranges(&[(0, size)])?;

    Ok(Variant::of(
        magic.first().map(|m| m.as_ref()).unwrap_or(&[]),
    ))
}

/// The record batches of a file's split range, read through the base's byte source.
///
/// `None` is every batch. A range is `[begin, end)` in batch numbers, which is what one worker
/// of a parallel `COPY FROM` is given; the reader stops at `end` rather than at the footer, so
/// the ranges of the workers cover the file exactly once between them.
fn source_file_reader(
    source: Arc<dyn ByteSource>,
    splits: Option<Range<i64>>,
) -> Result<SplitBatches> {
    let reader = SourceReader::open(source)?;

    let mut inner = FileReader::try_new(reader, None)
        .map_err(|e| format!("not a readable Arrow IPC file: {e}"))?;

    let num_batches = inner.num_batches() as i64;

    let splits = splits.unwrap_or(0..num_batches);

    if splits.start < 0 || splits.end > num_batches || splits.start > splits.end {
        return Err(format!(
            "record batches {}..{} were asked for of a file that holds {num_batches}",
            splits.start, splits.end
        ));
    }

    // set_index refuses the one-past-the-end batch, which an empty range is addressed by
    if splits.start < num_batches && splits.start != splits.end {
        inner
            .set_index(splits.start as usize)
            .map_err(|e| format!("cannot read from record batch {}: {e}", splits.start))?;
    }

    Ok(SplitBatches {
        inner,
        remaining: (splits.end - splits.start) as usize,
    })
}

struct SplitBatches {
    inner: FileReader<SourceReader>,
    remaining: usize,
}

impl SplitBatches {
    fn num_batches(&self) -> usize {
        self.inner.num_batches()
    }
}

impl Iterator for SplitBatches {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.remaining = self.remaining.checked_sub(1)?;

        self.inner.next()
    }
}

impl RecordBatchReader for SplitBatches {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

impl FormatProvider for ArrowIpc {
    fn name(&self) -> String {
        FORMAT_NAME.to_string()
    }

    fn file_extensions(&self) -> Vec<String> {
        FILE_EXTENSIONS.iter().map(|e| e.to_string()).collect()
    }

    fn validate_option(&self, name: &str, value: &str) -> Result<()> {
        // the base validates every option of a statement, in both directions, before it knows
        // whether it is reading or writing. So a write option on a COPY FROM is accepted here
        // and then ignored, exactly as an unused option is in core COPY.
        let mut options = FormatOptions::new();
        options.push(name, value);

        match name.to_ascii_lowercase().as_str() {
            "variant" => options.get_enum("variant", VARIANTS).map(|_| ()),
            "compression" => options.get_enum("compression", COMPRESSIONS).map(|_| ()),
            "compression_level" => crate::writer::compression_level(&options).map(|_| ()),
            _ => Err(format!(
                "the arrow format has no option \"{name}\"; it takes {}",
                OPTIONS.join(", ")
            )),
        }
    }

    fn schema(&self, data: &[u8]) -> Result<SchemaRef> {
        match Variant::of(data) {
            Variant::File => Ok(file_reader(data)?.schema()),
            Variant::Stream => Ok(stream_reader(data)?.schema()),
        }
    }

    /// The schema's metadata, plus the file variant's footer metadata.
    ///
    /// Every value is `Some`: an IPC `KeyValue` always carries one, so the key-without-a-value
    /// that parquet permits cannot be expressed here and never has to be reported.
    ///
    /// The two sources are merged, with the schema's winning a collision. They are written by
    /// different things — the schema's by whoever built the schema, the footer's by
    /// `FileWriter::write_metadata` — and a reader asking "what does this file say about
    /// itself" wants both.
    fn kv_metadata(&self, data: &[u8]) -> Result<Vec<(String, Option<String>)>> {
        // sorted and deduplicated, so that the same file always produces the same rows; the
        // underlying maps are unordered
        let mut metadata: BTreeMap<String, Option<String>> = BTreeMap::new();

        match Variant::of(data) {
            Variant::File => {
                let reader = file_reader(data)?;

                for (key, value) in reader.custom_metadata() {
                    metadata.insert(key.clone(), Some(value.clone()));
                }

                for (key, value) in reader.schema().metadata() {
                    metadata.insert(key.clone(), Some(value.clone()));
                }
            }
            Variant::Stream => {
                for (key, value) in stream_reader(data)?.schema().metadata() {
                    metadata.insert(key.clone(), Some(value.clone()));
                }
            }
        }

        Ok(metadata.into_iter().collect())
    }

    /// See the crate docs for why most of this is NULL. The short version: an IPC footer
    /// records where the record batches are, not what is in them.
    fn file_info(&self, data: &[u8]) -> Result<FileInfo> {
        let variant = Variant::of(data);

        let num_splits = match variant {
            // the footer's block list, so this is a read of the footer rather than of the file
            Variant::File => Some(file_reader(data)?.num_batches() as i64),
            // reading it would mean decoding the whole stream, which is not what a question
            // about the file's shape should cost
            Variant::Stream => {
                stream_reader(data)?;
                None
            }
        };

        Ok(FileInfo {
            num_rows: None,
            num_splits,
            split_label: Some(SPLIT_LABEL.to_string()),
            compression: None,
            format_version: Some(format!("Arrow IPC {}", variant.as_str())),
        })
    }

    fn open_reader(
        &self,
        data: Bytes,
        _options: &FormatOptions,
    ) -> Result<Box<dyn RecordBatchReader + Send>> {
        // `Cursor<Bytes>` rather than `Cursor<&[u8]>`: the reader outlives this call, and a
        // `Bytes` clone is a refcount bump rather than a copy of the file.
        match Variant::of(&data) {
            Variant::File => Ok(Box::new(
                FileReader::try_new(Cursor::new(data), None)
                    .map_err(|e| format!("not a readable Arrow IPC file: {e}"))?,
            )),
            Variant::Stream => Ok(Box::new(
                StreamReader::try_new(Cursor::new(data), None)
                    .map_err(|e| format!("not a readable Arrow IPC stream: {e}"))?,
            )),
        }
    }

    /// The footer of a file records where every record batch is, so a range of them can be read
    /// without the rest of the file. A stream cannot be, which is reported per file by
    /// [`Self::ranged_file_info`] rather than here: the answer to this question is about the
    /// format, and the format is both containers.
    fn can_read_ranges(&self) -> bool {
        true
    }

    /// What the footer says, out of a read of the footer.
    ///
    /// `num_splits` is `None` for a stream, which is how the base is told to read that file in
    /// one process; `num_rows` is `None` for both, because an IPC block records its size and
    /// not its row count.
    fn ranged_file_info(&self, source: Arc<dyn ByteSource>) -> Result<FileInfo> {
        let variant = source_variant(&source)?;

        let num_splits = match variant {
            Variant::File => Some(source_file_reader(source, None)?.num_batches() as i64),
            Variant::Stream => None,
        };

        Ok(FileInfo {
            num_rows: None,
            num_splits,
            split_label: Some(SPLIT_LABEL.to_string()),
            compression: None,
            format_version: Some(format!("Arrow IPC {}", variant.as_str())),
        })
    }

    /// Batches out of a file the base holds, optionally only the batches of a split range.
    ///
    /// A stream has to be read forwards from its first message, so a range of one is refused
    /// rather than served by reading everything before it: the base only asks for a range of a
    /// file whose [`Self::ranged_file_info`] counted splits, which a stream's does not.
    fn open_ranged_reader(
        &self,
        source: Arc<dyn ByteSource>,
        _options: &FormatOptions,
        splits: Option<Range<i64>>,
    ) -> Result<Box<dyn RecordBatchReader + Send>> {
        match source_variant(&source)? {
            Variant::File => Ok(Box::new(source_file_reader(source, splits)?)),
            Variant::Stream if splits.is_none() => {
                let reader = SourceReader::open(source)?;

                Ok(Box::new(StreamReader::try_new(reader, None).map_err(
                    |e| format!("not a readable Arrow IPC stream: {e}"),
                )?))
            }
            Variant::Stream => Err(
                "an Arrow IPC stream has no footer, so a range of its record batches cannot be \
                 read on its own"
                    .to_string(),
            ),
        }
    }

    fn open_writer(
        &self,
        schema: SchemaRef,
        options: &FormatOptions,
    ) -> Result<Box<dyn RecordBatchSink>> {
        Ok(Box::new(IpcSink::new(
            schema,
            variant_to_write(options)?,
            write_options(options)?,
        )?))
    }

    /// Both IPC writers write strictly forwards, so there is nothing to buffer for.
    fn can_stream_write(&self) -> bool {
        true
    }

    fn open_streaming_writer(
        &self,
        schema: SchemaRef,
        options: &FormatOptions,
        sink: Box<dyn ByteSink>,
    ) -> Result<Box<dyn StreamingSink>> {
        Ok(Box::new(StreamingIpcSink::new(
            schema,
            variant_to_write(options)?,
            write_options(options)?,
            sink,
        )?))
    }
}

/// The container to write, from the statement's options.
///
/// The file variant by default: it is the one that can be read out of order, and the one every
/// `.arrow` reader expects. A stream is for a consumer that wants to start reading before the
/// writer has finished, which an object store upload is not.
fn variant_to_write(options: &FormatOptions) -> Result<Variant> {
    Ok(match options.get_enum("variant", VARIANTS)?.as_deref() {
        Some("stream") => Variant::Stream,
        _ => Variant::File,
    })
}
