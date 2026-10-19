//! Writing, and the options that control it.

use std::io::{Error, Result as IoResult, Write};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::ipc::writer::{FileWriter, IpcWriteOptions, StreamWriter};
use arrow::ipc::{CompressionType, MetadataVersion};
use format_registry::options::FormatOptions;
use format_registry::provider::{ByteSink, RecordBatchSink, StreamingSink};
use format_registry::Result;

use crate::provider::{Variant, COMPRESSIONS};

/// Arrow's own default, and the only one worth offering: 8-byte alignment is what every reader
/// handles, and V5 is required for per-buffer compression to be legal at all.
const ALIGNMENT: usize = 8;

/// The `compression_level` option, parsed. Not `FormatOptions::get_usize`, because zstd's range
/// includes negative levels and silently rejecting `-1` as "not a number" would be wrong.
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

/// Turns the statement's options into arrow's write options.
///
/// The level is left to arrow to range check, rather than checked here against a range this
/// crate would have to keep in step with zstd's.
pub(crate) fn write_options(options: &FormatOptions) -> Result<IpcWriteOptions> {
    let mut write = IpcWriteOptions::try_new(ALIGNMENT, false, MetadataVersion::V5)
        .map_err(|e| format!("could not build the IPC write options: {e}"))?;

    if let Some(compression) = options.get_enum("compression", COMPRESSIONS)? {
        let codec = match compression.as_str() {
            "none" => None,
            "lz4" => Some(CompressionType::LZ4_FRAME),
            "zstd" => Some(CompressionType::ZSTD),
            // get_enum already refused anything else
            other => return Err(format!("unhandled compression \"{other}\"")),
        };

        write = write
            .try_with_compression(codec)
            .map_err(|e| format!("could not use compression \"{compression}\": {e}"))?;
    }

    if let Some(level) = compression_level(options)? {
        write = write
            .try_with_compression_level(Some(level))
            .map_err(|e| format!("could not use compression_level {level}: {e}"))?;
    }

    Ok(write)
}

/// One of the two IPC writers, over whatever they write into.
///
/// Both write strictly forwards and neither buffers a file: a `FileWriter` records each block's
/// offset as it goes and puts the list in the footer, rather than going back to patch anything.
/// That is why the same enum serves a `Vec<u8>` and an upload, and the only difference between
/// the two sinks below is what `W` is. The stream *variant* is a separate thing again — a
/// container without a footer, not an incremental write.
enum Writer<W: Write> {
    File(FileWriter<W>),
    Stream(StreamWriter<W>),
}

impl<W: Write> Writer<W> {
    fn new(
        inner: W,
        schema: SchemaRef,
        variant: Variant,
        options: IpcWriteOptions,
    ) -> Result<Self> {
        match variant {
            Variant::File => FileWriter::try_new_with_options(inner, schema.as_ref(), options)
                .map(Writer::File)
                .map_err(|e| format!("could not start an Arrow IPC file: {e}")),
            Variant::Stream => StreamWriter::try_new_with_options(inner, schema.as_ref(), options)
                .map(Writer::Stream)
                .map_err(|e| format!("could not start an Arrow IPC stream: {e}")),
        }
    }

    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        match self {
            Writer::File(writer) => writer.write(batch),
            Writer::Stream(writer) => writer.write(batch),
        }
        .map_err(|e| format!("could not write a record batch: {e}"))
    }

    /// Finishes the file and hands back what it was writing into.
    ///
    /// `into_inner` finishes first — the footer for a file, the end-of-stream marker for a
    /// stream — so the file is complete in both cases by the time this returns.
    fn finish(self) -> Result<W> {
        match self {
            Writer::File(writer) => writer.into_inner(),
            Writer::Stream(writer) => writer.into_inner(),
        }
        .map_err(|e| format!("could not finish the Arrow IPC file: {e}"))
    }
}

/// The buffered writer: the whole file in memory, handed back at `finish`.
pub struct IpcSink {
    writer: Writer<Vec<u8>>,
}

impl IpcSink {
    pub fn new(schema: SchemaRef, variant: Variant, options: IpcWriteOptions) -> Result<Self> {
        Ok(Self {
            writer: Writer::new(Vec::new(), schema, variant, options)?,
        })
    }
}

impl RecordBatchSink for IpcSink {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer.write_batch(batch)
    }

    fn finish(self: Box<Self>) -> Result<Vec<u8>> {
        self.writer.finish()
    }
}

/// The streaming writer: the same bytes, but through the base's sink as they are produced, so
/// that a `COPY TO` of a large table never holds the file in the backend's memory.
pub struct StreamingIpcSink {
    writer: Writer<SinkWrite>,
}

impl StreamingIpcSink {
    pub fn new(
        schema: SchemaRef,
        variant: Variant,
        options: IpcWriteOptions,
        sink: Box<dyn ByteSink>,
    ) -> Result<Self> {
        Ok(Self {
            writer: Writer::new(SinkWrite { sink }, schema, variant, options)?,
        })
    }
}

impl StreamingSink for StreamingIpcSink {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer.write_batch(batch)
    }

    fn finish(self: Box<Self>) -> Result<()> {
        // the footer goes through the sink like everything else; the base finishes the upload
        self.writer.finish().map(|_| ())
    }
}

/// A `ByteSink` as a `Write`.
///
/// The IPC writers are synchronous and the sink is the base's upload, so a `write` here is a
/// `put_part` down in tier 1 once a part has filled. A failure can only leave upstream's frames
/// as an `io::Error`, which carries the sink's message; the base reports its own message, which
/// names the uri, rather than this copy of it.
struct SinkWrite {
    sink: Box<dyn ByteSink>,
}

impl Write for SinkWrite {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        self.sink.write(buf).map_err(Error::other)?;

        // all of it, always: a `ByteSink` has no short writes
        Ok(buf.len())
    }

    fn flush(&mut self) -> IoResult<()> {
        // nothing is buffered here, and the upload is the base's to finish
        Ok(())
    }
}
