//! The two shapes of writer the provider ABI asks for.
//!
//! Both are the same `ArrowWriter` over a different `std::io::Write`: a `Vec<u8>` for the
//! buffered form, and an adapter over tier 2's [`ByteSink`] for the streaming one. Parquet can
//! stream because its footer is written last and everything before it is final once written,
//! so [`StreamingParquetSink`] is the form a `COPY TO` actually takes — the buffered one exists
//! for `open_writer`, which the base only calls for a format that cannot stream.
//!
//! Both also install the geospatial statistics accumulator factory before the first writer
//! exists, which is where upstream requires it to happen.
//!
//! Row group sizing is upstream's: `max_row_group_row_count` and `max_row_group_bytes` are set
//! from the statement's options, and parquet splits a batch across row groups itself rather
//! than overshooting to the end of one, which is what `pg_parquet` 0.6's flush-after-each-batch
//! did.

use std::io::{Error, Result as IoResult, Write};

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use format_registry::provider::{ByteSink, RecordBatchSink, StreamingSink};
use format_registry::Result;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use crate::geospatial_stats::install_geospatial_stats_accumulator_factory;

pub struct BufferedParquetSink {
    writer: ArrowWriter<Vec<u8>>,
}

impl BufferedParquetSink {
    pub fn new(schema: SchemaRef, properties: WriterProperties) -> Result<Self> {
        install_geospatial_stats_accumulator_factory();

        let writer = ArrowWriter::try_new(Vec::new(), schema, Some(properties))
            .map_err(|e| format!("could not start a parquet file: {e}"))?;

        Ok(Self { writer })
    }
}

impl RecordBatchSink for BufferedParquetSink {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        write_batch(&mut self.writer, batch)
    }

    fn finish(self: Box<Self>) -> Result<Vec<u8>> {
        // `into_inner` writes the footer first, so the bytes are a complete file
        self.writer
            .into_inner()
            .map_err(|e| format!("could not finish the parquet file: {e}"))
    }
}

pub struct StreamingParquetSink {
    writer: ArrowWriter<SinkWrite>,
}

impl StreamingParquetSink {
    pub fn new(
        schema: SchemaRef,
        properties: WriterProperties,
        sink: Box<dyn ByteSink>,
    ) -> Result<Self> {
        install_geospatial_stats_accumulator_factory();

        let writer = ArrowWriter::try_new(SinkWrite { sink }, schema, Some(properties))
            .map_err(|e| format!("could not start a parquet file: {e}"))?;

        Ok(Self { writer })
    }
}

impl StreamingSink for StreamingParquetSink {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        write_batch(&mut self.writer, batch)
    }

    /// Everything the parquet writer has serialized, including the row groups still inside its
    /// own buffer: the base only sees bytes once that buffer spills, and a file smaller than it
    /// would otherwise report no progress at all.
    fn bytes_written(&self) -> Option<i64> {
        Some(self.writer.bytes_written() as i64)
    }

    fn finish(self: Box<Self>) -> Result<()> {
        // the footer goes through the sink like everything else; the base finishes the upload
        self.writer
            .into_inner()
            .map(|_| ())
            .map_err(|e| format!("could not finish the parquet file: {e}"))
    }
}

fn write_batch<W: Write + Send>(writer: &mut ArrowWriter<W>, batch: &RecordBatch) -> Result<()> {
    writer
        .write(batch)
        .map_err(|e| format!("could not write a record batch: {e}"))
}

/// A `ByteSink` as a `Write`.
///
/// The writer upstream is synchronous and the sink is the base's upload, so a `write` here is a
/// `put_part` down in tier 1 once a part has filled. A failure comes back as an `io::Error`
/// carrying the sink's message, which is the only way out of upstream's frames; the base
/// reports its own message rather than this copy of it.
struct SinkWrite {
    sink: Box<dyn ByteSink>,
}

// `ArrowWriter<W>` requires `W: Write + Send` so that a writer *may* be moved onto a worker
// thread; the synchronous writer never does, and the base drives this one from the backend that
// opened it — the only thread on which tier 1's handle behind the sink is valid at all. So the
// bound is satisfied here rather than pushed onto `ByteSink`, where it would be a claim about
// the base's upload that is not true: that handle is backend-local by design.
unsafe impl Send for SinkWrite {}

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
