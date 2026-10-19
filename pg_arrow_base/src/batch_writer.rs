//! Tuples to a file, for whichever format the statement named.
//!
//! This is the middle of `COPY TO`: the dest receiver hands it batches of tuples, it turns them
//! into `RecordBatch`es with `pg-arrow-convert`, and the provider turns those into a file. The
//! two halves it joins are the two halves of the architecture, which is why the type exists at
//! all rather than the receiver calling a provider directly — a provider has two shapes and the
//! receiver should not have to know which one it got.
//!
//! Those shapes are:
//!
//! - **streaming**, if the provider reports `can_stream_write`: the file goes into tier 1's
//!   upload as it is produced, so a `COPY TO` of any size costs a 10 MiB buffer.
//! - **buffered**, otherwise: the provider holds the whole file and hands it over at `finish`,
//!   and the base writes it with one `put`. Correct, and bounded by the backend's memory.
//!
//! A format ends up buffered because of its container, not because of a missing feature — a
//! footer that has to record the size of what precedes it cannot be streamed — so this is a
//! fork in the path and not a fallback to be warned about.

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use format_registry::options::{FormatOptions, FILE_NAME_OPTION};
use format_registry::provider::{FormatProvider, RecordBatchSink, StreamingSink};
use pg_arrow_convert::pg_to_arrow::context::{
    collect_pg_to_arrow_attribute_contexts, PgToArrowAttributeContext,
};
use pg_arrow_convert::pg_to_arrow::to_arrow_array;
use pg_arrow_convert::pgrx_utils::{collect_attributes_for, CollectAttributesFor};
use pg_arrow_convert::schema::arrow_schema_from_attributes;
use pg_arrow_convert::type_compat::geometry::reset_postgis_context;
use pg_arrow_convert::type_compat::map::reset_map_context;
use pgrx::{pg_sys::Datum, prelude::*, PgTupleDesc};
use std::sync::Arc;

use crate::io::{self, Place, Upload};

/// How the file is actually being produced.
enum Sink {
    Streaming {
        writer: Box<dyn StreamingSink>,
        /// `Option` only so that `finish` can take it out; it is `Some` for the whole life of
        /// the writer. Dropping it without finishing is what discards the object.
        upload: Option<Upload>,
    },
    Buffered {
        writer: Box<dyn RecordBatchSink>,
        place: Place,
    },
}

pub(crate) struct BatchWriter {
    schema: SchemaRef,
    attribute_contexts: Vec<PgToArrowAttributeContext>,
    sink: Sink,
    /// Bytes handed to tier 1 so far. Only moves on the streaming path: a buffered provider has
    /// written nothing anywhere until `finish`, and reporting its in-memory size as progress
    /// would be reporting the wrong thing.
    bytes_written: i64,
    /// Arrow bytes handed to a *buffered* provider so far, which is the only size signal there
    /// is for one. See [`BatchWriter::size_so_far`].
    arrow_bytes_given: i64,
}

/// The statement's options plus the name of the file being written.
///
/// The name, not the uri: a provider has no business with a bucket or a credential, and the
/// only thing a format legitimately wants from a uri is what its own extensions say about it —
/// `orders.parquet.zst` is zstd. Everything after the last `/`, and the whole of a uri that has
/// none, with any query string left on, since that is the provider's to ignore.
fn with_file_name(options: &FormatOptions, uri: &str) -> FormatOptions {
    let mut options = options.clone();

    options.push(
        FILE_NAME_OPTION,
        uri.rsplit_once('/').map(|(_, name)| name).unwrap_or(uri),
    );

    options
}

impl BatchWriter {
    /// Works out the schema of the tuples, opens the provider's writer, and the object if the
    /// provider streams.
    pub(crate) fn open(
        provider: Arc<dyn FormatProvider>,
        place: &Place,
        options: &FormatOptions,
        tupledesc: &PgTupleDesc,
    ) -> BatchWriter {
        // both caches are keyed on oids that a dropped and recreated extension reuses, and a
        // COPY is the only thing that reads them, so the start of one is where they are cleared
        reset_postgis_context();
        reset_map_context();

        let attributes = collect_attributes_for(CollectAttributesFor::CopyTo, tupledesc);

        let schema: SchemaRef = Arc::new(arrow_schema_from_attributes(&attributes));

        let attribute_contexts =
            collect_pg_to_arrow_attribute_contexts(&attributes, &schema.fields);

        let options = &with_file_name(options, place.path());

        let sink = if provider.can_stream_write() {
            // the object is opened before the provider's writer, so that an unwritable uri is
            // reported before a format has had a chance to produce a header
            let upload = Upload::open(place);

            let writer = provider
                .open_streaming_writer(schema.clone(), options, upload.sink())
                .unwrap_or_else(|e| error!("{e}"));

            Sink::Streaming {
                writer,
                upload: Some(upload),
            }
        } else {
            let writer = provider
                .open_writer(schema.clone(), options)
                .unwrap_or_else(|e| error!("{e}"));

            Sink::Buffered {
                writer,
                place: place.clone(),
            }
        };

        BatchWriter {
            schema,
            attribute_contexts,
            sink,
            bytes_written: 0,
            arrow_bytes_given: 0,
        }
    }

    /// Converts a batch of staged columns and writes it.
    ///
    /// One `Vec` per attribute, each holding that attribute's datum for every row of the batch;
    /// `None` is a null value. The columns are in the order of the attribute contexts, which is
    /// the order of the schema.
    pub(crate) fn write_columns(&mut self, columns: &[Vec<Option<Datum>>]) {
        let arrays = self
            .attribute_contexts
            .iter()
            .zip(columns)
            .map(|(context, datums)| to_arrow_array(datums, context))
            .collect();

        let batch = RecordBatch::try_new(self.schema.clone(), arrays)
            .unwrap_or_else(|e| error!("could not build a record batch from the rows: {e}"));

        self.write_batch(&batch);
    }

    fn write_batch(&mut self, batch: &RecordBatch) {
        match &mut self.sink {
            Sink::Streaming { writer, upload } => {
                writer.write_batch(batch).unwrap_or_else(|e| error!("{e}"));

                // the provider's own count first: its writer buffers, so what has reached the
                // upload is behind the file by up to that buffer — far enough for a small
                // `COPY TO` to look like it never wrote a byte
                self.bytes_written = writer.bytes_written().unwrap_or_else(|| {
                    upload
                        .as_ref()
                        .map(Upload::bytes_written)
                        .unwrap_or(self.bytes_written)
                });
            }
            Sink::Buffered { writer, .. } => {
                writer.write_batch(batch).unwrap_or_else(|e| error!("{e}"));

                self.arrow_bytes_given += batch.get_array_memory_size() as i64;
            }
        }
    }

    /// Finishes the file and returns how many bytes it is.
    ///
    /// The object becomes visible here and nowhere earlier, on both paths: a statement that
    /// errors partway through leaves no file at all rather than a truncated one.
    pub(crate) fn finish(self) -> i64 {
        match self.sink {
            Sink::Streaming { writer, upload } => {
                // the provider writes its footer through the sink, and only then is the file
                // whole enough to complete the upload
                writer.finish().unwrap_or_else(|e| error!("{e}"));

                let upload = upload.expect("the upload outlives every write");
                let bytes = upload.bytes_written();

                upload.finish().unwrap_or_else(|e| error!("{e}"));

                bytes
            }
            Sink::Buffered { writer, place } => {
                let data = writer.finish().unwrap_or_else(|e| error!("{e}"));

                io::write_all(&place, &data);

                data.len() as i64
            }
        }
    }

    pub(crate) fn bytes_written(&self) -> i64 {
        self.bytes_written
    }

    /// How large the file being written is, as `file_size_bytes` measures it.
    ///
    /// Exact on the streaming path and an over-estimate on the buffered one, where the file does
    /// not exist yet and the Arrow bytes the provider was given are all there is to go on. A
    /// buffered format with a compressed container therefore splits earlier than asked — more
    /// files, each smaller than the threshold — which is the safe direction to be wrong in.
    pub(crate) fn size_so_far(&self) -> i64 {
        match &self.sink {
            Sink::Streaming { .. } => self.bytes_written,
            Sink::Buffered { .. } => self.arrow_bytes_given,
        }
    }
}
