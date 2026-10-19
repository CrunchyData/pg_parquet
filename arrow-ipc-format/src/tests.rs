//! The whole provider, with no Postgres anywhere.
//!
//! What is deliberately not here: anything about the C descriptor or `_PG_init`. Those live in
//! the `pg_arrow` crate next door and need a backend, so they are `#[pg_test]`s.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::array::{Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use format_registry::options::FormatOptions;
use format_registry::provider::{ByteSink, FormatProvider};
use format_registry::Result;

use crate::{ArrowIpc, Variant, OPTIONS};

fn schema() -> SchemaRef {
    let mut metadata = HashMap::new();
    metadata.insert("written_by".to_string(), "arrow-ipc-format".to_string());
    metadata.insert("purpose".to_string(), "a test".to_string());

    Arc::new(
        Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("label", DataType::Utf8, true),
        ])
        .with_metadata(metadata),
    )
}

/// Repetitive on purpose: the compression tests need output that a codec can actually shrink.
fn batch(rows: i32) -> RecordBatch {
    let ids = Int32Array::from_iter_values(0..rows);
    let labels = StringArray::from_iter_values((0..rows).map(|_| "the same string every time"));

    RecordBatch::try_new(schema(), vec![Arc::new(ids), Arc::new(labels)])
        .expect("the arrays match the schema")
}

fn options(pairs: &[(&str, &str)]) -> FormatOptions {
    let mut options = FormatOptions::new();

    for (name, value) in pairs {
        options.push(*name, *value);
    }

    options
}

fn write(batches: &[RecordBatch], pairs: &[(&str, &str)]) -> Vec<u8> {
    let mut sink = ArrowIpc
        .open_writer(schema(), &options(pairs))
        .expect("the options are valid");

    for batch in batches {
        sink.write_batch(batch).expect("failed to write a batch");
    }

    sink.finish().expect("failed to finish the file")
}

fn read(data: &[u8]) -> Vec<RecordBatch> {
    let reader = ArrowIpc
        .open_reader(Bytes::copy_from_slice(data), &FormatOptions::new())
        .expect("failed to open the file");

    reader
        .map(|batch| batch.expect("failed to read a batch"))
        .collect()
}

#[test]
fn a_file_round_trips() {
    let written = batch(64);
    let data = write(std::slice::from_ref(&written), &[]);

    assert_eq!(Variant::of(&data), Variant::File);
    assert_eq!(read(&data), vec![written]);
}

#[test]
fn a_stream_round_trips() {
    let written = batch(64);
    let data = write(std::slice::from_ref(&written), &[("variant", "stream")]);

    // no ARROW1, which is the only thing that distinguishes the two containers on read
    assert_eq!(Variant::of(&data), Variant::Stream);
    assert_eq!(read(&data), vec![written]);
}

#[test]
fn batches_are_kept_separate() {
    // one batch per split, because a reader that silently concatenated them would make
    // num_splits a lie and a per-split parallel read impossible
    let batches = vec![batch(8), batch(8), batch(8)];
    let data = write(&batches, &[]);

    assert_eq!(read(&data), batches);
}

#[test]
fn compression_round_trips_and_shrinks_the_file() {
    let written = batch(2048);
    let plain = write(std::slice::from_ref(&written), &[]);

    for codec in ["lz4", "zstd"] {
        let compressed = write(std::slice::from_ref(&written), &[("compression", codec)]);

        assert_eq!(read(&compressed), vec![written.clone()], "{codec}");

        // proves the option reached arrow rather than being parsed and dropped
        assert!(
            compressed.len() < plain.len(),
            "{codec} produced {} bytes, uncompressed is {}",
            compressed.len(),
            plain.len()
        );
    }
}

#[test]
fn a_compression_level_is_passed_through() {
    let written = batch(2048);

    let data = write(
        std::slice::from_ref(&written),
        &[("compression", "zstd"), ("compression_level", "9")],
    );

    assert_eq!(read(&data), vec![written]);
}

#[test]
fn compression_none_is_the_same_as_no_compression() {
    let written = batch(256);

    assert_eq!(
        write(std::slice::from_ref(&written), &[("compression", "none")]),
        write(std::slice::from_ref(&written), &[])
    );
}

#[test]
fn the_schema_survives_including_its_metadata() {
    let data = write(&[batch(4)], &[]);

    let read_back = ArrowIpc.schema(&data).expect("failed to read the schema");

    assert_eq!(read_back, schema());
}

#[test]
fn kv_metadata_is_the_schemas_metadata_sorted() {
    let data = write(&[batch(4)], &[]);

    let metadata = ArrowIpc
        .kv_metadata(&data)
        .expect("failed to read the metadata");

    // sorted, and every value present: an IPC KeyValue always carries one, so none of these can
    // be the key-without-a-value that parquet allows
    assert_eq!(
        metadata,
        vec![
            ("purpose".to_string(), Some("a test".to_string())),
            (
                "written_by".to_string(),
                Some("arrow-ipc-format".to_string())
            ),
        ]
    );
}

#[test]
fn a_files_shape_is_read_from_its_footer() {
    let data = write(&[batch(8), batch(8), batch(8)], &[]);

    let info = ArrowIpc
        .file_info(&data)
        .expect("failed to describe the file");

    assert_eq!(info.num_splits, Some(3));
    assert_eq!(info.split_label.as_deref(), Some("record batch"));
    assert_eq!(info.format_version.as_deref(), Some("Arrow IPC file"));

    // both unknowable without decoding every block; see the crate docs
    assert_eq!(info.num_rows, None);
    assert_eq!(info.compression, None);
}

#[test]
fn a_streams_shape_is_mostly_unknown() {
    let data = write(&[batch(8), batch(8)], &[("variant", "stream")]);

    let info = ArrowIpc
        .file_info(&data)
        .expect("failed to describe the stream");

    // no footer, so not even the number of batches
    assert_eq!(info.num_splits, None);
    assert_eq!(info.format_version.as_deref(), Some("Arrow IPC stream"));
}

#[test]
fn compressed_files_still_report_no_compression() {
    // a documented NULL rather than an oversight: the codec is per buffer in each record batch,
    // and a file is allowed to mix them, so there is no single honest answer
    let data = write(&[batch(64)], &[("compression", "zstd")]);

    let info = ArrowIpc
        .file_info(&data)
        .expect("failed to describe the file");

    assert_eq!(info.compression, None);
}

#[test]
fn garbage_is_reported_as_unreadable() {
    let error = ArrowIpc
        .schema(b"not an arrow file at all")
        .expect_err("garbage should not parse");

    assert!(error.contains("not a readable Arrow IPC stream"), "{error}");

    // the magic is what picks the reader, so a truncated file is reported as a bad *file*
    let error = ArrowIpc
        .schema(b"ARROW1\0\0truncated")
        .expect_err("a truncated file should not parse");

    assert!(error.contains("not a readable Arrow IPC file"), "{error}");
}

#[test]
fn an_unknown_option_names_the_options_that_exist() {
    let error = ArrowIpc
        .validate_option("compresion", "zstd")
        .expect_err("a typo should be refused");

    assert!(error.contains("has no option \"compresion\""), "{error}");

    for option in OPTIONS {
        assert!(error.contains(option), "{error} does not mention {option}");
    }
}

#[test]
fn an_option_value_is_checked_before_any_io() {
    let error = ArrowIpc
        .validate_option("compression", "brotli")
        .expect_err("brotli is not an IPC codec");

    assert!(error.contains("brotli"), "{error}");

    let error = ArrowIpc
        .validate_option("variant", "feather")
        .expect_err("the variants are file and stream");

    assert!(error.contains("feather"), "{error}");

    let error = ArrowIpc
        .validate_option("compression_level", "high")
        .expect_err("a level is a number");

    assert!(error.contains("must be an integer"), "{error}");

    // negative levels are real in zstd, which is why the level is not parsed as unsigned
    ArrowIpc
        .validate_option("compression_level", "-1")
        .expect("a negative level is a number");

    for value in ["file", "stream"] {
        ArrowIpc
            .validate_option("variant", value)
            .unwrap_or_else(|e| panic!("{value} should be a variant: {e}"));
    }
}

#[test]
fn the_providers_identity_is_what_the_descriptor_publishes() {
    // the C descriptor in `pg_arrow` repeats these as static strings, so a change here that is
    // not made there would silently register a provider under the wrong name
    assert_eq!(ArrowIpc.name(), "arrow");
    assert_eq!(
        ArrowIpc.file_extensions(),
        vec!["arrow", "arrows", "ipc", "feather"]
    );
}

/// A `ByteSink` that keeps what was written, so a streaming write can be read back.
#[derive(Clone, Default)]
struct Collected(Arc<Mutex<Vec<u8>>>);

impl ByteSink for Collected {
    fn write(&mut self, data: &[u8]) -> Result<()> {
        self.0
            .lock()
            .expect("the lock is never poisoned")
            .extend_from_slice(data);

        Ok(())
    }
}

/// A `ByteSink` that fails on the nth write, standing in for an upload that breaks partway.
struct Breaks {
    writes: usize,
    fails_on: usize,
}

impl ByteSink for Breaks {
    fn write(&mut self, _data: &[u8]) -> Result<()> {
        self.writes += 1;

        if self.writes == self.fails_on {
            return Err("the upload broke".to_string());
        }

        Ok(())
    }
}

/// Writes through the streaming sink, which is the path a `COPY TO` takes.
fn write_streaming(batches: &[RecordBatch], pairs: &[(&str, &str)]) -> Vec<u8> {
    let collected = Collected::default();

    let mut sink = ArrowIpc
        .open_streaming_writer(schema(), &options(pairs), Box::new(collected.clone()))
        .expect("the options are valid");

    for batch in batches {
        sink.write_batch(batch).expect("failed to write a batch");
    }

    sink.finish().expect("failed to finish the file");

    let data = collected.0.lock().expect("the lock is never poisoned");

    data.clone()
}

#[test]
fn a_streamed_file_is_byte_for_byte_the_buffered_one() {
    // the point of the streaming path is where the bytes go, not what they are. Both variants,
    // because the footer is written at `finish` for one and not at all for the other, and that
    // is the step most likely to differ between the two writers.
    for variant in ["file", "stream"] {
        let batches = [batch(64), batch(32)];
        let pairs = [("variant", variant), ("compression", "zstd")];

        assert_eq!(
            write_streaming(&batches, &pairs),
            write(&batches, &pairs),
            "{variant}"
        );
    }
}

#[test]
fn a_streamed_file_round_trips() {
    let written = [batch(64), batch(32)];
    let data = write_streaming(&written, &[]);

    assert_eq!(Variant::of(&data), Variant::File);
    assert_eq!(read(&data), written);
}

#[test]
fn a_broken_upload_is_reported_rather_than_swallowed() {
    // the first write is the magic and the schema, so failing there fails `open`; failing later
    // fails a `write_batch` or the `finish`. Whichever it is, it has to come back as an error
    // and not as a short file that reads as valid.
    for fails_on in 1..=4 {
        let sink = Breaks {
            writes: 0,
            fails_on,
        };

        let opened = ArrowIpc.open_streaming_writer(schema(), &options(&[]), Box::new(sink));

        let error = match opened {
            Err(error) => error,
            Ok(mut writer) => {
                let written = writer
                    .write_batch(&batch(64))
                    .and_then(|()| writer.finish());

                match written {
                    Err(error) => error,
                    Ok(()) => panic!("the sink failed on write {fails_on} and nothing said so"),
                }
            }
        };

        // upstream wraps our `io::Error`, so the sink's own words have to still be in there
        assert!(error.contains("the upload broke"), "{fails_on}: {error}");
    }
}

/// A `ByteSource` over a file in memory, which counts what was actually fetched: a ranged read
/// that quietly pulled the whole file would pass every assertion about its rows.
struct Served {
    data: Vec<u8>,
    fetched: Mutex<i64>,
}

impl Served {
    fn of(data: Vec<u8>) -> Arc<Self> {
        Arc::new(Self {
            data,
            fetched: Mutex::new(0),
        })
    }

    fn fetched(&self) -> i64 {
        *self.fetched.lock().expect("the lock is never poisoned")
    }
}

impl format_registry::provider::ByteSource for Served {
    fn size(&self) -> Result<i64> {
        Ok(self.data.len() as i64)
    }

    /// A range is `(offset, length)`, as tier 1's is, and a read past the end of the file is
    /// refused rather than truncated — which is what a real store does, and the reason this
    /// fake is strict about it.
    fn get_ranges(&self, ranges: &[(i64, i64)]) -> Result<Vec<Bytes>> {
        let mut served = vec![];

        for &(offset, length) in ranges {
            let end = offset.checked_add(length).unwrap_or(i64::MAX);

            if offset < 0 || length < 0 || end > self.data.len() as i64 {
                return Err(format!(
                    "{length} bytes at {offset} is not in a {} byte file",
                    self.data.len()
                ));
            }

            *self.fetched.lock().expect("the lock is never poisoned") += length;

            served.push(Bytes::copy_from_slice(
                &self.data[offset as usize..end as usize],
            ));
        }

        Ok(served)
    }
}

/// The batches of a split range, read the way one worker of a parallel `COPY FROM` reads them.
fn read_splits(source: Arc<Served>, splits: Option<std::ops::Range<i64>>) -> Vec<RecordBatch> {
    let reader = ArrowIpc
        .open_ranged_reader(source, &FormatOptions::new(), splits)
        .expect("failed to open the file");

    reader
        .map(|batch| batch.expect("failed to read a batch"))
        .collect()
}

#[test]
fn a_files_shape_is_read_from_its_footer_rather_than_from_the_file() {
    let source = Served::of(write(&[batch(512), batch(512), batch(512)], &[]));
    let size = source.data.len() as i64;

    let info = ArrowIpc
        .ranged_file_info(source.clone())
        .expect("failed to describe the file");

    assert_eq!(info.num_splits, Some(3));
    assert_eq!(info.format_version.as_deref(), Some("Arrow IPC file"));

    // the magic and the footer, not the blocks: what is being proved is that counting the
    // splits of a file does not download it
    assert!(
        source.fetched() < size / 2,
        "fetched {} of {size} bytes to read a footer",
        source.fetched()
    );
}

#[test]
fn a_split_range_reads_its_own_record_batches_and_no_others() {
    let written = write(&[batch(1), batch(2), batch(3), batch(4)], &[]);

    let rows = |splits| {
        read_splits(Served::of(written.clone()), splits)
            .iter()
            .map(|batch| batch.num_rows())
            .collect::<Vec<_>>()
    };

    assert_eq!(rows(Some(1..3)), vec![2, 3]);
    assert_eq!(rows(Some(0..1)), vec![1]);
    assert_eq!(rows(Some(3..4)), vec![4]);

    // the two halves a pair of workers would be given, which cover the file exactly once
    assert_eq!(rows(Some(0..2)), vec![1, 2]);
    assert_eq!(rows(Some(2..4)), vec![3, 4]);

    // and no range at all is the whole file, which is what a single process read asks for
    assert_eq!(rows(None), vec![1, 2, 3, 4]);
}

#[test]
fn a_split_read_fetches_only_the_blocks_it_wants() {
    // batches larger than the source adapter's window, so that a block is fetched as the one
    // range it is rather than as a window that happens to cover its neighbours too
    let written = write(&[batch(8192), batch(8192), batch(8192), batch(8192)], &[]);
    let size = written.len() as i64;

    let source = Served::of(written);
    read_splits(source.clone(), Some(1..2));

    // one block of four, plus the footer: well under half the file
    assert!(
        source.fetched() < size / 2,
        "fetched {} of {size} bytes to read one of four batches",
        source.fetched()
    );
}

#[test]
fn the_batches_of_a_range_are_the_ones_a_whole_read_gives() {
    let written = write(&[batch(8), batch(16), batch(32)], &[]);

    let whole = read(&written);
    let ranged = read_splits(Served::of(written.clone()), Some(0..3));

    assert_eq!(whole, ranged);
}

#[test]
fn a_stream_is_read_whole_or_not_at_all() {
    let written = write(&[batch(8), batch(16)], &[("variant", "stream")]);

    // no footer, so nothing to divide: the base reads such a file in one process, which it is
    // told by a split count it cannot have
    let info = ArrowIpc
        .ranged_file_info(Served::of(written.clone()))
        .expect("failed to describe the stream");

    assert_eq!(info.num_splits, None);
    assert_eq!(info.format_version.as_deref(), Some("Arrow IPC stream"));

    // and the whole of it still reads through a source
    let rows: Vec<_> = read_splits(Served::of(written.clone()), None)
        .iter()
        .map(|batch| batch.num_rows())
        .collect();

    assert_eq!(rows, vec![8, 16]);

    let error = ArrowIpc
        .open_ranged_reader(Served::of(written), &FormatOptions::new(), Some(0..1))
        .err()
        .expect("a range of a stream is not readable");

    assert!(error.contains("has no footer"), "{error}");
}

#[test]
fn a_range_the_file_does_not_hold_is_refused() {
    let source = Served::of(write(&[batch(8), batch(8)], &[]));

    let error = ArrowIpc
        .open_ranged_reader(source, &FormatOptions::new(), Some(1..5))
        .err()
        .expect("a range past the last batch is not readable");

    assert!(error.contains("file that holds 2"), "{error}");
}

#[test]
fn an_empty_file_has_no_splits_to_read() {
    let source = Served::of(write(&[], &[]));

    let info = ArrowIpc
        .ranged_file_info(source.clone())
        .expect("failed to describe the file");

    assert_eq!(info.num_splits, Some(0));
    assert!(read_splits(source, None).is_empty());
}
