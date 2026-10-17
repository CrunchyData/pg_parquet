//! The provider on its own: batches in, a parquet file out, batches back.
//!
//! No Postgres and no object store anywhere, which is the point of the crate boundary — a
//! `ByteSink` is a `Vec<u8>` here and an upload in the extension, and neither this code nor the
//! tests can tell the difference.

use std::sync::{Arc, Mutex};

use arrow::array::{new_null_array, Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::record_batch::RecordBatchReader;
use bytes::Bytes;
use format_registry::options::FormatOptions;
use format_registry::provider::{ByteSink, ByteSource, FormatProvider};
use format_registry::Result;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::field_ids::{stamp_field_ids, FieldIds};
use crate::options::FILE_NAME_OPTION;
use crate::provider::Parquet;

/// A `ByteSink` that keeps the file, so a streaming write can be read back.
#[derive(Clone, Default)]
struct Collected(Arc<Mutex<Vec<u8>>>);

impl Collected {
    fn bytes(&self) -> Bytes {
        Bytes::from(self.0.lock().expect("the lock is never poisoned").clone())
    }
}

impl ByteSink for Collected {
    fn write(&mut self, data: &[u8]) -> Result<()> {
        self.0
            .lock()
            .expect("the lock is never poisoned")
            .extend_from_slice(data);

        Ok(())
    }
}

/// A `ByteSource` over a buffer, counting what was asked for: a ranged read that fetched the
/// whole file would pass every assertion about its rows and still be the bug this exists to
/// avoid.
struct Buffer {
    data: Bytes,
    fetched: Arc<Mutex<usize>>,
}

impl Buffer {
    fn new(data: Bytes) -> Self {
        Self {
            data,
            fetched: Arc::new(Mutex::new(0)),
        }
    }
}

impl ByteSource for Buffer {
    fn size(&self) -> Result<i64> {
        Ok(self.data.len() as i64)
    }

    fn get_ranges(&self, ranges: &[(i64, i64)]) -> Result<Vec<Bytes>> {
        ranges
            .iter()
            .map(|(offset, length)| {
                let offset = *offset as usize;
                let length = *length as usize;

                if offset + length > self.data.len() {
                    return Err(format!("{offset}..{length} is past the end"));
                }

                *self.fetched.lock().expect("the lock is never poisoned") += length;

                Ok(self.data.slice(offset..offset + length))
            })
            .collect()
    }
}

fn two_columns() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("label", DataType::Utf8, true),
    ]))
}

/// `rows` rows of `(i, "row i")`, in batches of `batch_size`.
fn batches(rows: i32, batch_size: i32) -> Vec<RecordBatch> {
    let schema = two_columns();

    (0..rows)
        .step_by(batch_size as usize)
        .map(|start| {
            let end = (start + batch_size).min(rows);

            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from_iter_values(start..end)),
                    Arc::new(StringArray::from_iter_values(
                        (start..end).map(|i| format!("row {i}")),
                    )),
                ],
            )
            .expect("the arrays match the schema")
        })
        .collect()
}

fn options(pairs: &[(&str, &str)]) -> FormatOptions {
    FormatOptions::from_iter(pairs.iter().copied())
}

/// Writes through the streaming sink, which is the path a `COPY TO` takes.
fn write(batches: &[RecordBatch], options: &FormatOptions) -> Result<Bytes> {
    let collected = Collected::default();

    let mut writer = Parquet.open_streaming_writer(
        batches
            .first()
            .expect("at least one batch")
            .schema()
            .clone(),
        options,
        Box::new(collected.clone()),
    )?;

    for batch in batches {
        writer.write_batch(batch)?;
    }

    writer.finish()?;

    Ok(collected.bytes())
}

/// Every `id` in a file, read whole.
fn ids(data: Bytes) -> Vec<i32> {
    let reader = Parquet
        .open_reader(data, &FormatOptions::new())
        .expect("the file is readable");

    collect_ids(reader)
}

fn collect_ids(reader: Box<dyn RecordBatchReader + Send>) -> Vec<i32> {
    reader
        .flat_map(|batch| {
            let batch = batch.expect("the batch decodes");

            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("id is an int32")
                .values()
                .to_vec()
        })
        .collect()
}

#[test]
fn a_round_trip_keeps_every_row() {
    let written = write(&batches(2048, 512), &options(&[])).expect("the write succeeds");

    assert_eq!(ids(written), (0..2048).collect::<Vec<_>>());
}

#[test]
fn the_buffered_writer_produces_the_same_file() {
    let batches = batches(100, 50);

    let mut writer = Parquet
        .open_writer(batches[0].schema().clone(), &options(&[]))
        .expect("the writer opens");

    for batch in &batches {
        writer.write_batch(batch).expect("the batch is written");
    }

    let written = Bytes::from(writer.finish().expect("the file is finished"));

    assert_eq!(ids(written), (0..100).collect::<Vec<_>>());
}

/// The codec a file reports, without the level: the default level of a codec is upstream's to
/// change, and none of these tests are about which one it picked.
fn codec(info: &format_registry::provider::FileInfo) -> String {
    let reported = info
        .compression
        .as_deref()
        .expect("parquet knows its codec");

    reported
        .split_once('(')
        .map(|(codec, _)| codec)
        .unwrap_or(reported)
        .to_string()
}

#[test]
fn a_file_knows_its_rows_its_row_groups_and_its_codec() {
    let written = write(
        &batches(1000, 1000),
        &options(&[("row_group_size", "400"), ("compression", "zstd")]),
    )
    .expect("the write succeeds");

    let info = Parquet.file_info(&written).expect("the footer is readable");

    assert_eq!(info.num_rows, Some(1000));
    // 400, 400, 200: upstream splits a batch across row groups rather than overshooting
    assert_eq!(info.num_splits, Some(3));
    assert_eq!(info.split_label.as_deref(), Some("row group"));
    assert_eq!(codec(&info), "ZSTD");
    assert_eq!(info.format_version.as_deref(), Some("v1"));
}

#[test]
fn the_writer_version_is_an_option() {
    let written = write(&batches(10, 10), &options(&[("parquet_version", "v2")]))
        .expect("the write succeeds");

    let info = Parquet.file_info(&written).expect("the footer is readable");

    assert_eq!(info.format_version.as_deref(), Some("v2"));
}

#[test]
fn the_file_name_decides_the_codec_when_no_option_does() {
    for (file_name, expected) in [
        ("/data/orders.parquet", "SNAPPY"),
        ("/data/orders.parquet.zst", "ZSTD"),
        ("/data/orders.parquet.gz", "GZIP"),
        ("/data/orders.parquet.br", "BROTLI"),
        // says nothing about a codec, so the default stands
        ("/data/orders.bin", "SNAPPY"),
    ] {
        let written = write(&batches(10, 10), &options(&[(FILE_NAME_OPTION, file_name)]))
            .expect("the write succeeds");

        let info = Parquet.file_info(&written).expect("the footer is readable");

        assert_eq!(codec(&info), expected, "{file_name}");
    }
}

#[test]
fn an_explicit_codec_beats_the_file_name() {
    let written = write(
        &batches(10, 10),
        &options(&[
            (FILE_NAME_OPTION, "/data/orders.parquet.zst"),
            ("compression", "uncompressed"),
        ]),
    )
    .expect("the write succeeds");

    let info = Parquet.file_info(&written).expect("the footer is readable");

    assert_eq!(codec(&info), "UNCOMPRESSED");
}

#[test]
fn a_compression_level_is_refused_for_a_codec_that_has_none() {
    let failure = write(
        &batches(10, 10),
        &options(&[("compression", "snappy"), ("compression_level", "3")]),
    )
    .expect_err("snappy takes no level");

    assert!(
        failure.contains("compression level is not supported for \"snappy\""),
        "{failure}"
    );
}

#[test]
fn a_ranged_read_fetches_a_fraction_of_the_file() {
    // big enough that a window is a fraction of it: a file smaller than one window comes back
    // whole however careful the reader is, and would make this assertion say nothing
    let written = write(
        &batches(200_000, 50_000),
        &options(&[("row_group_size", "25000")]),
    )
    .expect("the write succeeds");

    let source = Buffer::new(written.clone());
    let fetched = source.fetched.clone();

    let reader = Parquet
        .open_ranged_reader(Arc::new(source), &FormatOptions::new(), Some(0..1))
        .expect("the reader opens");

    assert_eq!(collect_ids(reader), (0..25_000).collect::<Vec<_>>());

    let fetched = *fetched.lock().expect("the lock is never poisoned");
    assert!(
        fetched < written.len() / 2,
        "one row group of eight fetched {fetched} of the file's {} bytes",
        written.len()
    );
}

#[test]
fn a_range_of_splits_is_the_rows_of_those_row_groups() {
    let written = write(&batches(1000, 1000), &options(&[("row_group_size", "250")]))
        .expect("the write succeeds");

    let reader = Parquet
        .open_ranged_reader(
            Arc::new(Buffer::new(written)),
            &FormatOptions::new(),
            Some(1..3),
        )
        .expect("the reader opens");

    assert_eq!(collect_ids(reader), (250..750).collect::<Vec<_>>());
}

#[test]
fn a_whole_ranged_read_is_every_row() {
    let written = write(&batches(600, 600), &options(&[("row_group_size", "250")]))
        .expect("the write succeeds");

    let reader = Parquet
        .open_ranged_reader(Arc::new(Buffer::new(written)), &FormatOptions::new(), None)
        .expect("the reader opens");

    assert_eq!(collect_ids(reader), (0..600).collect::<Vec<_>>());
}

#[test]
fn a_split_range_past_the_end_is_refused() {
    let written = write(&batches(10, 10), &options(&[])).expect("the write succeeds");

    let failure = Parquet
        .open_ranged_reader(
            Arc::new(Buffer::new(written)),
            &FormatOptions::new(),
            Some(0..4),
        )
        // a reader is not `Debug`, so the message has to be the only thing left of it
        .map(|_| ())
        .expect_err("the file has one row group");

    assert!(failure.contains("that has 1"), "{failure}");
}

#[test]
fn an_unreadable_file_says_so_rather_than_panicking() {
    let data = Bytes::from_static(b"not a parquet file");

    for failure in [
        Parquet.schema(&data).err(),
        Parquet.file_info(&data).err(),
        Parquet.kv_metadata(&data).err(),
        Parquet
            .open_reader(data.clone(), &FormatOptions::new())
            .err(),
    ] {
        let failure = failure.expect("every read of this fails");
        assert!(failure.contains("not a readable parquet file"), "{failure}");
    }
}

#[test]
fn a_file_without_metadata_has_no_metadata_rather_than_none_at_all() {
    let written = write(&batches(10, 10), &options(&[])).expect("the write succeeds");

    let metadata = Parquet
        .kv_metadata(&written)
        .expect("the footer is readable");

    // arrow-rs writes the arrow schema into the footer, and nothing else is expected here
    assert!(
        metadata
            .iter()
            .all(|(key, _)| key == "ARROW:schema" || key == "ARROW:extension:metadata"),
        "{metadata:?}"
    );
}

#[test]
fn the_schema_read_back_is_the_schema_written() {
    let written = write(&batches(10, 10), &options(&[])).expect("the write succeeds");

    let schema = Parquet.schema(&written).expect("the footer is readable");

    assert_eq!(
        schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>(),
        vec!["id", "label"]
    );
}

#[test]
fn an_unknown_option_names_what_the_format_takes() {
    let failure = Parquet
        .validate_option("row_grp_size", "100")
        .expect_err("a typo is refused");

    assert!(
        failure.contains("has no option \"row_grp_size\""),
        "{failure}"
    );
    assert!(failure.contains("row_group_size"), "{failure}");
}

#[test]
fn every_option_the_format_lists_validates() {
    for (name, value) in [
        ("compression", "zstd"),
        ("compression_level", "-1"),
        ("row_group_size", "1000"),
        ("row_group_size_bytes", "1048576"),
        ("parquet_version", "v2"),
        ("bloom_filter", "all"),
        ("bloom_filter_fpp", "0.01"),
        ("dictionary", "{\"id\": false}"),
        ("field_ids", "auto"),
    ] {
        Parquet
            .validate_option(name, value)
            .unwrap_or_else(|e| panic!("{name} = {value}: {e}"));
    }
}

#[test]
fn a_bad_option_value_is_refused_before_any_io() {
    for (name, value, expected) in [
        ("compression", "snappie", "is not a recognized value"),
        ("compression_level", "high", "must be an integer"),
        ("row_group_size", "0", "must be greater than 0"),
        ("bloom_filter_fpp", "2", "between 0 and 1"),
        ("bloom_filter", "{not json}", "invalid column selection"),
        (
            "field_ids",
            "{not json}",
            "invalid JSON string for field_ids",
        ),
        ("parquet_version", "v3", "is not a recognized value"),
    ] {
        let failure = Parquet
            .validate_option(name, value)
            .expect_err(&format!("{name} = {value} is refused"));

        assert!(failure.contains(expected), "{name} = {value}: {failure}");
    }
}

#[test]
fn a_selection_naming_a_column_the_table_has_not_got_is_refused() {
    let failure = write(
        &batches(10, 10),
        &options(&[("bloom_filter", "{\"nonesuch\": true}")]),
    )
    .expect_err("the column does not exist");

    assert!(
        failure.contains("\"nonesuch\" in \"bloom_filter\""),
        "{failure}"
    );
    assert!(failure.contains("\"id\", \"label\""), "{failure}");
}

/// The id of a field, from the metadata the walk puts there.
fn field_id(schema: &Schema, path: &[&str]) -> Option<i32> {
    let mut field = schema
        .field_with_name(path[0])
        .expect("the field exists")
        .clone();

    for name in &path[1..] {
        field = match field.data_type() {
            DataType::Struct(children) => children
                .find(name)
                .unwrap_or_else(|| panic!("no child {name}"))
                .1
                .as_ref()
                .clone(),
            DataType::List(element) => element.as_ref().clone(),
            DataType::Map(entries, _) => match entries.data_type() {
                DataType::Struct(children) => children
                    .find(name)
                    .unwrap_or_else(|| panic!("no entry {name}"))
                    .1
                    .as_ref()
                    .clone(),
                other => panic!("the entries are {other:?}"),
            },
            other => panic!("{other:?} has no children"),
        };
    }

    field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .map(|id| id.parse().expect("an id is an integer"))
}

/// A struct, a list and a map, which is every shape the walk treats specially.
fn nested_schema() -> Schema {
    let entries = Field::new(
        "tags",
        DataType::Struct(Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("val", DataType::Int32, true),
        ])),
        false,
    );

    Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new(
            "address",
            DataType::Struct(Fields::from(vec![
                Field::new("city", DataType::Utf8, true),
                Field::new("zip", DataType::Int32, true),
            ])),
            true,
        ),
        Field::new(
            "scores",
            DataType::List(Arc::new(Field::new("element", DataType::Int32, true))),
            true,
        ),
        Field::new("tags", DataType::Map(Arc::new(entries), false), true),
    ])
}

#[test]
fn auto_ids_are_assigned_in_the_order_the_schema_is_walked() {
    let stamped =
        stamp_field_ids(&nested_schema(), &FieldIds::Auto).expect("auto needs no mapping");

    // a container's own id comes before its children's, which is the order pg_parquet 0.6
    // assigned them in while building the schema
    assert_eq!(field_id(&stamped, &["id"]), Some(0));
    assert_eq!(field_id(&stamped, &["address"]), Some(1));
    assert_eq!(field_id(&stamped, &["address", "city"]), Some(2));
    assert_eq!(field_id(&stamped, &["address", "zip"]), Some(3));
    assert_eq!(field_id(&stamped, &["scores"]), Some(4));
    assert_eq!(field_id(&stamped, &["scores", "element"]), Some(5));
    assert_eq!(field_id(&stamped, &["tags"]), Some(6));
    assert_eq!(field_id(&stamped, &["tags", "key"]), Some(7));
    assert_eq!(field_id(&stamped, &["tags", "val"]), Some(8));
}

#[test]
fn an_explicit_mapping_stamps_only_what_it_names() {
    let field_ids: FieldIds = r#"{
        "id": 10,
        "address": {"__root_field_id": 20, "city": 21},
        "scores": {"__root_field_id": 30, "element": 31},
        "tags": {"__root_field_id": 40, "key": 41, "val": 42}
    }"#
    .parse()
    .expect("the mapping is valid JSON");

    let stamped = stamp_field_ids(&nested_schema(), &field_ids).expect("the mapping fits");

    assert_eq!(field_id(&stamped, &["id"]), Some(10));
    assert_eq!(field_id(&stamped, &["address"]), Some(20));
    assert_eq!(field_id(&stamped, &["address", "city"]), Some(21));
    // not named, so it has no id at all rather than an invented one
    assert_eq!(field_id(&stamped, &["address", "zip"]), None);
    assert_eq!(field_id(&stamped, &["scores", "element"]), Some(31));
    assert_eq!(field_id(&stamped, &["tags", "val"]), Some(42));
}

#[test]
fn no_ids_are_asked_for_and_none_are_stamped() {
    let stamped = stamp_field_ids(&nested_schema(), &FieldIds::None).expect("none always fits");

    assert_eq!(field_id(&stamped, &["id"]), None);
    assert_eq!(field_id(&stamped, &["address", "city"]), None);
}

#[test]
fn a_duplicate_id_is_refused() {
    let field_ids: FieldIds = r#"{"id": 1, "address": {"__root_field_id": 1}}"#
        .parse()
        .expect("the mapping is valid JSON");

    let failure = stamp_field_ids(&nested_schema(), &field_ids).expect_err("1 is used twice");

    assert!(failure.contains("duplicate field id 1"), "{failure}");
}

#[test]
fn a_mapping_naming_a_field_the_schema_has_not_got_is_refused() {
    let field_ids: FieldIds = r#"{"nonesuch": 1}"#.parse().expect("valid JSON");

    let failure = stamp_field_ids(&nested_schema(), &field_ids).expect_err("no such field");

    assert!(failure.contains("'nonesuch' in the mapping"), "{failure}");
}

#[test]
fn stamped_ids_reach_the_file() {
    let schema = Arc::new(nested_schema());

    // one row, null in everything but `id`: this is about the schema reaching the footer, not
    // about any value reaching a page
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            new_null_array(schema.field(1).data_type(), 1),
            new_null_array(schema.field(2).data_type(), 1),
            new_null_array(schema.field(3).data_type(), 1),
        ],
    )
    .expect("the arrays match the schema");

    let written = write(&[batch], &options(&[("field_ids", "auto")])).expect("the write succeeds");

    let read_back = Parquet.schema(&written).expect("the footer is readable");

    assert_eq!(field_id(&read_back, &["id"]), Some(0));
    assert_eq!(field_id(&read_back, &["address", "zip"]), Some(3));
}
