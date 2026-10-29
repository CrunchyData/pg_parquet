#[pgrx::pg_schema]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::vec;

    use crate::pgrx_tests::common::{
        extension_exists, int96_timestamp, write_int96_timestamps_to_parquet,
        write_record_batch_to_parquet, write_record_batch_to_parquet_file, LOCAL_TEST_FILE_PATH,
    };
    use crate::type_compat::pg_arrow_type_conversions::{
        date_to_i32, time_to_i64, timestamp_to_i64, timestamptz_to_i64, timetz_to_i64,
    };
    use arrow::array::{
        ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array,
        Decimal128Array, Decimal256Array, Decimal32Array, Decimal64Array, DictionaryArray,
        FixedSizeBinaryArray, FixedSizeListArray, Float32Array, Float64Array, Int16Array,
        Int32Array, Int8Array, LargeBinaryArray, LargeListArray, LargeListViewArray,
        LargeStringArray, ListArray, ListViewArray, MapArray, NullArray, RecordBatch, StringArray,
        StringViewArray, StructArray, Time32MillisecondArray, Time64MicrosecondArray,
        TimestampMicrosecondArray, UInt16Array, UInt32Array, UInt64Array,
    };
    use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    use arrow::datatypes::i256;
    use arrow::datatypes::{Int32Type, UInt16Type};
    use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};
    use pgrx::pg_test;
    use pgrx::{
        datum::{Date, Time, TimeWithTimeZone, Timestamp, TimestampWithTimeZone},
        Spi,
    };

    #[pg_test]
    fn test_coerce_primitive_types() {
        // INT16 => {int, bigint}
        let x_nullable = false;
        let y_nullable = true;

        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int16, x_nullable),
            Field::new("y", DataType::Int16, y_nullable),
        ]));

        let x = Arc::new(Int16Array::from(vec![1]));
        let y = Arc::new(Int16Array::from(vec![2]));

        let batch = RecordBatch::try_new(schema.clone(), vec![x, y]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        let create_table = "CREATE TABLE test_table (x int, y bigint)";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let value = Spi::get_two::<i32, i64>("SELECT x, y FROM test_table LIMIT 1").unwrap();
        assert_eq!(value, (Some(1), Some(2)));

        let drop_table = "DROP TABLE test_table";
        Spi::run(drop_table).unwrap();

        // INT32 => {bigint}
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, true)]));

        let x = Arc::new(Int32Array::from(vec![1]));

        let batch = RecordBatch::try_new(schema.clone(), vec![x]).unwrap();

        write_record_batch_to_parquet(schema, batch);

        let create_table = "CREATE TABLE test_table (x bigint)";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let value = Spi::get_one::<i64>("SELECT x FROM test_table LIMIT 1")
            .unwrap()
            .unwrap();
        assert_eq!(value, 1);

        let drop_table = "DROP TABLE test_table";
        Spi::run(drop_table).unwrap();

        // FLOAT32 => {double}
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Float32, true)]));

        let x = Arc::new(Float32Array::from(vec![1.123]));

        let batch = RecordBatch::try_new(schema.clone(), vec![x]).unwrap();

        write_record_batch_to_parquet(schema, batch);

        let create_table = "CREATE TABLE test_table (x double precision)";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let value = Spi::get_one::<f64>("SELECT x FROM test_table LIMIT 1")
            .unwrap()
            .unwrap();
        assert_eq!(value as f32, 1.123);

        let drop_table = "DROP TABLE test_table";
        Spi::run(drop_table).unwrap();

        // FLOAT64 => {float}
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Float64, true)]));

        let x = Arc::new(Float64Array::from(vec![1.123]));

        let batch = RecordBatch::try_new(schema.clone(), vec![x]).unwrap();

        write_record_batch_to_parquet(schema, batch);

        let create_table = "CREATE TABLE test_table (x real)";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let value = Spi::get_one::<f32>("SELECT x FROM test_table LIMIT 1")
            .unwrap()
            .unwrap();
        assert_eq!(value, 1.123);

        let drop_table = "DROP TABLE test_table";
        Spi::run(drop_table).unwrap();

        // DATE32 => {timestamp}
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Date32, true)]));

        let date = Date::new(2022, 5, 5).unwrap();

        let x = Arc::new(Date32Array::from(vec![date_to_i32(date)]));

        let batch = RecordBatch::try_new(schema.clone(), vec![x]).unwrap();

        write_record_batch_to_parquet(schema, batch);

        let create_table = "CREATE TABLE test_table (x timestamp)";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let value = Spi::get_one::<Timestamp>("SELECT x FROM test_table LIMIT 1")
            .unwrap()
            .unwrap();
        assert_eq!(value, Timestamp::from(date));

        let drop_table = "DROP TABLE test_table";
        Spi::run(drop_table).unwrap();

        // TIMESTAMP => {timestamptz}
        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]));

        let timestamp = Timestamp::from(Date::new(2022, 5, 5).unwrap());

        let x = Arc::new(TimestampMicrosecondArray::from(vec![timestamp_to_i64(
            timestamp,
        )]));

        let batch = RecordBatch::try_new(schema.clone(), vec![x]).unwrap();

        write_record_batch_to_parquet(schema, batch);

        let create_table = "CREATE TABLE test_table (x timestamptz)";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let value = Spi::get_one::<TimestampWithTimeZone>("SELECT x FROM test_table LIMIT 1")
            .unwrap()
            .unwrap();
        assert_eq!(value.at_timezone("UTC").unwrap(), timestamp);

        let drop_table = "DROP TABLE test_table";
        Spi::run(drop_table).unwrap();

        // TIMESTAMPTZ => {timestamp}
        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            DataType::Timestamp(TimeUnit::Microsecond, Some("Europe/Paris".into())),
            true,
        )]));

        let timestamptz =
            TimestampWithTimeZone::with_timezone(2022, 5, 5, 0, 0, 0.0, "Europe/Paris").unwrap();

        let x = Arc::new(
            TimestampMicrosecondArray::from(vec![timestamptz_to_i64(timestamptz)])
                .with_timezone("Europe/Paris"),
        );

        let batch = RecordBatch::try_new(schema.clone(), vec![x]).unwrap();

        write_record_batch_to_parquet(schema, batch);

        let create_table = "CREATE TABLE test_table (x timestamp)";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let value = Spi::get_one::<Timestamp>("SELECT x FROM test_table LIMIT 1")
            .unwrap()
            .unwrap();
        assert_eq!(value, timestamptz.at_timezone("UTC").unwrap());

        let drop_table = "DROP TABLE test_table";
        Spi::run(drop_table).unwrap();

        // TIME64 => {timetz}
        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            DataType::Time64(TimeUnit::Microsecond),
            true,
        )]));

        let time = Time::new(13, 0, 0.0).unwrap();

        let x = Arc::new(Time64MicrosecondArray::from(vec![time_to_i64(time)]));

        let batch = RecordBatch::try_new(schema.clone(), vec![x]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        let create_table = "CREATE TABLE test_table (x timetz)";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let value = Spi::get_one::<TimeWithTimeZone>("SELECT x FROM test_table LIMIT 1")
            .unwrap()
            .unwrap();
        assert_eq!(value, time.into());

        let drop_table = "DROP TABLE test_table";
        Spi::run(drop_table).unwrap();

        // TIME64 => {time}
        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            DataType::Time64(TimeUnit::Microsecond),
            true,
        )
        .with_metadata(HashMap::from_iter(vec![(
            "adjusted_to_utc".into(),
            "true".into(),
        )]))]));

        let timetz = TimeWithTimeZone::with_timezone(13, 0, 0.0, "UTC").unwrap();

        let x = Arc::new(Time64MicrosecondArray::from(vec![timetz_to_i64(timetz)]));

        let batch = RecordBatch::try_new(schema.clone(), vec![x]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        let create_table = "CREATE TABLE test_table (x time)";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let value = Spi::get_one::<Time>("SELECT x FROM test_table LIMIT 1")
            .unwrap()
            .unwrap();
        assert_eq!(value, timetz.into());

        let drop_table = "DROP TABLE test_table";
        Spi::run(drop_table).unwrap();

        // UINT16 => {smallint, int, bigint}
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::UInt16, true),
            Field::new("y", DataType::UInt16, true),
            Field::new("z", DataType::UInt16, true),
        ]));

        let x = Arc::new(UInt16Array::from(vec![1]));
        let y = Arc::new(UInt16Array::from(vec![2]));
        let z = Arc::new(UInt16Array::from(vec![3]));

        let batch = RecordBatch::try_new(schema.clone(), vec![x, y, z]).unwrap();

        write_record_batch_to_parquet(schema, batch);

        let create_table = "CREATE TABLE test_table (x smallint, y int, z bigint)";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let value =
            Spi::get_three::<i16, i32, i64>("SELECT x, y, z FROM test_table LIMIT 1").unwrap();
        assert_eq!(value, (Some(1), Some(2), Some(3)));

        let drop_table = "DROP TABLE test_table";
        Spi::run(drop_table).unwrap();

        // UINT32 => {int, bigint}
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::UInt32, true),
            Field::new("y", DataType::UInt32, true),
        ]));

        let x = Arc::new(UInt32Array::from(vec![1]));
        let y = Arc::new(UInt32Array::from(vec![2]));

        let batch = RecordBatch::try_new(schema.clone(), vec![x, y]).unwrap();

        write_record_batch_to_parquet(schema, batch);

        let create_table = "CREATE TABLE test_table (x int, y bigint)";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let value = Spi::get_two::<i32, i64>("SELECT x, y FROM test_table LIMIT 1").unwrap();
        assert_eq!(value, (Some(1), Some(2)));

        let drop_table = "DROP TABLE test_table";
        Spi::run(drop_table).unwrap();

        // UINT64 => {bigint}
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::UInt64, true)]));

        let x = Arc::new(UInt64Array::from(vec![1]));

        let batch = RecordBatch::try_new(schema.clone(), vec![x]).unwrap();

        write_record_batch_to_parquet(schema, batch);

        let create_table = "CREATE TABLE test_table (x bigint)";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let value = Spi::get_one::<i64>("SELECT x FROM test_table LIMIT 1")
            .unwrap()
            .unwrap();
        assert_eq!(value, 1);

        let drop_table = "DROP TABLE test_table";
        Spi::run(drop_table).unwrap();

        // INT8 => {int64}
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int8, true)]));

        let x = Arc::new(Int8Array::from(vec![1]));

        let batch = RecordBatch::try_new(schema.clone(), vec![x]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        let create_table = "CREATE TABLE test_table (x bigint)";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let value = Spi::get_one::<i64>("SELECT x FROM test_table LIMIT 1")
            .unwrap()
            .unwrap();
        assert_eq!(value, 1);

        let drop_table = "DROP TABLE test_table";
        Spi::run(drop_table).unwrap();

        // BOOLEAN => {int}
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Boolean, true)]));

        let x = Arc::new(BooleanArray::from(vec![true]));

        let batch = RecordBatch::try_new(schema.clone(), vec![x]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        let create_table = "CREATE TABLE test_table (x int)";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let value = Spi::get_one::<i32>("SELECT x FROM test_table LIMIT 1")
            .unwrap()
            .unwrap();
        assert_eq!(value, 1);

        let drop_table = "DROP TABLE test_table";
        Spi::run(drop_table).unwrap();

        // DECIMAL128 => {float}
        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            DataType::Decimal128(8, 5),
            true,
        )]));

        let x = Arc::new(
            Decimal128Array::from(vec!["12345000".parse::<i128>().expect("invalid decimal")])
                .with_precision_and_scale(8, 5)
                .unwrap(),
        );

        let batch = RecordBatch::try_new(schema.clone(), vec![x]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        let create_table = "CREATE TABLE test_table (x float8)";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let value = Spi::get_one::<f64>("SELECT x FROM test_table LIMIT 1")
            .unwrap()
            .unwrap();
        assert_eq!(value, 123.45);

        let drop_table = "DROP TABLE test_table";
        Spi::run(drop_table).unwrap();

        // Binary => {text}
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Binary, true)]));

        let x = Arc::new(BinaryArray::from(vec!["abc".as_bytes()]));

        let batch = RecordBatch::try_new(schema.clone(), vec![x]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        let create_table = "CREATE TABLE test_table (x text)";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let value = Spi::get_one::<String>("SELECT x FROM test_table LIMIT 1")
            .unwrap()
            .unwrap();
        assert_eq!(value, "abc");

        let drop_table = "DROP TABLE test_table";
        Spi::run(drop_table).unwrap();

        // LargeUtf8 => {text}
        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            DataType::LargeUtf8,
            true,
        )]));

        let x = Arc::new(LargeStringArray::from(vec!["test"]));

        let batch = RecordBatch::try_new(schema.clone(), vec![x]).unwrap();

        write_record_batch_to_parquet(schema, batch);

        let create_table = "CREATE TABLE test_table (x text)";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let value = Spi::get_one::<String>("SELECT x FROM test_table LIMIT 1")
            .unwrap()
            .unwrap();
        assert_eq!(value, "test");

        let drop_table = "DROP TABLE test_table";
        Spi::run(drop_table).unwrap();

        // LargeBinary => {bytea}
        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            DataType::LargeBinary,
            true,
        )]));

        let x = Arc::new(LargeBinaryArray::from(vec!["abc".as_bytes()]));

        let batch = RecordBatch::try_new(schema.clone(), vec![x]).unwrap();

        write_record_batch_to_parquet(schema, batch);

        let create_table = "CREATE TABLE test_table (x bytea)";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let value = Spi::get_one::<Vec<u8>>("SELECT x FROM test_table LIMIT 1")
            .unwrap()
            .unwrap();
        assert_eq!(value, "abc".as_bytes());

        let drop_table = "DROP TABLE test_table";
        Spi::run(drop_table).unwrap();
    }

    #[pg_test]
    fn test_coerce_unsigned_types_into_types_not_castable_from_oid() {
        // the tests run concurrently, so each of them needs its own file
        let test_file = "/tmp/pg_parquet_test_coerce_unsigned.parquet";

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt32, true),
            Field::new("b", DataType::UInt32, true),
            Field::new("c", DataType::UInt32, true),
            Field::new("d", DataType::UInt32, true),
            Field::new("e", DataType::UInt32, true),
            Field::new("f", DataType::UInt64, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![u32::MAX])),
                Arc::new(UInt32Array::from(vec![u32::MAX])),
                Arc::new(UInt32Array::from(vec![u32::MAX])),
                Arc::new(UInt32Array::from(vec![u32::MAX])),
                Arc::new(UInt32Array::from(vec![1])),
                Arc::new(UInt64Array::from(vec![u64::MAX])),
            ],
        )
        .unwrap();
        write_record_batch_to_parquet_file(test_file, schema, batch);

        // "oid" cannot be cast to any of these types, so it must not stand in for
        // uint32 while the file's schema is checked against the table's schema
        let create_table = "CREATE TABLE test_table (a numeric, b double precision, c real,
                                                     d money, e boolean, f numeric)";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{test_file}'");
        Spi::run(&copy_from).unwrap();

        let value = Spi::get_one::<bool>(
            "SELECT a = 4294967295 AND b = 4294967295::float8 AND c = 4294967295::float4
             AND d::numeric = 4294967295 AND e AND f = 18446744073709551615
             FROM test_table",
        )
        .unwrap();
        assert_eq!(value, Some(true));

        let drop_table = "DROP TABLE test_table";
        Spi::run(drop_table).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "Can't cast value 4294967295 to type Int32")]
    fn test_coerce_unsigned_type_into_a_narrower_type() {
        // the tests run concurrently, so each of them needs its own file
        let test_file = "/tmp/pg_parquet_test_coerce_unsigned_narrower.parquet";

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::UInt32, true)]));

        let x = Arc::new(UInt32Array::from(vec![u32::MAX]));

        let batch = RecordBatch::try_new(schema.clone(), vec![x]).unwrap();
        write_record_batch_to_parquet_file(test_file, schema, batch);

        let create_table = "CREATE TABLE test_table (x int)";
        Spi::run(create_table).unwrap();

        // the cast is allowed by the schema check, but the value does not fit
        let copy_from = format!("COPY test_table FROM '{test_file}'");
        Spi::run(&copy_from).unwrap();
    }

    #[pg_test]
    fn test_coerce_list_types() {
        // [UINT16] => {int[], bigint[]}
        let x_nullable = false;
        let field_x = Field::new(
            "x",
            DataType::List(Field::new("item", DataType::UInt16, false).into()),
            x_nullable,
        );

        let x = Arc::new(UInt16Array::from(vec![1, 2]));
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, 2]));
        let x = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::UInt16, false)),
            offsets,
            x,
            None,
        ));

        let y_nullable = true;
        let field_y = Field::new(
            "y",
            DataType::List(Field::new("item", DataType::UInt16, true).into()),
            y_nullable,
        );

        let y = Arc::new(ListArray::from_iter_primitive::<UInt16Type, _, _>(vec![
            Some(vec![Some(3), Some(4)]),
        ]));

        let schema = Arc::new(Schema::new(vec![field_x, field_y]));

        let batch = RecordBatch::try_new(schema.clone(), vec![x, y]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        let create_table = "CREATE TABLE test_table (x int[], y bigint[])";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let value = Spi::get_two::<Vec<Option<i32>>, Vec<Option<i64>>>(
            "SELECT x, y FROM test_table LIMIT 1",
        )
        .unwrap();
        assert_eq!(
            value,
            (Some(vec![Some(1), Some(2)]), Some(vec![Some(3), Some(4)]))
        );

        let drop_table = "DROP TABLE test_table";
        Spi::run(drop_table).unwrap();
    }

    #[pg_test]
    fn test_coerce_large_list() {
        // [UINT16] => {int[], bigint[]}
        let x_nullable = false;
        let field_x = Field::new(
            "x",
            DataType::LargeList(Field::new("item", DataType::UInt16, false).into()),
            x_nullable,
        );

        let x = Arc::new(UInt16Array::from(vec![1, 2]));
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, 2]));
        let x = Arc::new(LargeListArray::new(
            Arc::new(Field::new("item", DataType::UInt16, false)),
            offsets,
            x,
            None,
        ));

        let y_nullable = true;
        let field_y = Field::new(
            "y",
            DataType::LargeList(Field::new("item", DataType::UInt16, true).into()),
            y_nullable,
        );

        let y = Arc::new(LargeListArray::from_iter_primitive::<UInt16Type, _, _>(
            vec![Some(vec![Some(3), Some(4)])],
        ));

        let schema = Arc::new(Schema::new(vec![field_x, field_y]));

        let batch = RecordBatch::try_new(schema.clone(), vec![x, y]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        let create_table = "CREATE TABLE test_table (x int[], y bigint[])";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let value = Spi::get_two::<Vec<Option<i32>>, Vec<Option<i64>>>(
            "SELECT x, y FROM test_table LIMIT 1",
        )
        .unwrap();
        assert_eq!(
            value,
            (Some(vec![Some(1), Some(2)]), Some(vec![Some(3), Some(4)]))
        );

        let drop_table = "DROP TABLE test_table";
        Spi::run(drop_table).unwrap();
    }

    #[pg_test]
    fn test_coerce_list_view() {
        // LISTVIEW[UINT16] => int[], LARGELISTVIEW[UTF8] => text[]
        let field_x = Field::new(
            "x",
            DataType::ListView(Field::new("item", DataType::UInt16, true).into()),
            true,
        );

        let x_values = Arc::new(UInt16Array::from(vec![Some(1), Some(2)]));
        let x = Arc::new(ListViewArray::new(
            Arc::new(Field::new("item", DataType::UInt16, true)),
            ScalarBuffer::from(vec![0, 2]),
            ScalarBuffer::from(vec![2, 0]),
            x_values,
            Some(NullBuffer::from(vec![true, false])),
        ));

        let field_y = Field::new(
            "y",
            DataType::LargeListView(Field::new("item", DataType::Utf8, true).into()),
            true,
        );

        let y_values = Arc::new(StringArray::from(vec![Some("a"), Some("b")]));
        let y = Arc::new(LargeListViewArray::new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            ScalarBuffer::from(vec![0, 2]),
            ScalarBuffer::from(vec![2, 0]),
            y_values,
            Some(NullBuffer::from(vec![true, false])),
        ));

        let schema = Arc::new(Schema::new(vec![field_x, field_y]));

        let batch = RecordBatch::try_new(schema.clone(), vec![x, y]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        let create_table = "CREATE TABLE test_table (x int[], y text[])";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let result = Spi::get_one::<bool>(
            "SELECT x = array[1, 2] AND y = array['a', 'b'] FROM test_table \
             WHERE x IS NOT NULL",
        )
        .unwrap();
        assert_eq!(result, Some(true));

        let nulls =
            Spi::get_one::<i64>("SELECT count(*) FROM test_table WHERE x IS NULL AND y IS NULL")
                .unwrap();
        assert_eq!(nulls, Some(1));

        let drop_table = "DROP TABLE test_table";
        Spi::run(drop_table).unwrap();
    }

    #[pg_test]
    fn test_coerce_struct_types() {
        // STRUCT {a: UINT16, b: UINT16} => test_type {a: int, b: bigint}
        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            DataType::Struct(
                vec![
                    Field::new("a", DataType::UInt16, false),
                    Field::new("b", DataType::UInt16, false),
                ]
                .into(),
            ),
            false,
        )]));

        let a: ArrayRef = Arc::new(UInt16Array::from(vec![Some(1)]));
        let b: ArrayRef = Arc::new(UInt16Array::from(vec![Some(2)]));

        let x = Arc::new(StructArray::try_from(vec![("a", a), ("b", b)]).unwrap());

        let batch = RecordBatch::try_new(schema.clone(), vec![x]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        let create_type = "CREATE TYPE test_type AS (a int, b bigint)";
        Spi::run(create_type).unwrap();

        let create_table = "CREATE TABLE test_table (x test_type)";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let value =
            Spi::get_two::<i32, i64>("SELECT (x).a, (x).b FROM test_table LIMIT 1").unwrap();
        assert_eq!(value, (Some(1), Some(2)));
    }

    #[pg_test]
    fn test_coerce_struct_fields_by_name() {
        // match_by 'name' matches the struct fields by name at any nesting level
        let inner_fields = vec![
            Field::new("d", DataType::UInt16, false),
            Field::new("c", DataType::UInt16, false),
        ];

        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            DataType::Struct(
                vec![
                    Field::new("y", DataType::Struct(inner_fields.clone().into()), false),
                    Field::new("b", DataType::UInt16, false),
                    Field::new("a", DataType::UInt16, false),
                ]
                .into(),
            ),
            false,
        )]));

        let c: ArrayRef = Arc::new(UInt16Array::from(vec![Some(3)]));
        let d: ArrayRef = Arc::new(UInt16Array::from(vec![Some(4)]));
        let y: ArrayRef = Arc::new(StructArray::try_from(vec![("d", d), ("c", c)]).unwrap());

        let a: ArrayRef = Arc::new(UInt16Array::from(vec![Some(1)]));
        let b: ArrayRef = Arc::new(UInt16Array::from(vec![Some(2)]));

        let x = Arc::new(StructArray::try_from(vec![("y", y), ("b", b), ("a", a)]).unwrap());

        let batch = RecordBatch::try_new(schema.clone(), vec![x]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        let create_inner_type = "CREATE TYPE test_inner_type AS (c int, d bigint)";
        Spi::run(create_inner_type).unwrap();

        let create_type = "CREATE TYPE test_type AS (a int, b bigint, y test_inner_type)";
        Spi::run(create_type).unwrap();

        let create_table = "CREATE TABLE test_table (x test_type)";
        Spi::run(create_table).unwrap();

        let copy_from =
            format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}' WITH (match_by 'name')");
        Spi::run(&copy_from).unwrap();

        let value =
            Spi::get_two::<i32, i64>("SELECT (x).a, (x).b FROM test_table LIMIT 1").unwrap();
        assert_eq!(value, (Some(1), Some(2)));

        let inner_value =
            Spi::get_two::<i32, i64>("SELECT ((x).y).c, ((x).y).d FROM test_table LIMIT 1")
                .unwrap();
        assert_eq!(inner_value, (Some(3), Some(4)));
    }

    // the values of a struct's field are rounded for the type of the field at the same position
    // in the target type, so a composite type that holds its attributes in another order than
    // the file holds its fields must not round a field's values for another field's type
    #[pg_test]
    fn test_coerce_struct_fields_by_name_rounds_the_matched_field() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            DataType::Struct(
                vec![
                    Field::new("a", DataType::Float64, false),
                    Field::new("b", DataType::Utf8, false),
                ]
                .into(),
            ),
            false,
        )]));

        let a: ArrayRef = Arc::new(Float64Array::from(vec![2.7]));
        let b: ArrayRef = Arc::new(StringArray::from(vec!["hello"]));

        let x = Arc::new(StructArray::try_from(vec![("a", a), ("b", b)]).unwrap());

        let batch = RecordBatch::try_new(schema.clone(), vec![x]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        // the attributes are in the other order than the file's fields, and only "a" is read
        // into an integer
        let create_type = "CREATE TYPE test_type AS (b text, a int)";
        Spi::run(create_type).unwrap();

        let create_table = "CREATE TABLE test_table (x test_type)";
        Spi::run(create_table).unwrap();

        let copy_from =
            format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}' WITH (match_by 'name')");
        Spi::run(&copy_from).unwrap();

        // 2.7 rounds to 3, the way float8::int4 does
        let value =
            Spi::get_two::<i32, &str>("SELECT (x).a, (x).b FROM test_table LIMIT 1").unwrap();
        assert_eq!(value, (Some(3), Some("hello")));
    }

    // nothing validates the values of a decimal column against the precision it is declared
    // with, so a file can carry values that do not fit it. The rounding that precedes an
    // integer cast is where that shows up, and its error has to name the column the values
    // are in: the file is otherwise the only place to look for them.
    #[pg_test]
    #[should_panic(expected = "failed to round column x")]
    fn test_coerce_out_of_precision_decimal_into_integer_names_the_column() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            DataType::Decimal128(4, 2),
            true,
        )]));

        // with_data_type carries the declared type without checking the value against it,
        // which is how a file with values past its own precision is written
        let x = Arc::new(
            Decimal128Array::from(vec![92233720368547758_i128])
                .with_data_type(DataType::Decimal128(4, 2)),
        );

        let batch = RecordBatch::try_new(schema.clone(), vec![x]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        let create_table = "CREATE TABLE test_table (x bigint)";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();
    }

    // the name of the column is carried into its children, so the values of an array column
    // are named by the column they are in rather than by nothing at all
    #[pg_test]
    #[should_panic(expected = "failed to round column x")]
    fn test_coerce_out_of_precision_decimal_array_into_integer_names_the_column() {
        let element = Arc::new(Field::new("element", DataType::Decimal128(4, 2), true));

        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            DataType::List(element.clone()),
            true,
        )]));

        let values = Decimal128Array::from(vec![92233720368547758_i128])
            .with_data_type(DataType::Decimal128(4, 2));

        let x = Arc::new(ListArray::new(
            element,
            OffsetBuffer::new(vec![0, 1].into()),
            Arc::new(values),
            None,
        ));

        let batch = RecordBatch::try_new(schema.clone(), vec![x]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        let create_table = "CREATE TABLE test_table (x bigint[])";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();
    }

    // a struct field whose name is longer than the identifier length limit matches an attribute
    // only once it is truncated, which is not a name arrow-cast can match, so it casts the
    // fields of the struct by position. The target type then has to hold its fields in the order
    // the file holds them, or the values land in the wrong attributes.
    #[pg_test]
    fn test_coerce_struct_fields_with_long_names_by_name() {
        let first_name = format!("{}1_in_the_file", "a".repeat(62));
        let truncated_first_name = format!("{}1", "a".repeat(62));
        let second_name = format!("{}2_in_the_file", "a".repeat(62));
        let truncated_second_name = format!("{}2", "a".repeat(62));

        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            DataType::Struct(
                vec![
                    Field::new(&first_name, DataType::Int32, false),
                    Field::new(&second_name, DataType::Int32, false),
                ]
                .into(),
            ),
            false,
        )]));

        let first: ArrayRef = Arc::new(Int32Array::from(vec![1]));
        let second: ArrayRef = Arc::new(Int32Array::from(vec![2]));

        let x = Arc::new(
            StructArray::try_from(vec![
                (first_name.as_str(), first),
                (second_name.as_str(), second),
            ])
            .unwrap(),
        );

        let batch = RecordBatch::try_new(schema.clone(), vec![x]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        // the attributes are in the other order than the file's fields
        let create_type = format!(
            "CREATE TYPE test_type AS (\"{truncated_second_name}\" bigint, \
             \"{truncated_first_name}\" bigint)"
        );
        Spi::run(&create_type).unwrap();

        let create_table = "CREATE TABLE test_table (x test_type)";
        Spi::run(create_table).unwrap();

        let copy_from =
            format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}' WITH (match_by 'name')");
        Spi::run(&copy_from).unwrap();

        let value = Spi::get_two::<i64, i64>(&format!(
            "SELECT (x).\"{truncated_first_name}\", (x).\"{truncated_second_name}\"
             FROM test_table LIMIT 1"
        ))
        .unwrap();
        assert_eq!(value, (Some(1), Some(2)));
    }

    #[pg_test]
    fn test_coerce_list_of_struct() {
        // [STRUCT {a: UINT16, b: UINT16}] => test_type {a: int, b: bigint}[]
        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            DataType::List(
                Field::new(
                    "item",
                    DataType::Struct(
                        vec![
                            Field::new("a", DataType::UInt16, false),
                            Field::new("b", DataType::UInt16, false),
                        ]
                        .into(),
                    ),
                    false,
                )
                .into(),
            ),
            false,
        )]));

        let a: ArrayRef = Arc::new(UInt16Array::from(vec![Some(1)]));
        let b: ArrayRef = Arc::new(UInt16Array::from(vec![Some(2)]));

        let x = Arc::new(StructArray::try_from(vec![("a", a), ("b", b)]).unwrap());
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, 1]));
        let x = Arc::new(ListArray::new(
            Arc::new(Field::new(
                "item",
                DataType::Struct(
                    vec![
                        Field::new("a", DataType::UInt16, false),
                        Field::new("b", DataType::UInt16, false),
                    ]
                    .into(),
                ),
                false,
            )),
            offsets,
            x,
            None,
        ));

        let batch = RecordBatch::try_new(schema.clone(), vec![x]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        let create_type = "CREATE TYPE test_type AS (a int, b bigint)";
        Spi::run(create_type).unwrap();

        let create_table = "CREATE TABLE test_table (x test_type[])";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let value =
            Spi::get_two::<i32, i64>("SELECT (x[1]).a, (x[1]).b FROM test_table LIMIT 1").unwrap();
        assert_eq!(value, (Some(1), Some(2)));
    }

    #[pg_test]
    #[should_panic(expected = "type mismatch for column \"x\" between table and parquet file.")]
    fn test_not_coercable_list_of_struct() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            DataType::List(
                Field::new(
                    "item",
                    DataType::Struct(vec![Field::new("a", DataType::UInt16, false)].into()),
                    false,
                )
                .into(),
            ),
            false,
        )]));

        let a: ArrayRef = Arc::new(UInt16Array::from(vec![Some(1)]));

        let x = Arc::new(StructArray::try_from(vec![("a", a)]).unwrap());
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, 1]));
        let x = Arc::new(ListArray::new(
            Arc::new(Field::new(
                "item",
                DataType::Struct(vec![Field::new("a", DataType::UInt16, false)].into()),
                false,
            )),
            offsets,
            x,
            None,
        ));

        let batch = RecordBatch::try_new(schema.clone(), vec![x]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        let create_type = "CREATE TYPE test_type AS (a int, b bigint)";
        Spi::run(create_type).unwrap();

        let create_table = "CREATE TABLE test_table (x test_type[])";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let value =
            Spi::get_two::<i32, i64>("SELECT (x[1]).a, (x[1]).b FROM test_table LIMIT 1").unwrap();
        assert_eq!(value, (Some(1), Some(2)));
    }

    #[pg_test]
    #[should_panic(expected = "type mismatch for column \"x\" between table and parquet file.")]
    fn test_coerce_struct_type_with_less_field() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            DataType::Struct(vec![Field::new("a", DataType::UInt16, false)].into()),
            false,
        )]));

        let a: ArrayRef = Arc::new(UInt16Array::from(vec![Some(1)]));

        let x = Arc::new(StructArray::try_from(vec![("a", a)]).unwrap());

        let batch = RecordBatch::try_new(schema.clone(), vec![x]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        let create_type = "CREATE TYPE test_type AS (a int, b bigint)";
        Spi::run(create_type).unwrap();

        let create_table = "CREATE TABLE test_table (x test_type)";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();
    }

    #[pg_test]
    fn test_coerce_struct_fields_by_position() {
        // no other writer names a struct's fields after the Postgres composite type's
        // attributes, so match_by 'position' has to see past the names
        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            DataType::Struct(
                vec![
                    Field::new("first", DataType::UInt16, false),
                    Field::new("second", DataType::Utf8, false),
                ]
                .into(),
            ),
            false,
        )]));

        let first: ArrayRef = Arc::new(UInt16Array::from(vec![Some(1)]));
        let second: ArrayRef = Arc::new(StringArray::from(vec![Some("p")]));

        let x =
            Arc::new(StructArray::try_from(vec![("first", first), ("second", second)]).unwrap());

        let batch = RecordBatch::try_new(schema.clone(), vec![x]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        let create_type = "CREATE TYPE test_type AS (a int, b text)";
        Spi::run(create_type).unwrap();

        let create_table = "CREATE TABLE test_table (x test_type)";
        Spi::run(create_table).unwrap();

        let copy_from =
            format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}' WITH (match_by 'position')");
        Spi::run(&copy_from).unwrap();

        let result =
            Spi::get_one::<bool>("SELECT (x).a = 1 AND (x).b = 'p' FROM test_table").unwrap();
        assert_eq!(result, Some(true));
    }

    #[pg_test]
    #[should_panic(expected = "type mismatch for column \"x\" between table and parquet file.")]
    fn test_coerce_struct_type_with_different_field_name() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            DataType::Struct(
                vec![
                    Field::new("b", DataType::UInt16, false),
                    Field::new("a", DataType::UInt16, false),
                ]
                .into(),
            ),
            false,
        )]));

        let a: ArrayRef = Arc::new(UInt16Array::from(vec![Some(1)]));
        let b: ArrayRef = Arc::new(UInt16Array::from(vec![Some(2)]));

        let x = Arc::new(StructArray::try_from(vec![("b", a), ("a", b)]).unwrap());

        let batch = RecordBatch::try_new(schema.clone(), vec![x]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        let create_type = "CREATE TYPE test_type AS (a int, b bigint)";
        Spi::run(create_type).unwrap();

        let create_table = "CREATE TABLE test_table (x test_type)";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "type mismatch for column \"x\" between table and parquet file.")]
    fn test_coerce_struct_type_with_not_castable_field_type() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            DataType::Struct(
                vec![
                    Field::new("a", DataType::UInt16, false),
                    Field::new("b", DataType::Boolean, false),
                ]
                .into(),
            ),
            false,
        )]));

        let a: ArrayRef = Arc::new(UInt16Array::from(vec![Some(1)]));
        let b: ArrayRef = Arc::new(BooleanArray::from(vec![Some(false)]));

        let x = Arc::new(StructArray::try_from(vec![("a", a), ("b", b)]).unwrap());

        let batch = RecordBatch::try_new(schema.clone(), vec![x]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        let create_type = "CREATE TYPE test_type AS (a int, b date)";
        Spi::run(create_type).unwrap();

        let create_table = "CREATE TABLE test_table (x test_type)";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();
    }

    #[pg_test]
    #[should_panic(
        expected = "type mismatch for column \"location\" between table and parquet file"
    )]
    fn test_type_mismatch_between_parquet_and_table() {
        let create_types = "create type dog as (name text, age int);
                            create type person as (id bigint, name text, dogs dog[]);
                            create type address as (loc text);";
        Spi::run(create_types).unwrap();

        let create_correct_table =
            "create table factory_correct (id bigint, workers person[], name text, location address);";
        Spi::run(create_correct_table).unwrap();

        let create_wrong_table =
            "create table factory_wrong (id bigint, workers person[], name text, location int);";
        Spi::run(create_wrong_table).unwrap();

        let copy_to_parquet = format!(
            "copy (select 1::int8 as id,
                                        array[
                                            row(1, 'ali', array[row('lady', 4), NULL]::dog[])
                                        ]::person[] as workers,
                                        'Microsoft' as name,
                                        row('istanbul')::address as location
                                 from generate_series(1,10) i) to '{LOCAL_TEST_FILE_PATH}';"
        );
        Spi::run(&copy_to_parquet).unwrap();

        // copy to correct table which matches the parquet schema
        let copy_to_correct_table = format!("copy factory_correct from '{LOCAL_TEST_FILE_PATH}';");
        Spi::run(&copy_to_correct_table).unwrap();

        // copy from wrong table which does not match the parquet schema
        let copy_from_wrong_table = format!("copy factory_wrong from '{LOCAL_TEST_FILE_PATH}';");
        Spi::run(&copy_from_wrong_table).unwrap();
    }

    #[pg_test]
    fn test_coerce_map_types() {
        // Skip the test if crunchy_map extension is not available
        if !extension_exists("crunchy_map") {
            return;
        }

        // MAP<TEXT, UINT16> => crunchy_map {key: text, val: bigint}
        let entries_field = Arc::new(Field::new(
            "x",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("val", DataType::UInt16, false),
                ]
                .into(),
            ),
            false,
        ));

        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            DataType::Map(entries_field.clone(), false),
            false,
        )]));

        let keys: ArrayRef = Arc::new(StringArray::from(vec![Some("aa"), Some("bb")]));
        let values: ArrayRef = Arc::new(UInt16Array::from(vec![Some(1), Some(2)]));

        let entries = StructArray::try_from(vec![("key", keys), ("val", values)]).unwrap();

        let map_offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, 2]));

        let map_nulls = NullBuffer::from(vec![true]);

        let x = Arc::new(MapArray::new(
            entries_field,
            map_offsets,
            entries,
            Some(map_nulls),
            false,
        ));

        let batch = RecordBatch::try_new(schema.clone(), vec![x]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        Spi::run("DROP EXTENSION IF EXISTS crunchy_map; CREATE EXTENSION crunchy_map;").unwrap();

        Spi::run("SELECT crunchy_map.create('text','bigint');").unwrap();

        let create_table = "CREATE TABLE test_table (x crunchy_map.key_text_val_bigint)";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let value = Spi::get_one::<bool>("select x = array[('aa',1),('bb',2)]::crunchy_map.key_text_val_bigint from test_table LIMIT 1;").unwrap().unwrap();
        assert!(value);
    }

    #[pg_test]
    fn test_coerce_map_with_spec_value_field_name() {
        // Skip the test if crunchy_map extension is not available
        if !extension_exists("crunchy_map") {
            return;
        }

        // the parquet format names the value field of a map "value", which is what the
        // writers other than pg_parquet write. pg_parquet names it "val" after the
        // crunchy_map entry type, and the two must still match each other
        let entries_field = Arc::new(Field::new(
            "key_value",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::UInt16, false),
                ]
                .into(),
            ),
            false,
        ));

        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            DataType::Map(entries_field.clone(), false),
            false,
        )]));

        let keys: ArrayRef = Arc::new(StringArray::from(vec![Some("aa"), Some("bb")]));
        let values: ArrayRef = Arc::new(UInt16Array::from(vec![Some(1), Some(2)]));

        let entries = StructArray::try_from(vec![("key", keys), ("value", values)]).unwrap();

        let map_offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, 2]));

        let map_nulls = NullBuffer::from(vec![true]);

        let x = Arc::new(MapArray::new(
            entries_field,
            map_offsets,
            entries,
            Some(map_nulls),
            false,
        ));

        let batch = RecordBatch::try_new(schema.clone(), vec![x]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        Spi::run("DROP EXTENSION IF EXISTS crunchy_map; CREATE EXTENSION crunchy_map;").unwrap();

        Spi::run("SELECT crunchy_map.create('text','bigint');").unwrap();

        let create_table = "CREATE TABLE test_table (x crunchy_map.key_text_val_bigint)";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let value = Spi::get_one::<bool>("select x = array[('aa',1),('bb',2)]::crunchy_map.key_text_val_bigint from test_table LIMIT 1;").unwrap().unwrap();
        assert!(value);
    }

    #[pg_test]
    fn test_coerce_list_of_map() {
        // Skip the test if crunchy_map extension is not available
        if !extension_exists("crunchy_map") {
            return;
        }

        // [MAP<TEXT, UINT16>] => crunchy_map {key: text, val: bigint}[]
        let entries_field = Arc::new(Field::new(
            "x",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("val", DataType::UInt16, false),
                ]
                .into(),
            ),
            false,
        ));

        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            DataType::List(
                Field::new("item", DataType::Map(entries_field.clone(), false), false).into(),
            ),
            false,
        )]));

        let keys: ArrayRef = Arc::new(StringArray::from(vec![Some("aa"), Some("bb")]));
        let values: ArrayRef = Arc::new(UInt16Array::from(vec![Some(1), Some(2)]));

        let entries = StructArray::try_from(vec![("key", keys), ("val", values)]).unwrap();

        let map_offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, 2]));

        let map_nulls = NullBuffer::from(vec![true]);

        let map = Arc::new(MapArray::new(
            entries_field.clone(),
            map_offsets,
            entries,
            Some(map_nulls),
            false,
        ));

        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, 1]));
        let x = Arc::new(ListArray::new(
            Arc::new(Field::new(
                "item",
                DataType::Map(entries_field, false),
                false,
            )),
            offsets,
            map,
            None,
        ));

        let batch = RecordBatch::try_new(schema.clone(), vec![x]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        Spi::run("DROP EXTENSION IF EXISTS crunchy_map; CREATE EXTENSION crunchy_map;").unwrap();

        Spi::run("SELECT crunchy_map.create('text','bigint');").unwrap();

        let create_table = "CREATE TABLE test_table (x crunchy_map.key_text_val_bigint[])";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let value = Spi::get_one::<bool>("select x = array[array[('aa',1),('bb',2)]::crunchy_map.key_text_val_bigint] from test_table LIMIT 1;").unwrap().unwrap();
        assert!(value);
    }

    #[pg_test]
    fn test_table_with_different_position_match_by_name() {
        let copy_to = format!("COPY (SELECT 1 as x, 'hello' as y) TO '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_to).unwrap();

        let create_table = "CREATE TABLE test_table (y text, x int)";
        Spi::run(create_table).unwrap();

        let copy_from =
            format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}' WITH (match_by 'name')");
        Spi::run(&copy_from).unwrap();

        let result = Spi::get_two::<&str, i32>("SELECT y, x FROM test_table LIMIT 1").unwrap();
        assert_eq!(result, (Some("hello"), Some(1)));
    }

    // PostgreSQL truncates an identifier to 63 bytes, so a column of the table can only hold the
    // first 63 bytes of a longer parquet column name, which is what match_by 'name' has to match
    // it with. The same truncation is what CREATE TABLE ... definition_from names the column of
    // such a file with, and what the nested fields of a struct are already matched by.
    #[pg_test]
    fn test_table_with_long_column_name_match_by_name() {
        // the names of the file are longer than the limit, and tell each other apart within the
        // first 63 bytes, so they are still two names once they are truncated. The multibyte one
        // must be truncated at a character boundary, the way the parser truncates it.
        let ascii_name = format!("{}1_in_the_file", "a".repeat(62));
        let truncated_ascii_name = format!("{}1", "a".repeat(62));
        let multibyte_name = "ü".repeat(40);
        let truncated_multibyte_name = "ü".repeat(31);

        let schema = Arc::new(Schema::new(vec![
            Field::new(&ascii_name, DataType::Int32, true),
            Field::new("short", DataType::Utf8, true),
            Field::new(&multibyte_name, DataType::Int32, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])) as ArrayRef,
                Arc::new(StringArray::from(vec!["hello"])) as ArrayRef,
                Arc::new(Int32Array::from(vec![2])) as ArrayRef,
            ],
        )
        .unwrap();
        write_record_batch_to_parquet(schema, batch);

        // the table holds the columns in another order than the file does, so a column that was
        // matched by position rather than by name would be read from the wrong one
        let create_table = format!(
            "CREATE TABLE test_table (\"{truncated_multibyte_name}\" int, short text, \
             \"{truncated_ascii_name}\" int)"
        );
        Spi::run(&create_table).unwrap();

        let copy_from =
            format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}' WITH (match_by 'name')");
        Spi::run(&copy_from).unwrap();

        let result = Spi::get_three::<i32, i32, &str>(&format!(
            "SELECT \"{truncated_ascii_name}\", \"{truncated_multibyte_name}\", short
             FROM test_table LIMIT 1"
        ))
        .unwrap();
        assert_eq!(result, (Some(1), Some(2), Some("hello")));
    }

    // two long names that are only told apart past the 63rd byte are one name once they are
    // truncated, and which of them a column is read from would be a matter of the order the file
    // happens to hold them in
    #[pg_test]
    #[should_panic(expected = "parquet file has more than one field named")]
    fn test_table_with_ambiguous_long_column_name_match_by_name() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(format!("{}_one", "a".repeat(63)), DataType::Int32, true),
            Field::new(format!("{}_two", "a".repeat(63)), DataType::Int32, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])) as ArrayRef,
                Arc::new(Int32Array::from(vec![2])) as ArrayRef,
            ],
        )
        .unwrap();
        write_record_batch_to_parquet(schema, batch);

        let create_table = format!("CREATE TABLE test_table (\"{}\" int)", "a".repeat(63));
        Spi::run(&create_table).unwrap();

        let copy_from =
            format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}' WITH (match_by 'name')");
        Spi::run(&copy_from).unwrap();
    }

    #[pg_test]
    fn test_table_with_different_name_match_by_position() {
        let copy_to = "COPY (SELECT 1 as a, 'hello' as b) TO '/tmp/test.parquet'";
        Spi::run(copy_to).unwrap();

        let create_table = "CREATE TABLE test_table (x bigint, y varchar)";
        Spi::run(create_table).unwrap();

        let copy_from = "COPY test_table FROM '/tmp/test.parquet' WITH (match_by 'position')";
        Spi::run(copy_from).unwrap();

        let result = Spi::get_two::<i64, &str>("SELECT x, y FROM test_table LIMIT 1").unwrap();
        assert_eq!(result, (Some(1), Some("hello")));
    }

    #[pg_test]
    #[should_panic(expected = "column count mismatch between table and parquet file")]
    fn test_table_with_different_name_match_by_position_fail() {
        let copy_to = "COPY (SELECT 1 as a, 'hello' as b) TO '/tmp/test.parquet'";
        Spi::run(copy_to).unwrap();

        let create_table = "CREATE TABLE test_table (x bigint, y varchar, z int)";
        Spi::run(create_table).unwrap();

        let copy_from = "COPY test_table FROM '/tmp/test.parquet'";
        Spi::run(copy_from).unwrap();
    }

    // a cast that fails while the rows are read names the file it read them from, which is
    // the only thing that tells the files of a pattern apart
    #[pg_test]
    #[should_panic(expected = "failed to cast column a of file \"/tmp/pg_parquet_test.parquet\"")]
    fn test_runtime_cast_failure_names_the_file() {
        let copy_to = format!("COPY (SELECT 'abc'::text a) TO '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_to).unwrap();

        Spi::run("CREATE TABLE test_table (a int)").unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "column \"name\" is not found in parquet file")]
    fn test_missing_column_in_parquet() {
        let create_table = "create table test_table(id int, name text);";
        Spi::run(create_table).unwrap();

        let copy_to_parquet = format!("copy (select 100 as id) to '{LOCAL_TEST_FILE_PATH}';");
        Spi::run(&copy_to_parquet).unwrap();

        let copy_from =
            format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}' with (match_by 'name')");
        Spi::run(&copy_from).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "type mismatch for column \"x\" between table and parquet file.")]
    fn test_coerce_custom_cast_fail() {
        let custom_cast = "CREATE FUNCTION float_to_date(float) RETURNS date AS $$
                             BEGIN
                                 RETURN now()::date;
                             END;
                             $$ LANGUAGE plpgsql;";
        Spi::run(custom_cast).unwrap();

        let copy_to = format!("COPY (SELECT 1.0::float as x) TO '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_to).unwrap();

        let create_table = "CREATE TABLE test_table (x date)";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();
    }

    #[pg_test]
    fn test_coerce_dictionary_of_fixed_len_byte_array_types() {
        // DICTIONARY(INT32, BOOLEAN) => {bool}, DICTIONARY(INT32, FLOAT16) => {real},
        // DICTIONARY(INT32, DECIMAL128) => {numeric}
        let keys = || Int32Array::from(vec![Some(0), Some(1), None]);

        let x = DictionaryArray::try_new(keys(), Arc::new(BooleanArray::from(vec![true, false])))
            .unwrap();

        let float16_values =
            arrow_cast::cast(&Float32Array::from(vec![1.5_f32, 2.5]), &DataType::Float16).unwrap();
        let y = DictionaryArray::try_new(keys(), float16_values).unwrap();

        let z = DictionaryArray::try_new(
            keys(),
            Arc::new(
                Decimal128Array::from(vec![1250, 2500])
                    .with_precision_and_scale(5, 3)
                    .unwrap(),
            ),
        )
        .unwrap();

        let dictionary_type =
            |value_type| DataType::Dictionary(Box::new(DataType::Int32), Box::new(value_type));

        let schema = Arc::new(Schema::new(vec![
            Field::new("x", dictionary_type(DataType::Boolean), true),
            Field::new("y", dictionary_type(DataType::Float16), true),
            Field::new("z", dictionary_type(DataType::Decimal128(5, 3)), true),
        ]));

        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(x), Arc::new(y), Arc::new(z)])
                .unwrap();
        write_record_batch_to_parquet(schema, batch);

        let create_table = "CREATE TABLE test_table (x bool, y real, z numeric)";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let result = Spi::get_one::<bool>(
            "SELECT array_agg(x) = array[true, false, null]::bool[]
                    AND array_agg(y) = array[1.5, 2.5, null]::real[]
                    AND array_agg(z) = array[1.25, 2.5, null]::numeric[]
             FROM test_table",
        )
        .unwrap();
        assert_eq!(result, Some(true));
    }

    #[pg_test]
    fn test_coerce_nested_dictionary_of_boolean() {
        // STRUCT {a: DICTIONARY(INT32, BOOLEAN)} => test_type {a: bool},
        // LIST(DICTIONARY(INT32, BOOLEAN)) => bool[]
        let values = || Arc::new(BooleanArray::from(vec![true, false]));

        let dictionary_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Boolean));

        let a = DictionaryArray::try_new(Int32Array::from(vec![0, 1]), values()).unwrap();
        let a_field = Arc::new(Field::new("a", dictionary_type.clone(), true));
        let x = Arc::new(StructArray::new(
            vec![a_field.clone()].into(),
            vec![Arc::new(a)],
            None,
        ));

        let list_values = DictionaryArray::try_new(Int32Array::from(vec![1, 0]), values()).unwrap();
        let list_field = Arc::new(Field::new("item", dictionary_type, true));
        let y = Arc::new(ListArray::new(
            list_field.clone(),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 2])),
            Arc::new(list_values),
            None,
        ));

        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Struct(vec![a_field].into()), true),
            Field::new("y", DataType::List(list_field), true),
        ]));

        let batch = RecordBatch::try_new(schema.clone(), vec![x, y]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        let create_type = "CREATE TYPE test_type AS (a bool)";
        Spi::run(create_type).unwrap();

        let create_table = "CREATE TABLE test_table (x test_type, y bool[])";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let result = Spi::get_one::<bool>(
            "SELECT array_agg((x).a) = array[true, false]::bool[]
                    AND array_agg(y) = array[array[false], array[true]]::bool[][]
             FROM test_table",
        )
        .unwrap();
        assert_eq!(result, Some(true));

        Spi::run("DROP TABLE test_table").unwrap();
        Spi::run("DROP TYPE test_type").unwrap();
    }

    #[pg_test]
    fn test_coerce_list_kinds_of_dictionary() {
        // LARGE_LIST(DICTIONARY(INT32, BOOLEAN)), FIXED_SIZE_LIST(DICTIONARY(INT32, BOOLEAN)),
        // LIST_VIEW(DICTIONARY(INT32, BOOLEAN)) and LARGE_LIST_VIEW(DICTIONARY(INT32, BOOLEAN))
        // => bool[]
        let values = || Arc::new(BooleanArray::from(vec![true, false]));
        let dictionary = |keys: Vec<i32>| {
            Arc::new(DictionaryArray::try_new(Int32Array::from(keys), values()).unwrap())
                as ArrayRef
        };

        let dictionary_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Boolean));
        let item_field = Arc::new(Field::new("item", dictionary_type, true));

        let x = Arc::new(LargeListArray::new(
            item_field.clone(),
            OffsetBuffer::new(ScalarBuffer::from(vec![0_i64, 1, 2])),
            dictionary(vec![0, 1]),
            None,
        ));

        let y = Arc::new(FixedSizeListArray::new(
            item_field.clone(),
            2,
            dictionary(vec![1, 0, 0, 1]),
            None,
        ));

        let z = Arc::new(ListViewArray::new(
            item_field.clone(),
            ScalarBuffer::from(vec![1, 0]),
            ScalarBuffer::from(vec![1, 1]),
            dictionary(vec![0, 1]),
            None,
        ));

        let w = Arc::new(LargeListViewArray::new(
            item_field.clone(),
            ScalarBuffer::from(vec![1_i64, 0]),
            ScalarBuffer::from(vec![1, 1]),
            dictionary(vec![1, 0]),
            None,
        ));

        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::LargeList(item_field.clone()), true),
            Field::new("y", DataType::FixedSizeList(item_field.clone(), 2), true),
            Field::new("z", DataType::ListView(item_field.clone()), true),
            Field::new("w", DataType::LargeListView(item_field), true),
        ]));

        let batch = RecordBatch::try_new(schema.clone(), vec![x, y, z, w]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        let create_table = "CREATE TABLE test_table (x bool[], y bool[], z bool[], w bool[])";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let result = Spi::get_one::<bool>(
            "SELECT array_agg(x) = array[array[true], array[false]]::bool[][]
                    AND array_agg(y) = array[array[false, true], array[true, false]]::bool[][]
                    AND array_agg(z) = array[array[false], array[true]]::bool[][]
                    AND array_agg(w) = array[array[true], array[false]]::bool[][]
             FROM test_table",
        )
        .unwrap();
        assert_eq!(result, Some(true));

        Spi::run("DROP TABLE test_table").unwrap();
    }

    #[pg_test]
    fn test_coerce_dictionary_types() {
        // DICTIONARY(INT32, UTF8) => {text}, LIST(DICTIONARY(INT32, UTF8)) => {text[]}
        let dictionary_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));

        let schema = Arc::new(Schema::new(vec![
            Field::new("x", dictionary_type.clone(), true),
            Field::new(
                "y",
                DataType::List(Field::new("item", dictionary_type.clone(), true).into()),
                true,
            ),
        ]));

        let x: DictionaryArray<Int32Type> = vec![Some("apple"), None].into_iter().collect();
        let x = Arc::new(x);

        let y_values: DictionaryArray<Int32Type> =
            vec![Some("apple"), Some("apple")].into_iter().collect();
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 2]));
        let y = Arc::new(ListArray::new(
            Arc::new(Field::new("item", dictionary_type, true)),
            offsets,
            Arc::new(y_values),
            None,
        ));

        let batch = RecordBatch::try_new(schema.clone(), vec![x, y]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        let create_table = "CREATE TABLE test_table (x text, y text[])";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let result = Spi::get_one::<bool>(
            "SELECT array_agg(x) = array['apple', null]::text[]
                    AND array_agg(y::text) = array['{apple,apple}', '{}']::text[]
             FROM test_table",
        )
        .unwrap();
        assert_eq!(result, Some(true));
    }

    #[pg_test]
    fn test_coerce_view_types() {
        // UTF8_VIEW => {text}, BINARY_VIEW => {bytea}
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Utf8View, true),
            Field::new("y", DataType::BinaryView, true),
        ]));

        let x = Arc::new(StringViewArray::from(vec![Some("hello"), None]));
        let y = Arc::new(BinaryViewArray::from(vec![Some("world".as_bytes()), None]));

        let batch = RecordBatch::try_new(schema.clone(), vec![x, y]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        let create_table = "CREATE TABLE test_table (x text, y bytea)";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let result = Spi::get_two::<&str, &str>(
            "SELECT x, encode(y, 'escape') FROM test_table ORDER BY x LIMIT 1",
        )
        .unwrap();
        assert_eq!(result, (Some("hello"), Some("world")));
    }

    #[pg_test]
    fn test_coerce_date64_and_time32() {
        // DATE64 => {date}, TIME32(MILLIS) => {time}
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Date64, true),
            Field::new("y", DataType::Time32(TimeUnit::Millisecond), true),
        ]));

        let date = Date::new(2022, 5, 1).unwrap();
        let millis_per_day = 24 * 60 * 60 * 1000;
        let x = Arc::new(Date64Array::from(vec![
            date_to_i32(date) as i64 * millis_per_day,
        ]));

        let y = Arc::new(Time32MillisecondArray::from(vec![15 * 60 * 1000]));

        let batch = RecordBatch::try_new(schema.clone(), vec![x, y]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        let create_table = "CREATE TABLE test_table (x date, y time)";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let result = Spi::get_two::<Date, Time>("SELECT x, y FROM test_table LIMIT 1").unwrap();
        assert_eq!(result, (Some(date), Some(Time::new(0, 15, 0.0).unwrap())));
    }

    #[pg_test]
    fn test_coerce_decimal_into_unbounded_numeric() {
        // an unbounded "numeric" adopts the file's decimal type instead of rescaling the value
        // to the default "numeric(38,9)", which would lose the digits Postgres is able to store
        let struct_fields = Fields::from(vec![Field::new("s", DataType::Decimal128(38, 18), true)]);

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Decimal128(38, 18), true),
            Field::new(
                "b",
                DataType::List(Arc::new(Field::new(
                    "element",
                    DataType::Decimal128(18, 12),
                    true,
                ))),
                true,
            ),
            Field::new("c", DataType::Struct(struct_fields.clone()), true),
            Field::new("d", DataType::Decimal128(38, 0), true),
            Field::new("e", DataType::Decimal128(10, 2), true),
        ]));

        let decimal = |precision: u8, scale: i8, digits: &str| {
            Arc::new(
                Decimal128Array::from(vec![digits.parse::<i128>().expect("invalid decimal")])
                    .with_precision_and_scale(precision, scale)
                    .unwrap(),
            )
        };

        let a = decimal(38, 18, "1234567890123456789");

        let b_values = Decimal128Array::from(vec![123456789012_i128, 9876543210000])
            .with_precision_and_scale(18, 12)
            .unwrap();
        let b = Arc::new(ListArray::new(
            Arc::new(Field::new("element", DataType::Decimal128(18, 12), true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 2])),
            Arc::new(b_values),
            None,
        ));

        let c = Arc::new(StructArray::new(
            struct_fields,
            vec![decimal(38, 18, "111111111111111111")],
            None,
        ));

        let d = decimal(38, 0, &"1".repeat(38));
        let e = decimal(10, 2, "123");

        let batch = RecordBatch::try_new(schema.clone(), vec![a, b, c, d, e]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        Spi::run("CREATE TYPE unbounded_numeric_struct AS (s numeric)").unwrap();

        let create_table = "CREATE TABLE test_table (a numeric, b numeric[], c unbounded_numeric_struct, d numeric, e numeric)";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let result = Spi::get_one::<bool>(
            "SELECT a::text = '1.234567890123456789'
                AND b::text = '{0.123456789012,9.876543210000}'
                AND (c).s::text = '0.111111111111111111'
                AND d::text = repeat('1', 38)
                -- a decimal that the default unbounded numeric represents exactly is rescaled to it
                AND e::text = '1.230000000' FROM test_table",
        )
        .unwrap();
        assert_eq!(result, Some(true));

        // a numeric with a precision and scale keeps the Postgres assignment semantics
        let create_table = "CREATE TABLE test_table_bounded (a numeric(38,9), b numeric(38,9)[], c unbounded_numeric_struct, d numeric(38,0), e numeric(38,9))";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table_bounded FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let result = Spi::get_one::<bool>(
            "SELECT a::text = '1.234567890'
                AND b::text = '{0.123456789,9.876543210}'
                AND e::text = '1.230000000' FROM test_table_bounded",
        )
        .unwrap();
        assert_eq!(result, Some(true));
    }

    #[pg_test]
    fn test_coerce_narrow_decimal_into_unbounded_numeric() {
        // the narrow decimal types are adopted by an unbounded "numeric" too. A "Decimal32"
        // always fits the default "numeric(38,9)", a "Decimal64" has the scale to lose digits
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Decimal64(18, 12), true),
            Field::new("y", DataType::Decimal32(9, 9), true),
        ]));

        let x = Arc::new(
            Decimal64Array::from(vec![1_000_000_000_001_i64])
                .with_precision_and_scale(18, 12)
                .unwrap(),
        );

        let y = Arc::new(
            Decimal32Array::from(vec![123_456_789_i32])
                .with_precision_and_scale(9, 9)
                .unwrap(),
        );

        let batch = RecordBatch::try_new(schema.clone(), vec![x, y]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        let create_table = "CREATE TABLE test_table (x numeric, y numeric)";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let result = Spi::get_one::<bool>(
            "SELECT x::text = '1.000000000001'
                AND y::text = '0.123456789' FROM test_table",
        )
        .unwrap();
        assert_eq!(result, Some(true));
    }

    #[pg_test]
    fn test_coerce_text_into_unbounded_numeric() {
        // a numeric with a precision above Postgres' decimal limit is written as text, which an
        // unbounded "numeric" parses instead of casting it to the default "numeric(38,9)"
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Utf8, true),
            Field::new("y", DataType::LargeUtf8, true),
            Field::new("z", DataType::Utf8View, true),
        ]));

        let x = Arc::new(StringArray::from(vec!["1".repeat(30)]));
        let y = Arc::new(LargeStringArray::from(vec![format!(
            "2.{}",
            "1".repeat(20)
        )]));
        let z = Arc::new(StringViewArray::from(vec![format!("3.{}", "1".repeat(20))]));

        let batch = RecordBatch::try_new(schema.clone(), vec![x, y, z]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        let create_table = "CREATE TABLE test_table (x numeric, y numeric, z numeric)";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let result = Spi::get_one::<bool>(
            "SELECT x::text = repeat('1', 30)
                AND y::text = '2.' || repeat('1', 20)
                AND z::text = '3.' || repeat('1', 20) FROM test_table",
        )
        .unwrap();
        assert_eq!(result, Some(true));
    }

    #[pg_test]
    fn test_coerce_decimal256_into_unbounded_numeric() {
        // DECIMAL256 => {numeric}
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Decimal256(38, 20), true),
            Field::new("y", DataType::Decimal256(45, 5), true),
        ]));

        let x = Arc::new(
            Decimal256Array::from(vec![i256::from_string("1".repeat(38).as_str()).unwrap()])
                .with_precision_and_scale(38, 20)
                .unwrap(),
        );

        // a precision above Postgres' decimal limit has no decimal type to read into, so the
        // column is read through the text path
        let y = Arc::new(
            Decimal256Array::from(vec![i256::from_string("2".repeat(45).as_str()).unwrap()])
                .with_precision_and_scale(45, 5)
                .unwrap(),
        );

        let batch = RecordBatch::try_new(schema.clone(), vec![x, y]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        let create_table = "CREATE TABLE test_table (x numeric, y numeric)";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let result = Spi::get_one::<bool>(
            "SELECT x::text = repeat('1', 18) || '.' || repeat('1', 20)
                AND y::text = repeat('2', 40) || '.' || repeat('2', 5) FROM test_table",
        )
        .unwrap();
        assert_eq!(result, Some(true));
    }

    #[pg_test]
    #[should_panic(
        expected = "string value in the parquet file contains a null byte, which PostgreSQL cannot store"
    )]
    fn test_coerce_string_with_null_byte() {
        // a string with a null byte is rejected instead of being truncated at it, which is what
        // the input function of a type read through the text path would do
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, true)]));

        let x = Arc::new(StringArray::from(vec!["a\0b"]));

        let batch = RecordBatch::try_new(schema.clone(), vec![x]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        let create_table = "CREATE TABLE test_table (x varchar(10))";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();
    }

    #[pg_test]
    fn test_coerce_decimal256() {
        // DECIMAL256 => {numeric}
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Decimal256(45, 5), true),
            Field::new("y", DataType::Decimal256(10, 2), true),
        ]));

        let x = Arc::new(
            Decimal256Array::from(vec![i256::from_string("1".repeat(45).as_str()).unwrap()])
                .with_precision_and_scale(45, 5)
                .unwrap(),
        );

        let y = Arc::new(
            Decimal256Array::from(vec![i256::from_i128(1234)])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        );

        let batch = RecordBatch::try_new(schema.clone(), vec![x, y]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        // numeric with precision greater than 38 is read as text by pg_parquet
        let create_table = "CREATE TABLE test_table (x numeric(45,5), y numeric(10,2))";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let result =
            Spi::get_two::<&str, &str>("SELECT x::text, y::text FROM test_table LIMIT 1").unwrap();
        assert_eq!(
            result,
            (
                Some(format!("{}.{}", "1".repeat(40), "1".repeat(5)).as_str()),
                Some("12.34")
            )
        );
    }

    #[pg_test]
    fn test_coerce_fixed_size_binary() {
        // FIXED_SIZE_BINARY => {bytea}
        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            DataType::FixedSizeBinary(4),
            true,
        )]));

        let x = Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                vec![Some(b"abcd"), None].into_iter(),
                4,
            )
            .unwrap(),
        );

        let batch = RecordBatch::try_new(schema.clone(), vec![x]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        let create_table = "CREATE TABLE test_table (x bytea)";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let result = Spi::get_one::<bool>(
            "SELECT array_agg(encode(x, 'escape')) = array['abcd', null]::text[] FROM test_table",
        )
        .unwrap();
        assert_eq!(result, Some(true));
    }

    #[pg_test]
    fn test_coerce_null_type() {
        // NULL => any type
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Null, true),
            Field::new("y", DataType::Null, true),
            Field::new("z", DataType::Null, true),
        ]));

        let x = Arc::new(NullArray::new(2));
        let y = Arc::new(NullArray::new(2));
        let z = Arc::new(NullArray::new(2));

        let batch = RecordBatch::try_new(schema.clone(), vec![x, y, z]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        let create_table = "CREATE TABLE test_table (x text, y timestamptz, z int[])";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let result = Spi::get_one::<i64>(
            "SELECT count(*) FROM test_table WHERE x IS NULL AND y IS NULL AND z IS NULL",
        )
        .unwrap();
        assert_eq!(result, Some(2));
    }

    #[pg_test]
    fn test_coerce_duckdb_infinity_sentinels() {
        // DuckDB writes -infinity as one above the minimum of the underlying integer and
        // infinity as its maximum
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Date32, true),
            Field::new("y", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new(
                "z",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ]));

        let x = Arc::new(Date32Array::from(vec![i32::MAX, i32::MIN + 1]));
        let y = Arc::new(TimestampMicrosecondArray::from(vec![
            i64::MAX,
            i64::MIN + 1,
        ]));
        let z = Arc::new(
            TimestampMicrosecondArray::from(vec![i64::MAX, i64::MIN + 1]).with_timezone("UTC"),
        );

        let batch = RecordBatch::try_new(schema.clone(), vec![x, y, z]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        let create_table = "CREATE TABLE test_table (x date, y timestamp, z timestamptz)";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let result = Spi::get_one::<bool>(
            "SELECT array_agg(x) = array['infinity','-infinity']::date[]
                    AND array_agg(y) = array['infinity','-infinity']::timestamp[]
                    AND array_agg(z) = array['infinity','-infinity']::timestamptz[]
             FROM test_table",
        )
        .unwrap();
        assert_eq!(result, Some(true));

        Spi::run("DROP TABLE test_table").unwrap();
    }

    #[pg_test]
    fn test_coerce_int96_timestamps() {
        // an INT96 column carries no logical type to record its resolution, so parquet-rs reads
        // it as nanoseconds, which wraps around outside 1677..2262. the resolution is hinted as
        // microseconds, which covers the whole range Postgres can store.
        let message_type = "message schema { OPTIONAL INT96 x; }";

        let timestamps = [
            int96_timestamp(2086303, 0),                  // 1000-01-01 00:00:00
            int96_timestamp(2440588, 0),                  // 1970-01-01 00:00:00
            int96_timestamp(2634531, 86_399_000_000_000), // 2500-12-31 23:59:59
        ];

        let def_levels = [1, 1, 1];

        write_int96_timestamps_to_parquet(message_type, &timestamps, &def_levels, None, None);

        // INT96 => {timestamp}
        let create_table = "CREATE TABLE test_table (x timestamp)";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let value = Spi::get_one::<bool>(
            "SELECT array_agg(x ORDER BY x) =
             ARRAY['1000-01-01', '1970-01-01', '2500-12-31 23:59:59']::timestamp[]
             FROM test_table",
        )
        .unwrap();
        assert_eq!(value, Some(true));

        let drop_table = "DROP TABLE test_table";
        Spi::run(drop_table).unwrap();

        // INT96 => {timestamptz}
        let create_table = "CREATE TABLE test_table (x timestamptz)";
        Spi::run(create_table).unwrap();

        Spi::run(&copy_from).unwrap();

        let value = Spi::get_one::<bool>(
            "SELECT array_agg(x ORDER BY x) =
             ARRAY['1000-01-01+00', '1970-01-01+00', '2500-12-31 23:59:59+00']::timestamptz[]
             FROM test_table",
        )
        .unwrap();
        assert_eq!(value, Some(true));

        let drop_table = "DROP TABLE test_table";
        Spi::run(drop_table).unwrap();

        // the resolution is hinted at any nesting level
        let message_type = "message schema { OPTIONAL group x { OPTIONAL INT96 ts; } }";

        let timestamps = [int96_timestamp(2305448, 0)]; // 1600-01-01 00:00:00

        let def_levels = [2];

        write_int96_timestamps_to_parquet(message_type, &timestamps, &def_levels, None, None);

        let create_type = "CREATE TYPE test_type AS (ts timestamp)";
        Spi::run(create_type).unwrap();

        let create_table = "CREATE TABLE test_table (x test_type)";
        Spi::run(create_table).unwrap();

        Spi::run(&copy_from).unwrap();

        let value = Spi::get_one::<Timestamp>("SELECT (x).ts FROM test_table LIMIT 1")
            .unwrap()
            .unwrap();
        assert_eq!(value, Timestamp::new(1600, 1, 1, 0, 0, 0.0).unwrap());

        let drop_table = "DROP TABLE test_table";
        Spi::run(drop_table).unwrap();

        // a list keeps its leaf two levels down, under the repeated group
        let message_type =
            "message schema { OPTIONAL group x (LIST) { REPEATED group list { OPTIONAL INT96 element; } } }";

        let timestamps = [
            int96_timestamp(2086303, 0), // 1000-01-01 00:00:00
            int96_timestamp(2634531, 0), // 2500-12-31 00:00:00
        ];

        let def_levels = [3, 3];
        let rep_levels = [0, 1];

        write_int96_timestamps_to_parquet(
            message_type,
            &timestamps,
            &def_levels,
            Some(&rep_levels),
            None,
        );

        let create_table = "CREATE TABLE test_table (x timestamp[])";
        Spi::run(create_table).unwrap();

        Spi::run(&copy_from).unwrap();

        let value = Spi::get_one::<bool>(
            "SELECT x = ARRAY['1000-01-01', '2500-12-31']::timestamp[] FROM test_table LIMIT 1",
        )
        .unwrap();
        assert_eq!(value, Some(true));

        let drop_table = "DROP TABLE test_table";
        Spi::run(drop_table).unwrap();

        // the legacy two level encoding, where the repeated level is the leaf itself
        let message_type = "message schema { OPTIONAL group x (LIST) { REPEATED INT96 array; } }";

        let def_levels = [2, 2];

        write_int96_timestamps_to_parquet(
            message_type,
            &timestamps,
            &def_levels,
            Some(&rep_levels),
            None,
        );

        let create_table = "CREATE TABLE test_table (x timestamp[])";
        Spi::run(create_table).unwrap();

        Spi::run(&copy_from).unwrap();

        let value = Spi::get_one::<bool>(
            "SELECT x = ARRAY['1000-01-01', '2500-12-31']::timestamp[] FROM test_table LIMIT 1",
        )
        .unwrap();
        assert_eq!(value, Some(true));

        let drop_table = "DROP TABLE test_table";
        Spi::run(drop_table).unwrap();

        // a file that records an Arrow schema declares the resolution itself, which is kept
        let message_type = "message schema { OPTIONAL INT96 x; }";

        // 2026-01-02 03:04:05.123456789
        let timestamps = [int96_timestamp(2461043, 11_045_123_456_789)];

        let def_levels = [1];

        let arrow_schema = Schema::new(vec![Field::new(
            "x",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )]);

        write_int96_timestamps_to_parquet(
            message_type,
            &timestamps,
            &def_levels,
            None,
            Some(&arrow_schema),
        );

        let create_table = "CREATE TABLE test_table (x text)";
        Spi::run(create_table).unwrap();

        Spi::run(&copy_from).unwrap();

        let value = Spi::get_one::<String>("SELECT x FROM test_table LIMIT 1").unwrap();
        assert_eq!(value, Some("2026-01-02T03:04:05.123456789".into()));

        let drop_table = "DROP TABLE test_table";
        Spi::run(drop_table).unwrap();

        // the declared resolution is not necessarily nanoseconds: pyarrow writes an INT96
        // column for a seconds, milliseconds or microseconds timestamp and records that
        // resolution, which parquet-rs applies, so such a file is already read correctly
        let message_type = "message schema { OPTIONAL INT96 x; }";

        let timestamps = [int96_timestamp(2086303, 0)]; // 1000-01-01 00:00:00

        let def_levels = [1];

        let arrow_schema = Schema::new(vec![Field::new(
            "x",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]);

        write_int96_timestamps_to_parquet(
            message_type,
            &timestamps,
            &def_levels,
            None,
            Some(&arrow_schema),
        );

        let create_table = "CREATE TABLE test_table (x timestamp)";
        Spi::run(create_table).unwrap();

        Spi::run(&copy_from).unwrap();

        let value = Spi::get_one::<Timestamp>("SELECT x FROM test_table LIMIT 1")
            .unwrap()
            .unwrap();
        assert_eq!(value, Timestamp::new(1000, 1, 1, 0, 0, 0.0).unwrap());
    }

    #[pg_test]
    fn test_coerce_decimal_to_integer_rounds() {
        // DECIMAL128 => {int}, rounded half away from zero like numeric::int4 does
        let decimals = || {
            Decimal128Array::from(vec![999, -999, 50, 150, 250, -50])
                .with_precision_and_scale(10, 2)
                .unwrap()
        };

        let element_field = Arc::new(Field::new("element", DataType::Decimal128(10, 2), true));
        let offsets = || OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 2, 3, 4, 5, 6]));

        let i = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6]));
        let x = Arc::new(decimals());
        let y = Arc::new(ListArray::new(
            element_field.clone(),
            offsets(),
            Arc::new(decimals()),
            None,
        ));
        let z =
            Arc::new(StructArray::try_from(vec![("a", Arc::new(decimals()) as ArrayRef)]).unwrap());
        let w = Arc::new(
            DictionaryArray::try_new(
                Int32Array::from(vec![0, 1, 2, 3, 4, 5]),
                Arc::new(decimals()),
            )
            .unwrap(),
        );
        let v = Arc::new(FixedSizeListArray::new(
            element_field.clone(),
            1,
            Arc::new(decimals()),
            None,
        ));

        // the narrower decimal types round the same way
        let u = Arc::new(
            Decimal32Array::from(vec![999, -999, 50, 150, 250, -50])
                .with_precision_and_scale(9, 2)
                .unwrap(),
        );
        let t = Arc::new(
            Decimal64Array::from(vec![999, -999, 50, 150, 250, -50])
                .with_precision_and_scale(18, 2)
                .unwrap(),
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int32, false),
            Field::new("x", DataType::Decimal128(10, 2), false),
            Field::new("y", DataType::List(element_field.clone()), false),
            Field::new(
                "z",
                DataType::Struct(vec![Field::new("a", DataType::Decimal128(10, 2), false)].into()),
                false,
            ),
            Field::new(
                "w",
                DataType::Dictionary(
                    Box::new(DataType::Int32),
                    Box::new(DataType::Decimal128(10, 2)),
                ),
                false,
            ),
            Field::new("v", DataType::FixedSizeList(element_field, 1), false),
            Field::new("u", DataType::Decimal32(9, 2), false),
            Field::new("t", DataType::Decimal64(18, 2), false),
        ]));

        let batch = RecordBatch::try_new(schema.clone(), vec![i, x, y, z, w, v, u, t]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        let create_type = "CREATE TYPE test_type AS (a int)";
        Spi::run(create_type).unwrap();

        let create_table =
            "CREATE TABLE test_table (i int, x int, y int[], z test_type, w smallint, v bigint[], u int, t bigint)";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let rounded = |column: &str| {
            Spi::get_one::<String>(&format!(
                "SELECT string_agg({column}::text, ',' ORDER BY i) FROM test_table"
            ))
            .unwrap()
        };

        assert_eq!(rounded("x").as_deref(), Some("10,-10,1,2,3,-1"));
        assert_eq!(rounded("y[1]").as_deref(), Some("10,-10,1,2,3,-1"));
        assert_eq!(rounded("(z).a").as_deref(), Some("10,-10,1,2,3,-1"));
        assert_eq!(rounded("w").as_deref(), Some("10,-10,1,2,3,-1"));
        assert_eq!(rounded("v[1]").as_deref(), Some("10,-10,1,2,3,-1"));
        assert_eq!(rounded("u").as_deref(), Some("10,-10,1,2,3,-1"));
        assert_eq!(rounded("t").as_deref(), Some("10,-10,1,2,3,-1"));
    }

    #[pg_test]
    fn test_coerce_decimal_to_integer_rounds_up_past_its_precision() {
        // a rounding that carries does not fit the scale of the column, e.g. DECIMAL(2,1) 9.6
        // rounds to 10, which needs both of the digits the precision allows
        let decimals = || {
            Decimal128Array::from(vec![96, -96, 5, 15, 25, -5])
                .with_precision_and_scale(2, 1)
                .unwrap()
        };

        let element_field = Arc::new(Field::new("element", DataType::Decimal128(2, 1), true));
        let offsets = || OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 2, 3, 4, 5, 6]));

        let i = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6]));
        let x = Arc::new(decimals());
        let y = Arc::new(ListArray::new(
            element_field.clone(),
            offsets(),
            Arc::new(decimals()),
            None,
        ));
        let z =
            Arc::new(StructArray::try_from(vec![("a", Arc::new(decimals()) as ArrayRef)]).unwrap());
        let w = Arc::new(
            DictionaryArray::try_new(
                Int32Array::from(vec![0, 1, 2, 3, 4, 5]),
                Arc::new(decimals()),
            )
            .unwrap(),
        );
        let v = Arc::new(FixedSizeListArray::new(
            element_field.clone(),
            1,
            Arc::new(decimals()),
            None,
        ));

        // the other decimal widths carry the same way
        let u = Arc::new(
            Decimal32Array::from(vec![96, -96, 5, 15, 25, -5])
                .with_precision_and_scale(2, 1)
                .unwrap(),
        );
        let t = Arc::new(
            Decimal64Array::from(vec![96, -96, 5, 15, 25, -5])
                .with_precision_and_scale(2, 1)
                .unwrap(),
        );
        let s = Arc::new(
            Decimal256Array::from(
                [96, -96, 5, 15, 25, -5]
                    .iter()
                    .map(|value| i256::from_i128(*value))
                    .collect::<Vec<_>>(),
            )
            .with_precision_and_scale(2, 1)
            .unwrap(),
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int32, false),
            Field::new("x", DataType::Decimal128(2, 1), false),
            Field::new("y", DataType::List(element_field.clone()), false),
            Field::new(
                "z",
                DataType::Struct(vec![Field::new("a", DataType::Decimal128(2, 1), false)].into()),
                false,
            ),
            Field::new(
                "w",
                DataType::Dictionary(
                    Box::new(DataType::Int32),
                    Box::new(DataType::Decimal128(2, 1)),
                ),
                false,
            ),
            Field::new("v", DataType::FixedSizeList(element_field, 1), false),
            Field::new("u", DataType::Decimal32(2, 1), false),
            Field::new("t", DataType::Decimal64(2, 1), false),
            Field::new("s", DataType::Decimal256(2, 1), false),
        ]));

        let batch = RecordBatch::try_new(schema.clone(), vec![i, x, y, z, w, v, u, t, s]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        let create_type = "CREATE TYPE test_type AS (a int)";
        Spi::run(create_type).unwrap();

        let create_table = "CREATE TABLE test_table (i int, x int, y int[], z test_type, w smallint, v bigint[], u int, t bigint, s int)";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let rounded = |column: &str| {
            Spi::get_one::<String>(&format!(
                "SELECT string_agg({column}::text, ',' ORDER BY i) FROM test_table"
            ))
            .unwrap()
        };

        assert_eq!(rounded("x").as_deref(), Some("10,-10,1,2,3,-1"));
        assert_eq!(rounded("y[1]").as_deref(), Some("10,-10,1,2,3,-1"));
        assert_eq!(rounded("(z).a").as_deref(), Some("10,-10,1,2,3,-1"));
        assert_eq!(rounded("w").as_deref(), Some("10,-10,1,2,3,-1"));
        assert_eq!(rounded("v[1]").as_deref(), Some("10,-10,1,2,3,-1"));
        assert_eq!(rounded("u").as_deref(), Some("10,-10,1,2,3,-1"));
        assert_eq!(rounded("t").as_deref(), Some("10,-10,1,2,3,-1"));
        assert_eq!(rounded("s").as_deref(), Some("10,-10,1,2,3,-1"));
    }

    #[pg_test]
    fn test_coerce_float_to_integer_rounds() {
        // FLOAT => {int}, rounded half to even like float8::int4 does through rint
        let floats = || vec![9.99, -9.99, 0.5, 1.5, 2.5, -0.5];

        // f16 cannot hold 9.99, and a tie is the point of the other values anyway
        let half_floats = || vec![9.5_f32, -9.5, 0.5, 1.5, 2.5, -0.5];

        let element_field = Arc::new(Field::new("element", DataType::Float64, true));

        let i = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6]));
        let x = Arc::new(Float64Array::from(floats()));
        let y = Arc::new(Float32Array::from(
            floats().into_iter().map(|f| f as f32).collect::<Vec<_>>(),
        ));
        let z = Arc::new(ListArray::new(
            element_field.clone(),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 2, 3, 4, 5, 6])),
            Arc::new(Float64Array::from(floats())),
            None,
        ));
        let w = arrow_cast::cast(&Float32Array::from(half_floats()), &DataType::Float16).unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int32, false),
            Field::new("x", DataType::Float64, false),
            Field::new("y", DataType::Float32, false),
            Field::new("z", DataType::List(element_field), false),
            Field::new("w", DataType::Float16, false),
        ]));

        let batch = RecordBatch::try_new(schema.clone(), vec![i, x, y, z, w]).unwrap();
        write_record_batch_to_parquet(schema, batch);

        let create_table = "CREATE TABLE test_table (i int, x int, y bigint, z int[], w smallint)";
        Spi::run(create_table).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let rounded = |column: &str| {
            Spi::get_one::<String>(&format!(
                "SELECT string_agg({column}::text, ',' ORDER BY i) FROM test_table"
            ))
            .unwrap()
        };

        assert_eq!(rounded("x").as_deref(), Some("10,-10,0,2,2,0"));
        assert_eq!(rounded("y").as_deref(), Some("10,-10,0,2,2,0"));
        assert_eq!(rounded("z[1]").as_deref(), Some("10,-10,0,2,2,0"));
        assert_eq!(rounded("w").as_deref(), Some("10,-10,0,2,2,0"));
    }
}
