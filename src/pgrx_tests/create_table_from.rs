#[pgrx::pg_schema]
mod tests {
    use std::{collections::HashMap, process::Command, str::FromStr, vec};

    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, Decimal128Array, DictionaryArray, DurationMicrosecondArray, FixedSizeBinaryArray,
        FixedSizeListBuilder, Int32Array, Int32Builder, LargeListBuilder, MapArray, RecordBatch,
        StringArray, StringViewArray, StructArray, Time32SecondArray, UInt16Array, UInt32Array,
        UInt64Array, UInt8Array,
    };
    use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    use arrow::datatypes::Int32Type;
    use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};
    use pgrx::{
        composite_type,
        datum::{Date, Time, Timestamp, TimestampWithTimeZone},
        pg_sys::{
            creating_extension as pg_creating_extension,
            CurrentExtensionObject as pg_current_extension_object, InvalidOid, Oid, BOOLARRAYOID,
            BOOLOID, BYTEAARRAYOID, BYTEAOID, DATEARRAYOID, DATEOID, FLOAT4ARRAYOID, FLOAT4OID,
            FLOAT8ARRAYOID, FLOAT8OID, INT2ARRAYOID, INT2OID, INT4ARRAYOID, INT4OID, INT8ARRAYOID,
            INT8OID, JSONARRAYOID, JSONOID, NUMERICARRAYOID, NUMERICOID, TEXTARRAYOID, TEXTOID,
            TIMEARRAYOID, TIMEOID, TIMESTAMPARRAYOID, TIMESTAMPOID, TIMESTAMPTZARRAYOID,
            TIMESTAMPTZOID, UUIDOID,
        },
        pg_test,
        spi::{quote_identifier, quote_literal},
        AnyNumeric, Json, Spi,
    };

    use crate::{
        arrow_parquet::schema::infer_schema::{
            map_typoid_from_name, STRUCT_TYPE_LOCK_CLASS, STRUCT_TYPE_LOCK_KEY,
        },
        pgrx_tests::common::{
            copy_to_helper, ensure_table_attribute_type, extension_exists, test_pg_port,
            write_record_batch_to_parquet_file,
        },
        pgrx_utils::array_typoid,
        type_compat::pg_arrow_type_conversions::{
            make_numeric_typmod, DEFAULT_UNBOUNDED_NUMERIC_PRECISION,
            DEFAULT_UNBOUNDED_NUMERIC_SCALE,
        },
    };

    #[pg_test]
    fn test_create_table_definition_from() {
        // the tests run concurrently, so each of them needs its own file
        let test_file = test_file("create_table_definition_from");
        copy_to_helper(&test_file);

        let create_table =
            format!("CREATE TABLE test_table () WITH (definition_from = '{test_file}')");
        Spi::run(&create_table).unwrap();

        ensure_table_schema();
    }

    #[pg_test]
    fn test_create_table_load_from() {
        // the tests run concurrently, so each of them needs its own file
        let test_file = test_file("create_table_load_from");
        copy_to_helper(&test_file);

        let create_table = format!("CREATE TABLE test_table () WITH (load_from = '{test_file}')");
        Spi::run(&create_table).unwrap();

        ensure_table_schema();

        let query = "SELECT * FROM test_table";
        let result = Spi::connect(|client| {
            let mut results = Vec::new();
            let tup_table = client.select(query, None, &[]).unwrap();

            for row in tup_table {
                let a = row["a"].value::<i16>().unwrap();
                let a_array = row["a_array"].value::<Vec<Option<i16>>>().unwrap();
                let b = row["b"].value::<i32>().unwrap();
                let b_array = row["b_array"].value::<Vec<Option<i32>>>().unwrap();
                let c = row["c"].value::<i64>().unwrap();
                let c_array = row["c_array"].value::<Vec<Option<i64>>>().unwrap();
                let d = row["d"].value::<f32>().unwrap();
                let d_array = row["d_array"].value::<Vec<Option<f32>>>().unwrap();
                let e = row["e"].value::<f64>().unwrap();
                let e_array = row["e_array"].value::<Vec<Option<f64>>>().unwrap();
                let f = row["f"].value::<AnyNumeric>().unwrap();
                let f_array = row["f_array"].value::<Vec<Option<AnyNumeric>>>().unwrap();
                let f_with_typmod = row["f_with_typmod"].value::<AnyNumeric>().unwrap();
                let f_with_typmod_array = row["f_with_typmod_array"]
                    .value::<Vec<Option<AnyNumeric>>>()
                    .unwrap();
                let g = row["g"].value::<bool>().unwrap();
                let g_array = row["g_array"].value::<Vec<Option<bool>>>().unwrap();
                let h = row["h"].value::<Date>().unwrap();
                let h_array = row["h_array"].value::<Vec<Option<Date>>>().unwrap();
                let i = row["i"].value::<Timestamp>().unwrap();
                let i_array = row["i_array"].value::<Vec<Option<Timestamp>>>().unwrap();
                let j = row["j"].value::<TimestampWithTimeZone>().unwrap();
                let j_array = row["j_array"]
                    .value::<Vec<Option<TimestampWithTimeZone>>>()
                    .unwrap();
                let k = row["k"].value::<Time>().unwrap();
                let k_array = row["k_array"].value::<Vec<Option<Time>>>().unwrap();
                let l = row["l"].value::<Time>().unwrap();
                let l_array = row["l_array"].value::<Vec<Option<Time>>>().unwrap();
                let m = row["m"].value::<String>().unwrap();
                let m_array = row["m_array"].value::<Vec<Option<String>>>().unwrap();
                let n = row["n"].value::<String>().unwrap();
                let n_array = row["n_array"].value::<Vec<Option<String>>>().unwrap();
                let o = row["o"].value::<String>().unwrap();
                let o_array = row["o_array"].value::<Vec<Option<String>>>().unwrap();
                let p = row["p"].value::<&[u8]>().unwrap();
                let q = row["q"].value::<String>().unwrap();
                let q_array = row["q_array"].value::<Vec<Option<String>>>().unwrap();
                let r = row["r"].value::<String>().unwrap();
                let r_array = row["r_array"].value::<Vec<Option<String>>>().unwrap();
                let s = row["s"].value::<String>().unwrap();
                let s_array = row["s_array"].value::<Vec<Option<String>>>().unwrap();
                let t = row["t"].value::<Json>().unwrap();
                let t_array = row["t_array"].value::<Vec<Option<Json>>>().unwrap();
                let u = row["u"].value::<Json>().unwrap();
                let u_array = row["u_array"].value::<Vec<Option<Json>>>().unwrap();
                let v = row["v"].value::<i64>().unwrap();
                let v_array = row["v_array"].value::<Vec<Option<i64>>>().unwrap();
                let y = row["y"].value::<composite_type!("parent")>().unwrap();

                results.push((
                    a,
                    a_array,
                    b,
                    b_array,
                    c,
                    c_array,
                    d,
                    d_array,
                    e,
                    e_array,
                    f,
                    f_array,
                    f_with_typmod,
                    f_with_typmod_array,
                    g,
                    g_array,
                    h,
                    h_array,
                    i,
                    i_array,
                    j,
                    j_array,
                    k,
                    k_array,
                    l,
                    l_array,
                    m,
                    m_array,
                    n,
                    n_array,
                    o,
                    o_array,
                    p,
                    q,
                    q_array,
                    r,
                    r_array,
                    s,
                    s_array,
                    t,
                    t_array,
                    u,
                    u_array,
                    v,
                    v_array,
                    y,
                ));
            }

            results
        });

        assert!(result.len() == 1);

        assert_eq!(result[0].0, Some(11));
        assert_eq!(result[0].1, Some(vec![Some(11), None]));
        assert_eq!(result[0].2, Some(232));
        assert_eq!(result[0].3, Some(vec![Some(232), None]));
        assert_eq!(result[0].4, Some(2342));
        assert_eq!(result[0].5, Some(vec![Some(2342), None]));
        assert_eq!(result[0].6, Some(12.34));
        assert_eq!(result[0].7, Some(vec![Some(12.34), None]));
        assert_eq!(result[0].8, Some(123.325));
        assert_eq!(result[0].9, Some(vec![Some(123.325), None]));
        assert_eq!(
            result[0].10,
            Some(AnyNumeric::from_str("123.24535").unwrap())
        );
        assert_eq!(
            result[0].11,
            Some(vec![Some(AnyNumeric::from_str("123.24535").unwrap()), None])
        );
        assert_eq!(
            result[0].12,
            Some(AnyNumeric::from_str("123.24535").unwrap())
        );
        assert_eq!(
            result[0].13,
            Some(vec![Some(AnyNumeric::from_str("123.24535").unwrap()), None])
        );
        assert_eq!(result[0].14, Some(false));
        assert_eq!(result[0].15, Some(vec![Some(false), None]));
        assert_eq!(result[0].16, Some(Date::from_str("2022-05-05").unwrap()));
        assert_eq!(
            result[0].17,
            Some(vec![Some(Date::from_str("2022-05-05").unwrap()), None])
        );
        assert_eq!(
            result[0].18,
            Some(Timestamp::from_str("2022-05-05 13:00:00").unwrap())
        );
        assert_eq!(
            result[0].19,
            Some(vec![
                Some(Timestamp::from_str("2022-05-05 13:00:00").unwrap()),
                None
            ])
        );
        assert_eq!(
            result[0].20,
            Some(TimestampWithTimeZone::from_str("2022-05-05 13:00:00-05").unwrap())
        );
        assert_eq!(
            result[0].21,
            Some(vec![
                Some(TimestampWithTimeZone::from_str("2022-05-05 13:00:00-05").unwrap()),
                None
            ])
        );
        assert_eq!(result[0].22, Some(Time::from_str("13:00:00").unwrap()));
        assert_eq!(
            result[0].23,
            Some(vec![Some(Time::from_str("13:00:00").unwrap()), None])
        );
        assert_eq!(result[0].24, Some(Time::from_str("18:00:00").unwrap()));
        assert_eq!(
            result[0].25,
            Some(vec![Some(Time::from_str("18:00:00").unwrap()), None])
        );
        assert_eq!(result[0].26, Some("2 years 00:03:03".into()));
        assert_eq!(
            result[0].27,
            Some(vec![Some("2 years 00:03:03".into()), None])
        );
        assert_eq!(result[0].28, Some("a".into()));
        assert_eq!(result[0].29, Some(vec![Some("a".into()), None]));
        assert_eq!(result[0].30, Some("hello".into()));
        assert_eq!(result[0].31, Some(vec![Some("hello".into()), None]));
        assert_eq!(result[0].32, Some("hello".as_bytes()));
        assert_eq!(result[0].33, Some("hello".into()));
        assert_eq!(result[0].34, Some(vec![Some("hello".into()), None]));
        assert_eq!(result[0].35, Some("hello".into()));
        assert_eq!(result[0].36, Some(vec![Some("hello".into()), None]));
        assert_eq!(result[0].37, Some("hello".into()));
        assert_eq!(result[0].38, Some(vec![Some("hello".into()), None]));
        assert_eq!(
            result[0].39.as_ref().unwrap().0,
            serde_json::from_str::<serde_json::Value>("{\"id\": 12, \"name\": \"Doe\"}").unwrap()
        );
        assert_eq!(
            result[0].40.as_ref().map(|json_arr| json_arr
                .iter()
                .map(|json| json.as_ref().map(|json| json.0.clone()))
                .collect::<Vec<_>>()),
            Some(vec![
                Some(
                    serde_json::from_str::<serde_json::Value>("{\"id\": 12, \"name\": \"Doe\"}")
                        .unwrap()
                ),
                None
            ])
        );
        assert_eq!(
            result[0].41.as_ref().unwrap().0,
            serde_json::from_str::<serde_json::Value>("{\"id\": 12, \"name\": \"Doe\"}").unwrap()
        );
        assert_eq!(
            result[0].42.as_ref().map(|json_arr| json_arr
                .iter()
                .map(|json| json.as_ref().map(|json| json.0.clone()))
                .collect::<Vec<_>>()),
            Some(vec![
                Some(
                    serde_json::from_str::<serde_json::Value>("{\"id\": 12, \"name\": \"Doe\"}")
                        .unwrap()
                ),
                None
            ])
        );
        assert_eq!(result[0].43, Some(123.into()));
        assert_eq!(result[0].44, Some(vec![Some(123.into()), None]));
        assert_eq!(
            result[0].45.as_ref().unwrap().get_by_name("id").unwrap(),
            Some(1)
        );
        assert_eq!(
            result[0]
                .45
                .as_ref()
                .unwrap()
                .get_by_name::<composite_type!("child")>("child")
                .unwrap()
                .unwrap()
                .get_by_name("id")
                .unwrap(),
            Some(10)
        );

        // Vec<Option<&[u8]>> does not implement FromDatum
        let query = "SELECT unnest(p_array) as p_array FROM test_table";
        let result = Spi::connect(|client| {
            let mut results = Vec::new();
            let tup_table = client.select(query, None, &[]).unwrap();

            for row in tup_table {
                let p_array = row["p_array"].value::<&[u8]>().unwrap();
                results.push(p_array);
            }

            results
        });

        assert_eq!(result.len(), 2);

        assert_eq!(result[0], Some("hello".as_bytes()));
        assert_eq!(result[1], None);

        // Vec<Option<&[PgHeapTuple]>> does not implement FromDatum
        let query = "SELECT unnest(y_array) as y_array FROM test_table";
        let result = Spi::connect(|client| {
            let mut results = Vec::new();
            let tup_table = client.select(query, None, &[]).unwrap();

            for row in tup_table {
                let y_array = row["y_array"].value::<composite_type!("parent")>().unwrap();
                results.push(y_array);
            }

            results
        });

        assert_eq!(result.len(), 2);

        assert_eq!(
            result[0].as_ref().unwrap().get_by_name("id").unwrap(),
            Some(1)
        );

        assert_eq!(
            result[0]
                .as_ref()
                .unwrap()
                .get_by_name::<composite_type!("child")>("child")
                .unwrap()
                .unwrap()
                .get_by_name("id")
                .unwrap(),
            Some(10)
        );

        assert!(result[1].is_none());
    }

    #[pg_test]
    #[should_panic(expected = "cannot specify both 'load_from' and 'definition_from' options")]
    fn test_create_table_specify_both_options() {
        // the tests run concurrently, so each of them needs its own file
        let test_file = test_file("create_table_specify_both_options");
        let create_table = format!("CREATE TABLE test_table () WITH (load_from = '{test_file}', definition_from = '{test_file}')");
        Spi::run(&create_table).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "cannot create a partition table from a parquet file")]
    fn test_create_partition_table() {
        // the tests run concurrently, so each of them needs its own file
        let test_file = test_file("create_partition_table");
        copy_to_helper(&test_file);

        let create_partitioned_table = format!("CREATE TABLE test_table () PARTITION BY RANGE (a) WITH (definition_from = '{test_file}')");
        Spi::run(&create_partitioned_table).unwrap();

        let create_partition_table =
            format!("CREATE TABLE test_table_p PARTITION OF test_table FOR VALUES FROM (0) TO (10) WITH (load_from = '{test_file}')");
        Spi::run(&create_partition_table).unwrap();
    }

    #[pg_test]
    #[should_panic(
        expected = "cannot create a table from a parquet file when column definitions are provided"
    )]
    fn test_create_table_with_explicit_columns() {
        // the tests run concurrently, so each of them needs its own file
        let test_file = test_file("create_table_with_explicit_columns");
        let create_table =
            format!("CREATE TABLE test_table (x int) WITH (load_from = '{test_file}')");
        Spi::run(&create_table).unwrap();
    }

    #[pg_test]
    #[should_panic(
        expected = "unsupported s3 uri https://account_id.r2.cloudflarestorage.com/bucket"
    )]
    fn test_create_table_with_unsupported_uri() {
        let create_table = "CREATE TABLE test_table () WITH (load_from = 'https://ACCOUNT_ID.r2.cloudflarestorage.com/bucket')";
        Spi::run(create_table).unwrap();
    }

    #[pg_test]
    fn test_create_table_unsigned_types() {
        // the tests run concurrently, so each of them needs its own file
        let test_file = test_file("create_table_unsigned_types");
        // Postgres has no unsigned integers, so each of them is widened
        let schema = Arc::new(Schema::new(vec![
            Field::new("u8", DataType::UInt8, true),
            Field::new("u16", DataType::UInt16, true),
            Field::new("u32", DataType::UInt32, true),
            Field::new("u64", DataType::UInt64, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt8Array::from(vec![0, u8::MAX])),
                Arc::new(UInt16Array::from(vec![0, u16::MAX])),
                Arc::new(UInt32Array::from(vec![0, u32::MAX])),
                Arc::new(UInt64Array::from(vec![0, u64::MAX])),
            ],
        )
        .unwrap();
        write_record_batch_to_parquet_file(&test_file, schema, batch);

        let create_table = format!("CREATE TABLE test_table () WITH (load_from = '{test_file}')");
        Spi::run(&create_table).unwrap();

        ensure_table_attribute_type("test_table", "u8", INT2OID, -1);
        ensure_table_attribute_type("test_table", "u16", INT4OID, -1);
        ensure_table_attribute_type("test_table", "u32", INT8OID, -1);
        ensure_table_attribute_type("test_table", "u64", NUMERICOID, make_numeric_typmod(20, 0));

        // no value is lost by the inferred types
        let max_u32 = Spi::get_one::<i64>("SELECT max(u32) FROM test_table").unwrap();
        assert_eq!(max_u32, Some(u32::MAX as i64));

        let max_u64 = Spi::get_one::<AnyNumeric>("SELECT max(u64) FROM test_table").unwrap();
        assert_eq!(
            max_u64,
            Some(AnyNumeric::from_str(&u64::MAX.to_string()).unwrap())
        );

        // "oid" would read 0 as NULL, which is why uint32 is not inferred as "oid"
        let zeros = Spi::get_one::<i64>(
            "SELECT count(*) FROM test_table WHERE u8 = 0 AND u16 = 0 AND u32 = 0 AND u64 = 0",
        )
        .unwrap();
        assert_eq!(zeros, Some(1));
    }

    #[pg_test]
    fn test_create_table_fixed_size_binary() {
        // the tests run concurrently, so each of them needs its own file
        let test_file = test_file("create_table_fixed_size_binary");

        // "COPY FROM" reads a 16 byte fixed binary into a uuid column whether or not the
        // file carries the arrow.uuid extension name, so both are inferred as uuid
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::FixedSizeBinary(16), true)
                .with_extension_type(arrow_schema::extension::Uuid),
            Field::new("b", DataType::FixedSizeBinary(16), true),
        ]));

        let value = |size| {
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                vec![Some(b"0123456789abcdef".to_vec())].into_iter(),
                size,
            )
            .unwrap()
        };

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(value(16)) as ArrayRef,
                Arc::new(value(16)) as ArrayRef,
            ],
        )
        .unwrap();
        write_record_batch_to_parquet_file(&test_file, schema, batch);

        let create_table = format!("CREATE TABLE test_table () WITH (load_from = '{test_file}')");
        Spi::run(&create_table).unwrap();

        ensure_table_attribute_type("test_table", "a", UUIDOID, -1);
        ensure_table_attribute_type("test_table", "b", UUIDOID, -1);

        // no byte is lost by reading the column as a uuid
        let value = Spi::get_one::<bool>(
            "SELECT a = '30313233-3435-3637-3839-616263646566'::uuid AND a = b FROM test_table",
        )
        .unwrap();
        assert_eq!(value, Some(true));
    }

    #[pg_test]
    fn test_create_table_fixed_size_binary_of_another_width() {
        // the tests run concurrently, so each of them needs its own file
        let test_file = test_file("create_table_fixed_size_binary_of_another_width");

        // 16 bytes are inferred as uuid, any other width as bytea
        let schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::FixedSizeBinary(8),
            true,
        )]));

        let value = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            vec![Some(b"01234567".to_vec())].into_iter(),
            8,
        )
        .unwrap();

        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(value) as ArrayRef]).unwrap();
        write_record_batch_to_parquet_file(&test_file, schema, batch);

        let create_table = format!("CREATE TABLE test_table () WITH (load_from = '{test_file}')");
        Spi::run(&create_table).unwrap();

        ensure_table_attribute_type("test_table", "a", BYTEAOID, -1);

        let value = Spi::get_one::<bool>("SELECT a = '01234567'::bytea FROM test_table").unwrap();
        assert_eq!(value, Some(true));
    }

    #[pg_test]
    fn test_create_table_misc_arrow_types() {
        // the tests run concurrently, so each of them needs its own file
        let test_file = test_file("create_table_misc_arrow_types");
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Time32(TimeUnit::Second), true),
            Field::new("b", DataType::Utf8View, true),
            Field::new(
                "c",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
            Field::new(
                "d",
                DataType::LargeList(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            ),
            Field::new(
                "e",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, true)), 2),
                true,
            ),
        ]));

        let mut large_list_builder = LargeListBuilder::new(Int32Builder::new());
        large_list_builder.values().append_value(1);
        large_list_builder.values().append_value(2);
        large_list_builder.append(true);

        let mut fixed_list_builder = FixedSizeListBuilder::new(Int32Builder::new(), 2);
        fixed_list_builder.values().append_value(3);
        fixed_list_builder.values().append_value(4);
        fixed_list_builder.append(true);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Time32SecondArray::from(vec![3661])),
                Arc::new(StringViewArray::from(vec!["view"])),
                Arc::new(
                    vec!["dict"]
                        .into_iter()
                        .collect::<DictionaryArray<Int32Type>>(),
                ),
                Arc::new(large_list_builder.finish()),
                Arc::new(fixed_list_builder.finish()),
            ],
        )
        .unwrap();
        write_record_batch_to_parquet_file(&test_file, schema, batch);

        let create_table = format!("CREATE TABLE test_table () WITH (load_from = '{test_file}')");
        Spi::run(&create_table).unwrap();

        ensure_table_attribute_type("test_table", "a", TIMEOID, -1);
        ensure_table_attribute_type("test_table", "b", TEXTOID, -1);
        ensure_table_attribute_type("test_table", "c", TEXTOID, -1);
        ensure_table_attribute_type("test_table", "d", INT4ARRAYOID, -1);
        ensure_table_attribute_type("test_table", "e", INT4ARRAYOID, -1);

        let row = Spi::get_one::<bool>(
            "SELECT a = '01:01:01'::time AND b = 'view' AND c = 'dict'
                    AND d = array[1,2] AND e = array[3,4] FROM test_table",
        )
        .unwrap();
        assert_eq!(row, Some(true));
    }

    #[pg_test]
    fn test_create_table_struct_field_name_is_an_identifier() {
        // the tests run concurrently, so each of them needs its own file
        let test_file = test_file("create_table_struct_field_name_is_an_identifier");
        // a struct field name must not be able to inject DDL
        let hostile_name = "x\" int); CREATE TABLE pwned(i int); --";

        let struct_fields: Fields = vec![Field::new(hostile_name, DataType::Int32, true)].into();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(struct_fields.clone()),
            true,
        )]));

        let struct_array = StructArray::new(
            struct_fields,
            vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
            None,
        );

        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(struct_array)]).unwrap();
        write_record_batch_to_parquet_file(&test_file, schema, batch);

        let create_table = format!("CREATE TABLE test_table () WITH (load_from = '{test_file}')");
        Spi::run(&create_table).unwrap();

        let injected = Spi::get_one::<bool>("SELECT to_regclass('pwned') IS NOT NULL").unwrap();
        assert_eq!(injected, Some(false));

        let attname = Spi::get_one::<String>(
            "SELECT a.attname::text FROM pg_attribute a, pg_type t
             WHERE t.typrelid = a.attrelid AND a.attnum > 0
             AND t.oid = (SELECT atttypid FROM pg_attribute
                          WHERE attrelid = 'test_table'::regclass AND attname = 's')",
        )
        .unwrap();
        assert_eq!(attname, Some(hostile_name.into()));

        let select_value = format!(
            "SELECT (to_jsonb(s) ->> {})::int FROM test_table",
            quote_literal(hostile_name)
        );
        let value = Spi::get_one::<i32>(&select_value).unwrap();
        assert_eq!(value, Some(1));
    }

    #[pg_test]
    fn test_create_table_struct_type_keeps_typmods() {
        // the tests run concurrently, so each of them needs its own file
        let test_file = test_file("create_table_struct_type_keeps_typmods");
        // two structs with the same field name but different typmods need two types
        let precise_fields: Fields =
            vec![Field::new("v", DataType::Decimal128(10, 2), true)].into();
        let coarse_fields: Fields = vec![Field::new("v", DataType::Decimal128(5, 1), true)].into();

        let schema = Arc::new(Schema::new(vec![
            Field::new("s1", DataType::Struct(precise_fields.clone()), true),
            Field::new("s2", DataType::Struct(coarse_fields.clone()), true),
        ]));

        let precise = Arc::new(StructArray::new(
            precise_fields,
            vec![Arc::new(
                Decimal128Array::from(vec![12345])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ) as ArrayRef],
            None,
        ));

        let coarse = Arc::new(StructArray::new(
            coarse_fields,
            vec![Arc::new(
                Decimal128Array::from(vec![123])
                    .with_precision_and_scale(5, 1)
                    .unwrap(),
            ) as ArrayRef],
            None,
        ));

        let batch = RecordBatch::try_new(schema.clone(), vec![precise, coarse]).unwrap();
        write_record_batch_to_parquet_file(&test_file, schema, batch);

        let create_table = format!("CREATE TABLE test_table () WITH (load_from = '{test_file}')");
        Spi::run(&create_table).unwrap();

        assert_eq!(struct_attribute_type("s1"), "numeric(10,2)");
        assert_eq!(struct_attribute_type("s2"), "numeric(5,1)");

        let value = Spi::get_one::<bool>(
            "SELECT (s1).v = 123.45::numeric AND (s2).v = 12.3::numeric FROM test_table",
        )
        .unwrap();
        assert_eq!(value, Some(true));
    }

    #[pg_test]
    #[should_panic(expected = "more than the 1600 a Postgres composite type can have")]
    fn test_create_table_struct_type_exceeds_column_limit() {
        // the tests run concurrently, so each of them needs its own file
        let test_file = test_file("create_table_struct_type_exceeds_column_limit");

        // one field more than a Postgres composite type can have
        let field_count: i32 = 1601;

        let struct_fields: Fields = (0..field_count)
            .map(|i| Field::new(format!("f{i}"), DataType::Int32, true))
            .collect::<Vec<_>>()
            .into();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(struct_fields.clone()),
            true,
        )]));

        let struct_array = StructArray::new(
            struct_fields,
            (0..field_count)
                .map(|i| Arc::new(Int32Array::from(vec![i])) as ArrayRef)
                .collect(),
            None,
        );

        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(struct_array)]).unwrap();
        write_record_batch_to_parquet_file(&test_file, schema, batch);

        let create_table =
            format!("CREATE TABLE test_table () WITH (definition_from = '{test_file}')");
        Spi::run(&create_table).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "already exists with a different definition")]
    fn test_create_table_struct_type_mismatches() {
        // the tests run concurrently, so each of them needs its own file
        let test_file = test_file("create_table_struct_type_mismatches");
        let struct_fields: Fields = vec![Field::new("v", DataType::Int32, true)].into();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(struct_fields.clone()),
            true,
        )]));

        let struct_array = StructArray::new(
            struct_fields,
            vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
            None,
        );

        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(struct_array)]).unwrap();
        write_record_batch_to_parquet_file(&test_file, schema, batch);

        let create_table = format!("CREATE TABLE test_table () WITH (load_from = '{test_file}')");
        Spi::run(&create_table).unwrap();

        // the type that is generated for the struct no longer matches the parquet file
        let struct_type = Spi::get_one::<String>(
            "SELECT format_type(atttypid, atttypmod) FROM pg_attribute
             WHERE attrelid = 'test_table'::regclass AND attname = 's'",
        )
        .unwrap()
        .unwrap();

        let alter_type = format!("ALTER TYPE {struct_type} ADD ATTRIBUTE extra int");
        Spi::run(&alter_type).unwrap();

        Spi::run("DROP TABLE test_table").unwrap();

        Spi::run(&create_table).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "is the rowtype of a relation")]
    fn test_create_table_struct_type_name_is_a_relation() {
        // the tests run concurrently, so each of them needs its own file. The attribute name
        // is unique as well, since it decides the name of the generated type
        let test_file = test_file("create_table_struct_type_name_is_a_relation");
        let struct_fields: Fields = vec![Field::new("rowtype_v", DataType::Int32, true)].into();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(struct_fields.clone()),
            true,
        )]));

        let struct_array = StructArray::new(
            struct_fields,
            vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
            None,
        );

        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(struct_array)]).unwrap();
        write_record_batch_to_parquet_file(&test_file, schema, batch);

        let create_table = format!("CREATE TABLE test_table () WITH (load_from = '{test_file}')");
        Spi::run(&create_table).unwrap();

        let struct_type = Spi::get_one::<String>(
            "SELECT format_type(atttypid, atttypmod) FROM pg_attribute
             WHERE attrelid = 'test_table'::regclass AND attname = 's'",
        )
        .unwrap()
        .unwrap();

        Spi::run("DROP TABLE test_table").unwrap();
        Spi::run(&format!("DROP TYPE {struct_type}")).unwrap();

        // the rowtype of this table has the attributes that the struct needs, but it belongs
        // to the table: its owner could change or drop the column of test_table with it
        Spi::run(&format!("CREATE TABLE {struct_type} (rowtype_v int)")).unwrap();

        Spi::run(&create_table).unwrap();
    }

    #[pg_test]
    fn test_create_table_if_not_exists() {
        // the tests run concurrently, so each of them needs its own file
        let test_file = test_file("create_table_if_not_exists");
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef],
        )
        .unwrap();
        write_record_batch_to_parquet_file(&test_file, schema, batch);

        let create_table =
            format!("CREATE TABLE IF NOT EXISTS test_table () WITH (load_from = '{test_file}')");
        Spi::run(&create_table).unwrap();

        // the parquet file is not loaded again into the existing table
        Spi::run(&create_table).unwrap();

        let count = Spi::get_one::<i64>("SELECT count(*) FROM test_table").unwrap();
        assert_eq!(count, Some(2));
    }

    #[pg_test]
    fn test_create_table_in_plpgsql_function() {
        // the tests run concurrently, so each of them needs its own file
        let test_file = test_file("create_table_in_plpgsql_function");
        let struct_fields: Fields = vec![Field::new("v", DataType::Int32, true)].into();

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("s", DataType::Struct(struct_fields.clone()), true),
        ]));

        let struct_array = StructArray::new(
            struct_fields,
            vec![Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef],
            None,
        );

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(struct_array),
            ],
        )
        .unwrap();
        write_record_batch_to_parquet_file(&test_file, schema, batch);

        let create_function = format!(
            "CREATE FUNCTION create_from_parquet() RETURNS void AS $$
             BEGIN
               CREATE TABLE test_table () WITH (load_from = '{test_file}');
             END;
             $$ LANGUAGE plpgsql"
        );
        Spi::run(&create_function).unwrap();

        // the cached plan of the function must survive the statement's rewrite
        for _ in 0..3 {
            Spi::run("SELECT create_from_parquet()").unwrap();

            let count = Spi::get_one::<i64>("SELECT count(*) FROM test_table").unwrap();
            assert_eq!(count, Some(2));

            Spi::run("DROP TABLE test_table").unwrap();
        }
    }

    #[pg_test]
    fn test_create_table_map_type() {
        // the tests run concurrently, so each of them needs its own file
        let test_file = test_file("create_table_map_type");
        // Skip the test if crunchy_map extension is not available
        if !extension_exists("crunchy_map") {
            return;
        }

        Spi::run("DROP EXTENSION IF EXISTS crunchy_map; CREATE EXTENSION crunchy_map;").unwrap();

        // the value field is named "val" by some writers and "value" by others,
        // so the map entries are matched by position
        let entries_fields: Fields = vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("val", DataType::Int32, true),
        ]
        .into();

        let entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(entries_fields.clone()),
            false,
        ));

        let schema = Arc::new(Schema::new(vec![Field::new(
            "m",
            DataType::Map(entries_field.clone(), false),
            true,
        )]));

        let keys: ArrayRef = Arc::new(StringArray::from(vec!["aa", "bb"]));
        let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));

        // the entries are built from the declared fields so that the nullability of the
        // value field matches the one in the schema
        let entries = StructArray::new(entries_fields, vec![keys, values], None);

        let map_array = MapArray::new(
            entries_field,
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 2])),
            entries,
            Some(NullBuffer::from(vec![true])),
            false,
        );

        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(map_array)]).unwrap();
        write_record_batch_to_parquet_file(&test_file, schema, batch);

        let create_table = format!("CREATE TABLE test_table () WITH (load_from = '{test_file}')");
        Spi::run(&create_table).unwrap();

        let value = Spi::get_one::<bool>(
            "SELECT m = array[('aa',1),('bb',2)]::crunchy_map.key_text_val_int4 FROM test_table",
        )
        .unwrap();
        assert_eq!(value, Some(true));
    }

    #[pg_test]
    #[should_panic(expected = "cannot infer a Postgres type for the parquet field \"a\"")]
    fn test_create_table_unsupported_arrow_type() {
        // the tests run concurrently, so each of them needs its own file
        let test_file = test_file("create_table_unsupported_arrow_type");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::Duration(TimeUnit::Microsecond),
            true,
        )]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(DurationMicrosecondArray::from(vec![1])) as ArrayRef],
        )
        .unwrap();
        write_record_batch_to_parquet_file(&test_file, schema, batch);

        let create_table =
            format!("CREATE TABLE test_table () WITH (definition_from = '{test_file}')");
        Spi::run(&create_table).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "parquet file has a field with an empty name")]
    fn test_create_table_empty_field_name() {
        // the tests run concurrently, so each of them needs its own file
        let test_file = test_file("create_table_empty_field_name");
        let schema = Arc::new(Schema::new(vec![Field::new("", DataType::Int32, true)]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
        )
        .unwrap();
        write_record_batch_to_parquet_file(&test_file, schema, batch);

        let create_table =
            format!("CREATE TABLE test_table () WITH (definition_from = '{test_file}')");
        Spi::run(&create_table).unwrap();
    }

    #[pg_test]
    #[should_panic(
        expected = "cannot create a table from a parquet file when the table inherits columns"
    )]
    fn test_create_table_inherits() {
        // the tests run concurrently, so each of them needs its own file
        let test_file = test_file("create_table_inherits");
        copy_to_helper(&test_file);

        Spi::run("CREATE TABLE test_parent (x int)").unwrap();

        let create_table = format!("CREATE TABLE test_table () INHERITS (test_parent) WITH (definition_from = '{test_file}')");
        Spi::run(&create_table).unwrap();
    }

    #[pg_test]
    #[should_panic(
        expected = "cannot create a table from a parquet file when the table is of a composite type"
    )]
    fn test_create_table_of_composite_type() {
        // the tests run concurrently, so each of them needs its own file
        let test_file = test_file("create_table_of_composite_type");
        copy_to_helper(&test_file);

        Spi::run("CREATE TYPE test_type AS (x int)").unwrap();

        let create_table =
            format!("CREATE TABLE test_table OF test_type WITH (definition_from = '{test_file}')");
        Spi::run(&create_table).unwrap();
    }

    #[pg_test]
    fn test_create_table_long_column_names() {
        // the tests run concurrently, so each of them needs its own file
        let test_file = test_file("create_table_long_column_names");
        // the names are longer than the identifier length limit, so they are truncated.
        // The multibyte one must be truncated at a character boundary, otherwise the
        // column name would not be valid UTF-8 anymore.
        let ascii_name = "a".repeat(70);
        let multibyte_name = "ü".repeat(40);

        let schema = Arc::new(Schema::new(vec![
            Field::new(&ascii_name, DataType::Int32, true),
            Field::new(&multibyte_name, DataType::Int32, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])) as ArrayRef,
                Arc::new(Int32Array::from(vec![2])) as ArrayRef,
            ],
        )
        .unwrap();
        write_record_batch_to_parquet_file(&test_file, schema, batch);

        let create_table = format!("CREATE TABLE test_table () WITH (load_from = '{test_file}')");
        Spi::run(&create_table).unwrap();

        let attnames = Spi::get_one::<Vec<String>>(
            "SELECT array_agg(attname::text ORDER BY attnum) FROM pg_attribute
             WHERE attrelid = 'test_table'::regclass AND attnum > 0",
        )
        .unwrap();
        assert_eq!(attnames, Some(vec!["a".repeat(63), "ü".repeat(31)]));

        let value = Spi::get_one::<bool>(&format!(
            "SELECT {} = 1 AND {} = 2 FROM test_table",
            quote_identifier("a".repeat(63)),
            quote_identifier("ü".repeat(31))
        ))
        .unwrap();
        assert_eq!(value, Some(true));
    }

    #[pg_test]
    fn test_create_table_long_struct_field_name() {
        // the tests run concurrently, so each of them needs its own file
        let test_file = test_file("create_table_long_struct_field_name");
        // the field name is longer than the identifier length limit. The composite type is
        // created with the truncated name, so the type must still be found and reused when
        // the same file is read again.
        let long_name = "f".repeat(70);

        let struct_fields: Fields = vec![Field::new(&long_name, DataType::Int32, true)].into();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(struct_fields.clone()),
            true,
        )]));

        let struct_array = StructArray::new(
            struct_fields,
            vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
            None,
        );

        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(struct_array)]).unwrap();
        write_record_batch_to_parquet_file(&test_file, schema, batch);

        let create_table = format!("CREATE TABLE test_table () WITH (load_from = '{test_file}')");
        Spi::run(&create_table).unwrap();

        let create_other_table =
            format!("CREATE TABLE test_table_2 () WITH (load_from = '{test_file}')");
        Spi::run(&create_other_table).unwrap();

        let same_type = Spi::get_one::<bool>(
            "SELECT (SELECT atttypid FROM pg_attribute
                     WHERE attrelid = 'test_table'::regclass AND attname = 's')
                  = (SELECT atttypid FROM pg_attribute
                     WHERE attrelid = 'test_table_2'::regclass AND attname = 's')",
        )
        .unwrap();
        assert_eq!(same_type, Some(true));

        let value = Spi::get_one::<i32>(&format!(
            "SELECT (s).{} FROM test_table",
            quote_identifier("f".repeat(63))
        ))
        .unwrap();
        assert_eq!(value, Some(1));
    }

    // test_file returns a distinct parquet file path per test
    #[pg_test]
    fn test_create_table_from_pattern_uri() {
        // the tests run concurrently, so each of them needs its own directory
        let test_dir = "/tmp/pg_parquet_test_create_table_pattern_uri";
        std::fs::create_dir_all(test_dir).unwrap();

        for file_name in ["a.parquet", "b.parquet"] {
            let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, true)]));

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
            )
            .unwrap();

            write_record_batch_to_parquet_file(&format!("{test_dir}/{file_name}"), schema, batch);
        }

        // the columns are inferred from the first file that matches the pattern and
        // COPY FROM loads all of them
        let create_table =
            format!("CREATE TABLE test_table () WITH (load_from = '{test_dir}/*.parquet')");
        Spi::run(&create_table).unwrap();

        ensure_table_attribute_type("test_table", "x", INT4OID, -1);

        let total = Spi::get_one::<i64>("SELECT count(*) FROM test_table").unwrap();
        assert_eq!(total, Some(2));
    }

    #[pg_test]
    #[should_panic(expected = "no files found that match the pattern")]
    fn test_create_table_from_pattern_uri_without_match() {
        let create_table =
            "CREATE TABLE test_table () WITH (definition_from = '/tmp/no_such_dir/*.parquet')";
        Spi::run(create_table).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "already exists with a different definition")]
    fn test_create_table_struct_type_name_taken_by_a_domain() {
        // the tests run concurrently, so each of them needs its own file
        let test_file = test_file("create_table_struct_type_name_taken_by_a_domain");
        write_struct_parquet_file(&test_file);

        let create_table = format!("CREATE TABLE test_table () WITH (load_from = '{test_file}')");
        Spi::run(&create_table).unwrap();

        // take the name of the type that pg_parquet generated with a domain
        let type_name = struct_type_name("test_table", "s");
        Spi::run("DROP TABLE test_table").unwrap();
        Spi::run(&format!("DROP TYPE {type_name}")).unwrap();
        Spi::run(&format!("CREATE DOMAIN {type_name} AS int")).unwrap();

        Spi::run(&create_table).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "already exists with a different definition")]
    fn test_create_table_struct_type_with_a_dropped_attribute() {
        // the tests run concurrently, so each of them needs its own file
        let test_file = test_file("create_table_struct_type_with_a_dropped_attribute");
        write_struct_parquet_file(&test_file);

        let create_table = format!("CREATE TABLE test_table () WITH (load_from = '{test_file}')");
        Spi::run(&create_table).unwrap();

        // a dropped attribute keeps its slot in the type, so the type is not reusable
        let type_name = struct_type_name("test_table", "s");
        Spi::run("DROP TABLE test_table").unwrap();
        Spi::run(&format!("ALTER TYPE {type_name} ADD ATTRIBUTE junk text")).unwrap();
        Spi::run(&format!("ALTER TYPE {type_name} DROP ATTRIBUTE junk")).unwrap();

        Spi::run(&create_table).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "parquet file has a field with an empty name in the struct \"s\"")]
    fn test_create_table_struct_with_an_empty_field_name() {
        // the tests run concurrently, so each of them needs its own file
        let test_file = test_file("create_table_struct_empty_field_name");

        let struct_fields: Fields = vec![Field::new("", DataType::Int32, true)].into();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(struct_fields.clone()),
            true,
        )]));

        let struct_array = StructArray::new(
            struct_fields,
            vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
            None,
        );

        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(struct_array)]).unwrap();
        write_record_batch_to_parquet_file(&test_file, schema, batch);

        let create_table = format!("CREATE TABLE test_table () WITH (load_from = '{test_file}')");
        Spi::run(&create_table).unwrap();
    }

    #[pg_test]
    #[should_panic(
        expected = "parquet file has more than one field named \"a\" in the struct \"s\""
    )]
    fn test_create_table_struct_with_duplicate_field_names() {
        // the tests run concurrently, so each of them needs its own file
        let test_file = test_file("create_table_struct_duplicate_field_names");

        let struct_fields: Fields = vec![
            Field::new("a", DataType::Int32, true),
            Field::new("a", DataType::Int32, true),
        ]
        .into();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(struct_fields.clone()),
            true,
        )]));

        let struct_array = StructArray::new(
            struct_fields,
            vec![
                Arc::new(Int32Array::from(vec![1])) as ArrayRef,
                Arc::new(Int32Array::from(vec![2])) as ArrayRef,
            ],
            None,
        );

        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(struct_array)]).unwrap();
        write_record_batch_to_parquet_file(&test_file, schema, batch);

        let create_table = format!("CREATE TABLE test_table () WITH (load_from = '{test_file}')");
        Spi::run(&create_table).unwrap();
    }

    #[pg_test]
    fn test_map_typoid_from_name() {
        // crunchy_map.create returns the name of the map type schema qualified, so a qualified
        // name must resolve. A bare name is resolved in the crunchy_map schema, which does not
        // exist without the extension, so it only resolves when it is in the search path.
        assert_eq!(map_typoid_from_name("pg_catalog.int4"), INT4OID);
        assert_eq!(map_typoid_from_name("int4"), INT4OID);
        assert_eq!(map_typoid_from_name("no_such_schema.int4"), InvalidOid);
        assert_eq!(map_typoid_from_name("no_such_type"), InvalidOid);
    }

    #[pg_test]
    #[should_panic(expected = "parquet field name \"nul13\\0name\" contains a nul byte")]
    fn test_create_table_from_field_name_with_a_nul_byte() {
        let test_file = test_file("create_table_field_name_with_a_nul_byte");

        // an identifier is a C string, so a name with a nul byte would silently lose
        // everything from the nul byte on
        let schema = Arc::new(Schema::new(vec![Field::new(
            "nul13\0name",
            DataType::Int32,
            true,
        )]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
        )
        .unwrap();

        write_record_batch_to_parquet_file(&test_file, schema, batch);

        let create_table = format!("CREATE TABLE test_table () WITH (load_from = '{test_file}')");
        Spi::run(&create_table).unwrap();
    }

    #[pg_test]
    fn test_create_table_while_an_extension_script_runs() {
        let test_file = test_file("create_table_while_an_extension_script_runs");

        // a field name no other test writes, so that the composite type for the struct is
        // generated by this test rather than found already created
        let struct_fields: Fields =
            vec![Field::new("extension_member_v", DataType::Int32, true)].into();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(struct_fields.clone()),
            true,
        )]));

        let struct_array = StructArray::new(
            struct_fields,
            vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
            None,
        );

        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(struct_array)]).unwrap();
        write_record_batch_to_parquet_file(&test_file, schema, batch);

        let extension_oid =
            Spi::get_one::<Oid>("SELECT oid FROM pg_extension WHERE extname = 'pg_parquet'")
                .unwrap()
                .expect("the pg_parquet extension is not created");

        // pretend that the command comes from the installation script of another extension,
        // which is what happens when that script creates a table from a parquet file
        unsafe {
            pg_creating_extension = true;
            pg_current_extension_object = extension_oid;
        }

        let create_table = format!("CREATE TABLE test_table () WITH (load_from = '{test_file}')");
        let result = Spi::run(&create_table);

        // restore the state before asserting anything, the backend is reused by the other tests
        unsafe {
            pg_creating_extension = false;
            pg_current_extension_object = InvalidOid;
        }

        result.unwrap();

        // the composite type is shared by every table that reads a matching parquet schema,
        // so it must not be dropped together with the extension that happened to create it
        let type_name = struct_type_name("test_table", "s");

        // the extension name is read with a scalar subquery, which gives a row even when
        // the type is a member of no extension, which is what this test expects
        let owner_extension = Spi::get_one::<String>(&format!(
            "SELECT (SELECT e.extname::text FROM pg_depend d, pg_extension e
                     WHERE d.classid = 'pg_type'::regclass AND d.deptype = 'e'
                     AND d.refobjid = e.oid AND d.objid = {}::regtype::oid)",
            quote_literal(&type_name)
        ))
        .unwrap();

        assert_eq!(
            owner_extension, None,
            "the generated type became a member of an extension"
        );
    }

    // write_struct_parquet_file writes a parquet file with a single struct column named "s"
    fn write_struct_parquet_file(test_file: &str) {
        let struct_fields: Fields = vec![Field::new("a", DataType::Int32, true)].into();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(struct_fields.clone()),
            true,
        )]));

        let struct_array = StructArray::new(
            struct_fields,
            vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
            None,
        );

        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(struct_array)]).unwrap();
        write_record_batch_to_parquet_file(test_file, schema, batch);
    }

    // struct_type_name returns the qualified name of the composite type that was generated
    // for the given struct column of the given table
    fn struct_type_name(table_name: &str, column_name: &str) -> String {
        let query = format!(
            "SELECT format_type(atttypid, atttypmod) FROM pg_attribute
             WHERE attrelid = '{table_name}'::regclass AND attname = '{column_name}'"
        );

        Spi::get_one(&query).unwrap().unwrap()
    }

    #[pg_test]
    fn test_create_table_from_without_the_extension() {
        // the tests run concurrently, so each of them needs its own file
        let test_file = test_file("create_table_without_extension");
        Spi::run(&format!("COPY (SELECT 1 AS a) TO '{test_file}'")).unwrap();

        // the hook is loaded via shared_preload_libraries, so it also runs in the databases
        // that do not have the extension created, like the default "postgres" one
        let create_table = format!("CREATE TABLE test_table () WITH (load_from = '{test_file}')");

        let output = Command::new("psql")
            .args([
                "-p",
                &test_pg_port(),
                "-h",
                "localhost",
                "-d",
                "postgres",
                "-c",
                &create_table,
            ])
            .output()
            .expect("failed to execute psql");

        let stderr = String::from_utf8_lossy(&output.stderr).to_string();

        assert!(
            stderr.contains("pg_parquet can handle this CREATE TABLE command but is not enabled"),
            "the extension warning is missing from the psql output: {stderr}"
        );

        // Postgres rejects the statement, since the option is not one of its own
        assert!(
            stderr.contains("unrecognized parameter \"load_from\""),
            "the statement was not left to Postgres: {stderr}"
        );
    }

    #[pg_test]
    fn test_create_table_from_arrow_extension_types() {
        // the tests run concurrently, so each of them needs its own file
        let test_file = test_file("create_table_arrow_extension_types");

        let extension_name = |name: &str| {
            HashMap::from_iter(vec![("ARROW:extension:name".into(), name.to_string())])
        };

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true)
                .with_extension_type(arrow_schema::extension::Json::default()),
            // an unrecognized extension type is read as its storage type
            Field::new("b", DataType::Utf8, true).with_metadata(extension_name("acme.secret")),
            Field::new("c", DataType::Utf8View, true).with_metadata(extension_name("acme.secret")),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["{\"x\": 1}"])),
                Arc::new(StringArray::from(vec!["secret"])),
                Arc::new(StringViewArray::from(vec!["view secret"])),
            ],
        )
        .unwrap();
        write_record_batch_to_parquet_file(&test_file, schema, batch);

        let create_table = format!("CREATE TABLE test_table () WITH (load_from = '{test_file}')");
        Spi::run(&create_table).unwrap();

        ensure_table_attribute_type("test_table", "a", JSONOID, -1);
        ensure_table_attribute_type("test_table", "b", TEXTOID, -1);
        ensure_table_attribute_type("test_table", "c", TEXTOID, -1);

        let row = Spi::get_one::<bool>(
            "SELECT a->>'x' = '1' AND b = 'secret' AND c = 'view secret' FROM test_table",
        )
        .unwrap();
        assert_eq!(row, Some(true));
    }

    #[pg_test]
    #[should_panic(expected = "unrecognized parameter \"load_from\"")]
    fn test_create_table_from_namespaced_option() {
        // the tests run concurrently, so each of them needs its own file
        let test_file = test_file("create_table_namespaced_option");
        copy_to_helper(&test_file);

        // "toast.load_from" belongs to the toast namespace, so Postgres rejects it
        let create_table =
            format!("CREATE TABLE test_table () WITH (toast.load_from = '{test_file}')");
        Spi::run(&create_table).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "parameter \"load_from\" specified more than once")]
    fn test_create_table_from_duplicate_option() {
        // the tests run concurrently, so each of them needs its own file
        let test_file = test_file("create_table_duplicate_option");
        copy_to_helper(&test_file);

        let create_table = format!(
            "CREATE TABLE test_table () WITH (load_from = '{test_file}', load_from = '{test_file}')"
        );
        Spi::run(&create_table).unwrap();
    }

    #[pg_test]
    fn test_create_table_from_keeps_namespaced_reloption() {
        // the tests run concurrently, so each of them needs its own file
        let test_file = test_file("create_table_keeps_namespaced_reloption");
        Spi::run(&format!("COPY (SELECT 'x'::text AS a) TO '{test_file}'")).unwrap();

        let create_table = format!(
            "CREATE TABLE test_table () WITH (load_from = '{test_file}', toast.vacuum_index_cleanup = off)"
        );
        Spi::run(&create_table).unwrap();

        // the option of the toast namespace survives the rewrite and reaches the toast relation
        let toast_option_is_set = Spi::get_one::<bool>(
            "SELECT t.reloptions @> array['vacuum_index_cleanup=off']
               FROM pg_class c JOIN pg_class t ON t.oid = c.reltoastrelid
              WHERE c.relname = 'test_table'",
        )
        .unwrap();
        assert_eq!(toast_option_is_set, Some(true));

        let a = Spi::get_one::<String>("SELECT a FROM test_table").unwrap();
        assert_eq!(a.as_deref(), Some("x"));
    }

    #[pg_test]
    fn test_create_table_from_if_not_exists_race() {
        let test_file = test_file("create_table_if_not_exists_race");
        let table_name = "create_table_from_race";

        // a struct column makes the column inference take the advisory lock that serializes the
        // composite type creation, which is where the other session catches up with us
        Spi::run("CREATE TYPE race_struct AS (race_attr_one bigint, race_attr_two text)").unwrap();
        Spi::run(&format!(
            "COPY (SELECT 42::bigint AS c, ROW(9, 'zz')::race_struct AS s) TO '{test_file}'"
        ))
        .unwrap();

        let database = Spi::get_one::<String>("SELECT current_database()::text")
            .unwrap()
            .expect("no current database");

        let psql_args = |command: &str| {
            vec![
                "-p".to_string(),
                test_pg_port(),
                "-h".to_string(),
                "localhost".to_string(),
                "-d".to_string(),
                database.clone(),
                "-c".to_string(),
                command.to_string(),
            ]
        };

        // the table of the other session is committed, so a leftover of a previous run
        // can only be dropped from another session
        Command::new("psql")
            .args(psql_args(&format!("DROP TABLE IF EXISTS {table_name}")))
            .output()
            .expect("failed to execute psql");

        // the concurrent tests take the same lock, so both sessions have to wait for each
        // other specifically. Waiting for any other session would let us take the lock while
        // the other session still waits for it, and then both of us would wait forever.
        let other_session_name = "pg_parquet_if_not_exists_race";

        let lock_granted_to_other_session = format!(
            "SELECT count(*) > 0 FROM pg_locks l JOIN pg_stat_activity a USING (pid)
              WHERE l.locktype = 'advisory' AND l.granted
                AND l.classid = {STRUCT_TYPE_LOCK_CLASS} AND l.objid = {STRUCT_TYPE_LOCK_KEY}
                AND a.application_name = {}",
            quote_literal(other_session_name)
        );

        let our_pid = Spi::get_one::<i32>("SELECT pg_backend_pid()")
            .unwrap()
            .expect("no backend pid");

        let we_wait_for_the_lock = format!(
            "SELECT count(*) > 0 FROM pg_locks
              WHERE locktype = 'advisory' AND NOT granted
                AND classid = {STRUCT_TYPE_LOCK_CLASS} AND objid = {STRUCT_TYPE_LOCK_KEY}
                AND pid = {our_pid}"
        );

        // the other session takes the lock, waits until we block on it, and then commits a
        // table with the name that we are about to create
        let concurrent_session = format!(
            "SET application_name = {};
             SELECT pg_advisory_xact_lock({STRUCT_TYPE_LOCK_CLASS}, {STRUCT_TYPE_LOCK_KEY});
             {}
             CREATE TABLE {table_name}(x text);
             INSERT INTO {table_name} VALUES ('from the other session');",
            quote_literal(other_session_name),
            wait_until(&we_wait_for_the_lock)
        );

        let mut concurrent_session = Command::new("psql")
            .args(psql_args(&concurrent_session))
            .spawn()
            .expect("failed to execute psql");

        Spi::run(&wait_until(&lock_granted_to_other_session)).unwrap();

        // taking the lock before the other session has it would make the two sessions wait for
        // each other, and Postgres cannot see that since we wait for a process, not for a lock
        let other_session_has_the_lock =
            Spi::get_one::<bool>(&lock_granted_to_other_session).unwrap();
        if other_session_has_the_lock != Some(true) {
            concurrent_session.kill().expect("failed to kill psql");
            concurrent_session.wait().expect("psql failed");
            panic!("the other session did not take the struct type creation lock");
        }

        // the inference blocks on the lock, so Postgres sees the table of the other session
        // and skips the creation. The file must not be loaded into that table.
        let create_table =
            format!("CREATE TABLE IF NOT EXISTS {table_name} () WITH (load_from = '{test_file}')");
        Spi::run(&create_table).unwrap();

        concurrent_session.wait().expect("psql failed");

        let rows =
            Spi::get_two::<i64, String>(&format!("SELECT count(*), min(x) FROM {table_name}"))
                .unwrap();
        assert_eq!(rows, (Some(1), Some("from the other session".into())));
    }

    // wait_until returns a command that waits until the given boolean query returns true.
    // It gives up after 5 minutes to not block the test suite forever. The timeout is that
    // long since the lock that the waits are about is also taken by the concurrent tests.
    fn wait_until(query: &str) -> String {
        format!(
            "DO $wait$
             DECLARE
                 attempt int := 0;
             BEGIN
                 WHILE attempt < 3000 AND NOT ({query}) LOOP
                     PERFORM pg_sleep(0.1);
                     attempt := attempt + 1;
                 END LOOP;
             END $wait$;"
        )
    }

    fn test_file(test_name: &str) -> String {
        format!("/tmp/pg_parquet_test_{test_name}.parquet")
    }

    // struct_attribute_type returns the type of the only attribute of the composite
    // type that was generated for the given struct column of "test_table"
    fn struct_attribute_type(column_name: &str) -> String {
        let query = format!(
            "SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a, pg_type t
             WHERE t.typrelid = a.attrelid AND a.attnum > 0
             AND t.oid = (SELECT atttypid FROM pg_attribute
                          WHERE attrelid = 'test_table'::regclass AND attname = '{column_name}')"
        );

        Spi::get_one(&query).unwrap().unwrap()
    }

    fn ensure_table_schema() {
        ensure_table_attribute_type("test_table", "a", INT2OID, -1);
        ensure_table_attribute_type("test_table", "a_array", INT2ARRAYOID, -1);
        ensure_table_attribute_type("test_table", "b", INT4OID, -1);
        ensure_table_attribute_type("test_table", "b_array", INT4ARRAYOID, -1);
        ensure_table_attribute_type("test_table", "c", INT8OID, -1);
        ensure_table_attribute_type("test_table", "c_array", INT8ARRAYOID, -1);
        ensure_table_attribute_type("test_table", "d", FLOAT4OID, -1);
        ensure_table_attribute_type("test_table", "d_array", FLOAT4ARRAYOID, -1);
        ensure_table_attribute_type("test_table", "e", FLOAT8OID, -1);
        ensure_table_attribute_type("test_table", "e_array", FLOAT8ARRAYOID, -1);
        ensure_table_attribute_type(
            "test_table",
            "f",
            NUMERICOID,
            make_numeric_typmod(
                DEFAULT_UNBOUNDED_NUMERIC_PRECISION as _,
                DEFAULT_UNBOUNDED_NUMERIC_SCALE as _,
            ),
        );
        ensure_table_attribute_type(
            "test_table",
            "f_array",
            NUMERICARRAYOID,
            make_numeric_typmod(
                DEFAULT_UNBOUNDED_NUMERIC_PRECISION as _,
                DEFAULT_UNBOUNDED_NUMERIC_SCALE as _,
            ),
        );
        ensure_table_attribute_type(
            "test_table",
            "f_with_typmod",
            NUMERICOID,
            make_numeric_typmod(8, 5),
        );
        ensure_table_attribute_type(
            "test_table",
            "f_with_typmod_array",
            NUMERICARRAYOID,
            make_numeric_typmod(8, 5),
        );
        ensure_table_attribute_type("test_table", "g", BOOLOID, -1);
        ensure_table_attribute_type("test_table", "g_array", BOOLARRAYOID, -1);
        ensure_table_attribute_type("test_table", "h", DATEOID, -1);
        ensure_table_attribute_type("test_table", "h_array", DATEARRAYOID, -1);
        ensure_table_attribute_type("test_table", "i", TIMESTAMPOID, -1);
        ensure_table_attribute_type("test_table", "i_array", TIMESTAMPARRAYOID, -1);
        ensure_table_attribute_type("test_table", "j", TIMESTAMPTZOID, -1);
        ensure_table_attribute_type("test_table", "j_array", TIMESTAMPTZARRAYOID, -1);
        ensure_table_attribute_type("test_table", "k", TIMEOID, -1);
        ensure_table_attribute_type("test_table", "k_array", TIMEARRAYOID, -1);
        ensure_table_attribute_type("test_table", "l", TIMEOID, -1);
        ensure_table_attribute_type("test_table", "l_array", TIMEARRAYOID, -1);
        ensure_table_attribute_type("test_table", "m", TEXTOID, -1);
        ensure_table_attribute_type("test_table", "m_array", TEXTARRAYOID, -1);
        ensure_table_attribute_type("test_table", "n", TEXTOID, -1);
        ensure_table_attribute_type("test_table", "n_array", TEXTARRAYOID, -1);
        ensure_table_attribute_type("test_table", "o", TEXTOID, -1);
        ensure_table_attribute_type("test_table", "o_array", TEXTARRAYOID, -1);
        ensure_table_attribute_type("test_table", "p", BYTEAOID, -1);
        ensure_table_attribute_type("test_table", "p_array", BYTEAARRAYOID, -1);
        ensure_table_attribute_type("test_table", "q", TEXTOID, -1);
        ensure_table_attribute_type("test_table", "q_array", TEXTARRAYOID, -1);
        ensure_table_attribute_type("test_table", "r", TEXTOID, -1);
        ensure_table_attribute_type("test_table", "r_array", TEXTARRAYOID, -1);
        ensure_table_attribute_type("test_table", "s", TEXTOID, -1);
        ensure_table_attribute_type("test_table", "s_array", TEXTARRAYOID, -1);
        ensure_table_attribute_type("test_table", "t", JSONOID, -1);
        ensure_table_attribute_type("test_table", "t_array", JSONARRAYOID, -1);
        ensure_table_attribute_type("test_table", "u", JSONOID, -1);
        ensure_table_attribute_type("test_table", "u_array", JSONARRAYOID, -1);
        // uint32 is widened to int8 since "oid" maps 0 to NULL
        ensure_table_attribute_type("test_table", "v", INT8OID, -1);
        ensure_table_attribute_type("test_table", "v_array", INT8ARRAYOID, -1);

        // last type created under parquet_structs schema since child type is created first
        let parent_struct_oid =
            Spi::get_one("select oid from pg_type where typnamespace = 'parquet_structs'::regnamespace and typname like 'struct_%' order by oid desc LIMIT 1;")
                .unwrap()
                .unwrap();

        ensure_table_attribute_type("test_table", "y", parent_struct_oid, -1);
        ensure_table_attribute_type("test_table", "y_array", array_typoid(parent_struct_oid), -1);
    }

    #[pg_test]
    fn test_create_table_from_struct_type_name_independent_of_search_path() {
        // the tests run concurrently, so each of them needs its own file
        let test_file = test_file("create_table_struct_search_path");

        // the name of the created outer type contains the name of the type of its struct
        // attribute, which is the name that a search_path can make unqualified
        Spi::run("CREATE TYPE sp_inner AS (sp_attr_one int, sp_attr_two text)").unwrap();
        Spi::run("CREATE TYPE sp_outer AS (sp_nested sp_inner, sp_attr_three bigint)").unwrap();
        Spi::run(&format!(
            "COPY (SELECT ROW(ROW(1, 'x'), 2)::sp_outer AS s) TO '{test_file}'"
        ))
        .unwrap();

        Spi::run(&format!(
            "CREATE TABLE sp_table_one () WITH (load_from = '{test_file}')"
        ))
        .unwrap();

        // the types that the first inference created are visible now
        Spi::run("SET search_path TO public, parquet_structs").unwrap();

        Spi::run(&format!(
            "CREATE TABLE sp_table_two () WITH (load_from = '{test_file}')"
        ))
        .unwrap();

        // the same file must give the same type, otherwise the two tables are not compatible
        let same_type = Spi::get_one::<bool>(
            "SELECT (SELECT atttypid FROM pg_attribute
                      WHERE attrelid = 'public.sp_table_one'::regclass AND attnum = 1)
                  = (SELECT atttypid FROM pg_attribute
                      WHERE attrelid = 'public.sp_table_two'::regclass AND attnum = 1)",
        )
        .unwrap();
        assert_eq!(same_type, Some(true));

        let row_count = Spi::get_one::<i64>("SELECT count(*) FROM public.sp_table_two").unwrap();
        assert_eq!(row_count, Some(1));
    }

    #[pg_test]
    fn test_create_table_struct_type_lock_not_hijackable_by_search_path() {
        // the tests run concurrently, so each of them needs its own file
        let test_file = test_file("create_table_struct_lock_search_path");

        Spi::run("CREATE TYPE hj_struct AS (hj_attr_one int, hj_attr_two text)").unwrap();
        Spi::run(&format!(
            "COPY (SELECT ROW(1, 'x')::hj_struct AS s) TO '{test_file}'"
        ))
        .unwrap();

        // pg_catalog is searched first only while the search_path does not name it, so an
        // earlier schema can shadow a function that is called without its schema. The
        // shadowing function throws, which fails the statement if it is the one that runs.
        Spi::run("CREATE SCHEMA hj").unwrap();
        Spi::run(
            "CREATE FUNCTION hj.pg_advisory_xact_lock(int, int) RETURNS void
             LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'hijacked'; END $$",
        )
        .unwrap();
        Spi::run("SET search_path TO hj, public, pg_catalog").unwrap();

        Spi::run(&format!(
            "CREATE TABLE public.hj_table () WITH (load_from = '{test_file}')"
        ))
        .unwrap();

        let row_count = Spi::get_one::<i64>("SELECT count(*) FROM public.hj_table").unwrap();
        assert_eq!(row_count, Some(1));
    }

    #[pg_test]
    #[should_panic(expected = "cannot execute CREATE TABLE in a read-only transaction")]
    fn test_create_table_from_read_only_transaction() {
        // the tests run concurrently, so each of them needs its own file
        let test_file = test_file("create_table_read_only");

        // a struct column makes the column inference create a composite type, which must not
        // be attempted in a transaction that cannot create the table in the first place
        Spi::run("CREATE TYPE ro_struct AS (ro_attr_one bigint, ro_attr_two text)").unwrap();
        Spi::run(&format!(
            "COPY (SELECT ROW(9, 'zz')::ro_struct AS s) TO '{test_file}'"
        ))
        .unwrap();

        Spi::run("SET TRANSACTION READ ONLY").unwrap();

        Spi::run(&format!(
            "CREATE TABLE test_table () WITH (load_from = '{test_file}')"
        ))
        .unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "parquet file has more than one field named \"a\"")]
    fn test_create_table_duplicate_field_names() {
        // the tests run concurrently, so each of them needs its own file
        let test_file = test_file("create_table_duplicate_field_names");

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("a", DataType::Int32, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])) as ArrayRef,
                Arc::new(Int32Array::from(vec![2])) as ArrayRef,
            ],
        )
        .unwrap();
        write_record_batch_to_parquet_file(&test_file, schema, batch);

        let create_table = format!("CREATE TABLE test_table () WITH (load_from = '{test_file}')");
        Spi::run(&create_table).unwrap();
    }

    #[pg_test]
    fn test_create_table_from_pattern_uri_picks_the_first_file_in_order() {
        // the tests run concurrently, so each of them needs its own directory
        let test_dir = "/tmp/pg_parquet_test_create_table_pattern_uri_order";
        std::fs::create_dir_all(test_dir).unwrap();

        // the file that comes first in lexicographic order is written last, so the local file
        // system lists it last. Only its column is expected in the table.
        for (file_name, column_name) in [
            ("d.parquet", "x"),
            ("c.parquet", "x"),
            ("b.parquet", "x"),
            ("a.parquet", "y"),
        ] {
            let schema = Arc::new(Schema::new(vec![Field::new(
                column_name,
                DataType::Int32,
                true,
            )]));

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
            )
            .unwrap();

            write_record_batch_to_parquet_file(&format!("{test_dir}/{file_name}"), schema, batch);
        }

        let create_table =
            format!("CREATE TABLE test_table () WITH (definition_from = '{test_dir}/*.parquet')");
        Spi::run(&create_table).unwrap();

        ensure_table_attribute_type("test_table", "y", INT4OID, -1);

        let column_count = Spi::get_one::<i64>(
            "SELECT count(*) FROM pg_attribute
              WHERE attrelid = 'test_table'::regclass AND attnum > 0",
        )
        .unwrap();
        assert_eq!(column_count, Some(1));
    }
    #[pg_test]
    fn test_create_table_in_a_latin1_database() {
        // the tests run concurrently, so each of them needs its own file and database
        let test_file = test_file("create_table_non_ascii_field_name");
        let test_database = "pg_parquet_test_latin1";

        let schema = Arc::new(Schema::new(vec![Field::new("ürün", DataType::Int32, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
        )
        .unwrap();
        write_record_batch_to_parquet_file(&test_file, schema, batch);

        // the client encoding is utf-8 so that Postgres converts what we send to the encoding
        // of the database, like a client of a latin1 database would
        let psql = |database: &str, command: &str| {
            Command::new("psql")
                .env("PGCLIENTENCODING", "UTF8")
                .args([
                    "-p",
                    &test_pg_port(),
                    "-h",
                    "localhost",
                    "-d",
                    database,
                    "-c",
                    command,
                ])
                .output()
                .expect("failed to execute psql")
        };

        // the database is created from template0 since the encoding differs from the template.
        // It is not dropped at the end: DROP DATABASE waits for every backend to absorb a
        // barrier, which the backend of this test cannot do while it waits for psql.
        let output = psql(
            "postgres",
            &format!(
                "CREATE DATABASE {test_database} ENCODING 'LATIN1'
                 LC_COLLATE 'C' LC_CTYPE 'C' TEMPLATE template0"
            ),
        );
        let create_database_stderr = String::from_utf8_lossy(&output.stderr).to_string();
        assert!(
            output.status.success() || create_database_stderr.contains("already exists"),
            "failed to create the latin1 database: {create_database_stderr}"
        );

        psql(test_database, "CREATE EXTENSION IF NOT EXISTS pg_parquet");

        // the field name is utf-8 in the file while the identifier has to be in the encoding of
        // the database, so the name is rejected rather than stored as its raw utf-8 bytes
        let output = psql(
            test_database,
            &format!("CREATE TABLE test_table () WITH (load_from = '{test_file}')"),
        );
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();

        assert!(
            stderr.contains("cannot be used as an identifier in a database with encoding"),
            "the field name was not rejected: {stderr}"
        );

        // a uri has to be utf-8 as well, and Postgres hands us the option in the encoding of
        // the database, so a uri that is not ascii cannot even be expressed here
        let output = psql(
            test_database,
            "CREATE TABLE test_table () WITH (load_from = '/tmp/ürün.parquet')",
        );
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();

        assert!(
            stderr.contains("the \"load_from\" option is not valid UTF-8"),
            "the uri was not rejected: {stderr}"
        );
    }
    #[pg_test]
    #[should_panic(expected = "test_renamed_away\" does not exist")]
    fn test_create_table_from_when_an_event_trigger_renames_the_table() {
        // the tests run concurrently, so each of them needs its own file
        let test_file = test_file("create_table_event_trigger_renames_the_table");
        Spi::run(&format!("COPY (SELECT 1 AS a) TO '{test_file}'")).unwrap();

        Spi::run(
            "CREATE FUNCTION rename_the_table() RETURNS event_trigger LANGUAGE plpgsql AS $$
             BEGIN
               IF EXISTS (SELECT 1 FROM pg_class WHERE relname = 'test_renamed_away') THEN
                 EXECUTE 'ALTER TABLE test_renamed_away RENAME TO test_renamed_away_new';
               END IF;
             END $$",
        )
        .unwrap();

        Spi::run(
            "CREATE EVENT TRIGGER rename_the_table_trg ON ddl_command_end
             WHEN TAG IN ('CREATE TABLE') EXECUTE FUNCTION rename_the_table()",
        )
        .unwrap();

        // the table we created is gone by the time the file is loaded, which "IF NOT EXISTS"
        // must report rather than skip the load as if a concurrent session owned the name
        let create_table = format!(
            "CREATE TABLE IF NOT EXISTS test_renamed_away () WITH (load_from = '{test_file}')"
        );
        Spi::run(&create_table).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "failed to find the struct type")]
    fn test_create_table_from_when_an_event_trigger_drops_the_generated_type() {
        // the tests run concurrently, so each of them needs its own file
        let test_file = test_file("create_table_event_trigger_drops_the_type");

        // the field name is unique to this test, so the type is generated here
        let struct_fields: Fields =
            vec![Field::new("dropped_type_v", DataType::Int32, true)].into();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Struct(struct_fields.clone()),
            true,
        )]));

        let struct_array = StructArray::new(
            struct_fields,
            vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
            None,
        );

        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(struct_array)]).unwrap();
        write_record_batch_to_parquet_file(&test_file, schema, batch);

        Spi::run(
            "CREATE FUNCTION drop_the_type() RETURNS event_trigger LANGUAGE plpgsql AS $$
             DECLARE command record;
             BEGIN
               FOR command IN SELECT * FROM pg_event_trigger_ddl_commands()
                              WHERE object_type = 'type' LOOP
                 EXECUTE format('DROP TYPE %s CASCADE', command.object_identity);
               END LOOP;
             END $$",
        )
        .unwrap();

        Spi::run(
            "CREATE EVENT TRIGGER drop_the_type_trg ON ddl_command_end
             WHEN TAG IN ('CREATE TYPE') EXECUTE FUNCTION drop_the_type()",
        )
        .unwrap();

        let create_table = format!("CREATE TABLE test_table () WITH (load_from = '{test_file}')");
        Spi::run(&create_table).unwrap();
    }
}
