#[pgrx::pg_schema]
mod tests {
    use std::{str::FromStr, vec};

    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, Decimal128Array, DictionaryArray, DurationMicrosecondArray, FixedSizeListBuilder,
        Int32Array, Int32Builder, LargeListBuilder, MapArray, RecordBatch, StringArray,
        StringViewArray, StructArray, Time32SecondArray, UInt16Array, UInt32Array, UInt64Array,
        UInt8Array,
    };
    use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    use arrow::datatypes::Int32Type;
    use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};
    use pgrx::{
        composite_type,
        datum::{Date, Time, Timestamp, TimestampWithTimeZone},
        pg_sys::{
            BOOLARRAYOID, BOOLOID, BYTEAARRAYOID, BYTEAOID, DATEARRAYOID, DATEOID, FLOAT4ARRAYOID,
            FLOAT4OID, FLOAT8ARRAYOID, FLOAT8OID, INT2ARRAYOID, INT2OID, INT4ARRAYOID, INT4OID,
            INT8ARRAYOID, INT8OID, JSONARRAYOID, JSONOID, NUMERICARRAYOID, NUMERICOID,
            TEXTARRAYOID, TEXTOID, TIMEARRAYOID, TIMEOID, TIMESTAMPARRAYOID, TIMESTAMPOID,
            TIMESTAMPTZARRAYOID, TIMESTAMPTZOID,
        },
        pg_test,
        spi::quote_literal,
        AnyNumeric, Json, Spi,
    };

    use crate::{
        pgrx_tests::common::{
            copy_to_helper, ensure_table_attribute_type, extension_exists,
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
        expected = "invalid uri: \"unsupported s3 uri https://account_id.r2.cloudflarestorage.com/bucket\""
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
        let entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("val", DataType::Int32, true),
                ]
                .into(),
            ),
            false,
        ));

        let schema = Arc::new(Schema::new(vec![Field::new(
            "m",
            DataType::Map(entries_field.clone(), false),
            true,
        )]));

        let keys: ArrayRef = Arc::new(StringArray::from(vec!["aa", "bb"]));
        let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));

        let entries = StructArray::try_from(vec![("key", keys), ("val", values)]).unwrap();

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

    // test_file returns a distinct parquet file path per test
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
}
