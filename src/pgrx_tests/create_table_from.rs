#[pgrx::pg_schema]
mod tests {
    use std::{str::FromStr, vec};

    use pgrx::{
        composite_type,
        datum::{Date, Time, Timestamp, TimestampWithTimeZone},
        pg_sys::{
            Oid, BOOLARRAYOID, BOOLOID, BYTEAARRAYOID, BYTEAOID, DATEARRAYOID, DATEOID,
            FLOAT4ARRAYOID, FLOAT4OID, FLOAT8ARRAYOID, FLOAT8OID, INT2ARRAYOID, INT2OID,
            INT4ARRAYOID, INT4OID, INT8ARRAYOID, INT8OID, JSONARRAYOID, JSONOID, NUMERICARRAYOID,
            NUMERICOID, OIDARRAYOID, OIDOID, TEXTARRAYOID, TEXTOID, TIMEARRAYOID, TIMEOID,
            TIMESTAMPARRAYOID, TIMESTAMPOID, TIMESTAMPTZARRAYOID, TIMESTAMPTZOID,
        },
        pg_test, AnyNumeric, Json, Spi,
    };

    use crate::{
        pgrx_tests::common::{copy_to_helper, ensure_table_attribute_type, LOCAL_TEST_FILE_PATH},
        pgrx_utils::array_typoid,
        type_compat::pg_arrow_type_conversions::{
            make_numeric_typmod, DEFAULT_UNBOUNDED_NUMERIC_PRECISION,
            DEFAULT_UNBOUNDED_NUMERIC_SCALE,
        },
    };

    #[pg_test]
    fn test_create_table_definition_from() {
        copy_to_helper(LOCAL_TEST_FILE_PATH);

        let create_table =
            format!("CREATE TABLE test_table () WITH (definition_from = '{LOCAL_TEST_FILE_PATH}')");
        Spi::run(&create_table).unwrap();

        ensure_table_schema();
    }

    #[pg_test]
    fn test_create_table_load_from() {
        copy_to_helper(LOCAL_TEST_FILE_PATH);

        let create_table =
            format!("CREATE TABLE test_table () WITH (load_from = '{LOCAL_TEST_FILE_PATH}')");
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
                let v = row["v"].value::<Oid>().unwrap();
                let v_array = row["v_array"].value::<Vec<Option<Oid>>>().unwrap();
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
        let create_table = format!("CREATE TABLE test_table () WITH (load_from = '{LOCAL_TEST_FILE_PATH}', definition_from = '{LOCAL_TEST_FILE_PATH}')");
        Spi::run(&create_table).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "cannot create a partition table from a parquet file")]
    fn test_create_partition_table() {
        let create_partitioned_table = format!("CREATE TABLE test_table () PARTITION BY RANGE (a) WITH (definition_from = '{LOCAL_TEST_FILE_PATH}')");
        Spi::run(&create_partitioned_table).unwrap();

        let create_partition_table =
            format!("CREATE TABLE test_table_p PARTITION OF test_table FOR VALUES FROM (0) TO (10) WITH (load_from = '{LOCAL_TEST_FILE_PATH}')");
        Spi::run(&create_partition_table).unwrap();
    }

    #[pg_test]
    #[should_panic(
        expected = "cannot create a table from a parquet file when column definitions are provided"
    )]
    fn test_create_table_with_explicit_columns() {
        let create_table =
            format!("CREATE TABLE test_table (x int) WITH (load_from = '{LOCAL_TEST_FILE_PATH}')");
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
        ensure_table_attribute_type("test_table", "v", OIDOID, -1);
        ensure_table_attribute_type("test_table", "v_array", OIDARRAYOID, -1);

        // last type created under parquet_structs schema since child type is created first
        let parent_struct_oid =
            Spi::get_one("select oid from pg_type where typnamespace = 'parquet_structs'::regnamespace and typname like 'struct_%' order by oid desc LIMIT 1;")
                .unwrap()
                .unwrap();

        ensure_table_attribute_type("test_table", "y", parent_struct_oid, -1);
        ensure_table_attribute_type("test_table", "y_array", array_typoid(parent_struct_oid), -1);
    }
}
