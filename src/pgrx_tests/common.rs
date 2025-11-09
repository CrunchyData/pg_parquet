use std::fs::File;
use std::marker::PhantomData;
use std::path::Path;
use std::{collections::HashMap, fmt::Debug};

use crate::type_compat::map::Map;

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use parquet::arrow::ArrowWriter;
use pgrx::{
    datum::{Time, TimeWithTimeZone},
    pg_sys::Oid,
    spi, FromDatum, IntoDatum, Spi,
};
use pgrx::{Json, JsonB};

pub(crate) enum CopyOptionValue {
    StringOption(String),
    IntOption(i64),
}

pub(crate) fn comma_separated_copy_options(options: &HashMap<String, CopyOptionValue>) -> String {
    let mut comma_sepated_options = String::new();

    for (option_idx, (key, value)) in options.iter().enumerate() {
        match value {
            CopyOptionValue::StringOption(value) => {
                comma_sepated_options.push_str(&format!("{key} '{value}'"));
            }
            CopyOptionValue::IntOption(value) => {
                comma_sepated_options.push_str(&format!("{key} {value}"));
            }
        }

        if option_idx < options.len() - 1 {
            comma_sepated_options.push_str(", ");
        }
    }

    comma_sepated_options
}

pub(crate) const LOCAL_TEST_FILE_PATH: &str = "/tmp/pg_parquet_test.parquet";

pub(crate) struct FileCleanup {
    path: String,
}

impl FileCleanup {
    pub(crate) fn new(path: &str) -> Self {
        let path = Path::new(path);
        std::fs::remove_dir_all(path).ok();
        std::fs::remove_file(path).ok();

        Self {
            path: path.to_str().unwrap().to_string(),
        }
    }
}

impl Drop for FileCleanup {
    fn drop(&mut self) {
        let path = Path::new(&self.path);
        std::fs::remove_dir_all(path).ok();
        std::fs::remove_file(path).ok();
    }
}

pub(crate) struct TestTable<T: IntoDatum + FromDatum> {
    uri: String,
    uri_pattern: Option<String>,
    order_by_col: String,
    copy_to_options: HashMap<String, CopyOptionValue>,
    copy_from_options: HashMap<String, CopyOptionValue>,
    _data: PhantomData<T>,
}

impl<T: IntoDatum + FromDatum> TestTable<T> {
    pub(crate) fn new(typename: String) -> Self {
        Spi::run("DROP TABLE IF EXISTS test_expected, test_result;").unwrap();

        let create_table_command = format!("CREATE TABLE test_expected (a {});", &typename);
        Spi::run(create_table_command.as_str()).unwrap();

        let create_table_command = format!("CREATE TABLE test_result (a {});", &typename);
        Spi::run(create_table_command.as_str()).unwrap();

        let mut copy_to_options = HashMap::new();
        copy_to_options.insert(
            "format".to_string(),
            CopyOptionValue::StringOption("parquet".to_string()),
        );

        let mut copy_from_options = HashMap::new();
        copy_from_options.insert(
            "format".to_string(),
            CopyOptionValue::StringOption("parquet".to_string()),
        );

        let uri = LOCAL_TEST_FILE_PATH.to_string();

        let order_by_col = "a".to_string();

        Self {
            uri,
            uri_pattern: None,
            order_by_col,
            copy_to_options,
            copy_from_options,
            _data: PhantomData,
        }
    }

    pub(crate) fn with_order_by_col(mut self, order_by_col: String) -> Self {
        self.order_by_col = order_by_col;
        self
    }

    pub(crate) fn with_copy_to_options(
        mut self,
        copy_to_options: HashMap<String, CopyOptionValue>,
    ) -> Self {
        self.copy_to_options = copy_to_options;
        self
    }

    pub(crate) fn with_copy_from_options(
        mut self,
        copy_from_options: HashMap<String, CopyOptionValue>,
    ) -> Self {
        self.copy_from_options = copy_from_options;
        self
    }

    pub(crate) fn with_uri(mut self, uri: String) -> Self {
        self.uri = uri;
        self
    }

    pub(crate) fn with_uri_pattern(mut self, uri_pattern: String) -> Self {
        self.uri_pattern = Some(uri_pattern);
        self
    }

    pub(crate) fn insert(&self, insert_command: &str) {
        Spi::run(insert_command).unwrap();
    }

    fn select_all(&self, table_name: &str) -> Vec<(Option<T>,)> {
        let select_command = format!(
            "SELECT a FROM {} ORDER BY {};",
            table_name, self.order_by_col
        );

        Spi::connect(|client| {
            let mut results = Vec::new();
            let tup_table = client.select(&select_command, None, &[]).unwrap();

            for row in tup_table {
                let val = row["a"].value::<T>();
                results.push((val.expect("could not select"),));
            }

            results
        })
    }

    pub(crate) fn copy_to_parquet(&self, table_name: &str) {
        let mut copy_to_query = format!("COPY (SELECT a FROM {}) TO '{}'", table_name, self.uri);

        if !self.copy_to_options.is_empty() {
            copy_to_query.push_str(" WITH (");

            let options_str = comma_separated_copy_options(&self.copy_to_options);
            copy_to_query.push_str(&options_str);

            copy_to_query.push(')');
        }

        copy_to_query.push(';');

        Spi::run(copy_to_query.as_str()).unwrap();
    }

    pub(crate) fn copy_from_parquet(&self, table_name: &str) {
        let uri = if let Some(uri_pattern) = &self.uri_pattern {
            uri_pattern
        } else {
            &self.uri
        };

        let mut copy_from_query = format!("COPY {} FROM '{}'", table_name, uri);

        if !self.copy_from_options.is_empty() {
            copy_from_query.push_str(" WITH (");

            let options_str = comma_separated_copy_options(&self.copy_from_options);
            copy_from_query.push_str(&options_str);

            copy_from_query.push(')');
        }

        copy_from_query.push(';');

        Spi::run(copy_from_query.as_str()).unwrap();
    }

    pub(crate) fn select_expected_and_result_rows(&self) -> TestResult<T> {
        self.copy_to_parquet("test_expected");
        self.copy_from_parquet("test_result");

        let expected = self.select_all("test_expected");
        let result = self.select_all("test_result");

        TestResult { expected, result }
    }

    pub(crate) fn assert_expected_and_result_rows(&self)
    where
        T: Debug + PartialEq,
    {
        let test_result = self.select_expected_and_result_rows();
        test_result.assert();
    }
}

pub(crate) struct TestResult<T> {
    pub(crate) expected: Vec<(Option<T>,)>,
    pub(crate) result: Vec<(Option<T>,)>,
}

impl<T> TestResult<T>
where
    T: Debug + PartialEq,
{
    // almost all types are comparable by common equality
    pub(crate) fn assert(&self) {
        for (expected, actual) in self.expected.iter().zip(self.result.iter()) {
            assert_eq!(expected, actual);
        }
    }
}

pub(crate) fn assert_int_text_map(expected: Option<Map>, actual: Option<Map>) {
    if let Some(expected) = expected {
        assert!(actual.is_some());

        let expected = expected.entries;
        let actual = actual.unwrap().entries;

        for (expected, actual) in expected.iter().zip(actual.iter()) {
            if let Some(expected) = expected {
                assert!(actual.is_some());

                let actual = actual.unwrap();

                let expected_key: Option<i32> = expected.get_by_name("key").unwrap();
                let actual_key: Option<i32> = actual.get_by_name("key").unwrap();

                assert_eq!(expected_key, actual_key);

                let expected_val: Option<String> = expected.get_by_name("val").unwrap();
                let actual_val: Option<String> = actual.get_by_name("val").unwrap();

                assert_eq!(expected_val, actual_val);
            } else {
                assert!(actual.is_none());
            }
        }
    } else {
        assert!(actual.is_none());
    }
}

pub(crate) fn assert_float(expected_result: Vec<Option<f32>>, result: Vec<Option<f32>>) {
    for (expected, actual) in expected_result.into_iter().zip(result.into_iter()) {
        if let Some(expected) = expected {
            assert!(actual.is_some());

            let actual = actual.unwrap();

            if expected.is_nan() {
                assert!(actual.is_nan());
            } else if expected.is_infinite() {
                assert!(actual.is_infinite());
                assert!(expected.is_sign_positive() == actual.is_sign_positive());
            } else {
                assert_eq!(expected, actual);
            }
        } else {
            assert!(actual.is_none());
        }
    }
}

pub(crate) fn assert_double(expected_result: Vec<Option<f64>>, result: Vec<Option<f64>>) {
    for (expected, actual) in expected_result.into_iter().zip(result.into_iter()) {
        if let Some(expected) = expected {
            assert!(actual.is_some());

            let actual = actual.unwrap();

            if expected.is_nan() {
                assert!(actual.is_nan());
            } else if expected.is_infinite() {
                assert!(actual.is_infinite());
                assert!(expected.is_sign_positive() == actual.is_sign_positive());
            } else {
                assert_eq!(expected, actual);
            }
        } else {
            assert!(actual.is_none());
        }
    }
}

pub(crate) fn assert_json(expected: Vec<Option<Json>>, result: Vec<Option<Json>>) {
    for (expected, actual) in expected.into_iter().zip(result.into_iter()) {
        if let Some(expected) = expected {
            assert!(actual.is_some());

            let actual = actual.unwrap();

            assert_eq!(expected.0, actual.0);
        } else {
            assert!(actual.is_none());
        }
    }
}

pub(crate) fn assert_jsonb(expected: Vec<Option<JsonB>>, result: Vec<Option<JsonB>>) {
    for (expected, actual) in expected.into_iter().zip(result.into_iter()) {
        if let Some(expected) = expected {
            assert!(actual.is_some());

            let actual = actual.unwrap();

            assert_eq!(expected.0, actual.0);
        } else {
            assert!(actual.is_none());
        }
    }
}

pub(crate) fn timetz_to_utc_time(timetz: TimeWithTimeZone) -> Option<Time> {
    Some(timetz.to_utc())
}

pub(crate) fn timetz_array_to_utc_time_array(
    timetz_array: Vec<Option<TimeWithTimeZone>>,
) -> Option<Vec<Option<Time>>> {
    Some(
        timetz_array
            .into_iter()
            .map(|timetz| timetz.map(|timetz| timetz.to_utc()))
            .collect(),
    )
}

pub(crate) fn is_extension_available(extension_name: &str) -> bool {
    let quoted_extension = spi::quote_literal(extension_name);
    let query =
        format!("select count(*) = 1 from pg_available_extensions where name = {quoted_extension}");

    Spi::get_one(&query).unwrap().unwrap()
}

pub(crate) fn write_record_batch_to_parquet(schema: SchemaRef, record_batch: RecordBatch) {
    let file = File::create(LOCAL_TEST_FILE_PATH).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();

    writer.write(&record_batch).unwrap();
    writer.close().unwrap();
}

pub(crate) fn create_crunchy_map_type(key_type: &str, val_type: &str) -> String {
    assert!(is_extension_available("crunchy_map"));

    let command = format!("SELECT crunchy_map.create('{key_type}','{val_type}')::text;",);
    Spi::get_one(&command).unwrap().unwrap()
}

pub(crate) fn ensure_table_attribute_type(
    table_name: &str,
    attribute_name: &str,
    expected_typoid: Oid,
    expected_typmod: i32,
) {
    let query = format!(
        "select atttypid, atttypmod from pg_attribute
          where attrelid = (select oid from pg_class where relname = '{}') and
                attname = '{}'",
        table_name, attribute_name
    );

    let (result_typoid, result_typmod) = Spi::get_two::<Oid, i32>(&query).unwrap();
    assert_eq!(expected_typoid, result_typoid.unwrap());
    assert_eq!(expected_typmod, result_typmod.unwrap());
}

pub(crate) fn copy_to_helper(uri: &str) {
    let create_type = "CREATE TYPE child AS (id int);
                       CREATE TYPE parent AS (id int, child child, children child[]);";
    Spi::run(create_type).unwrap();

    let copy_to = format!("COPY (SELECT 11::smallint AS a,
                                            array[11::smallint, null] AS a_array,
                                            232::int AS b,
                                            array[232::int, null] AS b_array,
                                            2342::bigint AS c,
                                            array[2342::bigint, null] AS c_array,
                                            12.34::float4 AS d,
                                            array[12.34::float4, null] AS d_array,
                                            123.325::float8 AS e,
                                            array[123.325::float8, null] AS e_array,
                                            123.24535::numeric AS f,
                                            array[123.24535::numeric, null] AS f_array,
                                            123.24535::numeric(8,5) AS f_with_typmod,
                                            array[123.24535::numeric(8,5), null::numeric(8,5)] AS f_with_typmod_array,
                                            false::bool AS g,
                                            array[false, null] AS g_array,
                                            '2022-05-05'::date AS h,
                                            array['2022-05-05'::date, null] AS h_array,
                                            '2022-05-05 13:00:00'::timestamp AS i,
                                            array['2022-05-05 13:00:00'::timestamp, null] AS i_array,
                                            '2022-05-05 13:00:00-05'::timestamptz AS j,
                                            array['2022-05-05 13:00:00-05'::timestamptz, null] AS j_array,
                                            '13:00:00'::time AS k,
                                            array['13:00:00'::time, null] AS k_array,
                                            '13:00:00-05'::timetz AS l,
                                            array['13:00:00-05'::timetz, null] AS l_array,
                                            '2 years 3 minutes 3 seconds'::interval AS m,
                                            array['2 years 3 minutes 3 seconds'::interval, null] AS m_array,
                                            'a'::\"char\" AS n,
                                            array['a'::\"char\", null] AS n_array,
                                            'hello'::text AS o,
                                            array['hello'::text, null] AS o_array,
                                            'hello'::bytea AS p,
                                            array['hello'::bytea, null] AS p_array,
                                            'hello'::varchar AS q,
                                            array['hello'::varchar, null] AS q_array,
                                            'hello'::bpchar AS r,
                                            array['hello'::bpchar, null] AS r_array,
                                            'hello'::name AS s,
                                            array['hello'::name, null] AS s_array,
                                            '{{\"id\": 12, \"name\": \"Doe\"}}'::json AS t,
                                            array['{{\"id\": 12, \"name\": \"Doe\"}}'::json, null] AS t_array,
                                            '{{\"id\": 12, \"name\": \"Doe\"}}'::jsonb AS u,
                                            array['{{\"id\": 12, \"name\": \"Doe\"}}'::jsonb, null] AS u_array,
                                            123::oid AS v,
                                            array[123::oid, null] AS v_array,
                                            row(1, row(10)::child, array[row(10), null]::child[])::parent AS y,
                                            array[row(1, row(10)::child, array[row(10), null]::child[])::parent, null] AS y_array) TO '{}'", uri);
    Spi::run(&copy_to).unwrap();
}
