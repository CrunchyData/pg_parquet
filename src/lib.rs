use parquet_copy_hook::hook::PARQUET_COPY_HOOK;
use pgrx::{prelude::*, register_hook};

mod arrow_parquet;
mod parquet_copy_hook;
mod pgrx_utils;
mod type_compat;

pgrx::pg_module_magic!();

#[allow(static_mut_refs)]
#[pg_guard]
pub extern "C" fn _PG_init() {
    unsafe { register_hook(&mut PARQUET_COPY_HOOK) };
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use std::marker::PhantomData;
    use std::{collections::HashMap, fmt::Debug};

    use crate::arrow_parquet::codec::ParquetCodecOption;
    use crate::parquet_copy_hook::copy_utils::DEFAULT_ROW_GROUP_SIZE;
    use crate::type_compat::{i128_to_numeric, Bit, Bpchar, Enum, Name, VarBit, Varchar};
    use pgrx::pg_sys::Oid;
    use pgrx::{
        composite_type, pg_test, AnyNumeric, Date, FromDatum, Interval, IntoDatum, Json, JsonB,
        Spi, Time, TimeWithTimeZone, Timestamp, TimestampWithTimeZone, Uuid,
    };
    enum CopyOptionValue {
        StringOption(String),
        IntOption(i64),
    }

    fn comma_separated_copy_options(options: &HashMap<String, CopyOptionValue>) -> String {
        let mut comma_sepated_options = String::new();

        let mut option_idx = 0;
        for (key, value) in options.iter() {
            match value {
                CopyOptionValue::StringOption(value) => {
                    comma_sepated_options.push_str(&format!("{} '{}'", key, value));
                }
                CopyOptionValue::IntOption(value) => {
                    comma_sepated_options.push_str(&format!("{} {}", key, value));
                }
            }

            if option_idx < options.len() - 1 {
                comma_sepated_options.push_str(", ");
            }

            option_idx += 1;
        }

        comma_sepated_options
    }

    struct TestTable<T: IntoDatum + FromDatum> {
        uri: String,
        order_by_col: String,
        copy_to_options: HashMap<String, CopyOptionValue>,
        copy_from_options: HashMap<String, CopyOptionValue>,
        _data: PhantomData<T>,
    }

    impl<T: IntoDatum + FromDatum> TestTable<T> {
        fn new(typename: String) -> Self {
            Spi::run("DROP TABLE IF EXISTS test;").unwrap();

            let create_table_command = format!("CREATE TABLE test (a {});", &typename);
            Spi::run(create_table_command.as_str()).unwrap();

            let mut copy_to_options = HashMap::new();
            copy_to_options.insert(
                "format".to_string(),
                CopyOptionValue::StringOption("parquet".to_string()),
            );
            copy_to_options.insert(
                "codec".to_string(),
                CopyOptionValue::StringOption(ParquetCodecOption::Uncompressed.to_string()),
            );
            copy_to_options.insert(
                "row_group_size".to_string(),
                CopyOptionValue::IntOption(DEFAULT_ROW_GROUP_SIZE as i64),
            );

            let mut copy_from_options = HashMap::new();
            copy_from_options.insert(
                "format".to_string(),
                CopyOptionValue::StringOption("parquet".to_string()),
            );

            let uri = "file:///tmp/test.parquet".to_string();

            let order_by_col = "a".to_string();

            Self {
                uri,
                order_by_col,
                copy_to_options,
                copy_from_options,
                _data: PhantomData,
            }
        }

        fn with_order_by_col(mut self, order_by_col: String) -> Self {
            self.order_by_col = order_by_col;
            self
        }

        fn with_copy_to_options(
            mut self,
            copy_to_options: HashMap<String, CopyOptionValue>,
        ) -> Self {
            self.copy_to_options = copy_to_options;
            self
        }

        fn with_copy_from_options(
            mut self,
            copy_from_options: HashMap<String, CopyOptionValue>,
        ) -> Self {
            self.copy_from_options = copy_from_options;
            self
        }

        fn with_uri(mut self, uri: String) -> Self {
            self.uri = uri;
            self
        }

        fn truncate(&self) {
            Spi::run("TRUNCATE test;").unwrap();
        }

        fn insert(&self, values: Vec<Option<T>>) {
            let insert_command = "INSERT INTO test VALUES ($1);";

            for value in values {
                let typoid = if let Some(typoid) = value.composite_type_oid() {
                    typoid
                } else {
                    T::type_oid()
                };

                Spi::run_with_args(
                    insert_command,
                    Some(vec![(typoid.into(), value.into_datum())]),
                )
                .unwrap();
            }
        }

        fn select_all(&self) -> Vec<(Option<T>,)> {
            let select_command = format!("SELECT a FROM test ORDER BY {};", self.order_by_col);

            Spi::connect(|client| {
                let mut results = Vec::new();
                let tup_table = client.select(&select_command, None, None).unwrap();

                for row in tup_table {
                    let val = row["a"].value::<T>();
                    results.push((val.unwrap(),));
                }

                results
            })
        }

        fn copy_to_parquet(&self) {
            let mut copy_to_query = format!("COPY (SELECT a FROM test) TO '{}'", self.uri);

            if !self.copy_to_options.is_empty() {
                copy_to_query.push_str(" WITH (");

                let options_str = comma_separated_copy_options(&self.copy_to_options);
                copy_to_query.push_str(&options_str);

                copy_to_query.push_str(")");
            }

            copy_to_query.push_str(";");

            Spi::run(copy_to_query.as_str()).unwrap();
        }

        fn copy_from_parquet(&self) {
            let mut copy_from_query = format!("COPY test FROM '{}'", self.uri);

            if !self.copy_from_options.is_empty() {
                copy_from_query.push_str(" WITH (");

                let options_str = comma_separated_copy_options(&self.copy_from_options);
                copy_from_query.push_str(&options_str);

                copy_from_query.push_str(")");
            }

            copy_from_query.push_str(";");

            Spi::run(copy_from_query.as_str()).unwrap();
        }
    }

    impl<T: IntoDatum + FromDatum> Drop for TestTable<T> {
        fn drop(&mut self) {
            if self.uri.starts_with("file://") {
                let path = self.uri.replace("file://", "");

                if std::path::Path::new(&path).exists() {
                    std::fs::remove_file(path).unwrap();
                }
            }
        }
    }

    fn timetz_to_utc_time(timetz: TimeWithTimeZone) -> Option<Time> {
        Some(timetz.to_utc())
    }

    fn timetz_array_to_utc_time_array(
        timetz_array: Vec<Option<TimeWithTimeZone>>,
    ) -> Option<Vec<Option<Time>>> {
        Some(
            timetz_array
                .into_iter()
                .map(|timetz| timetz.map(|timetz| timetz.to_utc()))
                .collect(),
        )
    }

    fn test_common<T: IntoDatum + FromDatum>(
        test_table: TestTable<T>,
        values: Vec<Option<T>>,
    ) -> (Vec<(Option<T>,)>, Vec<(Option<T>,)>) {
        test_table.insert(values);

        // Insert a NULL value to test NULL handling
        test_table.insert(vec![None]);

        let expected_result = test_table.select_all();

        test_table.copy_to_parquet();

        test_table.truncate();

        test_table.copy_from_parquet();

        let result = test_table.select_all();

        (expected_result, result)
    }

    fn test_assert<T>(expected_result: Vec<(Option<T>,)>, result: Vec<(Option<T>,)>)
    where
        T: Debug + PartialEq,
    {
        for (expected, actual) in expected_result.into_iter().zip(result.into_iter()) {
            assert_eq!(expected, actual);
        }
    }

    fn test_helper<T: IntoDatum + FromDatum + Debug + PartialEq>(
        test_table: TestTable<T>,
        values: Vec<Option<T>>,
    ) {
        let (expected_result, result) = test_common(test_table, values);
        test_assert(expected_result, result);
    }

    #[pg_test]
    fn test_int2() {
        let test_table = TestTable::<i16>::new("int2".into());
        let values = (1_i16..=10).into_iter().map(|v| Some(v)).collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_int2_array() {
        let test_table = TestTable::<Vec<Option<i16>>>::new("int2[]".into());
        let values = (1_i16..=10)
            .into_iter()
            .map(|v| Some(vec![Some(v), Some(v + 1), Some(v + 2)]))
            .collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_int4() {
        let test_table = TestTable::<i32>::new("int4".into());
        let values = (1_i32..=10).into_iter().map(|v| Some(v)).collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_int4_array() {
        let test_table = TestTable::<Vec<Option<i32>>>::new("int4[]".into());
        let values = (1_i32..=10)
            .into_iter()
            .map(|v| Some(vec![Some(v), Some(v + 1), Some(v + 2)]))
            .collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_int8() {
        let test_table = TestTable::<i64>::new("int8".into());
        let values = (1_i64..=10).into_iter().map(|v| Some(v)).collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_int8_array() {
        let test_table = TestTable::<Vec<Option<i64>>>::new("int8[]".into());
        let values = (1_i64..=10)
            .into_iter()
            .map(|v| Some(vec![Some(v), Some(v + 1), Some(v + 2)]))
            .collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_flaot4() {
        let test_table = TestTable::<f32>::new("float4".into());
        let values = (1..=10).into_iter().map(|v| Some(v as f32)).collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_float4_array() {
        let test_table = TestTable::<Vec<Option<f32>>>::new("float4[]".into());
        let values = (1..=10)
            .into_iter()
            .map(|v| {
                Some(vec![
                    Some(v as f32),
                    Some((v + 1) as f32),
                    Some((v + 2) as f32),
                ])
            })
            .collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_flaot8() {
        let test_table = TestTable::<f64>::new("float8".into());
        let values = (1..=10).into_iter().map(|v| Some(v as f64)).collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_float8_array() {
        let test_table = TestTable::<Vec<Option<f64>>>::new("float8[]".into());
        let values = (1..=10)
            .into_iter()
            .map(|v| {
                Some(vec![
                    Some(v as f64),
                    Some((v + 1) as f64),
                    Some((v + 2) as f64),
                ])
            })
            .collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_bool() {
        let test_table = TestTable::<bool>::new("bool".into());
        let values = (1..=10)
            .into_iter()
            .map(|v| Some(if v % 2 == 0 { false } else { true }))
            .collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_bool_array() {
        let test_table = TestTable::<Vec<Option<bool>>>::new("bool[]".into());
        let values = (1..=10)
            .into_iter()
            .map(|v| {
                Some(vec![
                    Some(if v % 2 == 0 { false } else { true }),
                    Some(if v % 2 == 0 { true } else { false }),
                    Some(if v % 2 == 0 { false } else { true }),
                ])
            })
            .collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_text() {
        let test_table = TestTable::<String>::new("text".into());
        let values = (1..=10)
            .into_iter()
            .map(|v| Some(format!("test_text_{}", v)))
            .collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_text_array() {
        let test_table = TestTable::<Vec<Option<String>>>::new("text[]".into());
        let values = (1..=10)
            .into_iter()
            .map(|v| {
                Some(vec![
                    Some(format!("test_text_{}", v)),
                    Some(format!("test_text_{}", v + 1)),
                    Some(format!("test_text_{}", v + 2)),
                ])
            })
            .collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_varchar() {
        let test_table = TestTable::<Varchar>::new("varchar".into());
        let values = (1..=10)
            .into_iter()
            .map(|v| Some(Varchar(format!("test_varchar_{}", v))))
            .collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_varchar_array() {
        let test_table = TestTable::<Vec<Option<Varchar>>>::new("varchar[]".into());
        let values = (1..=10)
            .into_iter()
            .map(|v| {
                Some(vec![
                    Some(Varchar(format!("test_varchar_{}", v))),
                    Some(Varchar(format!("test_varchar_{}", v + 1))),
                    Some(Varchar(format!("test_varchar_{}", v + 2))),
                ])
            })
            .collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_bpchar() {
        let test_table = TestTable::<Bpchar>::new("bpchar".into());
        let values = (1..=10)
            .into_iter()
            .map(|v| Some(Bpchar(format!("test_bpchar_{}", v))))
            .collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_bpchar_array() {
        let test_table = TestTable::<Vec<Option<Bpchar>>>::new("bpchar[]".into());
        let values = (1..=10)
            .into_iter()
            .map(|v| {
                Some(vec![
                    Some(Bpchar(format!("test_bpchar_{}", v))),
                    Some(Bpchar(format!("test_bpchar_{}", v + 1))),
                    Some(Bpchar(format!("test_bpchar_{}", v + 2))),
                ])
            })
            .collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_name() {
        let test_table = TestTable::<Name>::new("name".into());
        let values = (1..=10)
            .into_iter()
            .map(|v| Some(Name(format!("test_name_{}", v))))
            .collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_name_array() {
        let test_table = TestTable::<Vec<Option<Name>>>::new("name[]".into());
        let values = (1..=10)
            .into_iter()
            .map(|v| {
                Some(vec![
                    Some(Name(format!("test_name_{}", v))),
                    Some(Name(format!("test_name_{}", v + 1))),
                    Some(Name(format!("test_name_{}", v + 2))),
                ])
            })
            .collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_enum() {
        let create_enum_query = "CREATE TYPE color AS ENUM ('red', 'green', 'blue');";
        Spi::run(create_enum_query).unwrap();

        let enum_oid = Spi::get_one::<Oid>("SELECT 'color'::regtype::oid;")
            .unwrap()
            .unwrap();

        let test_table = TestTable::<Enum>::new("color".into());
        let values = vec![
            Some(Enum::new("red".into(), enum_oid)),
            Some(Enum::new("green".into(), enum_oid)),
            Some(Enum::new("blue".into(), enum_oid)),
        ];
        test_helper(test_table, values);

        let drop_enum_query = "DROP TYPE color CASCADE;";
        Spi::run(drop_enum_query).unwrap();
    }

    #[pg_test]
    fn test_enum_array() {
        let create_enum_query = "CREATE TYPE color AS ENUM ('red', 'green', 'blue');";
        Spi::run(create_enum_query).unwrap();

        let enum_oid = Spi::get_one::<Oid>("SELECT 'color'::regtype::oid;")
            .unwrap()
            .unwrap();

        let test_table = TestTable::<Vec<Option<Enum>>>::new("color[]".into());
        let values = vec![
            Some(vec![
                Some(Enum::new("red".into(), enum_oid)),
                Some(Enum::new("green".into(), enum_oid)),
                Some(Enum::new("blue".into(), enum_oid)),
            ]),
            Some(vec![
                Some(Enum::new("red".into(), enum_oid)),
                Some(Enum::new("green".into(), enum_oid)),
                Some(Enum::new("blue".into(), enum_oid)),
            ]),
            Some(vec![
                Some(Enum::new("red".into(), enum_oid)),
                Some(Enum::new("green".into(), enum_oid)),
                Some(Enum::new("blue".into(), enum_oid)),
            ]),
        ];
        test_helper(test_table, values);

        let drop_enum_query = "DROP TYPE color CASCADE;";
        Spi::run(drop_enum_query).unwrap();
    }

    #[pg_test]
    fn test_bit() {
        let test_table = TestTable::<Bit>::new("bit".into());
        let values = vec![Bit("0".into()), Bit("1".into())]
            .into_iter()
            .map(|v| Some(v))
            .collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_bit_array() {
        let test_table = TestTable::<Vec<Option<Bit>>>::new("bit[]".into());
        let values = vec![
            vec![Some(Bit("0".into())), Some(Bit("1".into()))],
            vec![Some(Bit("1".into()))],
            vec![Some(Bit("0".into()))],
        ]
        .into_iter()
        .map(|v| Some(v))
        .collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_varbit() {
        let test_table = TestTable::<VarBit>::new("varbit".into());
        let values = (1..=10)
            .into_iter()
            .map(|v| Some(VarBit(format!("0101").repeat(v))))
            .collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_varbit_array() {
        let test_table = TestTable::<Vec<Option<VarBit>>>::new("varbit[]".into());
        let values = (1..=10)
            .into_iter()
            .map(|v| {
                Some(vec![
                    Some(VarBit(format!("0101").repeat(v))),
                    Some(VarBit(format!("0101").repeat(v + 1))),
                    Some(VarBit(format!("0101").repeat(v + 2))),
                ])
            })
            .collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_char() {
        let test_table = TestTable::<i8>::new("\"char\"".into());
        let values = (1..=10).into_iter().map(|v| Some(v as i8)).collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_char_array() {
        let test_table = TestTable::<Vec<Option<i8>>>::new("\"char\"[]".into());
        let values = (1..=10)
            .into_iter()
            .map(|v| {
                Some(vec![
                    Some(v as i8),
                    Some((v + 1) as i8),
                    Some((v + 2) as i8),
                ])
            })
            .collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_bytea() {
        let test_table = TestTable::<Vec<u8>>::new("bytea".into());
        let values = (1..=10)
            .into_iter()
            .map(|v| Some(vec![v as u8, (v + 1) as u8, (v + 2) as u8]))
            .collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_bytea_array() {
        let test_table = TestTable::<Vec<Option<Vec<u8>>>>::new("bytea[]".into());
        let values = (1..=10)
            .into_iter()
            .map(|v| {
                Some(vec![
                    Some(vec![v as u8, (v + 1) as u8, (v + 2) as u8]),
                    Some(vec![(v + 3) as u8, (v + 4) as u8, (v + 5) as u8]),
                    Some(vec![(v + 6) as u8, (v + 7) as u8, (v + 8) as u8]),
                ])
            })
            .collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_oid() {
        let test_table = TestTable::<Oid>::new("oid".into());
        let values = (1_u32..=10).into_iter().map(|v| Some(v.into())).collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_oid_array() {
        let test_table = TestTable::<Vec<Option<Oid>>>::new("oid[]".into());
        let values = (1_u32..=10)
            .into_iter()
            .map(|v| {
                Some(vec![
                    Some(v.into()),
                    Some((v + 1).into()),
                    Some((v + 2).into()),
                ])
            })
            .collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_date() {
        let test_table = TestTable::<Date>::new("date".into());
        let values = (1_u8..=10)
            .into_iter()
            .map(|day| Some(Date::new(2022, 5, day).unwrap()))
            .collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_date_array() {
        let test_table = TestTable::<Vec<Option<Date>>>::new("date[]".into());
        let values = (1_u8..=10)
            .into_iter()
            .map(|day| {
                Some(vec![
                    Some(Date::new(2022, 5, day).unwrap()),
                    Some(Date::new(2022, 5, day + 1).unwrap()),
                    Some(Date::new(2022, 5, day + 2).unwrap()),
                ])
            })
            .collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_time() {
        let test_table = TestTable::<Time>::new("time".into());
        let values = (1_i64..=10).into_iter().map(|i| Some(i.into())).collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_time_array() {
        let test_table = TestTable::<Vec<Option<Time>>>::new("time[]".into());
        let values = (1_i64..=10)
            .into_iter()
            .map(|i| {
                Some(vec![
                    Some(i.into()),
                    Some((i + 1).into()),
                    Some((i + 2).into()),
                ])
            })
            .collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_timetz() {
        let test_table = TestTable::<TimeWithTimeZone>::new("timetz".into());
        let values = (1_u8..=10)
            .into_iter()
            .map(|minute| {
                Some(TimeWithTimeZone::with_timezone(5, minute, 15.0, "Europe/Istanbul").unwrap())
            })
            .collect();
        let (expected, actual) = test_common(test_table, values);

        // timetz is converted to utc timetz after copying to parquet,
        // so we need to the results to utc before comparing them
        let expected = expected
            .into_iter()
            .map(|(timetz,)| (timetz.and_then(timetz_to_utc_time),))
            .collect::<Vec<_>>();

        let actual = actual
            .into_iter()
            .map(|(timetz,)| (timetz.and_then(timetz_to_utc_time),))
            .collect::<Vec<_>>();

        test_assert(expected, actual);
    }

    #[pg_test]
    fn test_timetz_array() {
        let test_table = TestTable::<Vec<Option<TimeWithTimeZone>>>::new("timetz[]".into());
        let values = (1_u8..=10)
            .into_iter()
            .map(|minute| {
                Some(vec![
                    Some(
                        TimeWithTimeZone::with_timezone(5, minute, 15.0, "Europe/Istanbul")
                            .unwrap(),
                    ),
                    Some(
                        TimeWithTimeZone::with_timezone(5, minute + 1, 15.0, "Europe/Istanbul")
                            .unwrap(),
                    ),
                    Some(
                        TimeWithTimeZone::with_timezone(5, minute + 2, 15.0, "Europe/Istanbul")
                            .unwrap(),
                    ),
                ])
            })
            .collect();
        let (expected, actual) = test_common(test_table, values);

        // timetz is converted to utc timetz after copying to parquet,
        // so we need to the results to utc before comparing them
        let expected = expected
            .into_iter()
            .map(|(timetz,)| (timetz.and_then(timetz_array_to_utc_time_array),))
            .collect::<Vec<_>>();

        let actual = actual
            .into_iter()
            .map(|(timetz,)| (timetz.and_then(timetz_array_to_utc_time_array),))
            .collect::<Vec<_>>();

        test_assert(expected, actual);
    }

    #[pg_test]
    fn test_timestamp() {
        let test_table = TestTable::<Timestamp>::new("timestamp".into());
        let values = (1_i64..=10).into_iter().map(|i| Some(i.into())).collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_timestamp_array() {
        let test_table = TestTable::<Vec<Option<Timestamp>>>::new("timestamp[]".into());
        let values = (1_i64..=10)
            .into_iter()
            .map(|i| {
                Some(vec![
                    Some(i.into()),
                    Some((i + 1).into()),
                    Some((i + 2).into()),
                ])
            })
            .collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_timestamptz() {
        let test_table = TestTable::<TimestampWithTimeZone>::new("timestamptz".into());
        let values = (1_u8..=10)
            .into_iter()
            .map(|minute| {
                Some(
                    TimestampWithTimeZone::with_timezone(
                        2022,
                        3,
                        12,
                        5,
                        minute,
                        15.0,
                        "Europe/Istanbul",
                    )
                    .unwrap(),
                )
            })
            .collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_timestamptz_array() {
        let test_table =
            TestTable::<Vec<Option<TimestampWithTimeZone>>>::new("timestamptz[]".into());
        let values = (1_u8..=10)
            .into_iter()
            .map(|minute| {
                Some(vec![
                    Some(
                        TimestampWithTimeZone::with_timezone(
                            2022,
                            3,
                            12,
                            5,
                            minute,
                            15.0,
                            "Europe/Istanbul",
                        )
                        .unwrap(),
                    ),
                    Some(
                        TimestampWithTimeZone::with_timezone(
                            2022,
                            3,
                            12,
                            5,
                            minute + 1,
                            15.0,
                            "Europe/Istanbul",
                        )
                        .unwrap(),
                    ),
                    Some(
                        TimestampWithTimeZone::with_timezone(
                            2022,
                            3,
                            12,
                            5,
                            minute + 2,
                            15.0,
                            "Europe/Istanbul",
                        )
                        .unwrap(),
                    ),
                ])
            })
            .collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_interval() {
        let test_table = TestTable::<Interval>::new("interval".into());
        let values = (1_i32..=10)
            .into_iter()
            .map(|day| Some(Interval::new(5, day, 120).unwrap()))
            .collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_interval_array() {
        let test_table = TestTable::<Vec<Option<Interval>>>::new("interval[]".into());
        let values = (1_i32..=10)
            .into_iter()
            .map(|day| {
                Some(vec![
                    Some(Interval::new(5, day, 120).unwrap()),
                    Some(Interval::new(5, day + 1, 120).unwrap()),
                    Some(Interval::new(5, day + 2, 120).unwrap()),
                ])
            })
            .collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_uuid() {
        let uuids = vec![
            "00000000-0000-0000-0000-000000000001",
            "00000000-0000-0000-0000-000000000002",
            "00000000-0000-0000-0000-000000000003",
        ];

        let uuids = uuids
            .into_iter()
            .map(|uuid| {
                let uuid = uuid.replace("-", "");
                let bytes = u128::from_str_radix(&uuid, 16).unwrap();
                let bytes = bytes.to_be_bytes().to_vec();
                Uuid::from_slice(bytes.as_slice()).unwrap()
            })
            .collect::<Vec<_>>();

        let test_table = TestTable::<Uuid>::new("uuid".into());
        let values = uuids.into_iter().map(|v| Some(v)).collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_uuid_array() {
        let uuids = vec![
            "00000000-0000-0000-0000-000000000001",
            "00000000-0000-0000-0000-000000000002",
            "00000000-0000-0000-0000-000000000003",
        ];

        let uuids = uuids
            .into_iter()
            .map(|uuid| {
                let uuid = uuid.replace("-", "");
                let bytes = u128::from_str_radix(&uuid, 16).unwrap();
                let bytes = bytes.to_be_bytes().to_vec();
                Uuid::from_slice(bytes.as_slice()).unwrap()
            })
            .collect::<Vec<_>>();

        let test_table = TestTable::<Vec<Option<Uuid>>>::new("uuid[]".into());
        let values = uuids
            .into_iter()
            .map(|v| Some(vec![Some(v), Some(v), Some(v)]))
            .collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_json() {
        let test_table = TestTable::<Json>::new("json".into()).with_order_by_col("a->>'a'".into());
        let values = (1..=10)
            .into_iter()
            .map(|v| {
                Some(Json(
                    serde_json::from_str(format!("{{\"a\":\"test_json_{}\"}}", v).as_str())
                        .unwrap(),
                ))
            })
            .collect();
        let (expected_result, result) = test_common(test_table, values);

        for ((expected,), (actual,)) in expected_result.into_iter().zip(result.into_iter()) {
            if expected.is_none() {
                assert!(actual.is_none());
            }

            if expected.is_some() {
                assert!(actual.is_some());

                let expected = expected.unwrap();
                let actual = actual.unwrap();

                assert_eq!(expected.0, actual.0);
            }
        }
    }

    #[pg_test]
    fn test_json_array() {
        let test_table = TestTable::<Vec<Option<Json>>>::new("json[]".into())
            .with_order_by_col("a::text[]".into());
        let values = (1..=10)
            .into_iter()
            .map(|v| {
                Some(vec![
                    Some(Json(
                        serde_json::from_str(format!("{{\"a\":\"test_json_{}\"}}", v).as_str())
                            .unwrap(),
                    )),
                    Some(Json(
                        serde_json::from_str(format!("{{\"a\":\"test_json_{}\"}}", v + 1).as_str())
                            .unwrap(),
                    )),
                    Some(Json(
                        serde_json::from_str(format!("{{\"a\":\"test_json_{}\"}}", v + 2).as_str())
                            .unwrap(),
                    )),
                ])
            })
            .collect();
        let (expected_result, result) = test_common(test_table, values);

        for ((expected,), (actual,)) in expected_result.into_iter().zip(result.into_iter()) {
            if expected.is_none() {
                assert!(actual.is_none());
            }

            if expected.is_some() {
                assert!(actual.is_some());

                let expected = expected.unwrap();
                let actual = actual.unwrap();

                for (expected, actual) in expected.into_iter().zip(actual.into_iter()) {
                    if expected.is_none() {
                        assert!(actual.is_none());
                    }

                    if expected.is_some() {
                        assert!(actual.is_some());

                        let expected = expected.unwrap();
                        let actual = actual.unwrap();

                        assert_eq!(expected.0, actual.0);
                    }
                }
            }
        }
    }

    #[pg_test]
    fn test_jsonb() {
        let test_table =
            TestTable::<JsonB>::new("jsonb".into()).with_order_by_col("a->>'a'".into());
        let values = (1..=10)
            .into_iter()
            .map(|v| {
                Some(JsonB(
                    serde_json::from_str(format!("{{\"a\":\"test_jsonb_{}\"}}", v).as_str())
                        .unwrap(),
                ))
            })
            .collect();
        let (expected_result, result) = test_common(test_table, values);

        for ((expected,), (actual,)) in expected_result.into_iter().zip(result.into_iter()) {
            if expected.is_none() {
                assert!(actual.is_none());
            }

            if expected.is_some() {
                assert!(actual.is_some());

                let expected = expected.unwrap();
                let actual = actual.unwrap();

                assert_eq!(expected.0, actual.0);
            }
        }
    }

    #[pg_test]
    fn test_jsonb_array() {
        let test_table = TestTable::<Vec<Option<JsonB>>>::new("jsonb[]".into());
        let values = (1..=10)
            .into_iter()
            .map(|v| {
                Some(vec![
                    Some(JsonB(
                        serde_json::from_str(format!("{{\"a\":\"test_jsonb_{}\"}}", v).as_str())
                            .unwrap(),
                    )),
                    Some(JsonB(
                        serde_json::from_str(
                            format!("{{\"a\":\"test_jsonb_{}\"}}", v + 1).as_str(),
                        )
                        .unwrap(),
                    )),
                    Some(JsonB(
                        serde_json::from_str(
                            format!("{{\"a\":\"test_jsonb_{}\"}}", v + 2).as_str(),
                        )
                        .unwrap(),
                    )),
                ])
            })
            .collect();
        let (expected_result, result) = test_common(test_table, values);

        for ((expected,), (actual,)) in expected_result.into_iter().zip(result.into_iter()) {
            if expected.is_none() {
                assert!(actual.is_none());
            }

            if expected.is_some() {
                assert!(actual.is_some());

                let expected = expected.unwrap();
                let actual = actual.unwrap();

                for (expected, actual) in expected.into_iter().zip(actual.into_iter()) {
                    if expected.is_none() {
                        assert!(actual.is_none());
                    }

                    if expected.is_some() {
                        assert!(actual.is_some());

                        let expected = expected.unwrap();
                        let actual = actual.unwrap();

                        assert_eq!(expected.0, actual.0);
                    }
                }
            }
        }
    }

    #[pg_test]
    fn test_numeric() {
        let test_table = TestTable::<AnyNumeric>::new("numeric".into());
        let values = (1_i32..=10)
            .into_iter()
            .map(|v| i128_to_numeric(v as i128))
            .collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_numeric_array() {
        let test_table = TestTable::<Vec<Option<AnyNumeric>>>::new("numeric[]".into());
        let values = (1_i32..=10)
            .into_iter()
            .map(|v| {
                Some(vec![
                    i128_to_numeric(v as i128),
                    i128_to_numeric((v + 1) as i128),
                    i128_to_numeric((v + 2) as i128),
                ])
            })
            .collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_empty_array() {
        let test_table = TestTable::<Vec<Option<i32>>>::new("int4[]".into());
        let values = vec![Some(vec![Some(1), Some(2)]), Some(vec![])];
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_complex_composite() {
        Spi::run("CREATE TYPE dog AS (name text, age int);").unwrap();
        Spi::run("CREATE TYPE dog_owner AS (name text, dogs dog[], lucky_numbers int[]);").unwrap();
        Spi::run("CREATE TABLE dog_owners (owner dog_owner);").unwrap();

        Spi::run("INSERT INTO dog_owners VALUES (ROW('Alice', ARRAY[('Buddy', 2)::dog, ('Charlie', 3)::dog], ARRAY[1, 2, 3]));").unwrap();
        Spi::run("INSERT INTO dog_owners VALUES (ROW('Bob', ARRAY[('Daisy', 4)::dog, ('Ella', 5)::dog], ARRAY[4, 5, 6]));").unwrap();
        Spi::run("INSERT INTO dog_owners VALUES (ROW('Cathy', NULL, NULL));").unwrap();
        Spi::run("INSERT INTO dog_owners VALUES (NULL);").unwrap();

        let select_command = "SELECT owner FROM dog_owners ORDER BY owner;";
        let expected_result = Spi::connect(|client| {
            let mut results = Vec::new();
            let tup_table = client.select(select_command, None, None).unwrap();

            for row in tup_table {
                let owner = row["owner"].value::<composite_type!("dog_owner")>();
                results.push(owner.unwrap());
            }

            results
        });

        Spi::run("TRUNCATE dog_owners;").unwrap();

        let uri = "file:///tmp/test.parquet";

        let copy_to_query = format!(
            "COPY (SELECT owner FROM dog_owners) TO '{}' WITH (format parquet);",
            uri
        );
        Spi::run(copy_to_query.as_str()).unwrap();

        Spi::run("TRUNCATE dog_owners;").unwrap();

        let copy_from_query = format!("COPY dog_owners FROM '{}' WITH (format parquet);", uri);
        Spi::run(copy_from_query.as_str()).unwrap();

        let result = Spi::connect(|client| {
            let mut results = Vec::new();
            let tup_table = client.select(select_command, None, None).unwrap();

            for row in tup_table {
                let owner = row["owner"].value::<composite_type!("dog_owner")>();
                results.push(owner.unwrap());
            }

            results
        });

        for (expected, actual) in expected_result.into_iter().zip(result.into_iter()) {
            if expected.is_none() {
                assert!(actual.is_none());
            } else if expected.is_some() {
                assert!(actual.is_some());

                let expected = expected.unwrap();
                let actual = actual.unwrap();

                assert_eq!(
                    expected.get_by_name::<String>("name").unwrap(),
                    actual.get_by_name::<String>("name").unwrap()
                );

                let expected_dogs = expected
                    .get_by_name::<Vec<Option<composite_type!("dog")>>>("dogs")
                    .unwrap();
                let actual_dogs = actual
                    .get_by_name::<Vec<Option<composite_type!("dog")>>>("dogs")
                    .unwrap();

                if expected_dogs.is_none() {
                    assert!(actual_dogs.is_none());
                } else if expected_dogs.is_some() {
                    assert!(actual_dogs.is_some());

                    let expected_dogs = expected_dogs.unwrap();
                    let actual_dogs = actual_dogs.unwrap();

                    for (expected_dog, actual_dog) in
                        expected_dogs.into_iter().zip(actual_dogs.into_iter())
                    {
                        if expected_dog.is_none() {
                            assert!(actual_dog.is_none());
                        } else if expected_dog.is_some() {
                            assert!(actual_dog.is_some());

                            let expected_dog = expected_dog.unwrap();
                            let actual_dog = actual_dog.unwrap();

                            assert_eq!(
                                expected_dog.get_by_name::<String>("name").unwrap(),
                                actual_dog.get_by_name::<String>("name").unwrap()
                            );

                            assert_eq!(
                                expected_dog.get_by_name::<i32>("age").unwrap(),
                                actual_dog.get_by_name::<i32>("age").unwrap()
                            );
                        }
                    }
                }

                let expected_lucky_numbers = expected
                    .get_by_name::<Vec<Option<i32>>>("lucky_numbers")
                    .unwrap();

                let actual_lucky_numbers = actual
                    .get_by_name::<Vec<Option<i32>>>("lucky_numbers")
                    .unwrap();

                assert_eq!(expected_lucky_numbers, actual_lucky_numbers);
            }
        }

        Spi::run("DROP TABLE dog_owners;").unwrap();
        Spi::run("DROP TYPE dog_owner;").unwrap();
        Spi::run("DROP TYPE dog;").unwrap();
    }

    #[pg_test]
    fn test_copy_with_empty_options() {
        let test_table = TestTable::<i32>::new("int4".into())
            .with_copy_to_options(HashMap::new())
            .with_copy_from_options(HashMap::new());
        let values = (1_i32..=10).into_iter().map(|v| Some(v)).collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    fn test_with_generated_and_dropped_columns() {
        Spi::run("DROP TABLE IF EXISTS test;").unwrap();

        Spi::run("CREATE TABLE test (a int, b int generated always as (10) stored, c text);")
            .unwrap();

        Spi::run("ALTER TABLE test DROP COLUMN a;").unwrap();

        Spi::run("INSERT INTO test (c) VALUES ('test');").unwrap();

        let uri = "file:///tmp/test.parquet";

        let copy_to_query = format!(
            "COPY (SELECT * FROM test) TO '{}' WITH (format parquet);",
            uri
        );
        Spi::run(copy_to_query.as_str()).unwrap();

        let expected = vec![(Some(10), Some("test".to_string()))];

        Spi::run("TRUNCATE test;").unwrap();

        let copy_from_query = format!("COPY test FROM '{}' WITH (format parquet);", uri);
        Spi::run(copy_from_query.as_str()).unwrap();

        let select_command = "SELECT b, c FROM test ORDER BY b, c;";
        let result = Spi::connect(|client| {
            let mut results = Vec::new();
            let tup_table = client.select(select_command, None, None).unwrap();

            for row in tup_table {
                let b = row["b"].value::<i32>();
                let c = row["c"].value::<String>();
                results.push((b.unwrap(), c.unwrap()));
            }

            results
        });

        for (expected, actual) in expected.into_iter().zip(result.into_iter()) {
            assert_eq!(expected.0, actual.0);
            assert_eq!(expected.1, actual.1);
        }
    }

    #[pg_test]
    fn test_codecs() {
        let codecs = vec![
            ParquetCodecOption::Uncompressed,
            ParquetCodecOption::Gzip,
            ParquetCodecOption::Brotli,
            ParquetCodecOption::Snappy,
            ParquetCodecOption::Lz4,
            ParquetCodecOption::Lz4raw,
            ParquetCodecOption::Zstd,
        ];

        for codec in codecs {
            let mut copy_options = HashMap::new();
            copy_options.insert(
                "codec".to_string(),
                CopyOptionValue::StringOption(codec.to_string()),
            );

            let test_table =
                TestTable::<i32>::new("int4".into()).with_copy_to_options(copy_options);
            let values = (1_i32..=10).into_iter().map(|v| Some(v)).collect();
            test_helper(test_table, values);
        }
    }

    #[pg_test]
    #[ignore = "not yet implemented"]
    fn test_s3_object_store() {
        todo!("Implement tests for S3 object store");
    }

    #[pg_test]
    #[should_panic(expected = "unsupported uri invalid_uri")]
    fn test_invalid_uri() {
        let test_table = TestTable::<i32>::new("int4".into()).with_uri("invalid_uri".to_string());
        let values = (1_i32..=10).into_iter().map(|v| Some(v)).collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    #[should_panic(expected = "invalid_format is not a valid format")]
    fn test_invalid_format_copy_from() {
        let mut copy_options = HashMap::new();
        copy_options.insert(
            "format".to_string(),
            CopyOptionValue::StringOption("invalid_format".to_string()),
        );

        let test_table = TestTable::<i32>::new("int4".into()).with_copy_from_options(copy_options);
        let values = (1_i32..=10).into_iter().map(|v| Some(v)).collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    #[should_panic(expected = "nonexisted is not a valid option for COPY FROM PARQUET")]
    fn test_nonexistent_copy_from_option() {
        let mut copy_options = HashMap::new();
        copy_options.insert(
            "nonexisted".to_string(),
            CopyOptionValue::StringOption("nonexisted".to_string()),
        );

        let test_table = TestTable::<i32>::new("int4".into()).with_copy_from_options(copy_options);
        let values = (1_i32..=10).into_iter().map(|v| Some(v)).collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    #[should_panic(expected = "nonexisted is not a valid option for COPY TO PARQUET")]
    fn test_nonexistent_copy_to_option() {
        let mut copy_options = HashMap::new();
        copy_options.insert(
            "nonexisted".to_string(),
            CopyOptionValue::StringOption("nonexisted".to_string()),
        );

        let test_table = TestTable::<i32>::new("int4".into()).with_copy_to_options(copy_options);
        let values = (1_i32..=10).into_iter().map(|v| Some(v)).collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    #[should_panic(expected = "invalid_format is not a valid format")]
    fn test_invalid_format_copy_to() {
        let mut copy_options = HashMap::new();
        copy_options.insert(
            "format".to_string(),
            CopyOptionValue::StringOption("invalid_format".to_string()),
        );

        let test_table = TestTable::<i32>::new("int4".into()).with_copy_to_options(copy_options);
        let values = (1_i32..=10).into_iter().map(|v| Some(v)).collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    #[should_panic(expected = "invalid_codec is not a valid codec")]
    fn test_invalid_codec() {
        let mut copy_options = HashMap::new();
        copy_options.insert(
            "codec".to_string(),
            CopyOptionValue::StringOption("invalid_codec".to_string()),
        );

        let test_table = TestTable::<i32>::new("int4".into()).with_copy_to_options(copy_options);
        let values = (1_i32..=10).into_iter().map(|v| Some(v)).collect();
        test_helper(test_table, values);
    }

    #[pg_test]
    #[should_panic(expected = "row_group_size must be greater than 0")]
    fn test_invalid_row_group_size() {
        let mut copy_options = HashMap::new();
        copy_options.insert("row_group_size".to_string(), CopyOptionValue::IntOption(-1));

        let test_table = TestTable::<i32>::new("int4".into()).with_copy_to_options(copy_options);
        let values = (1_i32..=10).into_iter().map(|v| Some(v)).collect();
        test_helper(test_table, values);
    }
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec!["shared_preload_libraries = 'pg_parquet'"]
    }
}
