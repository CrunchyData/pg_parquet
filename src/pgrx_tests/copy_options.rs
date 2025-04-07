#[pgrx::pg_schema]
mod tests {
    use std::{cmp::Ordering, collections::HashMap, path::Path};

    use pgrx::{pg_test, Spi};

    use crate::{
        pgrx_tests::common::{
            CopyOptionValue, TestTable, LOCAL_TEST_FILE_PATH, LOCAL_TEST_FOLDER_PATH,
        },
        PgParquetCompression,
    };

    #[pg_test]
    fn test_copy_with_empty_options() {
        let test_table = TestTable::<i32>::new("int4".into())
            .with_copy_to_options(HashMap::new())
            .with_copy_from_options(HashMap::new());
        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();
    }

    #[pg_test]
    fn test_compression_from_option() {
        let compression_options = vec![
            PgParquetCompression::Uncompressed,
            PgParquetCompression::Snappy,
            PgParquetCompression::Gzip,
            PgParquetCompression::Brotli,
            PgParquetCompression::Lz4,
            PgParquetCompression::Lz4raw,
            PgParquetCompression::Zstd,
        ];

        let expected_compression = vec![
            "UNCOMPRESSED",
            "SNAPPY",
            "GZIP(GzipLevel(6))",
            "BROTLI(BrotliLevel(1))",
            "LZ4",
            "LZ4_RAW",
            "ZSTD(ZstdLevel(1))",
        ];

        for (compression_option, expected_compression) in
            compression_options.into_iter().zip(expected_compression)
        {
            let mut copy_options = HashMap::new();
            copy_options.insert(
                "compression".to_string(),
                CopyOptionValue::StringOption(compression_option.to_string()),
            );

            let test_table =
                TestTable::<i32>::new("int4".into()).with_copy_to_options(copy_options);
            test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
            test_table.assert_expected_and_result_rows();

            let parquet_metadata_command = format!(
                "select compression from parquet.metadata('{}');",
                LOCAL_TEST_FILE_PATH
            );

            let result_compression = Spi::get_one::<String>(&parquet_metadata_command)
                .unwrap()
                .unwrap();

            assert_eq!(expected_compression, result_compression);
        }
    }

    #[pg_test]
    fn test_compression_from_uri() {
        let parquet_uris = vec![
            format!("{}", LOCAL_TEST_FILE_PATH),
            format!("{}.snappy", LOCAL_TEST_FILE_PATH),
            format!("{}.gz", LOCAL_TEST_FILE_PATH),
            format!("{}.br", LOCAL_TEST_FILE_PATH),
            format!("{}.lz4", LOCAL_TEST_FILE_PATH),
            format!("{}.zst", LOCAL_TEST_FILE_PATH),
        ];

        let expected_compression = vec![
            "SNAPPY",
            "SNAPPY",
            "GZIP(GzipLevel(6))",
            "BROTLI(BrotliLevel(1))",
            "LZ4",
            "ZSTD(ZstdLevel(1))",
        ];

        for (uri, expected_compression) in parquet_uris.into_iter().zip(expected_compression) {
            let test_table = TestTable::<i32>::new("int4".into()).with_uri(uri.to_string());
            test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
            test_table.assert_expected_and_result_rows();

            let parquet_metadata_command =
                format!("select compression from parquet.metadata('{}');", uri);

            let result_compression = Spi::get_one::<String>(&parquet_metadata_command)
                .unwrap()
                .unwrap();

            assert_eq!(expected_compression, result_compression);
        }
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
        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();
    }

    #[pg_test]
    #[should_panic(expected = "nonexisted is not a valid option for \"copy from parquet\".")]
    fn test_nonexistent_copy_from_option() {
        let mut copy_options = HashMap::new();
        copy_options.insert(
            "nonexisted".to_string(),
            CopyOptionValue::StringOption("nonexisted".to_string()),
        );

        let test_table = TestTable::<i32>::new("int4".into()).with_copy_from_options(copy_options);
        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();
    }

    #[pg_test]
    #[should_panic(expected = "nonexisted is not a valid option for \"copy to parquet\".")]
    fn test_nonexistent_copy_to_option() {
        let mut copy_options = HashMap::new();
        copy_options.insert(
            "nonexisted".to_string(),
            CopyOptionValue::StringOption("nonexisted".to_string()),
        );

        let test_table = TestTable::<i32>::new("int4".into()).with_copy_to_options(copy_options);
        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();
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
        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();
    }

    #[pg_test]
    #[should_panic(expected = "invalid_compression is not a valid compression format")]
    fn test_invalid_compression() {
        let mut copy_options = HashMap::new();
        copy_options.insert(
            "compression".to_string(),
            CopyOptionValue::StringOption("invalid_compression".to_string()),
        );

        let test_table = TestTable::<i32>::new("int4".into()).with_copy_to_options(copy_options);
        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();
    }

    #[pg_test]
    #[should_panic(expected = "compression level is not supported for \"snappy\" compression")]
    fn test_unsupported_compression_level_with_snappy_option() {
        let mut copy_options = HashMap::new();
        copy_options.insert(
            "compression".to_string(),
            CopyOptionValue::StringOption("snappy".to_string()),
        );
        copy_options.insert(
            "compression_level".to_string(),
            CopyOptionValue::IntOption(1),
        );

        let test_table = TestTable::<i32>::new("int4".into()).with_copy_to_options(copy_options);
        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();
    }

    #[pg_test]
    #[should_panic(expected = "compression level is not supported for \"snappy\" compression")]
    fn test_unsupported_compression_level_with_snappy_file() {
        let mut copy_options = HashMap::new();
        copy_options.insert(
            "compression_level".to_string(),
            CopyOptionValue::IntOption(1),
        );

        let test_table = TestTable::<i32>::new("int4".into())
            .with_copy_to_options(copy_options)
            .with_uri(format!("{}.snappy", LOCAL_TEST_FILE_PATH));
        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();
    }

    #[pg_test]
    #[should_panic(expected = "valid compression range 0..=10 exceeded")]
    fn test_invalid_gzip_compression_level() {
        let mut copy_options = HashMap::new();
        copy_options.insert(
            "compression".to_string(),
            CopyOptionValue::StringOption("gzip".to_string()),
        );
        copy_options.insert(
            "compression_level".to_string(),
            CopyOptionValue::IntOption(20),
        );

        let test_table = TestTable::<i32>::new("int4".into()).with_copy_to_options(copy_options);
        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();
    }

    #[pg_test]
    fn test_valid_gzip_compression_level() {
        let mut copy_options = HashMap::new();
        copy_options.insert(
            "compression".to_string(),
            CopyOptionValue::StringOption("gzip".to_string()),
        );
        copy_options.insert(
            "compression_level".to_string(),
            CopyOptionValue::IntOption(1),
        );

        let test_table = TestTable::<i32>::new("int4".into()).with_copy_to_options(copy_options);
        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();

        let parquet_metadata_command = format!(
            "select compression from parquet.metadata('{}');",
            LOCAL_TEST_FILE_PATH
        );

        let result_compression = Spi::get_one::<String>(&parquet_metadata_command)
            .unwrap()
            .unwrap();

        // compression level is not read properly by parquet-rs (bug)
        assert!(result_compression.starts_with("GZIP"));
    }

    #[pg_test]
    #[should_panic(expected = "row_group_size must be greater than 0")]
    fn test_invalid_row_group_size() {
        let mut copy_options = HashMap::new();
        copy_options.insert("row_group_size".to_string(), CopyOptionValue::IntOption(-1));

        let test_table = TestTable::<i32>::new("int4".into()).with_copy_to_options(copy_options);
        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();
    }

    #[pg_test]
    #[should_panic(expected = "row_group_size_bytes must be greater than 0")]
    fn test_invalid_row_group_size_bytes() {
        let mut copy_options = HashMap::new();
        copy_options.insert(
            "row_group_size_bytes".to_string(),
            CopyOptionValue::IntOption(-1),
        );

        let test_table = TestTable::<i32>::new("int4".into()).with_copy_to_options(copy_options);
        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();
    }

    #[pg_test]
    #[should_panic(expected = "file_size_bytes must be at least 1048576 bytes")]
    fn test_invalid_file_size_bytes() {
        let parent_folder = Path::new(LOCAL_TEST_FOLDER_PATH);
        std::fs::remove_dir_all(parent_folder).ok();

        let mut copy_options = HashMap::new();
        copy_options.insert(
            "file_size_bytes".to_string(),
            CopyOptionValue::IntOption(100),
        );

        let test_table = TestTable::<i32>::new("int4".into()).with_copy_to_options(copy_options);
        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();
    }

    #[pg_test]
    fn test_large_arrow_array_limit() {
        // disable row group size bytes limit
        let mut copy_options = HashMap::new();
        copy_options.insert(
            "row_group_size_bytes".to_string(),
            CopyOptionValue::IntOption(10_000_000_000),
        );

        let test_table = TestTable::<String>::new("text".into()).with_copy_to_options(copy_options);
        test_table.insert(
            "INSERT INTO test_expected select repeat('a', 52000000) from generate_series(1,42) i;",
        );
        test_table.assert_expected_and_result_rows();

        let parquet_file_metadata_command = format!(
            "select * from parquet.file_metadata('{}');",
            LOCAL_TEST_FILE_PATH
        );
        let result_metadata = Spi::connect(|client| {
            let mut results = Vec::new();
            let tup_table = client
                .select(&parquet_file_metadata_command, None, &[])
                .unwrap();

            for row in tup_table {
                let num_row_groups = row["num_row_groups"].value::<i64>().unwrap().unwrap();
                results.push(num_row_groups);
            }

            results
        });

        assert_eq!(result_metadata, vec![1]);
    }

    #[pg_test]
    fn test_row_group_size() {
        let total_rows = 10;
        let row_group_size = 2;
        let total_row_groups = total_rows / row_group_size;

        let create_table = "create table test_table(id int);";
        Spi::run(create_table).unwrap();

        let copy_to_parquet = format!(
            "copy (select i as id from generate_series(1,{}) i) to '{}' with (row_group_size {});",
            total_rows, LOCAL_TEST_FILE_PATH, row_group_size
        );
        Spi::run(&copy_to_parquet).unwrap();

        let parquet_file_metadata_command = format!(
            "select * from parquet.file_metadata('{}');",
            LOCAL_TEST_FILE_PATH
        );
        let result_metadata = Spi::connect(|client| {
            let mut results = Vec::new();
            let tup_table = client
                .select(&parquet_file_metadata_command, None, &[])
                .unwrap();

            for row in tup_table {
                let num_row_groups = row["num_row_groups"].value::<i64>().unwrap().unwrap();
                results.push(num_row_groups);
            }

            results
        });

        assert_eq!(result_metadata, vec![total_row_groups]);
    }

    #[pg_test]
    fn test_row_group_size_bytes() {
        let create_table = "create table test_table(id int, name text);";
        Spi::run(create_table).unwrap();

        let insert_data =
            "insert into test_table select i, 'a' from generate_series(1, 1000000) i;";
        Spi::run(insert_data).unwrap();

        let id_bytes = 4;
        let name_bytes = 1;
        let total_rows_size_bytes = (id_bytes + name_bytes) * 1024 * 1024;

        let row_group_size_bytes = total_rows_size_bytes / 10;

        let copy_to_parquet = format!(
            "copy test_table to '{}' with (row_group_size_bytes {});",
            LOCAL_TEST_FILE_PATH, row_group_size_bytes
        );
        Spi::run(&copy_to_parquet).unwrap();

        let parquet_file_metadata_command = format!(
            "select * from parquet.file_metadata('{}');",
            LOCAL_TEST_FILE_PATH
        );
        let result_metadata = Spi::connect(|client| {
            let mut results = Vec::new();
            let tup_table = client
                .select(&parquet_file_metadata_command, None, &[])
                .unwrap();

            for row in tup_table {
                let num_row_groups = row["num_row_groups"].value::<i64>().unwrap().unwrap();
                results.push(num_row_groups);
            }

            results
        });

        assert_eq!(result_metadata, vec![12]);
    }

    #[pg_test]
    fn test_file_size_bytes() {
        let parent_folder = Path::new(LOCAL_TEST_FOLDER_PATH);

        let uris = [
            // with ".parquet" extension
            LOCAL_TEST_FILE_PATH.to_string(),
            // with ".parquet.gz" extension
            format!("{LOCAL_TEST_FILE_PATH}.gz"),
            // without extension
            LOCAL_TEST_FILE_PATH
                .strip_suffix(".parquet")
                .unwrap()
                .to_string(),
        ];

        let expected_file_counts = [5_usize, 3, 5];

        for (uri, expected_file_count) in uris.into_iter().zip(expected_file_counts) {
            // cleanup
            Spi::run("drop table if exists test_expected, test_result;").unwrap();
            std::fs::remove_dir_all(parent_folder).ok();

            const ONE_MB: i32 = 1024 * 1024;
            let setup_commands = format!(
                "create table test_expected(a text);\n\
                 create table test_result(a text);\n\
                 insert into test_expected select 'hellooooo' || i from generate_series(1, 1000000) i;\n\
                 copy test_expected to '{uri}' with (format parquet, file_size_bytes {ONE_MB})");
            Spi::run(&setup_commands).unwrap();

            // assert file count
            let file_name = Path::new(&uri)
                .file_name()
                .expect("invalid uri")
                .to_str()
                .expect("invalid uri");

            let file_extension = file_name
                .find('.')
                .map(|idx| &file_name[idx..])
                .unwrap_or("");

            let mut file_entries = parent_folder
                .read_dir()
                .unwrap()
                .map(|entry_res| entry_res.unwrap())
                .collect::<Vec<_>>();

            assert_eq!(file_entries.len(), expected_file_count);

            file_entries.sort_by(|a, b| {
                if a.file_name() <= b.file_name() {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            });

            // assert file paths
            for (file_idx, file_entry) in file_entries.iter().enumerate() {
                let expected_path = parent_folder.join(format!("data_{file_idx}{file_extension}"));

                let expected_path = expected_path.to_str().unwrap();

                assert_eq!(file_entry.path().to_str().unwrap(), expected_path);

                let copy_from_command =
                    format!("copy test_result from '{expected_path}' with (format parquet)");
                Spi::run(copy_from_command.as_str()).unwrap();
            }

            // assert rows
            let (expected_rows, result_rows) = Spi::connect(|client| {
                let mut expected_rows = Vec::new();
                let tup_table = client
                    .select("select a from test_expected order by 1", None, &[])
                    .unwrap();

                for row in tup_table {
                    let a = row["a"].value::<&str>().unwrap().unwrap();
                    expected_rows.push(a);
                }

                let mut result_rows = Vec::new();
                let tup_table = client
                    .select("select a from test_result order by 1", None, &[])
                    .unwrap();

                for row in tup_table {
                    let a = row["a"].value::<&str>().unwrap().unwrap();
                    result_rows.push(a);
                }

                (expected_rows, result_rows)
            });

            assert_eq!(result_rows, expected_rows);
        }
    }

    #[pg_test]
    #[should_panic(expected = "unrecognized match_by method: invalid_match_by")]
    fn test_invalid_match_by() {
        let mut copy_from_options = HashMap::new();
        copy_from_options.insert(
            "match_by".to_string(),
            CopyOptionValue::StringOption("invalid_match_by".to_string()),
        );

        let test_table =
            TestTable::<i32>::new("int4".into()).with_copy_from_options(copy_from_options);
        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();
    }
}
