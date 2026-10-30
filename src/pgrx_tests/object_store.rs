#[pgrx::pg_schema]
mod tests {
    use std::{collections::HashMap, io::Write};

    use object_store::{ClientConfigKey, ObjectStoreExt, PutPayload};
    use parquet::{arrow::async_reader::AsyncFileReader, errors::ParquetError};
    use pgrx::{pg_sys::Timestamp, pg_test, Spi};

    use crate::{
        arrow_parquet::uri_utils::{
            ensure_version_is_the_one_asked_for, ParquetObjectStoreReader, ParquetRowGroupOpener,
            ParsedUriInfo,
        },
        object_store::{
            client_options::load_client_options, given_up_upload_hint, no_multipart_upload_hint,
            object_store_cache::get_or_create_object_store, object_store_error_message, proxy_hint,
            write_error_message,
        },
        pgrx_tests::common::{
            copy_to_helper, CopyOptionValue, FileCleanup, TestTable, LOCAL_TEST_FILE_PATH,
        },
        PG_BACKEND_TOKIO_RUNTIME,
    };

    // put_empty_object writes an object that holds no byte, the way the marker of a folder an
    // object store browser created and the "_SUCCESS" of a job that wrote one are written
    fn put_empty_object(uri: &str) {
        let uri_info = ParsedUriInfo::try_from(uri).unwrap();

        let copy_from = false;
        let (store, location) = get_or_create_object_store(&uri_info, copy_from);

        PG_BACKEND_TOKIO_RUNTIME
            .block_on(async move { store.put(&location, PutPayload::new()).await })
            .unwrap();
    }

    // cut_the_object_short_after_its_footer rewrites an object as the head of the parquet file it
    // holds followed by the footer of it, which is the file an upload that stopped halfway and a
    // "cat a.parquet b.parquet" both leave behind: a footer that describes rows the object does
    // not hold, read from an object that ends in the marker a parquet file ends in
    fn cut_the_object_short_after_its_footer(uri: &str, head_len: usize) {
        let uri_info = ParsedUriInfo::try_from(uri).unwrap();

        // the object is read through a reader and written through a writer, since asking for a
        // writer of a local path truncates the file as the store is built
        let (reader, location) = get_or_create_object_store(&uri_info, true);

        PG_BACKEND_TOKIO_RUNTIME.block_on(async move {
            let file = reader.get(&location).await.unwrap().bytes().await.unwrap();

            // a parquet file ends in its metadata, the length of it and then the marker, so the
            // footer is the last length + 8 bytes of the file
            let metadata_length_at = file.len() - 8;
            let metadata_length = u32::from_le_bytes(
                file[metadata_length_at..metadata_length_at + 4]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let footer_at = file.len() - (metadata_length + 8);

            assert!(
                head_len < footer_at,
                "the head of the file reaches its footer"
            );

            let mut cut_short = file.slice(0..head_len).to_vec();
            cut_short.extend_from_slice(&file.slice(footer_at..));

            let (writer, location) = get_or_create_object_store(&uri_info, false);

            writer
                .put(&location, PutPayload::from(cut_short))
                .await
                .unwrap();
        });
    }

    fn object_store_cache_clear() {
        Spi::run("SELECT parquet_test.object_store_cache_clear();").unwrap();
    }

    fn object_store_cache_items() -> Vec<(&'static str, &'static str, Option<Timestamp>)> {
        Spi::connect(|client| {
            let mut results = Vec::new();
            let tup_table = client
                .select(
                    "SELECT * FROM parquet_test.object_store_cache_items() ORDER BY 1,2,3;",
                    None,
                    &[],
                )
                .unwrap();

            for row in tup_table {
                let scheme = row.get_by_name("scheme").unwrap().unwrap();
                let bucket = row.get_by_name("bucket").unwrap().unwrap();
                let expire_at = row.get_by_name("expire_at").unwrap();

                results.push((scheme, bucket, expire_at));
            }

            results
        })
    }

    fn object_store_expire_item(bucket: &str) {
        Spi::run(&format!(
            "SELECT parquet_test.object_store_cache_expire_bucket('{bucket}');"
        ))
        .unwrap();
    }

    #[pg_test]
    fn test_s3_from_env() {
        object_store_cache_clear();

        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let s3_uris = [
            format!("s3://{test_bucket_name}/pg_parquet_test.parquet"),
            format!("https://s3.amazonaws.com/{test_bucket_name}/pg_parquet_test.parquet"),
            format!("https://{test_bucket_name}.s3.amazonaws.com/pg_parquet_test.parquet"),
        ];

        for s3_uri in s3_uris {
            let test_table = TestTable::<i32>::new("int4".into()).with_uri(s3_uri);

            test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
            test_table.assert_expected_and_result_rows();
        }
    }

    #[pg_test]
    fn test_s3_from_env_glob_pattern() {
        object_store_cache_clear();

        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let s3_uri = format!(
            "s3://{test_bucket_name}/test_s3_from_env_glob_pattern/some/pg_parquet_test.parquet"
        );
        let s3_uri_pattern =
            format!("s3://{test_bucket_name}/test_s3_from_env_glob_pattern/**/*.parquet");

        let mut copy_options = HashMap::new();
        copy_options.insert(
            "file_size_bytes".to_string(),
            CopyOptionValue::StringOption("1MB".to_string()),
        );

        let test_table = TestTable::<i32>::new("int4".into())
            .with_uri(s3_uri)
            .with_uri_pattern(s3_uri_pattern)
            .with_order_by_col("a".into())
            .with_copy_to_options(copy_options);

        test_table
            .insert("INSERT INTO test_expected (a) select i from generate_series(1, 1000000) i;");
        test_table.assert_expected_and_result_rows();
    }

    #[pg_test]
    fn test_s3_uri_with_special_chars() {
        object_store_cache_clear();

        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let s3_uri = format!("s3://{test_bucket_name}/so\\mek*ey/**testme.parquet");

        let copy_to_command = format!(
            "COPY (SELECT a FROM generate_series(1,10) a) TO '{s3_uri}' WITH (format parquet);"
        );
        Spi::run(&copy_to_command).unwrap();

        let create_table = "create table test_table(a int);";
        Spi::run(create_table).unwrap();

        let copy_from_command = format!("COPY test_table FROM '{s3_uri}' WITH (format parquet);");
        Spi::run(copy_from_command.as_str()).unwrap();

        let count_query = "select count(*) from test_table;";
        let result = Spi::get_one::<i64>(count_query).unwrap().unwrap();
        assert_eq!(result, 10);
    }

    // the file whose schema does not match is named, since a pattern reads a file per reader
    // and the uri of the command is not the file any one of them failed on
    #[pg_test]
    #[should_panic(
        expected = "but table has 2 columns. The file is \"s3://testbucket/test_s3_pattern_schema_mismatch/two.parquet\""
    )]
    fn test_s3_uri_pattern_names_the_file_whose_columns_do_not_match() {
        object_store_cache_clear();

        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let folder = format!("s3://{test_bucket_name}/test_s3_pattern_schema_mismatch");

        Spi::run(&format!(
            "COPY (SELECT 1 a, 'x'::text b) TO '{folder}/one.parquet' WITH (format parquet);"
        ))
        .unwrap();

        Spi::run(&format!(
            "COPY (SELECT 2 a) TO '{folder}/two.parquet' WITH (format parquet);"
        ))
        .unwrap();

        Spi::run("create table test_table(a int, b text);").unwrap();

        Spi::run(&format!(
            "COPY test_table FROM '{folder}/**' WITH (format parquet);"
        ))
        .unwrap();
    }

    #[pg_test]
    #[should_panic(
        expected = "column \"b\" is not found in parquet file \"s3://testbucket/test_s3_pattern_match_by_name/two.parquet\""
    )]
    fn test_s3_uri_pattern_names_the_file_a_column_is_missing_from() {
        object_store_cache_clear();

        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let folder = format!("s3://{test_bucket_name}/test_s3_pattern_match_by_name");

        Spi::run(&format!(
            "COPY (SELECT 1 a, 'x'::text b) TO '{folder}/one.parquet' WITH (format parquet);"
        ))
        .unwrap();

        Spi::run(&format!(
            "COPY (SELECT 2 a) TO '{folder}/two.parquet' WITH (format parquet);"
        ))
        .unwrap();

        Spi::run("create table test_table(a int, b text);").unwrap();

        Spi::run(&format!(
            "COPY test_table FROM '{folder}/**' WITH (format parquet, match_by 'name');"
        ))
        .unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "uri s3://testbucket names no object. It names the bucket")]
    fn test_copy_to_s3_uri_without_an_object_is_refused() {
        let copy_to_command =
            "COPY (SELECT a FROM generate_series(1,10) a) TO 's3://testbucket' WITH (format parquet);";
        Spi::run(copy_to_command).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "uri az://testcontainer/ names no object. It names the container")]
    fn test_copy_from_azure_uri_without_an_object_is_refused() {
        let create_table = "create table test_table(a int);";
        Spi::run(create_table).unwrap();

        let copy_from_command = "COPY test_table FROM 'az://testcontainer/' WITH (format parquet);";
        Spi::run(copy_from_command).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "uri gs://testbucket names no object. It names the bucket")]
    fn test_file_metadata_of_a_gcs_uri_without_an_object_is_refused() {
        Spi::run("SELECT * FROM parquet.file_metadata('gs://testbucket');").unwrap();
    }

    #[pg_test]
    #[should_panic(
        expected = "holds a \"*\", which a read of a single file does not resolve into the names it matches"
    )]
    fn test_schema_of_an_s3_uri_pattern_names_the_pattern() {
        object_store_cache_clear();

        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let s3_uri_pattern = format!("s3://{test_bucket_name}/test_schema_of_a_pattern/**");

        Spi::run(&format!(
            "SELECT * FROM parquet.schema('{s3_uri_pattern}');"
        ))
        .unwrap();
    }

    #[pg_test]
    #[should_panic(
        expected = "holds a \"*\", which a read of a single file does not resolve into the names it matches"
    )]
    fn test_file_metadata_of_an_s3_uri_pattern_names_the_pattern() {
        object_store_cache_clear();

        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let s3_uri_pattern =
            format!("s3://{test_bucket_name}/test_file_metadata_of_a_pattern/*.parquet");

        Spi::run(&format!(
            "SELECT * FROM parquet.file_metadata('{s3_uri_pattern}');"
        ))
        .unwrap();
    }

    #[pg_test]
    fn test_schema_of_an_s3_object_named_with_a_star() {
        object_store_cache_clear();

        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        // the name of this object holds the characters a pattern is written with, and the read
        // of the uri as it is written comes first, so it is the object that is read
        let s3_uri = format!("s3://{test_bucket_name}/test_schema_of_a_star_name/*.parquet");

        Spi::run(&format!(
            "COPY (SELECT a FROM generate_series(1,10) a) TO '{s3_uri}' WITH (format parquet);"
        ))
        .unwrap();

        let columns = Spi::get_one::<i64>(&format!(
            "SELECT count(*) FROM parquet.schema('{s3_uri}') WHERE name = 'a';"
        ))
        .unwrap()
        .unwrap();
        assert_eq!(columns, 1);
    }

    #[pg_test]
    #[should_panic(expected = "unsupported port 9000 in uri s3://testbucket:9000/port.parquet")]
    fn test_copy_to_s3_uri_with_a_port_is_refused() {
        let s3_uri = "s3://testbucket:9000/port.parquet";

        let copy_to_command = format!(
            "COPY (SELECT a FROM generate_series(1,10) a) TO '{s3_uri}' WITH (format parquet);"
        );
        Spi::run(&copy_to_command).unwrap();
    }

    #[pg_test]
    #[should_panic(
        expected = "unsupported port 10000 in uri az://testcontainer:10000/port.parquet"
    )]
    fn test_copy_from_azure_uri_with_a_port_is_refused() {
        let azure_uri = "az://testcontainer:10000/port.parquet";

        let create_table = "create table test_table(a int);";
        Spi::run(create_table).unwrap();

        let copy_from_command =
            format!("COPY test_table FROM '{azure_uri}' WITH (format parquet);");
        Spi::run(&copy_from_command).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "unsupported port 4443 in uri gs://testbucket:4443/port.parquet")]
    fn test_list_gcs_uri_with_a_port_is_refused() {
        Spi::run("SELECT * FROM parquet.list('gs://testbucket:4443/port.parquet');").unwrap();
    }

    #[pg_test]
    fn test_s3_uri_pattern_over_an_empty_marker() {
        object_store_cache_clear();

        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let folder = format!("s3://{test_bucket_name}/test_s3_uri_pattern_over_an_empty_marker");
        let s3_uri = format!("{folder}/part-0.parquet");

        let copy_to_command = format!(
            "COPY (SELECT a FROM generate_series(1,10) a) TO '{s3_uri}' WITH (format parquet);"
        );
        Spi::run(&copy_to_command).unwrap();

        // the marker a job that wrote the folder leaves next to the files it wrote
        put_empty_object(&format!("{folder}/_SUCCESS"));

        // the marker holds no row, so the pattern matches the file written next to it alone
        let listed = Spi::get_one::<i64>(&format!(
            "SELECT count(*) FROM parquet.list('{folder}/**');"
        ))
        .unwrap()
        .unwrap();
        assert_eq!(listed, 1);

        let create_table = "create table test_table(a int);";
        Spi::run(create_table).unwrap();

        // and the copy reads that file rather than failing over the marker
        let copy_from_command =
            format!("COPY test_table FROM '{folder}/**' WITH (format parquet);");
        Spi::run(copy_from_command.as_str()).unwrap();

        let count_query = "select count(*) from test_table;";
        let result = Spi::get_one::<i64>(count_query).unwrap().unwrap();
        assert_eq!(result, 10);
    }

    // "[" and "]" are no pattern syntax of a uri, so a pattern holding them reads the object
    // whose name holds them rather than the one a glob class of a single character matches
    #[pg_test]
    fn test_s3_uri_pattern_with_a_bracket_in_the_name() {
        object_store_cache_clear();

        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let folder = format!("s3://{test_bucket_name}/test_s3_uri_pattern_with_a_bracket");

        // the object the pattern names, and the one a glob class would read instead
        Spi::run(&format!(
            "COPY (SELECT 1 AS a) TO '{folder}/[a].parquet' WITH (format parquet);"
        ))
        .unwrap();
        Spi::run(&format!(
            "COPY (SELECT 2 AS a) TO '{folder}/a.parquet' WITH (format parquet);"
        ))
        .unwrap();

        Spi::run("CREATE TABLE test_table (a int);").unwrap();
        Spi::run(&format!(
            "COPY test_table FROM '{folder}/[a]*.parquet' WITH (format parquet);"
        ))
        .unwrap();

        assert_eq!(
            Spi::get_one::<i64>("SELECT sum(a) FROM test_table;").unwrap(),
            Some(1)
        );

        assert_eq!(
            Spi::get_one::<String>(&format!(
                "SELECT string_agg(uri, ' ' ORDER BY uri) FROM parquet.list('{folder}/[a]*.parquet')"
            ))
            .unwrap(),
            Some(format!("{folder}/[a].parquet"))
        );
    }

    #[pg_test]
    #[should_panic(expected = "no files found that match the pattern")]
    fn test_s3_with_nonexistent_pattern_uri() {
        object_store_cache_clear();

        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let s3_uri_pattern = format!("s3://{test_bucket_name}/notexists/**");

        let create_table = "create table test_table(a int);";
        Spi::run(create_table).unwrap();

        let copy_from_command =
            format!("COPY test_table FROM '{s3_uri_pattern}' WITH (format parquet);");
        Spi::run(copy_from_command.as_str()).unwrap();
    }

    #[pg_test]
    fn test_s3_from_config_file() {
        object_store_cache_clear();

        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        // remove these to make sure the config file is used
        let access_key_id = std::env::var("AWS_ACCESS_KEY_ID").unwrap();
        std::env::remove_var("AWS_ACCESS_KEY_ID");
        let secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY").unwrap();
        std::env::remove_var("AWS_SECRET_ACCESS_KEY");
        let region = std::env::var("AWS_REGION").unwrap();
        std::env::remove_var("AWS_REGION");
        let endpoint = std::env::var("AWS_ENDPOINT_URL").unwrap();
        std::env::remove_var("AWS_ENDPOINT_URL");

        let profile = "pg_parquet_test";

        // create a config file
        let aws_config_file_content = format!(
            "[profile {profile}]\n\
            region={region}\n\
            aws_access_key_id={access_key_id}\n\
            aws_secret_access_key={secret_access_key}\n\
            endpoint_url={endpoint}\n"
        );
        std::env::set_var("AWS_PROFILE", profile);

        let aws_config_file_path = "/tmp/pg_parquet_aws_config";
        std::env::set_var("AWS_CONFIG_FILE", aws_config_file_path);

        let mut aws_config_file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(aws_config_file_path)
            .unwrap();

        aws_config_file
            .write_all(aws_config_file_content.as_bytes())
            .unwrap();

        let s3_uri = format!("s3://{test_bucket_name}/pg_parquet_test.parquet");

        let test_table = TestTable::<i32>::new("int4".into()).with_uri(s3_uri);

        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();

        // remove the config file
        std::fs::remove_file(aws_config_file_path).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "403 Forbidden")]
    fn test_s3_wrong_access_key_id() {
        object_store_cache_clear();

        std::env::set_var("AWS_ACCESS_KEY_ID", "wrong_access_key_id");

        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let s3_uri = format!("s3://{test_bucket_name}/pg_parquet_test.parquet");

        let test_table = TestTable::<i32>::new("int4".into()).with_uri(s3_uri);

        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();
    }

    #[pg_test]
    #[should_panic(expected = "403 Forbidden")]
    fn test_s3_wrong_secret_access_key() {
        object_store_cache_clear();

        std::env::set_var("AWS_SECRET_ACCESS_KEY", "wrong_secret_access_key");

        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let s3_uri = format!("s3://{test_bucket_name}/pg_parquet_test.parquet");

        let test_table = TestTable::<i32>::new("int4".into()).with_uri(s3_uri);

        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();
    }

    #[pg_test]
    fn test_client_options_are_collected_per_object_store() {
        std::env::set_var("AWS_TIMEOUT", "5m");
        std::env::set_var("AWS_CONNECT_TIMEOUT", "1m");

        // belongs to another object store
        std::env::set_var("AZURE_READ_TIMEOUT", "2m");

        // does not name an http client option
        std::env::set_var("AWS_NOT_A_CLIENT_OPTION", "3m");

        let client_options = load_client_options("AWS", false);

        assert_eq!(
            client_options.get_config_value(&ClientConfigKey::Timeout),
            Some("5m".into())
        );
        assert_eq!(
            client_options.get_config_value(&ClientConfigKey::ConnectTimeout),
            Some("1m".into())
        );
        assert_eq!(
            client_options.get_config_value(&ClientConfigKey::ReadTimeout),
            None
        );
    }

    #[pg_test]
    fn test_client_options_allow_http_is_owned_by_the_caller() {
        // only the caller decides, it parses "<prefix>_ALLOW_HTTP" itself
        std::env::set_var("AWS_ALLOW_HTTP", "no");

        let client_options = load_client_options("AWS", true);

        assert_eq!(
            client_options.get_config_value(&ClientConfigKey::AllowHttp),
            Some("true".into())
        );
    }

    #[pg_test]
    fn test_object_store_error_message_unwraps_wrapped_errors() {
        let object_store_error = object_store::Error::Generic {
            store: "S3",
            source: "404 Not Found".into(),
        };
        let expected_message = object_store_error.to_string();

        // arrow and parquet both wrap the error of the object store
        let error = ParquetError::External(Box::new(ParquetError::External(Box::new(
            object_store_error,
        ))));

        assert_eq!(object_store_error_message(error), expected_message);
    }

    #[pg_test]
    fn test_object_store_error_message_keeps_other_errors() {
        let error = ParquetError::General("not an error of an object store".into());
        let expected_message = error.to_string();

        assert_eq!(object_store_error_message(error), expected_message);
    }

    #[pg_test]
    fn test_proxy_hint() {
        // the error page that haproxy answers a request that it rejects with
        assert!(proxy_hint(
            "Generic S3 error: Server returned non-2xx status code: 400 Bad Request: \
             <html><body><h1>400 Bad request</h1></body></html>"
        )
        .is_some());

        assert!(proxy_hint("Error after 0 retries: <!DOCTYPE html><html></html>").is_some());

        // the error of an object store itself
        assert!(proxy_hint(
            "Generic S3 error: Server returned non-2xx status code: 404 Not Found: \
             <?xml version=\"1.0\"?><Error><Code>NoSuchKey</Code></Error>"
        )
        .is_none());
    }

    #[pg_test]
    fn test_object_store_error_message_hints_at_a_proxy() {
        let error = ParquetError::External(Box::new(object_store::Error::Generic {
            store: "S3",
            source: "Server returned non-2xx status code: 400 Bad Request: \
                     <html><body><h1>400 Bad request</h1></body></html>"
                .into(),
        }));

        let message = object_store_error_message(error);

        assert!(message.starts_with("Generic S3 error: "));
        assert!(message.ends_with("likely rejected the request)"));
    }

    #[pg_test]
    fn test_no_multipart_upload_hint() {
        // the store of an http(s) endpoint has no multipart upload
        let hint = no_multipart_upload_hint(&object_store::Error::NotImplemented {
            operation: "`put_multipart_opts`".into(),
            implementer: "HttpStore".into(),
        });
        assert!(hint
            .expect("no hint for a store without multipart upload")
            .contains("10 MiB"));

        // another operation that a store does not implement is not about a size
        assert!(
            no_multipart_upload_hint(&object_store::Error::NotImplemented {
                operation: "`put_opts` with a mode other than `PutMode::Overwrite`".into(),
                implementer: "HttpStore".into(),
            })
            .is_none()
        );

        assert!(no_multipart_upload_hint(&object_store::Error::Generic {
            store: "S3",
            source: "an error of the store itself".into(),
        })
        .is_none());
    }

    #[pg_test]
    fn test_object_store_error_message_hints_at_the_single_request_bound() {
        let error = ParquetError::External(Box::new(object_store::Error::NotImplemented {
            operation: "`put_multipart_opts`".into(),
            implementer: "HttpStore".into(),
        }));

        let message = object_store_error_message(error);

        assert!(message.starts_with("Operation `put_multipart_opts` not yet implemented"));
        assert!(message.contains("takes an object of at most 10 MiB"));
    }

    #[pg_test]
    fn test_given_up_upload_hint() {
        // the abort of an upload, which is the error a write that could not finish its upload
        // and could not give it up either is answered with
        assert!(given_up_upload_hint(
            "The operation lacked the necessary privileges to complete for path t.parquet: \
             Error performing DELETE http://localhost:9000/testbucket/t.parquet?uploadId=abc - \
             Server returned non-2xx status code: 403 Forbidden: <Code>AccessDenied</Code>"
        )
        .is_some());

        // the write of a part carries an upload id as well, and is the error of the write
        // itself rather than of giving the upload up
        assert!(given_up_upload_hint(
            "The operation lacked the necessary privileges to complete for path t.parquet: \
             Error performing PUT http://localhost:9000/testbucket/t.parquet?partNumber=1&\
             uploadId=abc - Server returned non-2xx status code: 403 Forbidden"
        )
        .is_none());

        // the removal of an object names no upload, so it is the copy giving up what it wrote
        // rather than the upload it wrote into
        assert!(given_up_upload_hint(
            "Error performing DELETE http://localhost:9000/testbucket/t.parquet - \
             Server returned non-2xx status code: 403 Forbidden"
        )
        .is_none());

        assert!(given_up_upload_hint("Generic S3 error: 404 Not Found").is_none());
    }

    #[pg_test]
    fn test_write_error_message_says_the_abort_is_not_the_write() {
        let abort_error = object_store::Error::PermissionDenied {
            path: "t.parquet".into(),
            source: "Error performing DELETE http://localhost:9000/testbucket/t.parquet\
                     ?uploadId=abc - Server returned non-2xx status code: 403 Forbidden"
                .into(),
        };

        let message = write_error_message(ParquetError::External(Box::new(abort_error)));

        assert!(message.starts_with("The operation lacked the necessary privileges"));
        assert!(message.ends_with("for unfinished uploads removes)"));

        // a write that failed on its own is described by its own error and nothing else
        let write_error = ParquetError::External(Box::new(object_store::Error::Generic {
            store: "S3",
            source: "Server returned non-2xx status code: 500 Internal Server Error".into(),
        }));
        let expected_message = object_store_error_message(ParquetError::External(Box::new(
            object_store::Error::Generic {
                store: "S3",
                source: "Server returned non-2xx status code: 500 Internal Server Error".into(),
            },
        )));

        assert_eq!(write_error_message(write_error), expected_message);
    }

    #[pg_test]
    fn test_s3_client_options_from_env() {
        object_store_cache_clear();

        // http client options of the object store are configurable via the environment
        std::env::set_var("AWS_TIMEOUT", "5m");
        std::env::set_var("AWS_CONNECT_TIMEOUT", "30s");

        // not a client option, it should just be ignored
        std::env::set_var("AWS_NOT_A_CLIENT_OPTION", "1");

        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let s3_uri = format!("s3://{test_bucket_name}/pg_parquet_test.parquet");

        let test_table = TestTable::<i32>::new("int4".into()).with_uri(s3_uri);

        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();
    }

    #[pg_test]
    #[should_panic(expected = "failed to parse \"banana\" as Duration")]
    fn test_s3_invalid_client_option_from_env() {
        object_store_cache_clear();

        std::env::set_var("AWS_TIMEOUT", "banana");

        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let s3_uri = format!("s3://{test_bucket_name}/pg_parquet_test.parquet");

        let test_table = TestTable::<i32>::new("int4".into()).with_uri(s3_uri);

        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();
    }

    #[pg_test]
    #[should_panic(expected = "permission denied to COPY from a remote uri")]
    fn test_s3_no_read_access() {
        object_store_cache_clear();

        // create regular user
        Spi::run("CREATE USER regular_user;").unwrap();

        // grant write access to the regular user but not read access
        Spi::run("GRANT parquet_object_store_write TO regular_user;").unwrap();

        // grant all permissions for public schema
        Spi::run("GRANT ALL ON SCHEMA public TO regular_user;").unwrap();

        // set the current user to the regular user
        Spi::run("SET SESSION AUTHORIZATION regular_user;").unwrap();

        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let s3_uri = format!("s3://{test_bucket_name}/pg_parquet_test.parquet");

        let test_table = TestTable::<i32>::new("int4".into()).with_uri(s3_uri.clone());

        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");

        // can write to s3
        let copy_to_command =
            format!("COPY (SELECT a FROM generate_series(1,10) a) TO '{s3_uri}';");
        Spi::run(copy_to_command.as_str()).unwrap();

        // cannot read from s3
        let copy_from_command = format!("COPY test_expected FROM '{s3_uri}';");
        Spi::run(copy_from_command.as_str()).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "permission denied to COPY to a remote uri")]
    fn test_s3_no_write_access() {
        object_store_cache_clear();

        // create regular user
        Spi::run("CREATE USER regular_user;").unwrap();

        // grant read access to the regular user but not write access
        Spi::run("GRANT parquet_object_store_read TO regular_user;").unwrap();

        // grant usage access to parquet schema and its udfs
        Spi::run("GRANT USAGE ON SCHEMA parquet TO regular_user;").unwrap();
        Spi::run("GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA parquet TO regular_user;").unwrap();

        // grant all permissions for public schema
        Spi::run("GRANT ALL ON SCHEMA public TO regular_user;").unwrap();

        // set the current user to the regular user
        Spi::run("SET SESSION AUTHORIZATION regular_user;").unwrap();

        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let s3_uri = format!("s3://{test_bucket_name}/pg_parquet_test.parquet");

        // can call metadata udf (requires read access)
        let metadata_query = format!("SELECT parquet.metadata('{}');", s3_uri.clone());
        Spi::run(&metadata_query).unwrap();

        let test_table = TestTable::<i32>::new("int4".into()).with_uri(s3_uri.clone());

        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");

        // can read from s3
        let copy_from_command = format!("COPY test_expected FROM '{s3_uri}';");
        Spi::run(copy_from_command.as_str()).unwrap();

        // cannot write to s3
        let copy_to_command =
            format!("COPY (SELECT a FROM generate_series(1,10) a) TO '{s3_uri}';");
        Spi::run(copy_to_command.as_str()).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "404 Not Found")]
    fn test_s3_write_wrong_bucket() {
        object_store_cache_clear();

        let s3_uri = "s3://randombucketwhichdoesnotexist/pg_parquet_test.parquet";

        let copy_to_command =
            format!("COPY (SELECT i FROM generate_series(1,10) i) TO '{s3_uri}';");
        Spi::run(copy_to_command.as_str()).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "404 Not Found")]
    fn test_s3_read_wrong_bucket() {
        object_store_cache_clear();

        let s3_uri = "s3://randombucketwhichdoesnotexist/pg_parquet_test.parquet";

        let create_table_command = "CREATE TABLE test_table (a int);";
        Spi::run(create_table_command).unwrap();

        let copy_from_command = format!("COPY test_table FROM '{s3_uri}';");
        Spi::run(copy_from_command.as_str()).unwrap();
    }

    #[pg_test]
    #[cfg(not(rhel8))]
    fn test_s3_temporary_token() {
        object_store_cache_clear();

        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        // remove these to make sure the config file is used
        let access_key_id = std::env::var("AWS_ACCESS_KEY_ID").unwrap();
        std::env::remove_var("AWS_ACCESS_KEY_ID");
        let secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY").unwrap();
        std::env::remove_var("AWS_SECRET_ACCESS_KEY");
        let region = std::env::var("AWS_REGION").unwrap();
        std::env::remove_var("AWS_REGION");
        std::env::remove_var("AWS_ENDPOINT_URL");
        let proxy_port = std::env::var("AWS_ENDPOINT_PROXY_PORT").unwrap();

        let profile = "pg_parquet_test";

        // create a config file
        let aws_config_file_content = format!(
            "[profile {profile}-source]\n\
            aws_access_key_id={access_key_id}\n\
            aws_secret_access_key={secret_access_key}\n\
            \n\
            [profile {profile}]\n\
            region={region}\n\
            source_profile={profile}-source\n\
            role_arn=arn:aws:iam::000000000000:role/DummyRole\n\
            endpoint_url=http://localhost:{proxy_port}\n"
        );
        std::env::set_var("AWS_PROFILE", profile);

        let aws_config_file_path = "/tmp/pg_parquet_aws_config";
        std::env::set_var("AWS_CONFIG_FILE", aws_config_file_path);

        let mut aws_config_file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(aws_config_file_path)
            .unwrap();

        aws_config_file
            .write_all(aws_config_file_content.as_bytes())
            .unwrap();

        let s3_uri = format!("s3://{test_bucket_name}/pg_parquet_test.parquet");

        let test_table = TestTable::<i32>::new("int4".into()).with_uri(s3_uri.clone());
        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();

        assert!(matches!(
            object_store_cache_items()[..],
            [("AmazonS3", "testbucket" , Some(expire_at) )] if expire_at > 0
        ));

        // expire the token for the bucket
        object_store_expire_item("testbucket");

        // next run will create a new token
        let test_table = TestTable::<i32>::new("int4".into()).with_uri(s3_uri);
        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();

        // remove the config file
        std::fs::remove_file(aws_config_file_path).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "relative path not allowed")]
    fn test_s3_unsupported_uri() {
        object_store_cache_clear();

        let create_table = "create table test_table(id int);";
        Spi::run(create_table).unwrap();

        Spi::run("copy test_table to 'https://ACCOUNT_ID.r2.cloudflarestorage.com/bucket';")
            .unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "column count mismatch")]
    fn test_s3_pattern_schema_mismatch() {
        object_store_cache_clear();

        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let copy_to_schema1 = format!("copy (select 1 as a) to 's3://{test_bucket_name}/dummy1.parquet' with (format parquet);");
        Spi::run(copy_to_schema1.as_str()).unwrap();

        let copy_to_schema2 = format!("copy (select 1 as a, 'asd' as b) to 's3://{test_bucket_name}/dummy2.parquet' with (format parquet);");
        Spi::run(copy_to_schema2.as_str()).unwrap();

        let create_table = "create table test_table(id int);";
        Spi::run(create_table).unwrap();

        let s3_uri_pattern = format!("s3://{test_bucket_name}/dummy*.parquet");
        let copy_from_command = format!("COPY test_table FROM '{}';", s3_uri_pattern);
        Spi::run(copy_from_command.as_str()).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "type mismatch")]
    fn test_s3_pattern_column_type_mismatch() {
        object_store_cache_clear();

        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let copy_to_schema1 = format!("copy (select '{{}}'::jsonb as a) to 's3://{test_bucket_name}/dummy2.parquet' with (format parquet);");
        Spi::run(copy_to_schema1.as_str()).unwrap();

        let copy_to_schema2 = format!("copy (select 1 as a) to 's3://{test_bucket_name}/dummy1.parquet' with (format parquet);");
        Spi::run(copy_to_schema2.as_str()).unwrap();

        let create_table = "create table test_table(a jsonb);";
        Spi::run(create_table).unwrap();

        let s3_uri_pattern = format!("s3://{test_bucket_name}/dummy*.parquet");
        let copy_from_command = format!("COPY test_table FROM '{}';", s3_uri_pattern);
        Spi::run(copy_from_command.as_str()).unwrap();
    }

    #[pg_test]
    fn test_s3_create_table_from_file() {
        object_store_cache_clear();

        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let s3_uri = format!("s3://{}/pg_parquet_test.parquet", test_bucket_name);

        copy_to_helper(s3_uri.as_str());

        let create_table = format!("CREATE TABLE test_table () WITH (load_from = '{}')", s3_uri);
        Spi::run(create_table.as_str()).unwrap();
    }

    #[pg_test]
    #[cfg(not(rhel8))]
    fn test_azure_blob_from_env() {
        object_store_cache_clear();

        // unset AZURE_STORAGE_CONNECTION_STRING to make sure the account name and key are used
        std::env::remove_var("AZURE_STORAGE_CONNECTION_STRING");

        let test_container_name: String = std::env::var("AZURE_TEST_CONTAINER_NAME")
            .expect("AZURE_TEST_CONTAINER_NAME not found");

        let test_account_name: String =
            std::env::var("AZURE_STORAGE_ACCOUNT").expect("AZURE_STORAGE_ACCOUNT not found");

        let azure_blob_uris = [
            format!("az://{test_container_name}/pg_parquet_test.parquet"),
            format!("azure://{test_container_name}/pg_parquet_test.parquet"),
            format!("https://{test_account_name}.blob.core.windows.net/{test_container_name}/pg_parquet_test.parquet"),
        ];

        for azure_blob_uri in azure_blob_uris {
            let test_table = TestTable::<i32>::new("int4".into()).with_uri(azure_blob_uri);

            test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
            test_table.assert_expected_and_result_rows();
        }
    }

    #[pg_test]
    #[cfg(not(rhel8))]
    fn test_azure_blob_from_env_glob_pattern() {
        object_store_cache_clear();

        // unset AZURE_STORAGE_CONNECTION_STRING to make sure the account name and key are used
        std::env::remove_var("AZURE_STORAGE_CONNECTION_STRING");

        let test_container_name: String = std::env::var("AZURE_TEST_CONTAINER_NAME")
            .expect("AZURE_TEST_CONTAINER_NAME not found");

        let azure_blob_uri = format!("az://{test_container_name}/test_azure_blob_from_env_glob_pattern/some/pg_parquet_test.parquet");

        let azure_blob_uri_pattern = format!(
            "az://{test_container_name}/test_azure_blob_from_env_glob_pattern/**/*.parquet"
        );

        let mut copy_options = HashMap::new();
        copy_options.insert(
            "file_size_bytes".to_string(),
            CopyOptionValue::StringOption("1MB".to_string()),
        );

        let test_table = TestTable::<i32>::new("int4".into())
            .with_uri(azure_blob_uri)
            .with_uri_pattern(azure_blob_uri_pattern)
            .with_order_by_col("a".into())
            .with_copy_to_options(copy_options);

        test_table.insert("INSERT INTO test_expected select i from generate_series(1, 1000000) i;");
        test_table.assert_expected_and_result_rows();
    }

    #[pg_test]
    #[cfg(not(rhel8))]
    fn test_azure_uri_with_special_chars() {
        object_store_cache_clear();

        let test_container_name: String = std::env::var("AZURE_TEST_CONTAINER_NAME")
            .expect("AZURE_TEST_CONTAINER_NAME not found");

        let azure_uri = format!("az://{test_container_name}/so\\mek*ey/**testme.parquet");

        let copy_to_command = format!(
            "COPY (SELECT a FROM generate_series(1,10) a) TO '{azure_uri}' WITH (format parquet);"
        );
        Spi::run(&copy_to_command).unwrap();

        let create_table = "create table test_table(a int);";
        Spi::run(create_table).unwrap();

        let copy_from_command =
            format!("COPY test_table FROM '{azure_uri}' WITH (format parquet);");
        Spi::run(copy_from_command.as_str()).unwrap();

        let count_query = "select count(*) from test_table;";
        let result = Spi::get_one::<i64>(count_query).unwrap().unwrap();
        assert_eq!(result, 10);
    }

    #[pg_test]
    #[cfg(not(rhel8))]
    #[should_panic(expected = "no files found that match the pattern")]
    fn test_azure_with_nonexistent_pattern_uri() {
        object_store_cache_clear();

        let test_container_name: String = std::env::var("AZURE_TEST_CONTAINER_NAME")
            .expect("AZURE_TEST_CONTAINER_NAME not found");

        let azure_uri_pattern = format!("az://{test_container_name}/notexists/**");

        let create_table = "create table test_table(a int);";
        Spi::run(create_table).unwrap();

        let copy_from_command =
            format!("COPY test_table FROM '{azure_uri_pattern}' WITH (format parquet);");
        Spi::run(copy_from_command.as_str()).unwrap();
    }

    #[pg_test]
    #[cfg(not(rhel8))]
    fn test_azure_from_config_file() {
        object_store_cache_clear();

        let test_container_name: String = std::env::var("AZURE_TEST_CONTAINER_NAME")
            .expect("AZURE_TEST_CONTAINER_NAME not found");

        // remove these to make sure the config file is used
        let account_name = std::env::var("AZURE_STORAGE_ACCOUNT").unwrap();
        std::env::remove_var("AZURE_STORAGE_ACCOUNT");
        let account_key = std::env::var("AZURE_STORAGE_KEY").unwrap();
        std::env::remove_var("AZURE_STORAGE_KEY");
        std::env::remove_var("AZURE_STORAGE_CONNECTION_STRING");

        // create a config file
        let azure_config_file_content = format!(
            "[storage]\n\
            account={account_name}\n\
            key={account_key}"
        );

        let azure_config_file_path = "/tmp/pg_parquet_azure_config";
        std::env::set_var("AZURE_CONFIG_FILE", azure_config_file_path);

        let mut azure_config_file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(azure_config_file_path)
            .unwrap();

        azure_config_file
            .write_all(azure_config_file_content.as_bytes())
            .unwrap();

        let azure_blob_uri = format!("az://{test_container_name}/pg_parquet_test.parquet");

        let test_table = TestTable::<i32>::new("int4".into()).with_uri(azure_blob_uri);

        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();

        // remove the config file
        std::fs::remove_file(azure_config_file_path).unwrap();
    }

    #[pg_test]
    #[cfg(not(rhel8))]
    fn test_azure_from_env_via_connection_string() {
        object_store_cache_clear();

        // unset AZURE_STORAGE_ACCOUNT AND AZURE_STORAGE_KEY to make sure the connection string is used
        std::env::remove_var("AZURE_STORAGE_ACCOUNT");
        std::env::remove_var("AZURE_STORAGE_KEY");

        let unquoted_connection_string = std::env::var("AZURE_STORAGE_CONNECTION_STRING")
            .unwrap()
            .trim_matches('"')
            .to_string();
        std::env::set_var(
            "AZURE_STORAGE_CONNECTION_STRING",
            unquoted_connection_string,
        );

        let test_container_name: String = std::env::var("AZURE_TEST_CONTAINER_NAME")
            .expect("AZURE_TEST_CONTAINER_NAME not found");

        let azure_blob_uri = format!("az://{test_container_name}/pg_parquet_test.parquet");

        let test_table = TestTable::<i32>::new("int4".into()).with_uri(azure_blob_uri);

        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();
    }

    #[pg_test]
    #[cfg(not(rhel8))]
    fn test_azure_from_config_via_connection_string() {
        object_store_cache_clear();

        let test_container_name: String = std::env::var("AZURE_TEST_CONTAINER_NAME")
            .expect("AZURE_TEST_CONTAINER_NAME not found");

        // remove these to make sure the config file is used
        std::env::remove_var("AZURE_STORAGE_ACCOUNT");
        std::env::remove_var("AZURE_STORAGE_KEY");
        let connection_string = std::env::var("AZURE_STORAGE_CONNECTION_STRING").unwrap();
        std::env::remove_var("AZURE_STORAGE_CONNECTION_STRING");

        // create a config file
        let azure_config_file_content =
            format!("[storage]\nconnection_string = {connection_string}\n");

        let azure_config_file_path = "/tmp/pg_parquet_azure_config";
        std::env::set_var("AZURE_CONFIG_FILE", azure_config_file_path);

        let mut azure_config_file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(azure_config_file_path)
            .unwrap();

        azure_config_file
            .write_all(azure_config_file_content.as_bytes())
            .unwrap();

        let azure_blob_uri = format!("az://{test_container_name}/pg_parquet_test.parquet");

        let test_table = TestTable::<i32>::new("int4".into()).with_uri(azure_blob_uri);

        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();

        // remove the config file
        std::fs::remove_file(azure_config_file_path).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "Account must be specified")]
    #[cfg(not(rhel8))]
    fn test_azure_no_storage_account() {
        object_store_cache_clear();

        // unset AZURE_STORAGE_CONNECTION_STRING to make sure the account name and key are used
        std::env::remove_var("AZURE_STORAGE_CONNECTION_STRING");

        std::env::remove_var("AZURE_STORAGE_ACCOUNT");

        let test_container_name: String = std::env::var("AZURE_TEST_CONTAINER_NAME")
            .expect("AZURE_TEST_CONTAINER_NAME not found");

        let azure_blob_uri = format!("az://{test_container_name}/pg_parquet_test.parquet");

        let test_table = TestTable::<i32>::new("int4".into()).with_uri(azure_blob_uri);

        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();
    }

    #[pg_test]
    #[should_panic(expected = "403 Forbidden")]
    #[cfg(not(rhel8))]
    fn test_azure_wrong_storage_key() {
        object_store_cache_clear();

        // unset AZURE_STORAGE_CONNECTION_STRING to make sure the account name and key are used
        std::env::remove_var("AZURE_STORAGE_CONNECTION_STRING");

        let wrong_account_key = String::from("FFy8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==");
        std::env::set_var("AZURE_STORAGE_KEY", wrong_account_key);

        let test_container_name: String = std::env::var("AZURE_TEST_CONTAINER_NAME")
            .expect("AZURE_TEST_CONTAINER_NAME not found");

        let test_account_name: String =
            std::env::var("AZURE_STORAGE_ACCOUNT").expect("AZURE_STORAGE_ACCOUNT not found");

        let azure_blob_uri =
            format!("https://{test_account_name}.blob.core.windows.net/{test_container_name}/pg_parquet_test.parquet");

        let test_table = TestTable::<i32>::new("int4".into()).with_uri(azure_blob_uri);

        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();
    }

    #[pg_test]
    #[should_panic(expected = "404 Not Found")]
    #[cfg(not(rhel8))]
    fn test_azure_write_wrong_container() {
        object_store_cache_clear();

        let test_account_name: String =
            std::env::var("AZURE_STORAGE_ACCOUNT").expect("AZURE_STORAGE_ACCOUNT not found");

        let azure_blob_uri =
            format!("https://{test_account_name}.blob.core.windows.net/nonexistentcontainer/pg_parquet_test.parquet");

        let copy_to_command = format!(
            "COPY (SELECT i FROM generate_series(1,10) i) TO '{azure_blob_uri}' WITH (format parquet);"
        );
        Spi::run(copy_to_command.as_str()).unwrap();
    }

    #[pg_test]
    #[cfg(not(rhel8))]
    fn test_azure_read_write_sas() {
        object_store_cache_clear();

        let test_container_name: String = std::env::var("AZURE_TEST_CONTAINER_NAME")
            .expect("AZURE_TEST_CONTAINER_NAME not found");

        let test_account_name: String =
            std::env::var("AZURE_STORAGE_ACCOUNT").expect("AZURE_STORAGE_ACCOUNT not found");

        let read_write_sas_token = std::env::var("AZURE_TEST_READ_WRITE_SAS")
            .expect("AZURE_TEST_READ_WRITE_SAS not found");

        // remove account key and connection string to make sure the sas token is used
        std::env::remove_var("AZURE_STORAGE_KEY");
        std::env::remove_var("AZURE_STORAGE_CONNECTION_STRING");
        std::env::set_var("AZURE_STORAGE_SAS_TOKEN", read_write_sas_token);

        let azure_blob_uri =
            format!("https://{test_account_name}.blob.core.windows.net/{test_container_name}/pg_parquet_test.parquet");

        let copy_to_command = format!(
            "COPY (SELECT i FROM generate_series(1,10) i) TO '{azure_blob_uri}' WITH (format parquet);"
        );
        Spi::run(copy_to_command.as_str()).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "403 Forbidden")]
    #[cfg(not(rhel8))]
    fn test_azure_read_only_sas() {
        object_store_cache_clear();

        let test_container_name: String = std::env::var("AZURE_TEST_CONTAINER_NAME")
            .expect("AZURE_TEST_CONTAINER_NAME not found");

        let test_account_name: String =
            std::env::var("AZURE_STORAGE_ACCOUNT").expect("AZURE_STORAGE_ACCOUNT not found");

        let read_only_sas_token: String =
            std::env::var("AZURE_TEST_READ_ONLY_SAS").expect("AZURE_TEST_READ_ONLY_SAS not found");

        // remove account key and connection string to make sure the sas token is used
        std::env::remove_var("AZURE_STORAGE_KEY");
        std::env::remove_var("AZURE_STORAGE_CONNECTION_STRING");
        std::env::set_var("AZURE_STORAGE_SAS_TOKEN", read_only_sas_token);

        let azure_blob_uri =
            format!("https://{test_account_name}.blob.core.windows.net/{test_container_name}/pg_parquet_test.parquet");

        let copy_to_command = format!(
            "COPY (SELECT i FROM generate_series(1,10) i) TO '{azure_blob_uri}' WITH (format parquet);"
        );
        Spi::run(copy_to_command.as_str()).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "could not open file")]
    #[cfg(not(rhel8))]
    fn test_azure_unsupported_uri() {
        object_store_cache_clear();

        let create_table = "create table test_table(id int);";
        Spi::run(create_table).unwrap();

        Spi::run("copy test_table from 'https://ACCOUNT.dfs.fabric.microsoft.com';").unwrap();
    }

    #[pg_test]
    fn test_http_uri() {
        object_store_cache_clear();

        let http_endpoint: String =
            std::env::var("HTTP_ENDPOINT").expect("HTTP_ENDPOINT not found");

        let http_uris = [
            format!("{http_endpoint}/pg_parquet_test.parquet"),
            format!("{http_endpoint}/dummy/pg_parquet_test"),
        ];

        for http_uri in http_uris {
            let test_table = TestTable::<i32>::new("int4".into()).with_uri(http_uri);

            test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
            test_table.assert_expected_and_result_rows();
        }
    }

    #[pg_test]
    fn test_http_uri_with_special_chars() {
        object_store_cache_clear();

        let http_endpoint: String =
            std::env::var("HTTP_ENDPOINT").expect("HTTP_ENDPOINT not found");

        let http_uri = format!("{http_endpoint}/so\\mek*ey/**testme.parquet");

        let copy_to_command = format!(
            "COPY (SELECT a FROM generate_series(1,10) a) TO '{http_uri}' WITH (format parquet);"
        );
        Spi::run(&copy_to_command).unwrap();

        let create_table = "create table test_table(a int);";
        Spi::run(create_table).unwrap();

        let copy_from_command = format!("COPY test_table FROM '{http_uri}' WITH (format parquet);");
        Spi::run(copy_from_command.as_str()).unwrap();

        let count_query = "select count(*) from test_table;";
        let result = Spi::get_one::<i64>(count_query).unwrap().unwrap();
        assert_eq!(result, 10);
    }

    // an http(s) endpoint takes an object in a single request, so a copy into one is bounded
    // by the buffer that the object is held in until the copy ends. The error says what the
    // bound is, rather than naming the operation that the store does not implement and
    // nothing else, since the same command writing a smaller table succeeds.
    #[pg_test]
    #[should_panic(expected = "takes an object of at most 10 MiB")]
    fn test_http_uri_above_the_single_request_bound() {
        object_store_cache_clear();

        let http_endpoint: String =
            std::env::var("HTTP_ENDPOINT").expect("HTTP_ENDPOINT not found");

        let http_uri = format!("{http_endpoint}/above_the_bound.parquet");

        Spi::run(&format!(
            "COPY (SELECT md5(i::text) || md5((-i)::text) a FROM generate_series(1, 250000) i)
             TO '{http_uri}' WITH (format parquet);"
        ))
        .unwrap();
    }

    // the way around that bound, which the error above names: a file_size_bytes below it
    // writes a larger table as a folder of files, each of which fits a single request. The
    // files are read back one name at a time, since an http(s) endpoint is the one store
    // that a uri pattern does not apply to.
    #[pg_test]
    fn test_http_folder_destination_above_the_single_request_bound() {
        object_store_cache_clear();

        let http_endpoint: String =
            std::env::var("HTTP_ENDPOINT").expect("HTTP_ENDPOINT not found");

        let http_uri = format!("{http_endpoint}/above_the_bound_folder");

        Spi::run(&format!(
            "COPY (SELECT md5(i::text) || md5((-i)::text) a FROM generate_series(1, 250000) i)
             TO '{http_uri}' WITH (format parquet, file_size_bytes '4MB');"
        ))
        .unwrap();

        Spi::run("CREATE TABLE test_table(a text);").unwrap();

        // the files of a folder are numbered, so they are read until a number that the
        // folder does not hold a file of
        Spi::run(&format!(
            "DO $$
             DECLARE
                 file_id int := 0;
             BEGIN
                 LOOP
                     BEGIN
                         EXECUTE format(
                             'COPY test_table FROM ''{http_uri}/data_%s.parquet'' \
                              WITH (format parquet)',
                             file_id
                         );
                     EXCEPTION WHEN others THEN
                         EXIT;
                     END;

                     file_id := file_id + 1;
                 END LOOP;
             END $$;"
        ))
        .unwrap();

        let count = Spi::get_one::<i64>("SELECT count(*) FROM test_table;")
            .unwrap()
            .unwrap();
        assert_eq!(count, 250000);
    }

    #[pg_test]
    #[should_panic(expected = "failed to get object store metadata for uri")]
    fn test_http_with_nonexistent_pattern_uri() {
        object_store_cache_clear();

        let http_endpoint: String =
            std::env::var("HTTP_ENDPOINT").expect("HTTP_ENDPOINT not found");

        let http_uri_pattern = format!("{http_endpoint}/notexists/**");

        let create_table = "create table test_table(a int);";
        Spi::run(create_table).unwrap();

        let copy_from_command =
            format!("COPY test_table FROM '{http_uri_pattern}' WITH (format parquet);");
        Spi::run(copy_from_command.as_str()).unwrap();
    }

    #[pg_test]
    fn test_gcs_from_env() {
        object_store_cache_clear();

        let test_bucket_name: String =
            std::env::var("GOOGLE_TEST_BUCKET").expect("GOOGLE_TEST_BUCKET not found");

        let gcs_uri = format!("gs://{test_bucket_name}/pg_parquet_test.parquet");

        let test_table = TestTable::<i32>::new("int4".into()).with_uri(gcs_uri);

        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();
    }

    #[pg_test]
    fn test_gcs_from_env_glob_pattern() {
        object_store_cache_clear();

        let test_bucket_name: String =
            std::env::var("GOOGLE_TEST_BUCKET").expect("GOOGLE_TEST_BUCKET not found");

        let gcs_uri = format!(
            "gs://{test_bucket_name}/test_gcs_from_env_glob_pattern/some/pg_parquet_test.parquet"
        );

        let gcs_uri_pattern = format!("gs://{test_bucket_name}/test_gcs_from_env_glob_pattern/**");

        let mut copy_options = HashMap::new();
        copy_options.insert(
            "file_size_bytes".to_string(),
            CopyOptionValue::StringOption("1MB".to_string()),
        );

        let test_table = TestTable::<i32>::new("int4".into())
            .with_uri(gcs_uri)
            .with_uri_pattern(gcs_uri_pattern)
            .with_order_by_col("a".into())
            .with_copy_to_options(copy_options);

        test_table.insert("INSERT INTO test_expected select i from generate_series(1, 1000000) i;");
        test_table.assert_expected_and_result_rows();
    }

    #[pg_test]
    fn test_gcs_uri_with_special_chars() {
        object_store_cache_clear();

        let test_bucket_name: String =
            std::env::var("GOOGLE_TEST_BUCKET").expect("GOOGLE_TEST_BUCKET not found");

        let gcs_uri = format!("gs://{test_bucket_name}/so\\mek*ey/**testme.parquet");

        let copy_to_command = format!(
            "COPY (SELECT a FROM generate_series(1,10) a) TO '{gcs_uri}' WITH (format parquet);"
        );
        Spi::run(&copy_to_command).unwrap();

        let create_table = "create table test_table(a int);";
        Spi::run(create_table).unwrap();

        let copy_from_command = format!("COPY test_table FROM '{gcs_uri}' WITH (format parquet);");
        Spi::run(copy_from_command.as_str()).unwrap();

        let count_query = "select count(*) from test_table;";
        let result = Spi::get_one::<i64>(count_query).unwrap().unwrap();
        assert_eq!(result, 10);
    }

    #[pg_test]
    #[should_panic(expected = "no files found that match the pattern")]
    fn test_gcs_with_nonexistent_pattern_uri() {
        object_store_cache_clear();

        let test_bucket_name: String =
            std::env::var("GOOGLE_TEST_BUCKET").expect("GOOGLE_TEST_BUCKET not found");

        let gcs_uri_pattern = format!("gs://{test_bucket_name}/notexists/**");

        let create_table = "create table test_table(a int);";
        Spi::run(create_table).unwrap();

        let copy_from_command =
            format!("COPY test_table FROM '{gcs_uri_pattern}' WITH (format parquet);");
        Spi::run(copy_from_command.as_str()).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "404 Not Found")]
    fn test_gcs_write_wrong_bucket() {
        object_store_cache_clear();

        let s3_uri = "gs://randombucketwhichdoesnotexist/pg_parquet_test.parquet";

        let copy_to_command =
            format!("COPY (SELECT i FROM generate_series(1,10) i) TO '{s3_uri}';");
        Spi::run(copy_to_command.as_str()).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "404 Not Found")]
    fn test_gcs_read_wrong_bucket() {
        object_store_cache_clear();

        let gcs_uri = "gs://randombucketwhichdoesnotexist/pg_parquet_test.parquet";

        let create_table_command = "CREATE TABLE test_table (a int);";
        Spi::run(create_table_command).unwrap();

        let copy_from_command = format!("COPY test_table FROM '{gcs_uri}';");
        Spi::run(copy_from_command.as_str()).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "relative path not allowed")]
    fn test_copy_to_unrecognized_scheme() {
        object_store_cache_clear();

        let create_table = "create table test_table(id int);";
        Spi::run(create_table).unwrap();

        Spi::run("copy test_table to 'dummy://testbucket/dummy.parquet';").unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "relative path not allowed")]
    fn test_copy_to_non_parquet_uri() {
        object_store_cache_clear();

        let create_table = "create table test_table(id int);";
        Spi::run(create_table).unwrap();

        Spi::run("copy test_table to 's3://testbucket/dummy.csv';").unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "could not open file")]
    fn test_copy_from_non_parquet_uri() {
        object_store_cache_clear();

        let create_table = "create table test_table(id int);";
        Spi::run(create_table).unwrap();

        Spi::run("copy test_table from 's3://testbucket/dummy.csv';").unwrap();
    }

    #[pg_test]
    #[cfg(not(rhel8))]
    fn test_object_store_cache() {
        object_store_cache_clear();

        // s3 scheme and bucket
        let test_table =
            TestTable::<i32>::new("int4".into()).with_uri("s3://testbucket/test1.parquet".into());
        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();

        assert_eq!(
            object_store_cache_items(),
            vec![("AmazonS3", "testbucket", None)]
        );

        // s3 scheme and same bucket
        let test_table =
            TestTable::<i32>::new("int4".into()).with_uri("s3://testbucket/test1.parquet".into());
        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();

        assert_eq!(
            object_store_cache_items(),
            vec![("AmazonS3", "testbucket", None)]
        );

        // s3 scheme and different bucket
        let test_table =
            TestTable::<i32>::new("int4".into()).with_uri("s3://testbucket2/test1.parquet".into());
        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();

        assert_eq!(
            object_store_cache_items(),
            vec![
                ("AmazonS3", "testbucket", None),
                ("AmazonS3", "testbucket2", None)
            ]
        );

        // az scheme and container
        let test_table = TestTable::<i32>::new("int4".into())
            .with_uri("az://testcontainer/test1.parquet".into());
        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();

        assert_eq!(
            object_store_cache_items(),
            vec![
                ("AmazonS3", "testbucket", None),
                ("AmazonS3", "testbucket2", None),
                ("MicrosoftAzure", "testcontainer", None)
            ]
        );

        // az scheme and same container
        let test_table = TestTable::<i32>::new("int4".into())
            .with_uri("az://testcontainer/test1.parquet".into());
        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();

        assert_eq!(
            object_store_cache_items(),
            vec![
                ("AmazonS3", "testbucket", None),
                ("AmazonS3", "testbucket2", None),
                ("MicrosoftAzure", "testcontainer", None)
            ]
        );

        // az scheme and different container
        let test_table = TestTable::<i32>::new("int4".into())
            .with_uri("az://testcontainer2/test1.parquet".into());
        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();

        assert_eq!(
            object_store_cache_items(),
            vec![
                ("AmazonS3", "testbucket", None),
                ("AmazonS3", "testbucket2", None),
                ("MicrosoftAzure", "testcontainer", None),
                ("MicrosoftAzure", "testcontainer2", None)
            ]
        );

        // gs scheme and bucket
        let test_table =
            TestTable::<i32>::new("int4".into()).with_uri("gs://testbucket/test1.parquet".into());
        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();

        assert_eq!(
            object_store_cache_items(),
            vec![
                ("AmazonS3", "testbucket", None),
                ("AmazonS3", "testbucket2", None),
                ("GoogleCloudStorage", "testbucket", None),
                ("MicrosoftAzure", "testcontainer", None),
                ("MicrosoftAzure", "testcontainer2", None)
            ]
        );

        // gs scheme and same bucket
        let test_table =
            TestTable::<i32>::new("int4".into()).with_uri("gs://testbucket/test1.parquet".into());
        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();

        assert_eq!(
            object_store_cache_items(),
            vec![
                ("AmazonS3", "testbucket", None),
                ("AmazonS3", "testbucket2", None),
                ("GoogleCloudStorage", "testbucket", None),
                ("MicrosoftAzure", "testcontainer", None),
                ("MicrosoftAzure", "testcontainer2", None)
            ]
        );

        // gs scheme and different bucket
        let test_table =
            TestTable::<i32>::new("int4".into()).with_uri("gs://testbucket2/test1.parquet".into());
        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();

        assert_eq!(
            object_store_cache_items(),
            vec![
                ("AmazonS3", "testbucket", None),
                ("AmazonS3", "testbucket2", None),
                ("GoogleCloudStorage", "testbucket", None),
                ("GoogleCloudStorage", "testbucket2", None),
                ("MicrosoftAzure", "testcontainer", None),
                ("MicrosoftAzure", "testcontainer2", None)
            ]
        );

        // https scheme and base uri
        let http_uri =
            "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-03.parquet";
        Spi::run(format!("SELECT * FROM parquet.schema('{http_uri}')").as_str()).unwrap();

        assert_eq!(
            object_store_cache_items(),
            vec![
                ("AmazonS3", "testbucket", None),
                ("AmazonS3", "testbucket2", None),
                ("GoogleCloudStorage", "testbucket", None),
                ("GoogleCloudStorage", "testbucket2", None),
                ("Http", "https://d37ci6vzurychx.cloudfront.net", None),
                ("MicrosoftAzure", "testcontainer", None),
                ("MicrosoftAzure", "testcontainer2", None)
            ]
        );

        // https scheme and same base uri
        let http_uri =
            "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-04.parquet";
        Spi::run(format!("SELECT * FROM parquet.schema('{http_uri}')").as_str()).unwrap();

        assert_eq!(
            object_store_cache_items(),
            vec![
                ("AmazonS3", "testbucket", None),
                ("AmazonS3", "testbucket2", None),
                ("GoogleCloudStorage", "testbucket", None),
                ("GoogleCloudStorage", "testbucket2", None),
                ("Http", "https://d37ci6vzurychx.cloudfront.net", None),
                ("MicrosoftAzure", "testcontainer", None),
                ("MicrosoftAzure", "testcontainer2", None)
            ]
        );

        // https scheme and different base uri
        let http_uri = "https://www.filesampleshub.com/download/code/parquet/sample1.parquet";
        Spi::run(format!("SELECT * FROM parquet.schema('{http_uri}')").as_str()).unwrap();

        assert_eq!(
            object_store_cache_items(),
            vec![
                ("AmazonS3", "testbucket", None),
                ("AmazonS3", "testbucket2", None),
                ("GoogleCloudStorage", "testbucket", None),
                ("GoogleCloudStorage", "testbucket2", None),
                ("Http", "https://d37ci6vzurychx.cloudfront.net", None),
                ("Http", "https://www.filesampleshub.com", None),
                ("MicrosoftAzure", "testcontainer", None),
                ("MicrosoftAzure", "testcontainer2", None)
            ]
        );
    }

    // a write that fails names the path it failed on, which the os error alone does not say
    #[pg_test]
    #[should_panic(
        expected = "is a file, so \"/tmp/pg_parquet_test.parquet/data_0.parquet\" cannot be created under it"
    )]
    fn test_local_file_under_a_file_names_the_path() {
        let _file_cleanup = FileCleanup::new(LOCAL_TEST_FILE_PATH);

        // the destination itself is written as a file first, so that the copy below finds a
        // file where it needs a folder, which is what a copy with file_size_bytes runs into
        Spi::run(&format!(
            "COPY (SELECT 1 AS a) TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();

        Spi::run(&format!(
            "COPY (SELECT 1 AS a) TO '{LOCAL_TEST_FILE_PATH}/data_0.parquet'
             WITH (format parquet);"
        ))
        .unwrap();
    }

    // A copy reads a parquet file with a request for its footer and more requests for the row
    // groups the footer points at, so the object all of them read has to be the object the
    // footer came from: one that is overwritten in between is otherwise read as a file made of
    // the footer of the object it was and the bytes of the object it became, which reports
    // nothing at all when the two are laid out alike.
    #[pg_test]
    fn test_read_of_an_overwritten_object_is_refused() {
        let other_file_path = "/tmp/pg_parquet_other_test.parquet";

        let _file_cleanup = FileCleanup::new(LOCAL_TEST_FILE_PATH);
        let _other_file_cleanup = FileCleanup::new(other_file_path);

        Spi::run(&format!(
            "COPY (SELECT i AS a FROM generate_series(1, 100) i) TO '{LOCAL_TEST_FILE_PATH}'
             WITH (format parquet);"
        ))
        .unwrap();

        // another parquet file of the same schema, which is what the object is overwritten with
        Spi::run(&format!(
            "COPY (SELECT i AS a FROM generate_series(1001, 1500) i) TO '{other_file_path}'
             WITH (format parquet);"
        ))
        .unwrap();

        let original_bytes = std::fs::read(LOCAL_TEST_FILE_PATH).unwrap();
        let other_bytes = std::fs::read(other_file_path).unwrap();

        let uri_info = ParsedUriInfo::try_from(LOCAL_TEST_FILE_PATH).unwrap();

        let copy_from = true;
        let (store, location) = get_or_create_object_store(&uri_info, copy_from);

        let (refused, read_without_a_version) = PG_BACKEND_TOKIO_RUNTIME.block_on(async move {
            let object = store.head(&location).await.unwrap();

            let mut reader = ParquetObjectStoreReader::new(
                store.clone(),
                location.clone(),
                object.size,
                object.e_tag,
            );

            // the footer is read from the object as it is before it is overwritten
            reader.get_metadata(None).await.unwrap();

            std::fs::copy(other_file_path, LOCAL_TEST_FILE_PATH).unwrap();

            let refused = reader.get_bytes(0..object.size).await.unwrap_err();

            // the same read of a reader that holds no version of the object, which is what a
            // store that reports none leaves: it reads the bytes of the object it became
            let mut reader_without_a_version =
                ParquetObjectStoreReader::new(store, location, object.size, None);

            let read_without_a_version = reader_without_a_version
                .get_bytes(0..object.size)
                .await
                .unwrap();

            (refused, read_without_a_version)
        });

        assert!(
            refused
                .to_string()
                .contains("was overwritten while it was being read"),
            "the read of the overwritten object was not described as one: {refused}"
        );

        assert_eq!(
            read_without_a_version.as_ref(),
            &other_bytes[..original_bytes.len()],
            "a read that asks for no version read something other than the object it found"
        );

        assert_ne!(
            read_without_a_version.as_ref(),
            original_bytes.as_slice(),
            "the object was not overwritten at all"
        );
    }

    // the same over a store that is reached over http, which answers a read of a version the
    // object is no longer at with a status of its own rather than refusing it locally
    #[pg_test]
    fn test_s3_read_of_a_replaced_object_fails() {
        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let uri = format!("s3://{test_bucket_name}/pg_parquet_test_replaced.parquet");

        Spi::run(&format!(
            "COPY (SELECT i AS a FROM generate_series(1, 100) i) TO '{uri}'
             WITH (format parquet);"
        ))
        .unwrap();

        let uri_info = ParsedUriInfo::try_from(uri.as_str()).unwrap();

        let copy_from = true;
        let (store, location) = get_or_create_object_store(&uri_info, copy_from);

        PG_BACKEND_TOKIO_RUNTIME.block_on(async move {
            let object = store.head(&location).await.unwrap();

            // the version the object is at, which is what a read of it is opened on
            let mut reader = ParquetObjectStoreReader::new(
                store.clone(),
                location.clone(),
                object.size,
                object.e_tag.clone(),
            );

            assert_eq!(reader.get_bytes(0..4).await.unwrap().as_ref(), b"PAR1");

            // a version that the object is no longer at, which is the object having been
            // overwritten since the read was opened on it
            let mut reader = ParquetObjectStoreReader::new(
                store,
                location,
                object.size,
                Some("\"00000000000000000000000000000000\"".into()),
            );

            let error = reader
                .get_bytes(0..4)
                .await
                .expect_err("the read of a version that is not there succeeded");

            assert!(
                error
                    .to_string()
                    .contains("was overwritten while it was being read"),
                "the read of an overwritten object failed with \"{error}\""
            );
        });
    }

    // an object whose footer describes rows beyond the end of the object says what is wrong with
    // the object. The read of such a row group is answered by nothing useful: a store answers a
    // range that begins in the object and ends after it with the bytes it does hold, which the
    // reader panics over while slicing the range it asked for out of them.
    #[pg_test]
    #[should_panic(
        expected = "which are beyond its end. The object is not the file that footer describes: a file that was cut short after its footer was written reads this way, as do two parquet files that were written into one object. Write the file again."
    )]
    fn test_s3_read_of_an_object_cut_short_after_its_footer_is_refused() {
        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let s3_uri = format!(
            "s3://{test_bucket_name}/test_s3_read_of_an_object_cut_short_after_its_footer/cut_short.parquet"
        );

        // the rows are enough that the row groups of the file lie beyond the head kept below
        Spi::run(&format!(
            "COPY (SELECT i AS a, i::text AS b FROM generate_series(1, 100000) i) TO '{s3_uri}'
             WITH (format parquet);"
        ))
        .unwrap();

        cut_the_object_short_after_its_footer(&s3_uri, 2000);

        Spi::run("CREATE TABLE test_table (a int, b text);").unwrap();

        Spi::run(&format!(
            "COPY test_table FROM '{s3_uri}' WITH (format parquet);"
        ))
        .unwrap();
    }

    // the local store answers such a read with a message of its own that reports the same number
    // as both what it expected of the object and what it found in it, so the object is reported
    // here as well rather than by the store
    #[pg_test]
    #[should_panic(expected = "the object \"tmp/pg_parquet_test_cut_short.parquet\" is ")]
    fn test_local_read_of_a_file_cut_short_after_its_footer_is_refused() {
        let local_path = "/tmp/pg_parquet_test_cut_short.parquet";

        let _file_cleanup = FileCleanup::new(local_path);

        Spi::run(&format!(
            "COPY (SELECT i AS a, i::text AS b FROM generate_series(1, 100000) i) TO '{local_path}'
             WITH (format parquet);"
        ))
        .unwrap();

        cut_the_object_short_after_its_footer(local_path, 2000);

        Spi::run("CREATE TABLE test_table (a int, b text);").unwrap();

        Spi::run(&format!(
            "COPY test_table FROM '{local_path}' WITH (format parquet);"
        ))
        .unwrap();
    }

    // The readers of one parallel COPY FROM read one version of a file: a worker is given the
    // row groups the leader found in the version it read the footer of, so a worker that finds
    // another version of the file reads nothing of it rather than reading row groups of one
    // version at the offsets of another.
    #[pg_test]
    fn test_open_of_another_version_of_a_file_is_refused() {
        let _file_cleanup = FileCleanup::new(LOCAL_TEST_FILE_PATH);

        Spi::run(&format!(
            "COPY (SELECT i AS a FROM generate_series(1, 100) i) TO '{LOCAL_TEST_FILE_PATH}'
             WITH (format parquet);"
        ))
        .unwrap();

        let uri_info = ParsedUriInfo::try_from(LOCAL_TEST_FILE_PATH).unwrap();

        let any_version = None;
        let opener = ParquetRowGroupOpener::try_new(&uri_info, any_version).unwrap();

        // the local store reports a version of its own for every file it holds
        let version = opener
            .version()
            .expect("the store reported no version of the file")
            .to_string();

        // the version the leader read the footer of opens the file again, which is what every
        // worker of the command does
        ParquetRowGroupOpener::try_new(&uri_info, Some(&version)).unwrap();

        let Err(error) = ParquetRowGroupOpener::try_new(&uri_info, Some("\"another-version\""))
        else {
            panic!("a file of another version was opened");
        };

        assert!(
            error.contains("was overwritten while it was being read"),
            "the open of another version of the file was not described as one: {error}"
        );
    }

    // the same over a store that is reached over http, which refuses the read of a version the
    // object is no longer at with a status of its own rather than answering it with the version
    // the object is at
    #[pg_test]
    fn test_s3_open_of_another_version_of_a_file_is_refused() {
        object_store_cache_clear();

        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let s3_uri = format!("s3://{test_bucket_name}/pg_parquet_test_another_version.parquet");

        Spi::run(&format!(
            "COPY (SELECT i AS a FROM generate_series(1, 100) i) TO '{s3_uri}'
             WITH (format parquet);"
        ))
        .unwrap();

        let uri_info = ParsedUriInfo::try_from(s3_uri.as_str()).unwrap();

        let any_version = None;
        let opener = ParquetRowGroupOpener::try_new(&uri_info, any_version).unwrap();

        let version = opener
            .version()
            .expect("the store reported no version of the object")
            .to_string();

        ParquetRowGroupOpener::try_new(&uri_info, Some(&version)).unwrap();

        let Err(error) =
            ParquetRowGroupOpener::try_new(&uri_info, Some("\"00000000000000000000000000000000\""))
        else {
            panic!("an object of another version was opened");
        };

        assert!(
            error.contains("was overwritten while it was being read")
                && error.contains("The read of the version it was asked for:"),
            "the open of another version of the object was not described as one the store \
             refused: {error}"
        );
    }

    // A store that answers a read which asks for a version of the object without checking that
    // the object still is that version hands back the bytes of the object it became, which the
    // version it reports for what it answered is what catches. A store that reports no version
    // for what it answered says nothing about which version that was.
    #[pg_test]
    fn test_read_answered_with_another_version_is_refused() {
        let _file_cleanup = FileCleanup::new(LOCAL_TEST_FILE_PATH);

        Spi::run(&format!(
            "COPY (SELECT i AS a FROM generate_series(1, 100) i) TO '{LOCAL_TEST_FILE_PATH}'
             WITH (format parquet);"
        ))
        .unwrap();

        let uri_info = ParsedUriInfo::try_from(LOCAL_TEST_FILE_PATH).unwrap();

        let copy_from = true;
        let (store, location) = get_or_create_object_store(&uri_info, copy_from);

        let version = "\"the-version-the-footer-came-from\"";
        let file_size = 0;
        let reader =
            ParquetObjectStoreReader::new(store, location, file_size, Some(version.to_string()));

        reader
            .ensure_version_was_read(Some(version))
            .expect("a read answered with the version it asked for was refused");

        reader
            .ensure_version_was_read(None)
            .expect("a read answered without a version was refused");

        let error = reader
            .ensure_version_was_read(Some("\"another-version\""))
            .expect_err("a read answered with another version was read")
            .to_string();

        assert!(
            error.contains("was overwritten while it was being read")
                && error.contains("\"another-version\""),
            "the read of another version of the object was not described as one: {error}"
        );
    }

    // the head a file is opened with is answered by such a store the same way, which is what
    // tells a worker that the version the leader read the footer of is gone before it reads a
    // row group of the object the file became
    #[pg_test]
    fn test_head_answered_with_another_version_is_refused() {
        let uri = "s3://testbucket/t.parquet";
        let location = object_store::path::Path::from("t.parquet");

        let version = "\"the-version-the-footer-came-from\"";

        // a head that asks for no version is answered by whichever version the object is at
        ensure_version_is_the_one_asked_for(uri, &location, None, Some(version))
            .expect("a head that asked for no version was refused");

        ensure_version_is_the_one_asked_for(uri, &location, Some(version), Some(version))
            .expect("a head answered with the version it asked for was refused");

        let error =
            ensure_version_is_the_one_asked_for(uri, &location, Some(version), Some("\"another\""))
                .expect_err("a head answered with another version was read");

        assert!(
            error.contains("was overwritten while it was being read")
                && error.contains("version \"another\""),
            "the head of another version of the object was not described as one: {error}"
        );

        // a store that reports no version for what it answered says nothing about which
        // version that was, so the head is refused with that rather than with a version
        let error = ensure_version_is_the_one_asked_for(uri, &location, Some(version), None)
            .expect_err("a head answered without a version was read");

        assert!(
            error.contains("no version at all"),
            "the head answered without a version was not described as one: {error}"
        );
    }

    #[pg_test]
    #[should_panic(expected = "failed to create the file \"/tmp\"")]
    fn test_local_file_that_is_a_folder_names_the_path() {
        Spi::run("COPY (SELECT 1 AS a) TO '/tmp' WITH (format parquet);").unwrap();
    }

    // a ".." segment of a local path is collapsed, the way it is collapsed in the path of an
    // s3://, az://, gs:// or https:// uri
    #[pg_test]
    fn test_local_path_with_a_parent_segment() {
        let _file_cleanup = FileCleanup::new(LOCAL_TEST_FILE_PATH);

        let parent_segment_uri = "/var/../tmp/pg_parquet_test.parquet";

        Spi::run(&format!(
            "COPY (SELECT 1 AS a) TO '{parent_segment_uri}' WITH (format parquet);"
        ))
        .unwrap();

        assert!(
            std::path::Path::new(LOCAL_TEST_FILE_PATH).exists(),
            "the copy wrote no \"{LOCAL_TEST_FILE_PATH}\""
        );

        Spi::run("CREATE TABLE test_table (a int);").unwrap();

        Spi::run(&format!(
            "COPY test_table FROM '{parent_segment_uri}' WITH (format parquet);"
        ))
        .unwrap();

        assert_eq!(
            Spi::get_one::<i32>("SELECT a FROM test_table;").unwrap(),
            Some(1)
        );
    }

    // a copy that asked for the parquet format is told what is wrong with its uri. Postgres
    // would only report that it has no format named parquet, which says nothing about the uri.
    #[pg_test]
    #[should_panic(expected = "unrecognized uri dummy://testbucket/dummy.parquet")]
    fn test_copy_to_unparseable_uri_reports_the_uri() {
        Spi::run("CREATE TABLE test_table (a int);").unwrap();

        Spi::run("COPY test_table TO 'dummy://testbucket/dummy.parquet' WITH (format parquet);")
            .unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "unrecognized uri ftp://host/dummy.parquet")]
    fn test_copy_from_unparseable_uri_reports_the_uri() {
        Spi::run("CREATE TABLE test_table (a int);").unwrap();

        Spi::run("COPY test_table FROM 'ftp://host/dummy.parquet' WITH (format parquet);").unwrap();
    }

    // a uri whose scheme is supported and whose path is not is reported as the path it is,
    // rather than as a scheme pg_parquet does not support
    #[pg_test]
    #[should_panic(
        expected = "unsupported path in uri s3://testbucket/empty//segment.parquet: Path \"/empty//segment.parquet\" contained empty path segment"
    )]
    fn test_copy_to_uri_with_an_empty_path_segment_reports_the_path() {
        Spi::run("CREATE TABLE test_table (a int);").unwrap();

        Spi::run(
            "COPY test_table TO 's3://testbucket/empty//segment.parquet' WITH (format parquet);",
        )
        .unwrap();
    }

    // a separator escaped into a segment is a path error too, and the uri is reported as it
    // was written rather than as the path the object store decoded it into
    #[pg_test]
    #[should_panic(expected = "unsupported path in uri s3://testbucket/..%2F..%2Fescape.parquet")]
    fn test_copy_from_uri_with_an_escaped_separator_reports_the_path() {
        Spi::run("CREATE TABLE test_table (a int);").unwrap();

        Spi::run(
            "COPY test_table FROM 's3://testbucket/..%2F..%2Fescape.parquet' WITH (format parquet);",
        )
        .unwrap();
    }

    // a uri that has both an unsupported scheme and a path the object store refuses is
    // reported as the scheme, which is the one the user has to fix first
    #[pg_test]
    #[should_panic(expected = "unrecognized uri ftp://host/empty//segment.parquet")]
    fn test_copy_to_unparseable_uri_with_a_bad_path_reports_the_scheme() {
        Spi::run("CREATE TABLE test_table (a int);").unwrap();

        Spi::run("COPY test_table TO 'ftp://host/empty//segment.parquet' WITH (format parquet);")
            .unwrap();
    }

    // a uri whose scheme needs a bucket and that has none, which is what a bucket name read
    // from an empty variable leaves behind, is reported as the bucket rather than the scheme
    #[pg_test]
    #[should_panic(expected = "no bucket in uri s3:///nobucket.parquet")]
    fn test_copy_to_uri_without_a_bucket_reports_the_bucket() {
        Spi::run("CREATE TABLE test_table (a int);").unwrap();

        Spi::run("COPY test_table TO 's3:///nobucket.parquet' WITH (format parquet);").unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "no bucket in uri az:///nocontainer.parquet")]
    fn test_copy_from_azure_uri_without_a_container_reports_the_container() {
        Spi::run("CREATE TABLE test_table (a int);").unwrap();

        Spi::run("COPY test_table FROM 'az:///nocontainer.parquet' WITH (format parquet);")
            .unwrap();
    }

    // a uri is parsed as the url standard asks it to be parsed, which removes the tab from
    // "s3://testbucket/cl<tab>ash.parquet" and leaves it naming "s3://testbucket/clash.parquet",
    // an object of someone else's. Such a uri is refused rather than copied, and the refusal
    // names the object the copy would have written.
    #[pg_test]
    #[should_panic(expected = "unsupported whitespace in uri s3://testbucket/clash.parquet")]
    fn test_copy_to_uri_with_a_tab_is_refused() {
        Spi::run("CREATE TABLE test_table (a int);").unwrap();

        Spi::run("COPY test_table TO 's3://testbucket/cl\tash.parquet' WITH (format parquet);")
            .unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "unsupported whitespace in uri s3://testbucket/newline.parquet")]
    fn test_copy_from_uri_with_a_newline_is_refused() {
        Spi::run("CREATE TABLE test_table (a int);").unwrap();

        Spi::run("COPY test_table FROM 's3://testbucket/new\nline.parquet' WITH (format parquet);")
            .unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "unsupported whitespace in uri s3://testbucket/around.parquet")]
    fn test_copy_to_uri_with_whitespace_around_it_is_refused() {
        Spi::run("CREATE TABLE test_table (a int);").unwrap();

        Spi::run("COPY test_table TO '  s3://testbucket/around.parquet  ' WITH (format parquet);")
            .unwrap();
    }

    // a space inside the path is no whitespace the url standard removes, so a name holding one
    // is written and read as it is
    #[pg_test]
    fn test_s3_uri_with_a_space_in_the_name() {
        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let s3_uri = format!("s3://{test_bucket_name}/pg_parquet_test with a space.parquet");

        let test_table = TestTable::<i32>::new("int4".into()).with_uri(s3_uri);

        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();
    }

    // failing_copy is what a COPY that is expected to fail reported, caught in plpgsql so
    // that the test can look at the message rather than failing on it
    fn failing_copy(copy: &str) -> String {
        Spi::run(&format!(
            "CREATE OR REPLACE FUNCTION failing_copy() RETURNS text
             LANGUAGE plpgsql AS $body$
             BEGIN
                 EXECUTE $copy${copy}$copy$;

                 RETURN 'no error';
             EXCEPTION WHEN others THEN
                 RETURN SQLERRM;
             END;
             $body$;"
        ))
        .unwrap();

        Spi::get_one::<String>("SELECT failing_copy()")
            .unwrap()
            .unwrap()
    }

    // the credentials that a uri can carry before its host are never used: the store is built
    // from the scheme, the host and the port, and signs with the credentials of its
    // environment. Such a uri is refused, and the message that refuses it names the uri
    // without them, since that message is written into the server log too.
    #[pg_test]
    fn test_uri_with_credentials_is_refused_without_echoing_them() {
        Spi::run("CREATE TABLE test_table (a int);").unwrap();

        let secret = "s3cr3tpw";

        for (uri, uri_without_credentials) in [
            (
                format!("s3://key:{secret}@testbucket/dummy.parquet"),
                "s3://testbucket/dummy.parquet",
            ),
            (
                format!("az://key:{secret}@testcontainer/dummy.parquet"),
                "az://testcontainer/dummy.parquet",
            ),
            (
                format!("gs://key:{secret}@testbucket/dummy.parquet"),
                "gs://testbucket/dummy.parquet",
            ),
            (
                format!("https://key:{secret}@example.com/dummy.parquet"),
                "https://example.com/dummy.parquet",
            ),
            // the credentials of a uri are the whole of what precedes its host, so a uri
            // carrying only a user name is one of them as well
            (
                format!("s3://{secret}@testbucket/dummy.parquet"),
                "s3://testbucket/dummy.parquet",
            ),
        ] {
            for copy in [
                format!("COPY test_table TO '{uri}' WITH (format parquet)"),
                format!("COPY test_table FROM '{uri}' WITH (format parquet)"),
            ] {
                let error = failing_copy(&copy);

                assert!(
                    error.contains(&format!(
                        "unsupported credentials in uri {uri_without_credentials}"
                    )),
                    "the copy reported \"{error}\" instead"
                );

                assert!(
                    !error.contains(secret),
                    "the copy wrote the credentials of the uri into \"{error}\""
                );
            }
        }
    }

    // an "@" is part of the name of an object rather than the end of credentials, which only
    // precede the host of a uri
    #[pg_test]
    fn test_s3_uri_with_an_at_sign_in_the_name() {
        let test_bucket_name =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let uri = format!("s3://{test_bucket_name}/pg_parquet_test_at@sign.parquet");

        Spi::run("CREATE TABLE test_table (a int);").unwrap();

        Spi::run(&format!(
            "COPY (SELECT 1 AS a) TO '{uri}' WITH (format parquet);
             COPY test_table FROM '{uri}' WITH (format parquet);"
        ))
        .unwrap();

        assert_eq!(
            Spi::get_one::<i64>("SELECT count(*) FROM test_table;").unwrap(),
            Some(1)
        );
    }

    // a "#" written in the name of an object ends the path of its uri, so such a uri names a
    // shorter object than it appears to, which a copy would read or write in its place
    #[pg_test]
    #[should_panic(
        expected = "unsupported \"#\" in uri s3://testbucket/report#2026.parquet. Everything from the \"#\" is the fragment of the uri rather than part of the name of the object, so this uri names the object \"report\" rather than the one it appears to name. Write a \"#\" that is part of the name of an object as \"%23\", e.g. s3://testbucket/report%232026.parquet."
    )]
    fn test_copy_to_s3_uri_with_a_fragment_is_refused() {
        Spi::run(
            "COPY (SELECT 1 AS a) TO 's3://testbucket/report#2026.parquet' WITH (format parquet);",
        )
        .unwrap();
    }

    #[pg_test]
    #[should_panic(
        expected = "unsupported \"?\" in uri az://testcontainer/query.parquet?a=1. Everything from the \"?\" is the query of the uri rather than part of the name of the object, so this uri names the object \"query.parquet\" rather than the one it appears to name. Write a \"?\" that is part of the name of an object as \"%3F\", e.g. az://testcontainer/query.parquet%3Fa=1."
    )]
    fn test_copy_from_azure_uri_with_a_query_is_refused() {
        Spi::run("CREATE TABLE test_table (a int);").unwrap();

        Spi::run(
            "COPY test_table FROM 'az://testcontainer/query.parquet?a=1' WITH (format parquet);",
        )
        .unwrap();
    }

    // a "?" that follows the "#" of a uri is part of its fragment rather than a query of its own
    #[pg_test]
    #[should_panic(
        expected = "unsupported \"#\" in uri gs://testbucket/both#a?b.parquet. Everything from the \"#\" is the fragment of the uri"
    )]
    fn test_list_gcs_uri_with_a_fragment_holding_a_query_is_refused() {
        Spi::run("SELECT * FROM parquet.list('gs://testbucket/both#a?b.parquet');").unwrap();
    }

    // the escapes the refusals above ask for name an object whose name holds the characters
    // themselves, which is written, listed and read like any other object
    #[pg_test]
    fn test_s3_uri_with_an_escaped_truncating_sign() {
        object_store_cache_clear();

        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        Spi::run("create table test_table(a int);").unwrap();

        for escaped_name in ["hash%231.parquet", "query%3Fa=b.parquet"] {
            let s3_uri = format!("s3://{test_bucket_name}/escaped_sign/{escaped_name}");

            let copy_to_command = format!(
                "COPY (SELECT a FROM generate_series(1,10) a) TO '{s3_uri}' WITH (format parquet);"
            );
            Spi::run(&copy_to_command).unwrap();

            let copy_from_command =
                format!("COPY test_table FROM '{s3_uri}' WITH (format parquet);");
            Spi::run(copy_from_command.as_str()).unwrap();
        }

        let count_query = "select count(*) from test_table;";
        let result = Spi::get_one::<i64>(count_query).unwrap().unwrap();
        assert_eq!(result, 20);

        let list_query =
            format!("select count(*) from parquet.list('s3://{test_bucket_name}/escaped_sign/*');");
        let listed = Spi::get_one::<i64>(&list_query).unwrap().unwrap();
        assert_eq!(listed, 2);
    }
}
