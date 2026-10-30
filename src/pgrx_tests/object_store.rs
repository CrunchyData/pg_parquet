#[pgrx::pg_schema]
mod tests {
    use std::{collections::HashMap, io::Write};

    use object_store::{ClientConfigKey, ObjectStoreExt, PutPayload};
    use parquet::{arrow::async_reader::AsyncFileReader, errors::ParquetError};
    use pgrx::{pg_sys::Timestamp, pg_test, Spi};

    use crate::{
        arrow_parquet::uri_utils::{ParquetObjectStoreReader, ParsedUriInfo},
        object_store::{
            client_options::load_client_options, object_store_cache::get_or_create_object_store,
            object_store_error_message, proxy_hint,
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
}
