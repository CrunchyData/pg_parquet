//! Reaching parquet files that live in an object store, against the emulators.
//!
//! What a uri means, which credentials are resolved from where, and who may reach it belong to
//! `pg_object_store`, which tests all of that without a cloud. These are the end to end cases:
//! a `COPY` of real parquet over s3, azure, gcs and http, through the store the uri names. They
//! need an emulator, so they are selected by name by `make check-s3`, `check-azure`,
//! `check-gcs` and `check-http` -- hence the `test_<scheme>_` prefixes, which are load bearing.

#[pgrx::pg_schema]
mod tests {
    use std::{collections::HashMap, io::Write};

    use pgrx::{pg_test, Spi};

    use crate::pgrx_tests::common::{
        copy_to_helper, CopyOptionValue, FileCleanup, TestTable, LOCAL_TEST_FILE_PATH,
    };

    fn object_store_cache_clear() {
        Spi::run("SELECT object_store.cache_clear();").unwrap();
    }

    /// The cached clients as (scheme, bucket, whether the credentials expire), in a stable order.
    fn object_store_cache_items() -> Vec<(String, String, bool)> {
        Spi::connect(|client| {
            client
                .select(
                    "SELECT scheme, bucket, expires_at IS NOT NULL
                       FROM object_store.cache_items() ORDER BY scheme, bucket",
                    None,
                    &[],
                )
                .expect("failed to read the object store cache")
                .map(|row| {
                    (
                        row.get::<String>(1).unwrap().unwrap(),
                        row.get::<String>(2).unwrap().unwrap(),
                        row.get::<bool>(3).unwrap().unwrap(),
                    )
                })
                .collect()
        })
    }

    fn object_store_expire_item(bucket: &str) {
        let expire_command = format!("SELECT object_store.cache_expire_bucket('{bucket}');");
        Spi::run(&expire_command).unwrap();
    }

    #[pg_test]
    fn test_s3_from_env() {
        object_store_cache_clear();

        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        // the three spellings of the same object: the path style and the virtual hosted style
        // of the aws endpoint have to resolve to the bucket the s3 scheme names
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
            "format".to_string(),
            CopyOptionValue::StringOption("parquet".to_string()),
        );
        // small enough that the rows below land in more than one file, so the pattern has more
        // than one object to match
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

        // a key that is not a valid glob: it names one object rather than a pattern
        let s3_uri = format!("s3://{test_bucket_name}/so\\mek*ey/**testme.parquet");

        let copy_to_command = format!(
            "COPY (SELECT a FROM generate_series(1,10) a) TO '{s3_uri}' WITH (format parquet);"
        );
        Spi::run(&copy_to_command).unwrap();

        Spi::run("create table test_table(a int);").unwrap();

        let copy_from_command = format!("COPY test_table FROM '{s3_uri}' WITH (format parquet);");
        Spi::run(&copy_from_command).unwrap();

        let result = Spi::get_one::<i64>("select count(*) from test_table;")
            .unwrap()
            .unwrap();
        assert_eq!(result, 10);
    }

    #[pg_test]
    #[should_panic(expected = "no files found that match the pattern")]
    fn test_s3_with_nonexistent_pattern_uri() {
        object_store_cache_clear();

        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let s3_uri_pattern = format!("s3://{test_bucket_name}/notexists/**");

        Spi::run("create table test_table(a int);").unwrap();

        let copy_from_command =
            format!("COPY test_table FROM '{s3_uri_pattern}' WITH (format parquet);");
        Spi::run(&copy_from_command).unwrap();
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
    #[should_panic(expected = "permission denied to read")]
    fn test_s3_no_read_access() {
        object_store_cache_clear();

        // the write role alone: writing a uri works, reading the same uri does not
        Spi::run("CREATE USER regular_user;").unwrap();
        Spi::run("GRANT object_store_write TO regular_user;").unwrap();
        Spi::run("GRANT ALL ON SCHEMA public TO regular_user;").unwrap();
        Spi::run("SET SESSION AUTHORIZATION regular_user;").unwrap();

        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let s3_uri = format!("s3://{test_bucket_name}/pg_parquet_test.parquet");

        let test_table = TestTable::<i32>::new("int4".into()).with_uri(s3_uri.clone());

        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");

        let copy_to_command = format!(
            "COPY (SELECT a FROM generate_series(1,10) a) TO '{s3_uri}' WITH (format parquet);"
        );
        Spi::run(&copy_to_command).unwrap();

        let copy_from_command =
            format!("COPY test_expected FROM '{s3_uri}' WITH (format parquet);");
        Spi::run(&copy_from_command).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "permission denied to write")]
    fn test_s3_no_write_access() {
        object_store_cache_clear();

        // the read role alone, which is also what the udfs of this extension need
        Spi::run("CREATE USER regular_user;").unwrap();
        Spi::run("GRANT object_store_read TO regular_user;").unwrap();
        Spi::run("GRANT ALL ON SCHEMA public TO regular_user;").unwrap();
        Spi::run("SET SESSION AUTHORIZATION regular_user;").unwrap();

        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let s3_uri = format!("s3://{test_bucket_name}/pg_parquet_test.parquet");

        let metadata_query = format!("SELECT parquet.metadata('{s3_uri}');");
        Spi::run(&metadata_query).unwrap();

        let test_table = TestTable::<i32>::new("int4".into()).with_uri(s3_uri.clone());

        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");

        let copy_from_command =
            format!("COPY test_expected FROM '{s3_uri}' WITH (format parquet);");
        Spi::run(&copy_from_command).unwrap();

        let copy_to_command = format!(
            "COPY (SELECT a FROM generate_series(1,10) a) TO '{s3_uri}' WITH (format parquet);"
        );
        Spi::run(&copy_to_command).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "404 Not Found")]
    fn test_s3_write_wrong_bucket() {
        object_store_cache_clear();

        let s3_uri = "s3://randombucketwhichdoesnotexist/pg_parquet_test.parquet";

        let copy_to_command = format!(
            "COPY (SELECT i FROM generate_series(1,10) i) TO '{s3_uri}' WITH (format parquet);"
        );
        Spi::run(&copy_to_command).unwrap();
    }

    #[pg_test]
    // a read starts by asking for the object's size, and a bucket that is not there answers the
    // same 404 a missing key does, so what the user hears is that the file is missing
    #[should_panic(expected = "there is no file at s3://randombucketwhichdoesnotexist")]
    fn test_s3_read_wrong_bucket() {
        object_store_cache_clear();

        let s3_uri = "s3://randombucketwhichdoesnotexist/pg_parquet_test.parquet";

        Spi::run("CREATE TABLE test_table (a int);").unwrap();

        let copy_from_command = format!("COPY test_table FROM '{s3_uri}' WITH (format parquet);");
        Spi::run(&copy_from_command).unwrap();
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

        // a profile that assumes a role, so the credentials come from STS and expire
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

        // a temporary credential is cached with the moment it stops working
        assert_eq!(
            object_store_cache_items(),
            vec![("s3".to_string(), test_bucket_name.clone(), true)]
        );

        object_store_expire_item(&test_bucket_name);

        // the expired entry is not served: this run resolves a new token
        let test_table = TestTable::<i32>::new("int4".into()).with_uri(s3_uri);
        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();

        std::fs::remove_file(aws_config_file_path).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "column count mismatch")]
    fn test_s3_pattern_schema_mismatch() {
        object_store_cache_clear();

        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let copy_to_schema1 = format!(
            "copy (select 1 as a) to 's3://{test_bucket_name}/dummy1.parquet' \
             with (format parquet);"
        );
        Spi::run(&copy_to_schema1).unwrap();

        let copy_to_schema2 = format!(
            "copy (select 1 as a, 'asd' as b) to 's3://{test_bucket_name}/dummy2.parquet' \
             with (format parquet);"
        );
        Spi::run(&copy_to_schema2).unwrap();

        Spi::run("create table test_table(id int);").unwrap();

        let copy_from_command = format!(
            "COPY test_table FROM 's3://{test_bucket_name}/dummy*.parquet' \
             WITH (format parquet);"
        );
        Spi::run(&copy_from_command).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "type mismatch")]
    fn test_s3_pattern_column_type_mismatch() {
        object_store_cache_clear();

        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let copy_to_schema1 = format!(
            "copy (select '{{}}'::jsonb as a) to 's3://{test_bucket_name}/dummy2.parquet' \
             with (format parquet);"
        );
        Spi::run(&copy_to_schema1).unwrap();

        let copy_to_schema2 = format!(
            "copy (select 1 as a) to 's3://{test_bucket_name}/dummy1.parquet' \
             with (format parquet);"
        );
        Spi::run(&copy_to_schema2).unwrap();

        Spi::run("create table test_table(a jsonb);").unwrap();

        let copy_from_command = format!(
            "COPY test_table FROM 's3://{test_bucket_name}/dummy*.parquet' \
             WITH (format parquet);"
        );
        Spi::run(&copy_from_command).unwrap();
    }

    #[pg_test]
    fn test_s3_create_table_from_file() {
        object_store_cache_clear();

        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let s3_uri = format!("s3://{test_bucket_name}/pg_parquet_test.parquet");

        copy_to_helper(&s3_uri);

        let create_table = format!("CREATE TABLE test_table () WITH (load_from = '{s3_uri}')");
        Spi::run(&create_table).unwrap();
    }

    #[pg_test]
    #[cfg(not(rhel8))]
    fn test_azure_blob_from_env() {
        object_store_cache_clear();

        // unset the connection string to make sure the account name and key are used
        std::env::remove_var("AZURE_STORAGE_CONNECTION_STRING");

        let test_container_name: String = std::env::var("AZURE_TEST_CONTAINER_NAME")
            .expect("AZURE_TEST_CONTAINER_NAME not found");

        let test_account_name: String =
            std::env::var("AZURE_STORAGE_ACCOUNT").expect("AZURE_STORAGE_ACCOUNT not found");

        let azure_blob_uris = [
            format!("az://{test_container_name}/pg_parquet_test.parquet"),
            format!("azure://{test_container_name}/pg_parquet_test.parquet"),
            format!(
                "https://{test_account_name}.blob.core.windows.net/\
                 {test_container_name}/pg_parquet_test.parquet"
            ),
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

        std::env::remove_var("AZURE_STORAGE_CONNECTION_STRING");

        let test_container_name: String = std::env::var("AZURE_TEST_CONTAINER_NAME")
            .expect("AZURE_TEST_CONTAINER_NAME not found");

        let azure_blob_uri = format!(
            "az://{test_container_name}/test_azure_blob_from_env_glob_pattern/some/\
             pg_parquet_test.parquet"
        );
        let azure_blob_uri_pattern = format!(
            "az://{test_container_name}/test_azure_blob_from_env_glob_pattern/**/*.parquet"
        );

        let mut copy_options = HashMap::new();
        copy_options.insert(
            "format".to_string(),
            CopyOptionValue::StringOption("parquet".to_string()),
        );
        copy_options.insert(
            "file_size_bytes".to_string(),
            CopyOptionValue::StringOption("1MB".to_string()),
        );

        let test_table = TestTable::<i32>::new("int4".into())
            .with_uri(azure_blob_uri)
            .with_uri_pattern(azure_blob_uri_pattern)
            .with_order_by_col("a".into())
            .with_copy_to_options(copy_options);

        test_table
            .insert("INSERT INTO test_expected (a) select i from generate_series(1, 1000000) i;");
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

        Spi::run("create table test_table(a int);").unwrap();

        let copy_from_command =
            format!("COPY test_table FROM '{azure_uri}' WITH (format parquet);");
        Spi::run(&copy_from_command).unwrap();

        let result = Spi::get_one::<i64>("select count(*) from test_table;")
            .unwrap()
            .unwrap();
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

        Spi::run("create table test_table(a int);").unwrap();

        let copy_from_command =
            format!("COPY test_table FROM '{azure_uri_pattern}' WITH (format parquet);");
        Spi::run(&copy_from_command).unwrap();
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

        std::fs::remove_file(azure_config_file_path).unwrap();
    }

    #[pg_test]
    #[cfg(not(rhel8))]
    fn test_azure_from_env_via_connection_string() {
        object_store_cache_clear();

        // unset the account and key to make sure the connection string is used
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

        std::fs::remove_file(azure_config_file_path).unwrap();
    }

    #[pg_test]
    #[cfg(not(rhel8))]
    #[should_panic(expected = "Account must be specified")]
    fn test_azure_no_storage_account() {
        object_store_cache_clear();

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
    #[cfg(not(rhel8))]
    #[should_panic(expected = "403 Forbidden")]
    fn test_azure_wrong_storage_key() {
        object_store_cache_clear();

        std::env::remove_var("AZURE_STORAGE_CONNECTION_STRING");

        let wrong_account_key = String::from(
            "FFy8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/\
             K1SZFPTOtr/KBHBeksoGMGw==",
        );
        std::env::set_var("AZURE_STORAGE_KEY", wrong_account_key);

        let test_container_name: String = std::env::var("AZURE_TEST_CONTAINER_NAME")
            .expect("AZURE_TEST_CONTAINER_NAME not found");

        let test_account_name: String =
            std::env::var("AZURE_STORAGE_ACCOUNT").expect("AZURE_STORAGE_ACCOUNT not found");

        let azure_blob_uri = format!(
            "https://{test_account_name}.blob.core.windows.net/\
             {test_container_name}/pg_parquet_test.parquet"
        );

        let test_table = TestTable::<i32>::new("int4".into()).with_uri(azure_blob_uri);

        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();
    }

    #[pg_test]
    #[cfg(not(rhel8))]
    #[should_panic(expected = "404 Not Found")]
    fn test_azure_write_wrong_container() {
        object_store_cache_clear();

        let test_account_name: String =
            std::env::var("AZURE_STORAGE_ACCOUNT").expect("AZURE_STORAGE_ACCOUNT not found");

        let azure_blob_uri = format!(
            "https://{test_account_name}.blob.core.windows.net/\
             nonexistentcontainer/pg_parquet_test.parquet"
        );

        let copy_to_command = format!(
            "COPY (SELECT i FROM generate_series(1,10) i) TO '{azure_blob_uri}' \
             WITH (format parquet);"
        );
        Spi::run(&copy_to_command).unwrap();
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

        // remove the account key and the connection string to make sure the sas token is used
        std::env::remove_var("AZURE_STORAGE_KEY");
        std::env::remove_var("AZURE_STORAGE_CONNECTION_STRING");
        std::env::set_var("AZURE_STORAGE_SAS_TOKEN", read_write_sas_token);

        let azure_blob_uri = format!(
            "https://{test_account_name}.blob.core.windows.net/\
             {test_container_name}/pg_parquet_test.parquet"
        );

        let copy_to_command = format!(
            "COPY (SELECT i FROM generate_series(1,10) i) TO '{azure_blob_uri}' \
             WITH (format parquet);"
        );
        Spi::run(&copy_to_command).unwrap();
    }

    #[pg_test]
    #[cfg(not(rhel8))]
    #[should_panic(expected = "403 Forbidden")]
    fn test_azure_read_only_sas() {
        object_store_cache_clear();

        let test_container_name: String = std::env::var("AZURE_TEST_CONTAINER_NAME")
            .expect("AZURE_TEST_CONTAINER_NAME not found");

        let test_account_name: String =
            std::env::var("AZURE_STORAGE_ACCOUNT").expect("AZURE_STORAGE_ACCOUNT not found");

        let read_only_sas_token: String =
            std::env::var("AZURE_TEST_READ_ONLY_SAS").expect("AZURE_TEST_READ_ONLY_SAS not found");

        std::env::remove_var("AZURE_STORAGE_KEY");
        std::env::remove_var("AZURE_STORAGE_CONNECTION_STRING");
        std::env::set_var("AZURE_STORAGE_SAS_TOKEN", read_only_sas_token);

        let azure_blob_uri = format!(
            "https://{test_account_name}.blob.core.windows.net/\
             {test_container_name}/pg_parquet_test.parquet"
        );

        let copy_to_command = format!(
            "COPY (SELECT i FROM generate_series(1,10) i) TO '{azure_blob_uri}' \
             WITH (format parquet);"
        );
        Spi::run(&copy_to_command).unwrap();
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
            "format".to_string(),
            CopyOptionValue::StringOption("parquet".to_string()),
        );
        copy_options.insert(
            "file_size_bytes".to_string(),
            CopyOptionValue::StringOption("1MB".to_string()),
        );

        let test_table = TestTable::<i32>::new("int4".into())
            .with_uri(gcs_uri)
            .with_uri_pattern(gcs_uri_pattern)
            .with_order_by_col("a".into())
            .with_copy_to_options(copy_options);

        test_table
            .insert("INSERT INTO test_expected (a) select i from generate_series(1, 1000000) i;");
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

        Spi::run("create table test_table(a int);").unwrap();

        let copy_from_command = format!("COPY test_table FROM '{gcs_uri}' WITH (format parquet);");
        Spi::run(&copy_from_command).unwrap();

        let result = Spi::get_one::<i64>("select count(*) from test_table;")
            .unwrap()
            .unwrap();
        assert_eq!(result, 10);
    }

    #[pg_test]
    #[should_panic(expected = "no files found that match the pattern")]
    fn test_gcs_with_nonexistent_pattern_uri() {
        object_store_cache_clear();

        let test_bucket_name: String =
            std::env::var("GOOGLE_TEST_BUCKET").expect("GOOGLE_TEST_BUCKET not found");

        let gcs_uri_pattern = format!("gs://{test_bucket_name}/notexists/**");

        Spi::run("create table test_table(a int);").unwrap();

        let copy_from_command =
            format!("COPY test_table FROM '{gcs_uri_pattern}' WITH (format parquet);");
        Spi::run(&copy_from_command).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "404 Not Found")]
    fn test_gcs_write_wrong_bucket() {
        object_store_cache_clear();

        let gcs_uri = "gs://randombucketwhichdoesnotexist/pg_parquet_test.parquet";

        let copy_to_command = format!(
            "COPY (SELECT i FROM generate_series(1,10) i) TO '{gcs_uri}' WITH (format parquet);"
        );
        Spi::run(&copy_to_command).unwrap();
    }

    #[pg_test]
    // as on s3: a missing bucket answers the read the way a missing key does
    #[should_panic(expected = "there is no file at gs://randombucketwhichdoesnotexist")]
    fn test_gcs_read_wrong_bucket() {
        object_store_cache_clear();

        let gcs_uri = "gs://randombucketwhichdoesnotexist/pg_parquet_test.parquet";

        Spi::run("CREATE TABLE test_table (a int);").unwrap();

        let copy_from_command = format!("COPY test_table FROM '{gcs_uri}' WITH (format parquet);");
        Spi::run(&copy_from_command).unwrap();
    }

    #[pg_test]
    fn test_http_uri() {
        object_store_cache_clear();

        let http_endpoint: String =
            std::env::var("HTTP_ENDPOINT").expect("HTTP_ENDPOINT not found");

        // the second one has no extension to infer a format from, which the explicit format
        // option of TestTable covers
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

        Spi::run("create table test_table(a int);").unwrap();

        let copy_from_command = format!("COPY test_table FROM '{http_uri}' WITH (format parquet);");
        Spi::run(&copy_from_command).unwrap();

        let result = Spi::get_one::<i64>("select count(*) from test_table;")
            .unwrap()
            .unwrap();
        assert_eq!(result, 10);
    }

    #[pg_test]
    #[should_panic(expected = "no files found that match the pattern")]
    fn test_http_with_nonexistent_pattern_uri() {
        object_store_cache_clear();

        let http_endpoint: String =
            std::env::var("HTTP_ENDPOINT").expect("HTTP_ENDPOINT not found");

        let http_uri_pattern = format!("{http_endpoint}/notexists/**");

        Spi::run("create table test_table(a int);").unwrap();

        let copy_from_command =
            format!("COPY test_table FROM '{http_uri_pattern}' WITH (format parquet);");
        Spi::run(&copy_from_command).unwrap();
    }

    /// One cached client per bucket, across every scheme a parquet uri can name.
    ///
    /// `pg_object_store` proves the keying on its own, with no cloud. This is the same claim
    /// made the way a user reaches it: through `COPY` and the udfs of this extension.
    #[pg_test]
    #[cfg(not(rhel8))]
    fn test_object_store_cache() {
        object_store_cache_clear();

        let copy_to_uri = |uri: &str| {
            let test_table = TestTable::<i32>::new("int4".into()).with_uri(uri.to_string());
            test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
            test_table.copy_to_parquet("test_expected");
        };

        // two uris in the same bucket, then one in another: the bucket is the key, not the path
        copy_to_uri("s3://testbucket/test1.parquet");
        copy_to_uri("s3://testbucket/test2.parquet");
        copy_to_uri("s3://testbucket2/test1.parquet");

        assert_eq!(
            object_store_cache_items(),
            vec![
                ("s3".to_string(), "testbucket".to_string(), false),
                ("s3".to_string(), "testbucket2".to_string(), false),
            ]
        );

        copy_to_uri("az://testcontainer/test1.parquet");
        copy_to_uri("az://testcontainer/test2.parquet");
        copy_to_uri("az://testcontainer2/test1.parquet");

        copy_to_uri("gs://testbucket/test1.parquet");
        copy_to_uri("gs://testbucket/test2.parquet");
        copy_to_uri("gs://testbucket2/test1.parquet");

        // the same bucket name in two schemes is two clients
        assert_eq!(
            object_store_cache_items(),
            vec![
                ("azure".to_string(), "testcontainer".to_string(), false),
                ("azure".to_string(), "testcontainer2".to_string(), false),
                ("gcs".to_string(), "testbucket".to_string(), false),
                ("gcs".to_string(), "testbucket2".to_string(), false),
                ("s3".to_string(), "testbucket".to_string(), false),
                ("s3".to_string(), "testbucket2".to_string(), false),
            ]
        );

        // an http store has no buckets, so the base uri is what a client belongs to
        let http_endpoint: String =
            std::env::var("HTTP_ENDPOINT").expect("HTTP_ENDPOINT not found");

        object_store_cache_clear();

        copy_to_uri(&format!("{http_endpoint}/cache_one.parquet"));

        let schema_query =
            format!("SELECT * FROM parquet.schema('{http_endpoint}/cache_one.parquet')");
        Spi::run(&schema_query).unwrap();

        assert_eq!(
            object_store_cache_items(),
            vec![("http".to_string(), http_endpoint, false)]
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

    #[pg_test]
    #[should_panic(expected = "/tmp cannot be written")]
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
}
