#[pgrx::pg_schema]
mod tests {
    use std::io::Write;

    use pgrx::{pg_sys::Timestamp, pg_test, Spi};

    use crate::pgrx_tests::common::TestTable;

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
            format!("https://{test_account_name}.blob.core.windows.net/{test_container_name}"),
        ];

        for azure_blob_uri in azure_blob_uris {
            let test_table = TestTable::<i32>::new("int4".into()).with_uri(azure_blob_uri);

            test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
            test_table.assert_expected_and_result_rows();
        }
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
            format!("https://{test_account_name}.blob.core.windows.net/{test_container_name}");

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
            format!("https://{test_account_name}.blob.core.windows.net/nonexistentcontainer");

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
            format!("https://{test_account_name}.blob.core.windows.net/{test_container_name}");

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
            format!("https://{test_account_name}.blob.core.windows.net/{test_container_name}");

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
    fn test_s3_from_guc_credentials() {
        object_store_cache_clear();

        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        // Get credentials from environment variables
        let access_key_id =
            std::env::var("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID not found");
        let secret_access_key =
            std::env::var("AWS_SECRET_ACCESS_KEY").expect("AWS_SECRET_ACCESS_KEY not found");
        let region = std::env::var("AWS_REGION").expect("AWS_REGION not found");
        let endpoint_url = std::env::var("AWS_ENDPOINT_URL").expect("AWS_ENDPOINT_URL not found");

        // Remove environment variables to ensure GUCs are used
        std::env::remove_var("AWS_ACCESS_KEY_ID");
        std::env::remove_var("AWS_SECRET_ACCESS_KEY");
        std::env::remove_var("AWS_REGION");
        std::env::remove_var("AWS_ENDPOINT_URL");

        // Set credentials via GUCs
        Spi::run(&format!(
            "SET pg_parquet.aws_access_key_id = '{access_key_id}';"
        ))
        .unwrap();
        Spi::run(&format!(
            "SET pg_parquet.aws_secret_access_key = '{secret_access_key}';"
        ))
        .unwrap();
        Spi::run(&format!("SET pg_parquet.aws_region = '{region}';")).unwrap();
        Spi::run(&format!(
            "SET pg_parquet.aws_endpoint_url = '{endpoint_url}';"
        ))
        .unwrap();

        let s3_uri = format!("s3://{test_bucket_name}/pg_parquet_test.parquet");

        let test_table = TestTable::<i32>::new("int4".into()).with_uri(s3_uri);

        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();

        // Restore environment variables
        std::env::set_var("AWS_ACCESS_KEY_ID", access_key_id);
        std::env::set_var("AWS_SECRET_ACCESS_KEY", secret_access_key);
        std::env::set_var("AWS_REGION", region);
        std::env::set_var("AWS_ENDPOINT_URL", endpoint_url);
    }

    #[pg_test]
    fn test_s3_guc_priority_over_env() {
        object_store_cache_clear();

        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        // Get real credentials
        let real_access_key_id =
            std::env::var("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID not found");
        let real_secret_access_key =
            std::env::var("AWS_SECRET_ACCESS_KEY").expect("AWS_SECRET_ACCESS_KEY not found");
        let real_region = std::env::var("AWS_REGION").expect("AWS_REGION not found");
        let real_endpoint_url =
            std::env::var("AWS_ENDPOINT_URL").expect("AWS_ENDPOINT_URL not found");

        // Set wrong credentials in environment variables
        std::env::set_var("AWS_ACCESS_KEY_ID", "wrong_access_key");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "wrong_secret_key");
        std::env::set_var("AWS_REGION", "wrong-region");
        std::env::set_var("AWS_ENDPOINT_URL", "http://wrong-endpoint");

        // Set correct credentials via GUCs (should take priority)
        Spi::run(&format!(
            "SET pg_parquet.aws_access_key_id = '{real_access_key_id}';"
        ))
        .unwrap();
        Spi::run(&format!(
            "SET pg_parquet.aws_secret_access_key = '{real_secret_access_key}';"
        ))
        .unwrap();
        Spi::run(&format!("SET pg_parquet.aws_region = '{real_region}';")).unwrap();
        Spi::run(&format!(
            "SET pg_parquet.aws_endpoint_url = '{real_endpoint_url}';"
        ))
        .unwrap();

        let s3_uri = format!("s3://{test_bucket_name}/pg_parquet_test.parquet");

        let test_table = TestTable::<i32>::new("int4".into()).with_uri(s3_uri);

        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();

        // Restore original environment variables
        std::env::set_var("AWS_ACCESS_KEY_ID", real_access_key_id);
        std::env::set_var("AWS_SECRET_ACCESS_KEY", real_secret_access_key);
        std::env::set_var("AWS_REGION", real_region);
        std::env::set_var("AWS_ENDPOINT_URL", real_endpoint_url);
    }

    #[pg_test]
    #[should_panic(expected = "403 Forbidden")]
    fn test_s3_guc_wrong_credentials() {
        object_store_cache_clear();

        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        // Remove environment variables
        std::env::remove_var("AWS_ACCESS_KEY_ID");
        std::env::remove_var("AWS_SECRET_ACCESS_KEY");
        std::env::remove_var("AWS_REGION");
        std::env::remove_var("AWS_ENDPOINT_URL");

        // Set wrong credentials via GUCs
        Spi::run("SET pg_parquet.aws_access_key_id = 'wrong_access_key';").unwrap();
        Spi::run("SET pg_parquet.aws_secret_access_key = 'wrong_secret_key';").unwrap();
        Spi::run("SET pg_parquet.aws_region = 'us-east-1';").unwrap();
        Spi::run("SET pg_parquet.aws_endpoint_url = 'https://s3.amazonaws.com';").unwrap();

        let s3_uri = format!("s3://{test_bucket_name}/pg_parquet_test.parquet");

        let copy_to_command =
            format!("COPY (SELECT i FROM generate_series(1,10) i) TO '{s3_uri}';");
        Spi::run(copy_to_command.as_str()).unwrap();
    }

    #[pg_test]
    fn test_gcs_from_guc_service_account_key() {
        object_store_cache_clear();

        let test_bucket_name: String =
            std::env::var("GOOGLE_TEST_BUCKET").expect("GOOGLE_TEST_BUCKET not found");

        // Get service account key from environment variable
        let service_account_key = std::env::var("GOOGLE_SERVICE_ACCOUNT_KEY")
            .expect("GOOGLE_SERVICE_ACCOUNT_KEY not found");

        // Remove environment variable to ensure GUC is used
        std::env::remove_var("GOOGLE_SERVICE_ACCOUNT_KEY");

        // Set service account key via GUC
        Spi::run(&format!(
            "SET pg_parquet.google_service_account_key = '{service_account_key}';"
        ))
        .unwrap();

        let gcs_uri = format!("gs://{test_bucket_name}/pg_parquet_test.parquet");

        let test_table = TestTable::<i32>::new("int4".into()).with_uri(gcs_uri);

        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();

        // Restore environment variable
        std::env::set_var("GOOGLE_SERVICE_ACCOUNT_KEY", service_account_key);
    }

    #[pg_test]
    fn test_gcs_guc_priority_over_env() {
        object_store_cache_clear();

        let test_bucket_name: String =
            std::env::var("GOOGLE_TEST_BUCKET").expect("GOOGLE_TEST_BUCKET not found");

        // Get real service account key
        let real_service_account_key = std::env::var("GOOGLE_SERVICE_ACCOUNT_KEY")
            .expect("GOOGLE_SERVICE_ACCOUNT_KEY not found");

        // Set wrong service account key in environment variable
        std::env::set_var("GOOGLE_SERVICE_ACCOUNT_KEY", "wrong_service_account_key");

        // Set correct service account key via GUC (should take priority)
        Spi::run(&format!(
            "SET pg_parquet.google_service_account_key = '{real_service_account_key}';"
        ))
        .unwrap();

        let gcs_uri = format!("gs://{test_bucket_name}/pg_parquet_test.parquet");

        let test_table = TestTable::<i32>::new("int4".into()).with_uri(gcs_uri);

        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();

        // Restore original environment variable
        std::env::set_var("GOOGLE_SERVICE_ACCOUNT_KEY", real_service_account_key);
    }

    #[pg_test]
    #[should_panic(expected = "404 Not Found")]
    fn test_gcs_guc_wrong_credentials() {
        object_store_cache_clear();

        // Remove environment variables
        std::env::remove_var("GOOGLE_SERVICE_ACCOUNT_KEY");
        std::env::remove_var("GOOGLE_SERVICE_ACCOUNT_PATH");

        // Set wrong service account key via GUC
        Spi::run("SET pg_parquet.google_service_account_key = '{\"type\": \"service_account\", \"project_id\": \"wrong-project\"}';").unwrap();

        let gcs_uri = "gs://randombucketwhichdoesnotexist/pg_parquet_test.parquet";

        let copy_to_command =
            format!("COPY (SELECT i FROM generate_series(1,10) i) TO '{gcs_uri}';");
        Spi::run(copy_to_command.as_str()).unwrap();
    }

    #[pg_test]
    fn test_gcs_guc_debug() {
        // Test that GUCs are being read correctly
        let test_key = "{\"type\": \"service_account\", \"project_id\": \"test-project\"}";
        let test_path = "/tmp/test-service-account.json";

        // Set GUCs
        Spi::run(&format!(
            "SET pg_parquet.google_service_account_key = '{test_key}';"
        ))
        .unwrap();
        Spi::run(&format!(
            "SET pg_parquet.google_service_account_path = '{test_path}';"
        ))
        .unwrap();

        // Remove environment variables to ensure GUCs are used
        std::env::remove_var("GOOGLE_SERVICE_ACCOUNT_KEY");
        std::env::remove_var("GOOGLE_SERVICE_ACCOUNT_PATH");

        // Test that we can read the GUCs
        let result = Spi::get_one::<String>(
            "SELECT current_setting('pg_parquet.google_service_account_key');",
        )
        .unwrap();
        assert_eq!(result, Some(test_key.to_string()));

        let result = Spi::get_one::<String>(
            "SELECT current_setting('pg_parquet.google_service_account_path');",
        )
        .unwrap();
        assert_eq!(result, Some(test_path.to_string()));

        // Restore environment variables
        if let Ok(key) = std::env::var("GOOGLE_SERVICE_ACCOUNT_KEY") {
            std::env::set_var("GOOGLE_SERVICE_ACCOUNT_KEY", key);
        }
        if let Ok(path) = std::env::var("GOOGLE_SERVICE_ACCOUNT_PATH") {
            std::env::set_var("GOOGLE_SERVICE_ACCOUNT_PATH", path);
        }
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
}
