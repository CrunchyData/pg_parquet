#[pgrx::pg_schema]
mod tests {
    use std::io::Write;

    use pgrx::{pg_test, Spi};

    use crate::pgrx_tests::common::TestTable;

    #[pg_test]
    fn test_s3_object_store_from_env() {
        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let s3_uri = format!("s3://{}/pg_parquet_test.parquet", test_bucket_name);

        let test_table = TestTable::<i32>::new("int4".into()).with_uri(s3_uri);

        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();
    }

    #[pg_test]
    fn test_s3_object_store_from_config_file() {
        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        // remove these to make sure the config file is used
        let access_key_id = std::env::var("AWS_ACCESS_KEY_ID").unwrap();
        std::env::remove_var("AWS_ACCESS_KEY_ID");
        let secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY").unwrap();
        std::env::remove_var("AWS_SECRET_ACCESS_KEY");
        let region = std::env::var("AWS_REGION").unwrap();
        std::env::remove_var("AWS_REGION");

        // create a config file
        let aws_config_file_content = format!(
            "[profile pg_parquet_test]\nregion = {}\naws_access_key_id = {}\naws_secret_access_key = {}\n",
            region, access_key_id, secret_access_key
        );
        std::env::set_var("AWS_PROFILE", "pg_parquet_test");

        let aws_config_file = "/tmp/pg_parquet_aws_config";
        std::env::set_var("AWS_CONFIG_FILE", aws_config_file);

        let mut aws_config_file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(aws_config_file)
            .unwrap();

        aws_config_file
            .write_all(aws_config_file_content.as_bytes())
            .unwrap();

        let s3_uri = format!("s3://{}/pg_parquet_test.parquet", test_bucket_name);

        let test_table = TestTable::<i32>::new("int4".into()).with_uri(s3_uri);

        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();
    }

    #[pg_test]
    fn test_s3_object_store_from_config_file_via_guc() {
        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        // remove these to make sure the config file is used
        let access_key_id = std::env::var("AWS_ACCESS_KEY_ID").unwrap();
        std::env::remove_var("AWS_ACCESS_KEY_ID");
        let secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY").unwrap();
        std::env::remove_var("AWS_SECRET_ACCESS_KEY");
        let region = std::env::var("AWS_REGION").unwrap();
        std::env::remove_var("AWS_REGION");

        let profile_name = "pg_parquet_test";

        // create a config file
        let aws_config_file_content = format!("[profile {}]\nregion = {}\n", profile_name, region,);

        let aws_config_file_name = "/tmp/pg_parquet_aws_config";

        let mut aws_config_file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(aws_config_file_name)
            .unwrap();

        aws_config_file
            .write_all(aws_config_file_content.as_bytes())
            .unwrap();

        // create a shared credentials file
        let aws_shared_credentials_file_content = format!(
            "[{}]\naws_access_key_id = {}\naws_secret_access_key = {}\n",
            profile_name, access_key_id, secret_access_key
        );

        let aws_shared_credentials_file_name = "/tmp/pg_parquet_aws_shared_credentials";

        let mut aws_shared_credentials_file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(aws_shared_credentials_file_name)
            .unwrap();

        aws_shared_credentials_file
            .write_all(aws_shared_credentials_file_content.as_bytes())
            .unwrap();

        Spi::run(
            format!(
                "SET pg_parquet.aws_config_file TO '{}';",
                aws_config_file_name
            )
            .as_str(),
        )
        .unwrap();

        Spi::run(
            format!(
                "SET pg_parquet.aws_shared_credentials_file TO '{}';",
                aws_shared_credentials_file_name
            )
            .as_str(),
        )
        .unwrap();

        Spi::run(format!("SET pg_parquet.aws_profile TO '{}';", profile_name).as_str()).unwrap();

        let s3_uri = format!("s3://{}/pg_parquet_test.parquet", test_bucket_name);

        let test_table = TestTable::<i32>::new("int4".into()).with_uri(s3_uri);

        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();
    }

    #[pg_test]
    #[should_panic(expected = "permission denied to COPY from a remote uri")]
    fn test_s3_no_read_access() {
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

        let s3_uri = format!("s3://{}/pg_parquet_test.parquet", test_bucket_name);

        let test_table = TestTable::<i32>::new("int4".into()).with_uri(s3_uri.clone());

        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");

        // can write to s3
        let copy_to_command = format!(
            "COPY (SELECT a FROM generate_series(1,10) a) TO '{}';",
            s3_uri
        );
        Spi::run(copy_to_command.as_str()).unwrap();

        // cannot read from s3
        let copy_from_command = format!("COPY test_expected FROM '{}';", s3_uri);
        Spi::run(copy_from_command.as_str()).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "permission denied to COPY to a remote uri")]
    fn test_s3_no_write_access() {
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

        let s3_uri = format!("s3://{}/pg_parquet_test.parquet", test_bucket_name);

        // can call metadata udf (requires read access)
        let metadata_query = format!("SELECT parquet.metadata('{}');", s3_uri.clone());
        Spi::run(&metadata_query).unwrap();

        let test_table = TestTable::<i32>::new("int4".into()).with_uri(s3_uri.clone());

        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");

        // can read from s3
        let copy_from_command = format!("COPY test_expected FROM '{}';", s3_uri);
        Spi::run(copy_from_command.as_str()).unwrap();

        // cannot write to s3
        let copy_to_command = format!(
            "COPY (SELECT a FROM generate_series(1,10) a) TO '{}';",
            s3_uri
        );
        Spi::run(copy_to_command.as_str()).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "404 Not Found")]
    fn test_s3_object_store_write_invalid_uri() {
        let s3_uri = "s3://randombucketwhichdoesnotexist/pg_parquet_test.parquet";

        let copy_to_command = format!(
            "COPY (SELECT i FROM generate_series(1,10) i) TO '{}';",
            s3_uri
        );
        Spi::run(copy_to_command.as_str()).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "404 Not Found")]
    fn test_s3_object_store_read_invalid_uri() {
        let s3_uri = "s3://randombucketwhichdoesnotexist/pg_parquet_test.parquet";

        let create_table_command = "CREATE TABLE test_table (a int);";
        Spi::run(create_table_command).unwrap();

        let copy_from_command = format!("COPY test_table FROM '{}';", s3_uri);
        Spi::run(copy_from_command.as_str()).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "unsupported uri gs://testbucket")]
    fn test_unsupported_uri() {
        let test_table =
            TestTable::<i32>::new("int4".into()).with_uri("gs://testbucket".to_string());
        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();
    }
}
