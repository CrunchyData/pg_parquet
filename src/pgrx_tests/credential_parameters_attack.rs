// Tests that try to reach a store with the credentials of the server while the session put
// its own in the parameters, and to reach one with the parameters while the server allows
// neither the session nor the parameters.
#[pgrx::pg_schema]
mod tests {
    use pgrx::{pg_test, Spi};

    use crate::pgrx_tests::common::TestTable;

    fn test_bucket() -> String {
        std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found")
    }

    // the_parameters_are_the_only_credentials_that_work leaves the environment with
    // credentials that do not, so a request made with the environment's is a request that
    // fails: a fallback that reaches the store anyway would otherwise read as a success
    fn the_parameters_are_the_only_credentials_that_work() {
        let access_key_id = std::env::var("AWS_ACCESS_KEY_ID").unwrap();
        let secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY").unwrap();

        std::env::set_var("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "not_the_secret_of_the_environment");

        Spi::run(&format!(
            "SET pg_parquet.allow_credentials_in_parameters TO on;
             SET pg_parquet.aws_access_key_id TO '{access_key_id}';
             SET pg_parquet.aws_secret_access_key TO '{secret_access_key}';"
        ))
        .unwrap();
    }

    // the workers of a parallel copy open stores of their own, so a worker that does not
    // carry the parameters of the session would make its requests as the server: the rows of
    // one copy would be written by two identities, or by none at all
    #[pg_test]
    fn test_a_parallel_copy_to_carries_the_parameters_into_its_workers() {
        let test_bucket_name = test_bucket();

        Spi::run("CREATE TABLE parameters_parallel AS SELECT i FROM generate_series(1, 20000) i")
            .unwrap();

        the_parameters_are_the_only_credentials_that_work();
        Spi::run("SET pg_parquet.max_parallel_workers TO 4").unwrap();

        let uri = format!("s3://{test_bucket_name}/pg_parquet_parameters_parallel.parquet");
        Spi::run(&format!("COPY parameters_parallel TO '{uri}'")).unwrap();

        // every row is there, so no worker was refused and none wrote somewhere else
        Spi::run("CREATE TABLE parameters_parallel_read (i int)").unwrap();
        Spi::run(&format!("COPY parameters_parallel_read FROM '{uri}'")).unwrap();

        let rows = Spi::get_one::<i64>("SELECT count(*) FROM parameters_parallel_read")
            .unwrap()
            .unwrap();

        assert_eq!(rows, 20000, "a worker of the parallel copy wrote no rows");
    }

    // and the workers of a parallel read open the files of the copy the same way
    #[pg_test]
    fn test_a_parallel_copy_from_carries_the_parameters_into_its_workers() {
        let test_bucket_name = test_bucket();
        let uri = format!("s3://{test_bucket_name}/pg_parquet_parameters_parallel_read.parquet");

        // written while the environment still works, with row groups for the workers to split
        Spi::run(&format!(
            "COPY (SELECT i FROM generate_series(1, 20000) i) TO '{uri}'
             WITH (row_group_size 1000)"
        ))
        .unwrap();

        the_parameters_are_the_only_credentials_that_work();
        Spi::run("SET pg_parquet.max_parallel_workers TO 4").unwrap();

        Spi::run("CREATE TABLE parameters_parallel_from (i int)").unwrap();
        Spi::run(&format!("COPY parameters_parallel_from FROM '{uri}'")).unwrap();

        let rows = Spi::get_one::<i64>("SELECT count(*) FROM parameters_parallel_from")
            .unwrap()
            .unwrap();

        assert_eq!(rows, 20000, "a worker of the parallel read read no rows");
    }

    // the parameters say which credentials a copy is made with and not whether the role may
    // make one at all, so a role without the role for writing an object store is refused
    // however good the credentials it put in the parameters are
    #[pg_test]
    #[should_panic(expected = "permission denied to COPY to a remote uri")]
    fn test_the_parameters_are_not_a_way_past_the_object_store_role() {
        let test_bucket_name = test_bucket();

        Spi::run(
            "DROP ROLE IF EXISTS parameters_unprivileged;
             CREATE ROLE parameters_unprivileged;",
        )
        .unwrap();

        the_parameters_are_the_only_credentials_that_work();

        // the role is granted neither parquet_object_store_read nor _write
        Spi::run("SET ROLE parameters_unprivileged").unwrap();

        Spi::run(&format!(
            "COPY (SELECT 1 as a) TO \
             's3://{test_bucket_name}/pg_parquet_parameters_test.parquet'"
        ))
        .unwrap();
    }

    // an endpoint is not a credential, so it is read with the credentials of the session and
    // never with the ones of the server: a session that could point the server's credentials
    // at an endpoint of its own would have the server sign its requests for it
    #[pg_test]
    fn test_an_endpoint_alone_does_not_redirect_the_servers_credentials() {
        let test_bucket_name = test_bucket();

        // the environment still holds the credentials that work, and the session names an
        // endpoint of its own while naming no credentials at all
        Spi::run(
            "SET pg_parquet.allow_credentials_in_parameters TO on;
             SET pg_parquet.aws_endpoint_url TO 'http://127.0.0.1:1';
             SET pg_parquet.aws_region TO 'nowhere';",
        )
        .unwrap();

        // the copy reaches the store of the server rather than the endpoint of the session,
        // which is what says the server's credentials were not sent there
        let uri = format!("s3://{test_bucket_name}/pg_parquet_endpoint_test.parquet");
        let test_table = TestTable::<i32>::new("int4".into()).with_uri(uri);
        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();
    }

    // the same for azure, whose endpoint is a parameter of its own as well
    #[pg_test]
    #[cfg(not(rhel8))]
    fn test_an_azure_endpoint_alone_does_not_redirect_the_servers_credentials() {
        let test_container_name = std::env::var("AZURE_TEST_CONTAINER_NAME")
            .expect("AZURE_TEST_CONTAINER_NAME not found");

        Spi::run(
            "SET pg_parquet.allow_credentials_in_parameters TO on;
             SET pg_parquet.azure_storage_endpoint TO 'http://127.0.0.1:1';",
        )
        .unwrap();

        let uri = format!("az://{test_container_name}/pg_parquet_endpoint_test.parquet");
        let test_table = TestTable::<i32>::new("int4".into()).with_uri(uri);
        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();
    }
}
