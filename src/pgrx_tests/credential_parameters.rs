#[pgrx::pg_schema]
mod tests {
    use pgrx::{pg_test, Spi};

    use crate::pgrx_tests::common::TestTable;

    const CREDENTIAL_PARAMETERS: &[&str] = &[
        "pg_parquet.aws_access_key_id",
        "pg_parquet.aws_secret_access_key",
        "pg_parquet.aws_session_token",
        "pg_parquet.azure_storage_key",
        "pg_parquet.azure_storage_sas_token",
        "pg_parquet.azure_storage_connection_string",
        "pg_parquet.gcs_service_account_key",
    ];

    fn allow_credentials_in_parameters() {
        Spi::run("SET pg_parquet.allow_credentials_in_parameters TO on").unwrap();
    }

    fn create_plain_role(role: &str) {
        Spi::run(&format!(
            "DROP ROLE IF EXISTS {role};
             CREATE ROLE {role};
             GRANT parquet_object_store_read, parquet_object_store_write TO {role};"
        ))
        .unwrap();
    }

    fn test_bucket() -> String {
        std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found")
    }

    // statement_error is the message and the detail a statement failed with. A postgres error
    // inside spi unwinds rather than answering with an Err, and aborts the transaction with
    // it, so the statement runs inside a plpgsql block that catches it: the block is a
    // subtransaction, which is what lets a table of cases run one after another. The detail is
    // where the check hook of a parameter says why it refused the credential, so SQLERRM alone
    // would not carry it.
    fn statement_error(statement: &str) -> String {
        Spi::run(&format!(
            "CREATE OR REPLACE FUNCTION try_statement() RETURNS text LANGUAGE plpgsql AS
             $body$
             DECLARE detail text;
             BEGIN
                 EXECUTE $stmt${statement}$stmt$;
                 RETURN 'no error';
             EXCEPTION WHEN others THEN
                 GET STACKED DIAGNOSTICS detail = PG_EXCEPTION_DETAIL;
                 RETURN SQLERRM || ' DETAIL: ' || coalesce(detail, '');
             END
             $body$;"
        ))
        .unwrap();

        Spi::get_one::<String>("SELECT try_statement()")
            .unwrap()
            .unwrap()
    }

    // the credentials of the session reach the store instead of the ones of the environment,
    // which is what the parameters are for
    #[pg_test]
    fn test_s3_credentials_in_parameters_are_used() {
        let test_bucket_name = test_bucket();

        let access_key_id = std::env::var("AWS_ACCESS_KEY_ID").unwrap();
        let secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY").unwrap();

        // the environment is left without credentials, so only the parameters can answer
        std::env::remove_var("AWS_ACCESS_KEY_ID");
        std::env::remove_var("AWS_SECRET_ACCESS_KEY");

        allow_credentials_in_parameters();
        Spi::run(&format!(
            "SET pg_parquet.aws_access_key_id TO '{access_key_id}';
             SET pg_parquet.aws_secret_access_key TO '{secret_access_key}';"
        ))
        .unwrap();

        let uri = format!("s3://{test_bucket_name}/pg_parquet_parameters_test.parquet");
        let test_table = TestTable::<i32>::new("int4".into()).with_uri(uri);
        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();
    }

    // and they are the ones that reach it, so credentials that do not work fail the copy
    // rather than falling back to the ones of the environment
    #[pg_test]
    #[should_panic(expected = "403 Forbidden")]
    fn test_s3_credentials_in_parameters_do_not_fall_back() {
        let test_bucket_name = test_bucket();

        allow_credentials_in_parameters();
        Spi::run(
            "SET pg_parquet.aws_access_key_id TO 'AKIAIOSFODNN7EXAMPLE';
             SET pg_parquet.aws_secret_access_key TO 'not_the_secret_of_the_environment';",
        )
        .unwrap();

        Spi::run(&format!(
            "COPY (SELECT 1 as a) TO \
             's3://{test_bucket_name}/pg_parquet_parameters_test.parquet'"
        ))
        .unwrap();
    }

    // the parameters are what the session asked for, so they are read ahead of the profile
    // it named rather than after it
    #[pg_test]
    fn test_credentials_in_parameters_override_the_profile() {
        let test_bucket_name = test_bucket();

        let access_key_id = std::env::var("AWS_ACCESS_KEY_ID").unwrap();
        let secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY").unwrap();

        create_plain_role("parameters_role");

        // the role is mapped to no profile at all, and names one anyway
        Spi::run(&format!(
            "SET pg_parquet.allow_credentials_in_parameters TO on;
             SET ROLE parameters_role;
             SET pg_parquet.profile_name TO 'a_profile_nobody_is_mapped_to';
             SET pg_parquet.aws_access_key_id TO '{access_key_id}';
             SET pg_parquet.aws_secret_access_key TO '{secret_access_key}';"
        ))
        .unwrap();

        // the profile would be refused, and the parameters are read instead of it
        Spi::run(&format!(
            "COPY (SELECT 1 as a) TO \
             's3://{test_bucket_name}/pg_parquet_parameters_test.parquet'"
        ))
        .unwrap();
    }

    // a store built from the parameters is not cached, so changing them changes the identity
    // the next copy is made with
    #[pg_test]
    fn test_a_store_of_the_parameters_is_not_cached() {
        let test_bucket_name = test_bucket();

        let access_key_id = std::env::var("AWS_ACCESS_KEY_ID").unwrap();
        let secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY").unwrap();

        Spi::run("SELECT parquet_test.object_store_cache_clear()").unwrap();

        allow_credentials_in_parameters();
        Spi::run(&format!(
            "SET pg_parquet.aws_access_key_id TO '{access_key_id}';
             SET pg_parquet.aws_secret_access_key TO '{secret_access_key}';"
        ))
        .unwrap();

        let uri = format!("s3://{test_bucket_name}/pg_parquet_parameters_test.parquet");
        Spi::run(&format!("COPY (SELECT 1 as a) TO '{uri}'")).unwrap();

        let cached =
            Spi::get_one::<i64>("SELECT count(*) FROM parquet_test.object_store_cache_items()")
                .unwrap()
                .unwrap();

        assert_eq!(
            cached, 0,
            "a store built from the parameters of the session was cached"
        );

        // and the next copy is made with whatever the parameters say then
        Spi::run("SET pg_parquet.aws_secret_access_key TO 'not_the_secret_any_more'").unwrap();

        let error = statement_error(&format!("COPY (SELECT 1 as a) TO '{uri}'"));

        assert!(
            error.contains("403") || error.contains("Forbidden"),
            "the copy after the credentials were replaced failed with: {error}"
        );
    }

    // ===================== the guardrails =====================

    // the parameters are shut while the server does not allow them, and a session is told so
    // rather than finding that what it set was ignored
    #[pg_test]
    fn test_a_credential_is_refused_while_the_server_does_not_allow_one() {
        for parameter in CREDENTIAL_PARAMETERS {
            let error = statement_error(&format!("SET {parameter} TO 'a_credential'"));

            assert!(
                error.contains("does not allow credentials in parameters"),
                "{parameter} was refused with: {error}"
            );
        }
    }

    // only a superuser opens them, since whether the credentials of the sessions of a server
    // may be written into its log is the server's decision and not a session's
    #[pg_test]
    #[should_panic(expected = "permission denied to set parameter")]
    fn test_only_a_superuser_allows_credentials_in_parameters() {
        create_plain_role("parameters_gate_role");

        Spi::run(
            "SET ROLE parameters_gate_role;
             SET pg_parquet.allow_credentials_in_parameters TO on;",
        )
        .unwrap();
    }

    // a setting of a role is kept in pg_db_role_setting, which every role of the cluster may
    // read and pg_dumpall writes out, so a credential is refused from ALTER ROLE
    #[pg_test]
    fn test_a_credential_is_refused_from_alter_role() {
        allow_credentials_in_parameters();
        create_plain_role("parameters_alter_role");

        for parameter in CREDENTIAL_PARAMETERS {
            let error = statement_error(&format!(
                "ALTER ROLE parameters_alter_role SET {parameter} TO 'a_credential'"
            ));

            assert!(
                error.contains("pg_db_role_setting"),
                "{parameter} was refused from ALTER ROLE with: {error}"
            );
        }

        let in_the_catalog = Spi::get_one::<i64>(
            "SELECT count(*) FROM pg_db_role_setting
             WHERE setrole = 'parameters_alter_role'::regrole::oid",
        )
        .unwrap()
        .unwrap();

        assert_eq!(in_the_catalog, 0, "a credential reached pg_db_role_setting");
    }

    // and from ALTER DATABASE, which is kept in the same catalog
    #[pg_test]
    fn test_a_credential_is_refused_from_alter_database() {
        allow_credentials_in_parameters();

        let database = Spi::get_one::<String>("SELECT current_database()::text")
            .unwrap()
            .unwrap();

        let error = statement_error(&format!(
            "ALTER DATABASE {database} SET pg_parquet.aws_secret_access_key TO 'a_credential'"
        ));

        assert!(
            error.contains("pg_db_role_setting"),
            "ALTER DATABASE was refused with: {error}"
        );
    }

    // a definer's rights function must not set a credential on a caller's behalf, which
    // GUC_NOT_WHILE_SEC_REST is what refuses
    #[pg_test]
    fn test_a_credential_is_refused_in_a_security_restricted_operation() {
        allow_credentials_in_parameters();

        Spi::run(
            "CREATE FUNCTION set_a_credential() RETURNS void LANGUAGE plpgsql
                 SECURITY DEFINER AS
                 $$ BEGIN SET pg_parquet.aws_secret_access_key TO 'a_credential'; END $$;",
        )
        .unwrap();

        let error = statement_error("SELECT set_a_credential()");

        assert!(
            error.contains("within security-definer function")
                || error.contains("security-restricted"),
            "a definer's rights function was refused with: {error}"
        );
    }

    // the parameters holding a credential are kept out of SHOW ALL and pg_settings. Note that
    // SHOW <name> and current_setting() read them all the same, which the documentation says
    // rather than the code pretending otherwise.
    #[pg_test]
    fn test_a_credential_is_hidden_from_show_all_but_not_from_show() {
        allow_credentials_in_parameters();
        Spi::run("SET pg_parquet.aws_secret_access_key TO 'a_credential'").unwrap();

        let in_pg_settings = Spi::get_one::<i64>(
            "SELECT count(*) FROM pg_settings WHERE name = 'pg_parquet.aws_secret_access_key'",
        )
        .unwrap()
        .unwrap();

        assert_eq!(
            in_pg_settings, 0,
            "a parameter holding a credential has a row in pg_settings"
        );

        // and the honest half: it is still readable by name
        let by_name =
            Spi::get_one::<String>("SELECT current_setting('pg_parquet.aws_secret_access_key')")
                .unwrap()
                .unwrap();

        assert_eq!(
            by_name, "a_credential",
            "current_setting does not read the parameter, so the comment about it is wrong"
        );
    }

    // taking the permission away stops the next copy of a session that already set them,
    // rather than only the sessions that start after it
    #[pg_test]
    fn test_turning_the_permission_off_stops_reading_the_parameters() {
        let test_bucket_name = test_bucket();

        let access_key_id = std::env::var("AWS_ACCESS_KEY_ID").unwrap();

        allow_credentials_in_parameters();
        Spi::run(&format!(
            "SET pg_parquet.aws_access_key_id TO '{access_key_id}';
             SET pg_parquet.aws_secret_access_key TO 'not_the_secret';"
        ))
        .unwrap();

        // the environment still has credentials that work, and they are what answers once
        // the parameters stop being read
        Spi::run("SET pg_parquet.allow_credentials_in_parameters TO off").unwrap();

        Spi::run(&format!(
            "COPY (SELECT 1 as a) TO \
             's3://{test_bucket_name}/pg_parquet_parameters_test.parquet'"
        ))
        .unwrap();
    }

    // ===================== the order the three are read in =====================

    // the parameters, then the profile, then the environment: each level is proven by making
    // the level below it one that does not work, so only the level above can be answering
    #[pg_test]
    fn test_the_order_the_credentials_are_read_in() {
        use std::io::Write;

        let test_bucket_name = test_bucket();
        let uri = format!("s3://{test_bucket_name}/pg_parquet_order_test.parquet");

        let access_key_id = std::env::var("AWS_ACCESS_KEY_ID").unwrap();
        let secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY").unwrap();
        let region = std::env::var("AWS_REGION").unwrap();
        let endpoint = std::env::var("AWS_ENDPOINT_URL").unwrap();

        // level three, the environment, is what works to begin with
        Spi::run(&format!("COPY (SELECT 1 as a) TO '{uri}'")).unwrap();

        // level two is a profile, whose credentials do not work, so a copy that reaches the
        // store after naming it is a copy that read the environment instead of the profile
        let profile = "pg_parquet_order_profile";
        let config = format!(
            "[profile {profile}]\n\
             region={region}\n\
             aws_access_key_id=AKIAIOSFODNN7EXAMPLE\n\
             aws_secret_access_key=not_the_secret_of_the_environment\n\
             endpoint_url={endpoint}\n"
        );
        let config_path = "/tmp/pg_parquet_order_aws_config";
        std::env::set_var("AWS_CONFIG_FILE", config_path);
        std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(config_path)
            .unwrap()
            .write_all(config.as_bytes())
            .unwrap();

        Spi::run(&format!("SET pg_parquet.profile_name TO '{profile}'")).unwrap();

        let error = statement_error(&format!("COPY (SELECT 1 as a) TO '{uri}'"));
        assert!(
            error.contains("403") || error.contains("Forbidden"),
            "the profile was not read ahead of the environment: {error}"
        );

        // level one is the parameters, which are read ahead of the profile that does not work
        allow_credentials_in_parameters();
        Spi::run(&format!(
            "SET pg_parquet.aws_access_key_id TO '{access_key_id}';
             SET pg_parquet.aws_secret_access_key TO '{secret_access_key}';"
        ))
        .unwrap();

        Spi::run(&format!("COPY (SELECT 1 as a) TO '{uri}'")).unwrap();

        // and resetting them goes back to the profile, which is still the one that fails
        Spi::run("RESET pg_parquet.aws_access_key_id; RESET pg_parquet.aws_secret_access_key;")
            .unwrap();

        let error = statement_error(&format!("COPY (SELECT 1 as a) TO '{uri}'"));
        assert!(
            error.contains("403") || error.contains("Forbidden"),
            "resetting the parameters did not go back to the profile: {error}"
        );
    }

    // a set of parameters that names no identity is refused, rather than the rest of them
    // being read from a profile or from the environment and the requests made as the server
    #[pg_test]
    fn test_an_incomplete_set_of_parameters_is_refused() {
        let test_bucket_name = test_bucket();
        let uri = format!("s3://{test_bucket_name}/pg_parquet_order_test.parquet");

        allow_credentials_in_parameters();

        // a secret with no key id
        Spi::run("SET pg_parquet.aws_secret_access_key TO 'a_secret'").unwrap();
        let error = statement_error(&format!("COPY (SELECT 1 as a) TO '{uri}'"));
        assert!(
            error.contains("pg_parquet.aws_access_key_id is not set"),
            "a secret with no key id was read: {error}"
        );

        // a key id with no secret
        Spi::run(
            "RESET pg_parquet.aws_secret_access_key;
             SET pg_parquet.aws_access_key_id TO 'a_key_id';",
        )
        .unwrap();
        let error = statement_error(&format!("COPY (SELECT 1 as a) TO '{uri}'"));
        assert!(
            error.contains("pg_parquet.aws_secret_access_key is not set"),
            "a key id with no secret was read: {error}"
        );

        // and an azure key with no account
        Spi::run("RESET pg_parquet.aws_access_key_id").unwrap();
        Spi::run("SET pg_parquet.azure_storage_key TO 'a_key'").unwrap();
        let error = statement_error(
            "COPY (SELECT 1 as a) TO 'az://testcontainer/pg_parquet_order_test.parquet'",
        );
        assert!(
            error.contains("pg_parquet.azure_storage_account is not set"),
            "an azure key with no account was read: {error}"
        );
    }

    // ===================== the other two stores =====================

    // the azure parameters are read the same way, which only a copy that reaches azurite
    // with them while the environment names another account can show
    #[pg_test]
    #[cfg(not(rhel8))]
    fn test_azure_credentials_in_parameters_are_used() {
        let test_container_name = std::env::var("AZURE_TEST_CONTAINER_NAME")
            .expect("AZURE_TEST_CONTAINER_NAME not found");

        let account_name = std::env::var("AZURE_STORAGE_ACCOUNT").unwrap();
        let account_key = std::env::var("AZURE_STORAGE_KEY").unwrap();

        // the environment is left naming an account that does not work, so a copy that
        // reaches the container is one that read the parameters instead of it
        std::env::set_var("AZURE_STORAGE_ACCOUNT", "wrong_account");
        std::env::set_var("AZURE_STORAGE_KEY", "wrong_key");
        std::env::remove_var("AZURE_STORAGE_CONNECTION_STRING");

        allow_credentials_in_parameters();
        Spi::run(&format!(
            "SET pg_parquet.azure_storage_account TO '{account_name}';
             SET pg_parquet.azure_storage_key TO '{account_key}';"
        ))
        .unwrap();

        let uri = format!("az://{test_container_name}/pg_parquet_parameters_test.parquet");
        let test_table = TestTable::<i32>::new("int4".into()).with_uri(uri);
        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();
    }

    // a connection string carries the rest of the credentials, so it is read on its own
    #[pg_test]
    #[cfg(not(rhel8))]
    fn test_an_azure_connection_string_in_parameters_is_used() {
        let test_container_name = std::env::var("AZURE_TEST_CONTAINER_NAME")
            .expect("AZURE_TEST_CONTAINER_NAME not found");

        // the connection string is built here rather than read from
        // AZURE_STORAGE_CONNECTION_STRING: that is the one value of the test env file written
        // in quotes, and the Makefile's include keeps them, so what the environment carries
        // under ci is a string no connection string parser accepts
        let connection_string = format!(
            "DefaultEndpointsProtocol=http;AccountName={};AccountKey={};BlobEndpoint={};",
            std::env::var("AZURE_STORAGE_ACCOUNT").unwrap(),
            std::env::var("AZURE_STORAGE_KEY").unwrap(),
            std::env::var("AZURE_STORAGE_ENDPOINT").unwrap(),
        );

        std::env::set_var("AZURE_STORAGE_ACCOUNT", "wrong_account");
        std::env::set_var("AZURE_STORAGE_KEY", "wrong_key");
        std::env::remove_var("AZURE_STORAGE_CONNECTION_STRING");

        allow_credentials_in_parameters();
        Spi::run(&format!(
            "SET pg_parquet.azure_storage_connection_string TO '{connection_string}'"
        ))
        .unwrap();

        let uri = format!("az://{test_container_name}/pg_parquet_parameters_test.parquet");
        let test_table = TestTable::<i32>::new("int4".into()).with_uri(uri);
        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();
    }

    // and the gcs key, which is the whole of the credentials of that store
    #[pg_test]
    fn test_gcs_credentials_in_parameters_are_used() {
        let test_bucket_name =
            std::env::var("GOOGLE_TEST_BUCKET").expect("GOOGLE_TEST_BUCKET not found");

        let service_account_key = std::env::var("GOOGLE_SERVICE_ACCOUNT_KEY").unwrap();

        // the environment is left without a key, so only the parameters can answer
        std::env::remove_var("GOOGLE_SERVICE_ACCOUNT_KEY");
        std::env::remove_var("GOOGLE_SERVICE_ACCOUNT_PATH");

        allow_credentials_in_parameters();
        Spi::run(&format!(
            "SET pg_parquet.gcs_service_account_key TO {}",
            pgrx::spi::quote_literal(&service_account_key)
        ))
        .unwrap();

        let uri = format!("gs://{test_bucket_name}/pg_parquet_parameters_test.parquet");
        let test_table = TestTable::<i32>::new("int4".into()).with_uri(uri);
        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();
    }

    // a parameter set to nothing is a parameter that was not set, so it names no credential
    // rather than an empty one
    #[pg_test]
    fn test_a_parameter_set_to_nothing_is_not_a_credential() {
        let test_bucket_name = test_bucket();

        allow_credentials_in_parameters();

        // the empty string reads as unset, so the environment is what answers
        Spi::run(
            "SET pg_parquet.aws_access_key_id TO '';
             SET pg_parquet.aws_secret_access_key TO '';",
        )
        .unwrap();

        Spi::run(&format!(
            "COPY (SELECT 1 as a) TO \
             's3://{test_bucket_name}/pg_parquet_parameters_test.parquet'"
        ))
        .unwrap();
    }
}
