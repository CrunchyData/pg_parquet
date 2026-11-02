#[pgrx::pg_schema]
mod tests {
    use std::io::Write;

    use pgrx::{pg_test, Spi};

    use crate::{
        object_store::profile::profile_file_path,
        pgrx_tests::common::{TestTable, LOCAL_TEST_FILE_PATH},
    };

    const PROFILE: &str = "pg_parquet_profile_test";

    // a role that is not a superuser, which is what the mapping is read for: a superuser
    // reads the credentials of every profile from the files of the server anyway
    fn create_plain_role(role: &str) {
        Spi::run(&format!(
            "DROP ROLE IF EXISTS {role};
             CREATE ROLE {role};
             GRANT parquet_object_store_read, parquet_object_store_write TO {role};"
        ))
        .unwrap();
    }

    fn map_profiles(role: &str, profiles: &str) {
        Spi::run(&format!(
            "INSERT INTO parquet.user_profile_mapping (userid, profiles)
             VALUES ('{role}', '{profiles}'::jsonb)"
        ))
        .unwrap();
    }

    // the mapping is keyed by the role rather than by its oid, which is what a dump of it
    // carries into another cluster and what a row that names no role is refused by
    #[pg_test]
    #[should_panic(expected = "role \"no_such_role_at_all\" does not exist")]
    fn test_mapping_refuses_a_role_that_does_not_exist() {
        map_profiles(
            "no_such_role_at_all",
            &format!(r#"{{"aws_profiles": ["{PROFILE}"]}}"#),
        );
    }

    // write_aws_config_file writes a config file that holds the credentials of the test
    // bucket under a profile of its own, and points the sdk at it
    fn write_aws_config_file(profile: &str) {
        let region = std::env::var("AWS_REGION").unwrap();
        let access_key_id = std::env::var("AWS_ACCESS_KEY_ID").unwrap();
        let secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY").unwrap();
        let endpoint = std::env::var("AWS_ENDPOINT_URL").unwrap();

        let content = format!(
            "[profile {profile}]\n\
             region={region}\n\
             aws_access_key_id={access_key_id}\n\
             aws_secret_access_key={secret_access_key}\n\
             endpoint_url={endpoint}\n"
        );

        let path = "/tmp/pg_parquet_profile_aws_config";
        std::env::set_var("AWS_CONFIG_FILE", path);

        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(path)
            .unwrap();

        file.write_all(content.as_bytes()).unwrap();
    }

    #[pg_test]
    fn test_s3_profile_reads_its_own_credentials() {
        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        write_aws_config_file(PROFILE);

        // the credentials of the environment are left in place on purpose: the profile must
        // be read instead of them, which is what makes a profile a per session credential
        // rather than a fallback for a server that was started without one
        Spi::run(&format!("SET pg_parquet.profile_name TO '{PROFILE}'")).unwrap();

        let s3_uri = format!("s3://{test_bucket_name}/pg_parquet_profile_test.parquet");

        let test_table = TestTable::<i32>::new("int4".into()).with_uri(s3_uri);
        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();
    }

    // a profile that names no credentials must fail the copy rather than fall back to the
    // credentials of the server, which every session of the server shares
    #[pg_test]
    #[should_panic(expected = "failed to load aws credentials")]
    fn test_s3_profile_does_not_fall_back_to_the_environment() {
        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        // a config file that holds another profile, so the one that is named is missing
        // while AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are still set
        write_aws_config_file("another_profile");

        Spi::run(&format!("SET pg_parquet.profile_name TO '{PROFILE}'")).unwrap();

        let copy_to_command = format!(
            "COPY (SELECT 1 as a) TO \
             's3://{test_bucket_name}/pg_parquet_profile_test.parquet'"
        );
        Spi::run(&copy_to_command).unwrap();
    }

    #[pg_test]
    fn test_cache_is_keyed_by_the_profile() {
        let test_bucket_name: String =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        Spi::run("SELECT parquet_test.object_store_cache_clear()").unwrap();

        write_aws_config_file(PROFILE);

        let uri = format!("s3://{test_bucket_name}/pg_parquet_profile_test.parquet");

        // the store of the environment's credentials
        Spi::run(&format!("COPY (SELECT 1 as a) TO '{uri}'")).unwrap();

        // and the store of the profile's, which is the same bucket with another identity
        Spi::run(&format!("SET pg_parquet.profile_name TO '{PROFILE}'")).unwrap();
        Spi::run(&format!("COPY (SELECT 1 as a) TO '{uri}'")).unwrap();

        let profiles = Spi::get_one::<String>(
            "SELECT string_agg(coalesce(profile, '<none>'), ',' ORDER BY profile NULLS FIRST)
             FROM parquet_test.object_store_cache_items()",
        )
        .unwrap()
        .unwrap();

        assert_eq!(
            profiles,
            format!("<none>,{PROFILE}"),
            "the store of the profile and the store of the environment share a cache entry"
        );
    }

    // a local path is read with no credentials at all, so there is no identity for a
    // profile to pick between and nothing for it to be refused for
    #[pg_test]
    fn test_profile_does_not_apply_to_a_local_path() {
        create_plain_role("profile_local_role");

        // the role is mapped to no profile at all, so a scheme that took one would refuse
        // the copy rather than ignore the profile the session named
        Spi::run(&format!(
            "GRANT pg_read_server_files, pg_write_server_files TO profile_local_role;
             SET ROLE profile_local_role;
             SET pg_parquet.profile_name TO '{PROFILE}';
             CREATE TEMP TABLE local_profile_test (a int);"
        ))
        .unwrap();

        Spi::run(&format!(
            "COPY (SELECT 1 as a) TO '{LOCAL_TEST_FILE_PATH}';
             COPY local_profile_test FROM '{LOCAL_TEST_FILE_PATH}';"
        ))
        .unwrap();

        let rows = Spi::get_one::<i64>("SELECT count(*) FROM local_profile_test")
            .unwrap()
            .unwrap();

        assert_eq!(
            rows, 1,
            "a local path was not copied while a profile was set"
        );
    }

    #[pg_test]
    #[should_panic(expected = "permission denied for profile \"pg_parquet_profile_test\"")]
    fn test_profile_without_a_mapping_is_refused() {
        create_plain_role("profile_unmapped_role");

        Spi::run(&format!(
            "SET ROLE profile_unmapped_role;
             SET pg_parquet.profile_name TO '{PROFILE}';"
        ))
        .unwrap();

        Spi::run("COPY (SELECT 1 as a) TO 's3://testbucket/pg_parquet_profile_test.parquet'")
            .unwrap();
    }

    // the mapping is keyed by the scheme as well: the credentials of an s3 profile say
    // nothing about which azure storage account the session may reach
    #[pg_test]
    #[should_panic(expected = "permission denied for profile \"pg_parquet_profile_test\"")]
    fn test_profile_mapped_for_another_scheme_is_refused() {
        create_plain_role("profile_scheme_role");
        map_profiles(
            "profile_scheme_role",
            &format!(r#"{{"aws_profiles": ["{PROFILE}"]}}"#),
        );

        Spi::run(&format!(
            "SET ROLE profile_scheme_role;
             SET pg_parquet.profile_name TO '{PROFILE}';"
        ))
        .unwrap();

        Spi::run("COPY (SELECT 1 as a) TO 'az://testcontainer/pg_parquet_profile_test.parquet'")
            .unwrap();
    }

    // the mapping of another role is no mapping of the session's. The check reads the
    // table through a view rather than through a SECURITY DEFINER function for this: in
    // such a function current_user is the superuser that installed the extension, so every
    // profile the table maps to anybody at all would be one the session may select.
    #[pg_test]
    #[should_panic(expected = "permission denied for profile \"pg_parquet_profile_test\"")]
    fn test_profile_mapped_to_another_role_is_refused() {
        create_plain_role("profile_owner_role");
        create_plain_role("profile_thief_role");

        map_profiles(
            "profile_owner_role",
            &format!(r#"{{"aws_profiles": ["{PROFILE}"]}}"#),
        );

        Spi::run(&format!(
            "SET ROLE profile_thief_role;
             SET pg_parquet.profile_name TO '{PROFILE}';"
        ))
        .unwrap();

        let allowed = Spi::get_one::<bool>(&format!(
            "SELECT parquet.profile_is_allowed('s3', '{PROFILE}')"
        ))
        .unwrap()
        .unwrap();

        assert!(
            !allowed,
            "the profile of another role was allowed to the session"
        );

        Spi::run("COPY (SELECT 1 as a) TO 's3://testbucket/pg_parquet_profile_test.parquet'")
            .unwrap();
    }

    #[pg_test]
    fn test_profile_mapped_to_a_group_role_is_allowed() {
        create_plain_role("profile_member_role");
        Spi::run(
            "DROP ROLE IF EXISTS profile_group_role;
             CREATE ROLE profile_group_role;
             GRANT profile_group_role TO profile_member_role;",
        )
        .unwrap();

        // the profile is mapped to the group rather than to the member of it
        map_profiles(
            "profile_group_role",
            &format!(r#"{{"aws_profiles": ["{PROFILE}"]}}"#),
        );

        Spi::run("SET ROLE profile_member_role").unwrap();

        let allowed = Spi::get_one::<bool>(&format!(
            "SELECT parquet.profile_is_allowed('s3', '{PROFILE}')"
        ))
        .unwrap()
        .unwrap();

        assert!(allowed, "a profile mapped to a group role was refused");

        let other = Spi::get_one::<bool>("SELECT parquet.profile_is_allowed('s3', 'another')")
            .unwrap()
            .unwrap();

        assert!(!other, "a profile that is mapped to nobody was allowed");
    }

    #[pg_test]
    #[should_panic(expected = "is not a profile name")]
    fn test_profile_name_that_names_a_path_is_refused() {
        Spi::run("SET pg_parquet.profile_name TO '../../etc/credentials'").unwrap();

        Spi::run("COPY (SELECT 1 as a) TO 's3://testbucket/pg_parquet_profile_test.parquet'")
            .unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "permission denied for table user_profile_mapping")]
    fn test_mapping_is_not_writable_by_a_plain_role() {
        create_plain_role("profile_writer_role");

        Spi::run("SET ROLE profile_writer_role").unwrap();

        map_profiles(
            "profile_writer_role",
            &format!(r#"{{"aws_profiles": ["{PROFILE}"]}}"#),
        );
    }

    #[pg_test]
    fn test_mapping_is_writable_by_the_profile_manager() {
        create_plain_role("profile_manager_role");
        Spi::run("GRANT parquet_profile_manager TO profile_manager_role").unwrap();

        Spi::run("SET ROLE profile_manager_role").unwrap();

        map_profiles(
            "profile_manager_role",
            &format!(r#"{{"aws_profiles": ["{PROFILE}"]}}"#),
        );

        let allowed = Spi::get_one::<bool>(&format!(
            "SELECT parquet.profile_is_allowed('s3', '{PROFILE}')"
        ))
        .unwrap()
        .unwrap();

        assert!(allowed, "a profile the manager mapped was refused");
    }

    // the mapping tells a role which profiles it may name and nothing about the other
    // roles, which the table itself would
    #[pg_test]
    fn test_my_profiles_shows_the_sessions_rows_only() {
        create_plain_role("profile_reader_role");
        create_plain_role("profile_other_role");

        map_profiles(
            "profile_reader_role",
            &format!(r#"{{"aws_profiles": ["{PROFILE}"]}}"#),
        );
        map_profiles("profile_other_role", r#"{"aws_profiles": ["another"]}"#);

        Spi::run("SET ROLE profile_reader_role").unwrap();

        let mapped_to = Spi::get_one::<String>(
            "SELECT string_agg(mapped_to::text, ',' ORDER BY mapped_to::text)
             FROM parquet.my_profiles",
        )
        .unwrap()
        .unwrap();

        assert_eq!(mapped_to, "profile_reader_role");

        let table_is_readable = Spi::get_one::<bool>(
            "SELECT has_table_privilege('parquet.user_profile_mapping', 'SELECT')",
        )
        .unwrap()
        .unwrap();

        assert!(
            !table_is_readable,
            "the mapping of every role is readable by a role it maps"
        );
    }

    #[pg_test]
    #[should_panic(expected = "profiles_keys_are_known")]
    fn test_mapping_refuses_a_misspelled_key() {
        create_plain_role("profile_typo_role");

        // aws_profile rather than aws_profiles would map no profile at all, which reads as
        // a profile that is refused rather than as the typo that it is
        map_profiles("profile_typo_role", r#"{"aws_profile": ["x"]}"#);
    }

    #[pg_test]
    #[should_panic(expected = "profiles_are_arrays_of_names")]
    fn test_mapping_refuses_a_name_that_is_not_in_a_list() {
        create_plain_role("profile_scalar_role");

        map_profiles("profile_scalar_role", r#"{"aws_profiles": "x"}"#);
    }

    // the key of a gcs profile is a file next to the key the server reads, so a server
    // whose own key is inline in GOOGLE_SERVICE_ACCOUNT_KEY names no directory to read a
    // profile's key from
    #[pg_test]
    #[should_panic(expected = "GOOGLE_SERVICE_ACCOUNT_PATH is not set")]
    fn test_gcs_profile_needs_the_service_account_path() {
        std::env::remove_var("GOOGLE_SERVICE_ACCOUNT_PATH");

        Spi::run(&format!("SET pg_parquet.profile_name TO '{PROFILE}'")).unwrap();

        Spi::run("COPY (SELECT 1 as a) TO 'gs://testbucket/pg_parquet_profile_test.parquet'")
            .unwrap();
    }

    // the file of a profile is named after it next to the one the server reads, keeping the
    // extension of it, so a key file and an azure config file are both named the same way
    #[pg_test]
    fn test_profile_file_is_named_next_to_the_configured_one() {
        assert_eq!(
            profile_file_path("/etc/pg_parquet/gcs/server.json", "analytics"),
            "/etc/pg_parquet/gcs/analytics.json"
        );

        // an azure config file carries no extension, and neither does the profile's
        assert_eq!(
            profile_file_path("/etc/pg_parquet/azure/config", "analytics"),
            "/etc/pg_parquet/azure/analytics"
        );

        // a profile holding a dot of its own keeps it, ahead of the extension
        assert_eq!(
            profile_file_path("/keys/server.json", "team.analytics"),
            "/keys/team.analytics.json"
        );

        // a configured path that is a bare file name stays in the directory it named
        assert_eq!(profile_file_path("config", "analytics"), "analytics");
    }

    // the config file of the server is not the config file of a profile, so a profile with
    // no file of its own is refused rather than read from the server's
    #[pg_test]
    #[should_panic(expected = "of the profile cannot be read")]
    fn test_azure_profile_without_a_config_file_is_refused() {
        let azure_config_file_path = "/tmp/pg_parquet_profile_azure/config";
        std::fs::create_dir_all("/tmp/pg_parquet_profile_azure").unwrap();
        std::fs::remove_file(format!("/tmp/pg_parquet_profile_azure/{PROFILE}")).ok();
        std::env::set_var("AZURE_CONFIG_FILE", azure_config_file_path);

        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(azure_config_file_path)
            .unwrap();

        file.write_all(b"[storage]\naccount=someaccount\nkey=somekey\n")
            .unwrap();

        Spi::run(&format!("SET pg_parquet.profile_name TO '{PROFILE}'")).unwrap();

        Spi::run("COPY (SELECT 1 as a) TO 'az://testcontainer/pg_parquet_profile_test.parquet'")
            .unwrap();
    }

    #[pg_test]
    #[cfg(not(rhel8))]
    fn test_azure_profile_reads_its_own_config_file() {
        let test_container_name: String = std::env::var("AZURE_TEST_CONTAINER_NAME")
            .expect("AZURE_TEST_CONTAINER_NAME not found");

        let account_name = std::env::var("AZURE_STORAGE_ACCOUNT").unwrap();
        let account_key = std::env::var("AZURE_STORAGE_KEY").unwrap();
        let endpoint = std::env::var("AZURE_STORAGE_ENDPOINT").unwrap();
        let allow_http = std::env::var("AZURE_ALLOW_HTTP").unwrap();

        // the connection string of the environment would override the section
        std::env::remove_var("AZURE_STORAGE_CONNECTION_STRING");

        std::fs::create_dir_all("/tmp/pg_parquet_profile_azure").unwrap();

        // the config file of the server holds credentials that do not work, so the copy
        // below only succeeds if the file of the profile is the one that was read
        let azure_config_file_path = "/tmp/pg_parquet_profile_azure/config";
        std::env::set_var("AZURE_CONFIG_FILE", azure_config_file_path);
        std::fs::write(
            azure_config_file_path,
            "[storage]\naccount=wrong_account\nkey=wrong_key\n",
        )
        .unwrap();

        // and the file of the profile is a file of the same format named after it
        std::fs::write(
            format!("/tmp/pg_parquet_profile_azure/{PROFILE}"),
            format!(
                "[storage]\n\
                 account={account_name}\n\
                 key={account_key}\n\
                 endpoint={endpoint}\n\
                 allow_http={allow_http}\n"
            ),
        )
        .unwrap();

        Spi::run(&format!("SET pg_parquet.profile_name TO '{PROFILE}'")).unwrap();

        let azure_uri = format!("az://{test_container_name}/pg_parquet_profile_test.parquet");

        let test_table = TestTable::<i32>::new("int4".into()).with_uri(azure_uri);
        test_table.insert("INSERT INTO test_expected (a) VALUES (1), (2), (null);");
        test_table.assert_expected_and_result_rows();
    }
}
