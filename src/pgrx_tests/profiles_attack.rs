// Tests that try to reach the credentials of a profile a session is not mapped to, and to
// write the mapping that says which those are.
#[pgrx::pg_schema]
mod tests {
    use std::io::Write;

    use pgrx::{pg_test, PgTryBuilder, Spi};

    use crate::object_store::profile::profile_name_is_a_name;

    const PROFILE: &str = "pg_parquet_profile_test";

    // write_aws_config_file gives the profile the credentials of the test bucket, so that a
    // copy naming it reaches the store rather than failing to load any credentials at all
    fn write_aws_config_file(profile: &str) {
        let content = format!(
            "[profile {profile}]\n\
             region={}\n\
             aws_access_key_id={}\n\
             aws_secret_access_key={}\n\
             endpoint_url={}\n",
            std::env::var("AWS_REGION").unwrap(),
            std::env::var("AWS_ACCESS_KEY_ID").unwrap(),
            std::env::var("AWS_SECRET_ACCESS_KEY").unwrap(),
            std::env::var("AWS_ENDPOINT_URL").unwrap(),
        );

        let path = "/tmp/pg_parquet_attack_aws_config";
        std::env::set_var("AWS_CONFIG_FILE", path);

        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(path)
            .unwrap();

        file.write_all(content.as_bytes()).unwrap();
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

    // the workers of a parallel copy open stores of their own and skip the mapping, which
    // the leader read for them. A copy whose leader never read it would reach the
    // credentials of a profile through its workers alone.
    #[pg_test]
    #[should_panic(expected = "permission denied for profile \"pg_parquet_profile_test\"")]
    fn test_parallel_copy_to_does_not_skip_the_mapping() {
        let test_bucket_name = test_bucket();

        create_plain_role("parallel_profile_role");

        // the profile has credentials that work, so nothing but the mapping can refuse the
        // copy, and a table of its own that the role may read
        write_aws_config_file(PROFILE);

        Spi::run(
            "CREATE TABLE parallel_profile_test AS SELECT i FROM generate_series(1, 20000) i;
             GRANT SELECT ON parallel_profile_test TO parallel_profile_role;",
        )
        .unwrap();

        Spi::run(&format!(
            "SET ROLE parallel_profile_role;
             SET pg_parquet.max_parallel_workers TO 4;
             SET pg_parquet.profile_name TO '{PROFILE}';"
        ))
        .unwrap();

        Spi::run(&format!(
            "COPY parallel_profile_test TO \
             's3://{test_bucket_name}/pg_parquet_profile_parallel.parquet'"
        ))
        .unwrap();
    }

    // the same for the workers of a parallel read, which open the files of a copy from
    #[pg_test]
    #[should_panic(expected = "permission denied for profile \"pg_parquet_profile_test\"")]
    fn test_parallel_copy_from_does_not_skip_the_mapping() {
        let test_bucket_name = test_bucket();
        let uri = format!("s3://{test_bucket_name}/pg_parquet_profile_parallel_read.parquet");

        write_aws_config_file(PROFILE);

        // written by the superuser, which names any profile, with row groups for the
        // workers of the read below to split between themselves
        Spi::run(&format!(
            "COPY (SELECT i FROM generate_series(1, 20000) i) TO '{uri}'
             WITH (row_group_size 1000)"
        ))
        .unwrap();

        create_plain_role("parallel_read_profile_role");
        Spi::run(
            "CREATE TABLE IF NOT EXISTS parallel_read_profile_test (i int);
             GRANT SELECT, INSERT ON parallel_read_profile_test TO parallel_read_profile_role;",
        )
        .unwrap();

        Spi::run(&format!(
            "SET ROLE parallel_read_profile_role;
             SET pg_parquet.max_parallel_workers TO 4;
             SET pg_parquet.profile_name TO '{PROFILE}';"
        ))
        .unwrap();

        Spi::run(&format!("COPY parallel_read_profile_test FROM '{uri}'")).unwrap();
    }

    // the store of a profile is cached for the session, so a role that the session becomes
    // after it was cached would reach it without the mapping ever being read for that role
    #[pg_test]
    #[should_panic(expected = "permission denied for profile \"pg_parquet_profile_test\"")]
    fn test_cached_store_is_not_reached_by_another_role() {
        let test_bucket_name = test_bucket();
        let uri = format!("s3://{test_bucket_name}/pg_parquet_profile_cached.parquet");

        create_plain_role("cached_profile_role");
        write_aws_config_file(PROFILE);

        // the superuser names any profile, which puts the store of it in the cache
        Spi::run(&format!("SET pg_parquet.profile_name TO '{PROFILE}'")).unwrap();
        Spi::run(&format!("COPY (SELECT 1 as a) TO '{uri}'")).unwrap();

        let cached = Spi::get_one::<i64>(
            "SELECT count(*) FROM parquet_test.object_store_cache_items()
             WHERE profile IS NOT NULL",
        )
        .unwrap()
        .unwrap();
        assert_eq!(cached, 1, "the store of the profile was not cached");

        // and the role the session becomes is mapped to nothing at all
        Spi::run("SET ROLE cached_profile_role").unwrap();
        Spi::run(&format!("COPY (SELECT 1 as a) TO '{uri}'")).unwrap();
    }

    // my_profiles is a view of one table with a where clause, which postgres makes an
    // updatable view of, so a write through it would reach the mapping as the view's owner
    #[pg_test]
    #[should_panic(expected = "permission denied for view my_profiles")]
    fn test_my_profiles_cannot_be_inserted_into() {
        create_plain_role("view_insert_role");
        Spi::run(
            "SET ROLE view_insert_role;
             INSERT INTO parquet.my_profiles (mapped_to, profiles)
             VALUES ('view_insert_role', '{\"aws_profiles\": [\"x\"]}');",
        )
        .unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "permission denied for view my_profiles")]
    fn test_my_profiles_cannot_be_updated() {
        create_plain_role("view_update_role");
        Spi::run(
            "SET ROLE view_update_role;
             UPDATE parquet.my_profiles SET profiles = '{\"aws_profiles\": [\"x\"]}';",
        )
        .unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "permission denied for view my_profiles")]
    fn test_my_profiles_cannot_be_deleted_from() {
        create_plain_role("view_delete_role");
        Spi::run("SET ROLE view_delete_role; DELETE FROM parquet.my_profiles;").unwrap();
    }

    // the check reads parquet.profile_is_allowed by name, so a role that could create or
    // replace a function of that name in the schema would answer the check for itself
    #[pg_test]
    #[should_panic(expected = "permission denied for schema parquet")]
    fn test_a_function_cannot_be_created_in_the_schema() {
        create_plain_role("shadow_create_role");
        Spi::run(
            "SET ROLE shadow_create_role;
             CREATE FUNCTION parquet.profile_is_allowed(text, text, text) RETURNS boolean
                 LANGUAGE sql AS $$ SELECT true $$;",
        )
        .unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "permission denied for schema parquet")]
    fn test_the_check_function_cannot_be_replaced() {
        create_plain_role("shadow_replace_role");
        Spi::run(
            "SET ROLE shadow_replace_role;
             CREATE OR REPLACE FUNCTION parquet.profile_is_allowed(\"scheme\" text,
                 \"profile\" text) RETURNS boolean LANGUAGE sql AS $$ SELECT true $$;",
        )
        .unwrap();
    }

    // a profile the session carries in pg_db_role_setting is still only a name, and the
    // mapping is read for it the same way
    #[pg_test]
    #[should_panic(expected = "permission denied for profile \"pg_parquet_profile_test\"")]
    fn test_a_profile_set_on_the_role_is_still_checked() {
        let test_bucket_name = test_bucket();

        create_plain_role("alter_role_profile_role");

        // the role sets it on itself, which lands in the world readable catalog
        Spi::run(&format!(
            "SET ROLE alter_role_profile_role;
             ALTER ROLE alter_role_profile_role SET pg_parquet.profile_name TO '{PROFILE}';"
        ))
        .unwrap();

        let in_the_catalog = Spi::get_one::<String>(
            "SELECT array_to_string(setconfig, ',') FROM pg_db_role_setting
             WHERE setrole = 'alter_role_profile_role'::regrole::oid",
        )
        .unwrap()
        .unwrap();

        assert_eq!(
            in_the_catalog,
            format!("pg_parquet.profile_name={PROFILE}"),
            "the setting of the role is not the name of the profile alone"
        );

        Spi::run(&format!("SET pg_parquet.profile_name TO '{PROFILE}'")).unwrap();
        Spi::run(&format!(
            "COPY (SELECT 1 as a) TO 's3://{test_bucket_name}/pg_parquet_profile_test.parquet'"
        ))
        .unwrap();
    }

    // the mapping follows the role of the session however the session became it
    #[pg_test]
    fn test_session_authorization_is_the_role_the_mapping_is_read_for() {
        create_plain_role("session_auth_mapped_role");
        create_plain_role("session_auth_plain_role");

        Spi::run(&format!(
            "INSERT INTO parquet.user_profile_mapping (userid, profiles)
             VALUES ('session_auth_mapped_role', '{{\"aws_profiles\": [\"{PROFILE}\"]}}')"
        ))
        .unwrap();

        Spi::run("SET SESSION AUTHORIZATION session_auth_mapped_role").unwrap();
        let mapped = Spi::get_one::<bool>(&format!(
            "SELECT parquet.profile_is_allowed('s3', '{PROFILE}')"
        ))
        .unwrap()
        .unwrap();
        assert!(mapped, "the role the session was authorized as was refused");

        Spi::run("RESET SESSION AUTHORIZATION").unwrap();
        Spi::run("SET SESSION AUTHORIZATION session_auth_plain_role").unwrap();
        let mapped = Spi::get_one::<bool>(&format!(
            "SELECT parquet.profile_is_allowed('s3', '{PROFILE}')"
        ))
        .unwrap()
        .unwrap();
        assert!(!mapped, "a role the mapping does not name was allowed");
    }

    // a name that is not a name names a file or a section of something else, so it is
    // refused before it is resolved to anything
    #[pg_test]
    fn test_profile_names_that_are_not_names() {
        for profile in [
            "",
            "..",
            "../../etc/shadow",
            "a/b",
            "a\\b",
            ".hidden",
            ".",
            // the separator the azure section form used, and a windows drive
            "storage:analytics",
            "C:analytics",
            "a b",
            "ana!ytics",
            "analytics\n",
            "analytics\t",
            "analytics;DROP",
            "ana'lytics",
            "ana\"lytics",
            "%2e%2e",
            "a..b",
            // not ascii
            "анализ",
            "analytik\u{00e9}",
            // one character too long
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ] {
            assert!(
                !profile_name_is_a_name(profile),
                "the profile name {profile:?} was taken for a name"
            );
        }

        for profile in [
            "analytics",
            "Analytics",
            "analytics-team",
            "analytics_team",
            "team.analytics",
            "a",
            "0",
            // the longest name that is one
            &"a".repeat(63),
        ] {
            assert!(
                profile_name_is_a_name(profile),
                "the profile name {profile:?} was refused as one"
            );
        }
    }

    // and the refusal is what a copy naming one is answered with
    #[pg_test]
    #[should_panic(expected = "is not a profile name")]
    fn test_a_profile_name_that_names_a_path_fails_the_copy() {
        let test_bucket_name = test_bucket();

        Spi::run("SET pg_parquet.profile_name TO '../../etc/shadow'").unwrap();

        Spi::run(&format!(
            "COPY (SELECT 1 as a) TO 's3://{test_bucket_name}/pg_parquet_profile_test.parquet'"
        ))
        .unwrap();
    }

    // the mapping is read for the role the copy runs as, so a definer's rights function is
    // how a mapped role lends its profile out, and an invoker's rights one is not
    #[pg_test]
    fn test_a_definers_rights_function_runs_with_the_definers_mapping() {
        create_plain_role("definer_mapped_role");
        create_plain_role("definer_caller_role");

        Spi::run(&format!(
            "INSERT INTO parquet.user_profile_mapping (userid, profiles)
             VALUES ('definer_mapped_role', '{{\"aws_profiles\": [\"{PROFILE}\"]}}');
             CREATE FUNCTION allowed_as_definer() RETURNS boolean
                 LANGUAGE sql STABLE SECURITY DEFINER AS
                 $$ SELECT parquet.profile_is_allowed('s3', '{PROFILE}') $$;
             CREATE FUNCTION allowed_as_invoker() RETURNS boolean
                 LANGUAGE sql STABLE AS
                 $$ SELECT parquet.profile_is_allowed('s3', '{PROFILE}') $$;
             ALTER FUNCTION allowed_as_definer() OWNER TO definer_mapped_role;
             GRANT EXECUTE ON FUNCTION allowed_as_definer(), allowed_as_invoker()
                 TO definer_caller_role;"
        ))
        .unwrap();

        Spi::run("SET ROLE definer_caller_role").unwrap();

        let as_definer = Spi::get_one::<bool>("SELECT allowed_as_definer()")
            .unwrap()
            .unwrap();
        let as_invoker = Spi::get_one::<bool>("SELECT allowed_as_invoker()")
            .unwrap()
            .unwrap();

        assert!(
            as_definer,
            "a definer's rights function did not run with the mapping of its owner"
        );
        assert!(
            !as_invoker,
            "an invoker's rights function ran with the mapping of its owner"
        );
    }

    // the file of a profile holds credentials, so an error about one must not carry what
    // the file held: a file that is not a key at all is the case that would print it
    #[pg_test]
    fn test_a_malformed_profile_key_does_not_reach_the_error() {
        let canary = "FAKE-CANARY-7f3a9c1e-not-a-real-key";

        std::fs::create_dir_all("/tmp/pg_parquet_canary_gcs").unwrap();
        std::fs::write(
            format!("/tmp/pg_parquet_canary_gcs/{PROFILE}.json"),
            format!(
                "{{\"type\": \"service_account\", \"private_key\": \"{canary}\", \
                  \"client_email\": \"{canary}@example.com\"}}"
            ),
        )
        .unwrap();

        std::env::set_var(
            "GOOGLE_SERVICE_ACCOUNT_DIRECTORY",
            "/tmp/pg_parquet_canary_gcs",
        );

        Spi::run(&format!("SET pg_parquet.profile_name TO '{PROFILE}'")).unwrap();

        // the error is caught rather than let through, so that what it says can be read:
        // a postgres error inside spi unwinds instead of answering with an Err
        let message = PgTryBuilder::new(|| {
            Spi::run("COPY (SELECT 1 as a) TO 'gs://testbucket/pg_parquet_canary.parquet'")
                .expect("a key that is not one was accepted");

            String::new()
        })
        .catch_others(|error| format!("{error:?}"))
        .execute();

        assert!(
            !message.is_empty(),
            "a key that is not one was accepted without an error"
        );

        assert!(
            !message.contains(canary),
            "the contents of the key file reached the error: {message}"
        );
    }

    // the mapping is matched by the name exactly, so a profile that differs in case is
    // another profile and not the one the session was mapped to
    #[pg_test]
    fn test_a_profile_of_another_case_is_another_profile() {
        create_plain_role("case_profile_role");

        Spi::run(
            "INSERT INTO parquet.user_profile_mapping (userid, profiles)
             VALUES ('case_profile_role', '{\"aws_profiles\": [\"analytics\"]}')",
        )
        .unwrap();

        Spi::run("SET ROLE case_profile_role").unwrap();

        assert!(
            Spi::get_one::<bool>("SELECT parquet.profile_is_allowed('s3', 'analytics')")
                .unwrap()
                .unwrap(),
            "the profile the role was mapped to was refused"
        );

        for other in ["Analytics", "ANALYTICS", "analytic", "analyticss"] {
            assert!(
                !Spi::get_one::<bool>(&format!(
                    "SELECT parquet.profile_is_allowed('s3', '{other}')"
                ))
                .unwrap()
                .unwrap(),
                "the profile {other:?} was allowed by a mapping of \"analytics\""
            );
        }
    }
}
