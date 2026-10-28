//! Who may reach a uri.
//!
//! This is the check every consumer of the extension inherits, so it is asserted from the
//! outside — through the SQL functions, as an unprivileged role — rather than by calling the
//! predicate. A remote uri is refused before any store is built, which is why these need no
//! endpoint.

#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    use crate::pgrx_tests::common::error_of;

    #[pg_test]
    fn test_a_remote_uri_needs_the_object_store_roles() {
        Spi::run("CREATE ROLE pgos_nobody; SET ROLE pgos_nobody").unwrap();

        let write = error_of("SELECT object_store.put('s3://pgos-bucket/key', 'x'::bytea)")
            .expect("an unprivileged write should be refused");

        assert!(
            write.contains("permission denied to write a remote uri"),
            "{write}"
        );

        let read = error_of("SELECT object_store.get('s3://pgos-bucket/key')")
            .expect("an unprivileged read should be refused");

        assert!(
            read.contains("permission denied to read a remote uri"),
            "{read}"
        );

        // every entry point goes through the same gate, including the ones that only report
        for statement in [
            "SELECT object_store.exists('s3://pgos-bucket/key')",
            "SELECT object_store.size('s3://pgos-bucket/key')",
            "SELECT * FROM object_store.list('s3://pgos-bucket/')",
            "SELECT object_store.get_range('s3://pgos-bucket/key', 0, 1)",
        ] {
            let error = error_of(statement)
                .unwrap_or_else(|| panic!("{statement} was allowed without a role"));

            assert!(error.contains("permission denied"), "{statement}: {error}");
        }

        Spi::run("RESET ROLE").unwrap();
    }

    #[pg_test]
    fn test_the_write_role_opens_the_gate_without_opening_the_read_side() {
        Spi::run(
            "CREATE ROLE pgos_writer;
             GRANT object_store_write TO pgos_writer;
             SET ROLE pgos_writer",
        )
        .unwrap();

        // the write gets past the gate and fails on the host instead, which is as far as this
        // can go without an endpoint; what matters is that the message is no longer about
        // privileges
        let write = error_of("SELECT object_store.put('https://pgos-writer.invalid/key', 'x')")
            .expect("an unreachable host should still fail");

        assert!(
            !write.contains("permission denied"),
            "the write role did not open the gate: {write}"
        );

        let read = error_of("SELECT object_store.get('https://pgos-writer.invalid/key')")
            .expect("reading should still be refused");

        assert!(
            read.contains("permission denied to read a remote uri"),
            "the write role should not grant reads: {read}"
        );

        Spi::run("RESET ROLE").unwrap();
    }

    #[pg_test]
    fn test_a_local_path_follows_the_server_file_roles() {
        let uri = "/tmp/pg_object_store_privileges.bin";

        std::fs::write(uri, b"local").expect("failed to seed the file");

        Spi::run("CREATE ROLE pgos_local; SET ROLE pgos_local").unwrap();

        // a local path is Postgres' own business, so it is governed by Postgres' own roles and
        // not by this extension's
        let read = error_of(&format!("SELECT object_store.get('{uri}')"))
            .expect("an unprivileged read should be refused");

        assert!(read.contains("permission denied to read a file"), "{read}");

        let write = error_of(&format!("SELECT object_store.put('{uri}', 'x'::bytea)"))
            .expect("an unprivileged write should be refused");

        assert!(
            write.contains("permission denied to write a file"),
            "{write}"
        );

        // object_store_read is the wrong role for a local path, and does not stand in for
        // pg_read_server_files
        Spi::run("RESET ROLE; GRANT object_store_read TO pgos_local; SET ROLE pgos_local").unwrap();

        let read = error_of(&format!("SELECT object_store.get('{uri}')"))
            .expect("the object store role should not reach a local path");

        assert!(read.contains("permission denied to read a file"), "{read}");

        Spi::run("RESET ROLE; GRANT pg_read_server_files TO pgos_local; SET ROLE pgos_local")
            .unwrap();

        let contents = Spi::get_one_with_args::<String>(
            "SELECT convert_from(object_store.get($1), 'utf8')",
            &[uri.into()],
        )
        .expect("the granted read should succeed");

        assert_eq!(contents.as_deref(), Some("local"));

        Spi::run("RESET ROLE").unwrap();

        std::fs::remove_file(uri).ok();
    }
}
