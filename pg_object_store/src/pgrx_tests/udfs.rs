//! The SQL surface: how a uri is parsed, and what reading and writing one through the
//! functions in the `object_store` schema does.

#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_a_listed_location_is_a_uri_that_can_be_read() {
        let dir = "/tmp/pg_object_store_listing";

        std::fs::create_dir_all(dir).expect("failed to make the directory");

        for name in ["one.bin", "two.bin"] {
            Spi::run_with_args(
                "SELECT object_store.put($1, 'listed'::bytea)",
                &[format!("{dir}/{name}").into()],
            )
            .expect("failed to write the file");
        }

        // the location an object store hands back is relative to its root, which for a local
        // path means it comes back without the leading slash. What is listed has to be what can
        // be read, so the listing renders it the way the prefix was written.
        let locations = Spi::get_one_with_args::<String>(
            "SELECT string_agg(location, ',' ORDER BY location)
               FROM object_store.list($1)",
            &[dir.into()],
        )
        .expect("failed to list the directory");

        assert_eq!(
            locations.as_deref(),
            Some("/tmp/pg_object_store_listing/one.bin,/tmp/pg_object_store_listing/two.bin")
        );

        let contents = Spi::get_one_with_args::<String>(
            "SELECT convert_from(object_store.get((SELECT min(location) FROM object_store.list($1))), 'utf8')",
            &[dir.into()],
        )
        .expect("a listed location should be readable as it stands");

        assert_eq!(contents.as_deref(), Some("listed"));

        std::fs::remove_dir_all(dir).ok();
    }

    #[pg_test]
    fn test_parse_uri_recognizes_every_supported_scheme() {
        let cases = [
            ("s3://bucket/key", "s3", Some("bucket"), "key"),
            (
                "az://container/dir/key",
                "azure",
                Some("container"),
                "dir/key",
            ),
            ("gs://bucket/key", "gcs", Some("bucket"), "key"),
            (
                "https://example.com/dir/key",
                "http",
                Some("https://example.com"),
                "dir/key",
            ),
            ("/tmp/local.file", "local", None, "local.file"),
        ];

        for (uri, scheme, bucket, path) in cases {
            let result = Spi::get_three_with_args::<String, String, String>(
                "SELECT scheme, bucket, path FROM object_store.parse_uri($1)",
                &[uri.into()],
            )
            .expect("failed to parse the uri");

            assert_eq!(result.0.as_deref(), Some(scheme), "scheme of {uri}");
            assert_eq!(result.1.as_deref(), bucket, "bucket of {uri}");
            let parsed_path = result.2.expect("the path is never null");
            assert!(
                parsed_path.ends_with(path),
                "path of {uri} was {parsed_path}"
            );
        }
    }

    // the whole message, not a prefix: pgrx compares it for equality
    #[pg_test(
        error = "unrecognized uri ftp://host/key. Supported: local paths, https://, s3://, az:// or gs:// schemes."
    )]
    fn test_parse_uri_rejects_a_scheme_we_cannot_reach() {
        Spi::get_one::<String>("SELECT scheme FROM object_store.parse_uri('ftp://host/key')")
            .expect("an ftp uri should not parse");
    }

    #[pg_test]
    fn test_normalize_uri_renders_a_local_uri_as_a_path() {
        let normalized =
            Spi::get_one::<String>("SELECT object_store.normalize_uri('file:///tmp/local.file')")
                .expect("failed to normalize the uri");

        assert_eq!(normalized.as_deref(), Some("/tmp/local.file"));
    }

    #[pg_test]
    fn test_local_file_round_trip() {
        let uri = "/tmp/pg_object_store_round_trip.bin";

        let written = Spi::get_one_with_args::<i64>(
            "SELECT object_store.put($1, 'hello object store'::bytea)",
            &[uri.into()],
        )
        .expect("failed to write the file");

        assert_eq!(written, Some(18));

        let exists =
            Spi::get_one_with_args::<bool>("SELECT object_store.exists($1)", &[uri.into()])
                .expect("failed to stat the file");

        assert_eq!(exists, Some(true));

        let size = Spi::get_one_with_args::<i64>("SELECT object_store.size($1)", &[uri.into()])
            .expect("failed to size the file");

        assert_eq!(size, Some(18));

        let contents = Spi::get_one_with_args::<String>(
            "SELECT convert_from(object_store.get($1), 'utf8')",
            &[uri.into()],
        )
        .expect("failed to read the file");

        assert_eq!(contents.as_deref(), Some("hello object store"));

        let tail = Spi::get_one_with_args::<String>(
            "SELECT convert_from(object_store.get_range($1, 6, 12), 'utf8')",
            &[uri.into()],
        )
        .expect("failed to read a range of the file");

        assert_eq!(tail.as_deref(), Some("object store"));

        std::fs::remove_file(uri).ok();
    }

    #[pg_test]
    fn test_a_missing_object_is_not_an_error() {
        let uri = "/tmp/pg_object_store_absent.bin";

        std::fs::remove_file(uri).ok();

        let exists =
            Spi::get_one_with_args::<bool>("SELECT object_store.exists($1)", &[uri.into()])
                .expect("failed to stat the absent file");

        assert_eq!(exists, Some(false));

        let size = Spi::get_one_with_args::<i64>("SELECT object_store.size($1)", &[uri.into()])
            .expect("the query itself should succeed");

        assert_eq!(size, None, "the size of an absent object is null");
    }

    #[pg_test]
    fn test_a_zero_length_range_is_empty_rather_than_invalid() {
        let uri = "/tmp/pg_object_store_zero_range.bin";

        Spi::get_one_with_args::<i64>(
            "SELECT object_store.put($1, 'some bytes'::bytea)",
            &[uri.into()],
        )
        .expect("failed to write the file");

        // a store rejects an empty range as invalid, which is no reason for a request for no
        // bytes to fail
        let empty = Spi::get_one_with_args::<Vec<u8>>(
            "SELECT object_store.get_range($1, 0, 0)",
            &[uri.into()],
        )
        .expect("a zero length read should succeed");

        assert_eq!(empty, Some(vec![]));

        // the same, past the end of the file, and of an empty file
        let past_end = Spi::get_one_with_args::<Vec<u8>>(
            "SELECT object_store.get_range($1, 1000, 0)",
            &[uri.into()],
        )
        .expect("a zero length read past the end should succeed");

        assert_eq!(past_end, Some(vec![]));

        std::fs::remove_file(uri).ok();

        std::fs::write(uri, b"").expect("failed to write an empty file");

        let of_empty = Spi::get_one_with_args::<Vec<u8>>(
            "SELECT object_store.get_range($1, 0, 0)",
            &[uri.into()],
        )
        .expect("a zero length read of an empty file should succeed");

        assert_eq!(of_empty, Some(vec![]));

        std::fs::remove_file(uri).ok();
    }

    #[pg_test(
        error = "2000000000 bytes of /tmp/pg_object_store_over_cap.bin is more than the 1073741823 a bytea holds; read it a range at a time with object_store.get_range()"
    )]
    fn test_a_range_too_large_for_a_bytea_is_refused_by_its_own_message() {
        // without the check this is an assertion about u32::MAX from inside the varlena
        // conversion, after the bytes have already been fetched
        Spi::get_one::<Vec<u8>>(
            "SELECT object_store.get_range('/tmp/pg_object_store_over_cap.bin', 0, 2000000000)",
        )
        .expect("the read should have been refused");
    }

    #[pg_test(
        error = "2147483648 bytes of /tmp/pg_object_store_big_object.bin is more than the 1073741823 a bytea holds; read it a range at a time with object_store.get_range()"
    )]
    fn test_an_object_too_large_for_a_bytea_is_refused_by_its_size() {
        let uri = "/tmp/pg_object_store_big_object.bin";

        // sparse, so the size is a header rather than two gigabytes of writing
        let file = std::fs::File::create(uri).expect("failed to create the file");
        file.set_len(2 * 1024 * 1024 * 1024)
            .expect("failed to size the file");
        drop(file);

        Spi::get_one_with_args::<Vec<u8>>("SELECT object_store.get($1)", &[uri.into()])
            .expect("the read should have been refused");
    }

    #[pg_test]
    fn test_the_roles_exist() {
        for role in ["object_store_read", "object_store_write"] {
            let exists = Spi::get_one_with_args::<bool>(
                "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)",
                &[role.into()],
            )
            .expect("failed to look the role up");

            assert_eq!(exists, Some(true), "{role} was not created");
        }
    }
}
