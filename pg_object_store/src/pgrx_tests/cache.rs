//! How long a resolved store lives.
//!
//! Building one resolves credentials, which for s3 can mean a round trip to the metadata
//! service or to STS, so a store is kept for the session and keyed by what its credentials
//! belong to. The hosts here are under the reserved `.invalid` domain: a store is cached when
//! it is built, before anything is sent, so none of this needs a reachable endpoint.

#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    use crate::pgrx_tests::common::{error_of, resolve_store};

    /// The cache as (scheme, bucket, whether it is marked expired), in a stable order.
    fn cached() -> Vec<(String, String, bool)> {
        Spi::connect(|client| {
            client
                .select(
                    "SELECT scheme, bucket, expires_at IS NOT NULL
                       FROM object_store.cache_items() ORDER BY scheme, bucket",
                    None,
                    &[],
                )
                .expect("failed to read the cache")
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

    #[pg_test]
    fn test_a_store_is_cached_once_per_bucket() {
        Spi::run("SELECT object_store.cache_clear()").unwrap();

        resolve_store("https://pgos-one.invalid/first");
        resolve_store("https://pgos-one.invalid/second");
        resolve_store("https://pgos-two.invalid/first");

        // two buckets from three uris: the path is not part of the key, the base uri is
        assert_eq!(
            cached(),
            vec![
                ("http".into(), "https://pgos-one.invalid".into(), false),
                ("http".into(), "https://pgos-two.invalid".into(), false),
            ]
        );

        Spi::run("SELECT object_store.cache_clear()").unwrap();

        assert!(cached().is_empty(), "the cache was not cleared");
    }

    #[pg_test]
    fn test_expiring_a_bucket_leaves_the_others_alone() {
        Spi::run("SELECT object_store.cache_clear()").unwrap();

        resolve_store("https://pgos-expiring.invalid/key");
        resolve_store("https://pgos-staying.invalid/key");

        Spi::run("SELECT object_store.cache_expire_bucket('https://pgos-expiring.invalid')")
            .unwrap();

        assert_eq!(
            cached(),
            vec![
                ("http".into(), "https://pgos-expiring.invalid".into(), true),
                ("http".into(), "https://pgos-staying.invalid".into(), false),
            ],
            "only the named bucket is marked expired"
        );

        // an expired entry is not served: the next access drops it and builds a new store,
        // which is how a credential that was revoked mid-session gets re-resolved
        resolve_store("https://pgos-expiring.invalid/key");

        assert_eq!(
            cached(),
            vec![
                ("http".into(), "https://pgos-expiring.invalid".into(), false),
                ("http".into(), "https://pgos-staying.invalid".into(), false),
            ],
            "the expired entry was not rebuilt"
        );
    }

    #[pg_test]
    fn test_nothing_is_cached_while_the_cache_is_off() {
        Spi::run("SELECT object_store.cache_clear()").unwrap();
        Spi::run("SET object_store.enable_store_cache = off").unwrap();

        resolve_store("https://pgos-uncached.invalid/key");

        assert!(
            cached().is_empty(),
            "a store was cached with the cache turned off"
        );

        Spi::run("RESET object_store.enable_store_cache").unwrap();
    }

    #[pg_test]
    fn test_enable_store_cache_is_ours_and_settable() {
        let setting = Spi::get_one::<String>("SHOW object_store.enable_store_cache")
            .expect("the GUC is not defined");

        assert_eq!(setting.as_deref(), Some("on"));

        Spi::run("SET object_store.enable_store_cache = off").expect("failed to set the GUC");

        let setting = Spi::get_one::<String>("SHOW object_store.enable_store_cache")
            .expect("the GUC is not defined");

        assert_eq!(setting.as_deref(), Some("off"));

        Spi::run("RESET object_store.enable_store_cache").expect("failed to reset the GUC");
    }

    #[pg_test]
    fn test_local_paths_are_never_cached() {
        let uri = "/tmp/pg_object_store_uncached.bin";

        Spi::run_with_args("SELECT object_store.put($1, 'x'::bytea)", &[uri.into()])
            .expect("failed to write the file");

        let cached = Spi::get_one::<i64>("SELECT count(*) FROM object_store.cache_items()")
            .expect("failed to read the cache");

        assert_eq!(
            cached,
            Some(0),
            "a local path resolves no credentials, so there is nothing to cache"
        );

        std::fs::remove_file(uri).ok();
    }

    #[pg_test]
    fn test_an_unreachable_bucket_is_an_error_and_not_a_wrong_answer() {
        // the cache is populated before the request is made, so a store that is cached says
        // nothing about whether the bucket is there: the access itself still has to fail
        let error = error_of("SELECT object_store.get('https://pgos-gone.invalid/key')")
            .expect("reading an unreachable host should fail");

        assert!(
            error.contains("pgos-gone.invalid"),
            "the message names neither the host nor the failure: {error}"
        );
    }
}
