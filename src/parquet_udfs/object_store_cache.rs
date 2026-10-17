// Functions for looking into, and poking at, the object store cache that object-store-core
// keeps per session. They exist so that the tests can prove that one store is built per
// (scheme, bucket) and that an expiring credential is in fact re-resolved, without waiting
// out a real expiry. Nothing here is part of pg_parquet's SQL surface.
#[cfg(feature = "pg_test")]
#[pgrx::pg_schema]
mod parquet_test {
    use std::time::UNIX_EPOCH;

    use object_store_core::cache::{cache_items, clear_cache, expire_bucket};
    use pgrx::{iter::TableIterator, name, pg_extern, pg_sys::Timestamp};

    #[pg_extern]
    fn object_store_cache_clear() {
        clear_cache();
    }

    #[pg_extern]
    fn object_store_cache_expire_bucket(bucket: &str) {
        expire_bucket(bucket);
    }

    #[pg_extern]
    fn object_store_cache_items() -> TableIterator<
        'static,
        (
            name!(scheme, String),
            name!(bucket, String),
            name!(expire_at, Option<Timestamp>),
        ),
    > {
        let rows = cache_items()
            .into_iter()
            .map(|(scheme, bucket, expire_at)| {
                let expire_at = expire_at.map(|expiry| {
                    expiry
                        .duration_since(UNIX_EPOCH)
                        .expect("a credential expiry before the epoch")
                        .as_micros() as Timestamp
                });

                (scheme, bucket, expire_at)
            })
            .collect::<Vec<_>>();

        TableIterator::new(rows)
    }
}
