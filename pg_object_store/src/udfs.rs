//! The SQL surface of the extension.
//!
//! This is the consumption mode that costs a caller nothing: no linking, no ABI, no version
//! coupling. It is usable from PL/pgSQL, from SPI inside another extension, or from an FDW,
//! and it is enough for the things that are small and awkward rather than bulk — a manifest, a
//! `catalog.json`, an Iceberg metadata file, probing whether a prefix exists before starting
//! work. Everything materializes into a `bytea`, so the 1 GB varlena limit is the ceiling; a
//! bulk reader wants the vtable in [`crate::vtable`] instead.

use std::sync::Arc;

use ::object_store::{path::Path, ObjectStore, ObjectStoreExt};
use object_store_core::cache::{
    cache_items as cached_stores, clear_cache, expire_bucket, scheme_name,
};
use object_store_core::error::object_store_error_message;
use object_store_core::runtime::block_on;
use object_store_core::uri::ParsedUri;
use object_store_core::{get_or_create_object_store, uri_as_string};
use pgrx::prelude::*;

use crate::privilege::ensure_access_privilege_to_uri;
use crate::sync_core_settings;

/// Parses a uri, checks that the current user may reach it in the given direction, and opens
/// the store. Every function below starts here, so that none of them can be the one that
/// forgot the privilege check.
fn open(uri: &str, copy_from: bool) -> (Arc<dyn ObjectStore>, Path) {
    sync_core_settings();

    let parsed = ParsedUri::try_from(uri).unwrap_or_else(|e| error!("{e}"));

    ensure_access_privilege_to_uri(&parsed, copy_from);

    get_or_create_object_store(&parsed, copy_from).unwrap_or_else(|e| error!("{e}"))
}

/// Errors unless a byte count fits in a `bytea`. These functions return one, and a varlena is
/// capped at 1GB; without the check the cap is still reached, as an assertion about `u32::MAX`
/// from deep inside the conversion.
fn too_large_for_a_bytea(length: i64, uri: &str) {
    if length > crate::vtable::MAX_RANGE_BYTES {
        error!(
            "{length} bytes of {uri} is more than the {} a bytea holds; read it a range at a \
             time with object_store.get_range()",
            crate::vtable::MAX_RANGE_BYTES
        );
    }
}

#[pgrx::pg_schema]
mod object_store {
    use super::*;

    // the module is named for the SQL schema it becomes, which shadows the crate of the same
    // name inside it
    use ::object_store::Error as ObjectStoreError;

    /// How a uri is understood: which kind of store it names, which bucket (or container, or
    /// http base uri) in it, and the path of the object inside that bucket. Resolves no
    /// credentials and reaches no network, so it is the first thing to reach for when a uri is
    /// rejected and the message is not obvious.
    #[pg_extern]
    fn parse_uri(
        uri: &str,
    ) -> TableIterator<
        'static,
        (
            name!(scheme, String),
            name!(bucket, Option<String>),
            name!(path, String),
        ),
    > {
        let parsed = ParsedUri::try_from(uri).unwrap_or_else(|e| error!("{e}"));

        TableIterator::once((
            scheme_name(&parsed.scheme).to_string(),
            parsed.bucket.clone(),
            parsed.path.to_string(),
        ))
    }

    /// The objects under a uri, treated as a prefix. Requires the read role.
    #[pg_extern]
    fn list(
        uri: &str,
    ) -> TableIterator<
        'static,
        (
            name!(location, String),
            name!(size, i64),
            name!(last_modified, String),
        ),
    > {
        let copy_from = true;
        let parsed = ParsedUri::try_from(uri).unwrap_or_else(|e| error!("{e}"));
        let (store, prefix) = open(uri, copy_from);

        let objects = block_on(async {
            use futures::TryStreamExt;
            store.list(Some(&prefix)).try_collect::<Vec<_>>().await
        })
        .unwrap_or_else(|e| error!("failed to list {uri}: {}", object_store_error_message(e)));

        TableIterator::new(
            objects
                .into_iter()
                .map(|meta| {
                    let location = parsed
                        .location_as_uri(&meta.location)
                        .unwrap_or_else(|e| error!("{e}"));

                    (location, meta.size as i64, meta.last_modified.to_rfc3339())
                })
                .collect::<Vec<_>>(),
        )
    }

    /// Whether an object exists at the uri. Requires the read role.
    #[pg_extern]
    fn exists(uri: &str) -> bool {
        let copy_from = true;
        let (store, path) = open(uri, copy_from);

        match block_on(async { store.head(&path).await }) {
            Ok(_) => true,
            Err(ObjectStoreError::NotFound { .. }) => false,
            Err(e) => error!("failed to stat {uri}: {}", object_store_error_message(e)),
        }
    }

    /// The size of the object at the uri, in bytes, or NULL if there is none. Requires the
    /// read role.
    #[pg_extern]
    fn size(uri: &str) -> Option<i64> {
        let copy_from = true;
        let (store, path) = open(uri, copy_from);

        match block_on(async { store.head(&path).await }) {
            Ok(meta) => Some(meta.size as i64),
            Err(ObjectStoreError::NotFound { .. }) => None,
            Err(e) => error!("failed to stat {uri}: {}", object_store_error_message(e)),
        }
    }

    /// The whole object at the uri. Requires the read role.
    ///
    /// The result is a `bytea`, so an object has to fit in one: a varlena is capped at 1 GB,
    /// and the copy in the backend's memory is on top of that. This is for metadata, not for
    /// data files.
    #[pg_extern]
    fn get(uri: &str) -> Vec<u8> {
        let copy_from = true;
        let (store, path) = open(uri, copy_from);

        // the size is taken off the response rather than off the collected bytes, so an object
        // too large to return is not downloaded first. Reported out here rather than in the
        // async block: an `error!` is a longjmp, and one through the runtime is not survivable
        let fetched: Result<bytes::Bytes, u64> = block_on(async {
            let got = store.get(&path).await?;
            let size = got.meta.size;

            if size as i64 > crate::vtable::MAX_RANGE_BYTES {
                return Ok(Err(size));
            }

            Ok(Ok(got.bytes().await?))
        })
        .unwrap_or_else(|e: ObjectStoreError| {
            error!("failed to read {uri}: {}", object_store_error_message(e))
        });

        match fetched {
            Ok(bytes) => bytes.to_vec(),
            Err(size) => {
                too_large_for_a_bytea(size as i64, uri);
                unreachable!("the size check reports an error");
            }
        }
    }

    /// A byte range of the object at the uri, which is how to read the tail of a file without
    /// paying for the rest of it. Requires the read role.
    #[pg_extern]
    fn get_range(uri: &str, offset: i64, length: i64) -> Vec<u8> {
        if offset < 0 || length < 0 {
            error!("offset and length must not be negative");
        }

        // a request for no bytes is answered without a request: a store rejects an empty range
        // as invalid, and the backends disagree about how (a 416, a generic error)
        if length == 0 {
            return vec![];
        }

        too_large_for_a_bytea(length, uri);

        let copy_from = true;
        let (store, path) = open(uri, copy_from);

        let range = (offset as u64)..(offset as u64 + length as u64);

        let bytes = block_on(async { store.get_range(&path, range).await }).unwrap_or_else(|e| {
            error!(
                "failed to read {length} bytes at {offset} of {uri}: {}",
                object_store_error_message(e)
            )
        });

        bytes.to_vec()
    }

    /// Writes an object at the uri, replacing anything already there, and returns how many
    /// bytes were written. Requires the write role.
    #[pg_extern]
    fn put(uri: &str, data: &[u8]) -> i64 {
        let copy_from = false;
        let (store, path) = open(uri, copy_from);

        let payload = bytes::Bytes::copy_from_slice(data).into();

        block_on(async { store.put(&path, payload).await })
            .unwrap_or_else(|e| error!("failed to write {uri}: {}", object_store_error_message(e)));

        data.len() as i64
    }

    /// The object store clients cached in this session, and when each one's credentials stop
    /// working. A row here is a client, not a connection: one serves every object in its
    /// bucket. Local paths are never cached and so never appear.
    #[pg_extern]
    fn cache_items() -> TableIterator<
        'static,
        (
            name!(scheme, String),
            name!(bucket, String),
            name!(expires_at, Option<String>),
        ),
    > {
        TableIterator::new(
            cached_stores()
                .into_iter()
                .map(|(scheme, bucket, expire_at)| {
                    let expires_at = expire_at.map(|expire_at| {
                        chrono::DateTime::<chrono::Utc>::from(expire_at).to_rfc3339()
                    });

                    (scheme, bucket, expires_at)
                }),
        )
    }

    /// Drops every cached client, so the next access re-resolves credentials. Restricted to
    /// superusers: it is a session-wide side effect, and the reason to reach for it is almost
    /// always that something about the environment changed underneath Postgres.
    #[pg_extern]
    fn cache_clear() {
        if !unsafe { pgrx::pg_sys::superuser() } {
            error!("only superusers may clear the object store cache");
        }

        clear_cache();
    }

    /// Marks a bucket's cached client as expired, so the next access rebuilds it. Exists so
    /// that a test can prove an expiring credential really is re-resolved without waiting out
    /// a real expiry. Restricted to superusers for the same reason as `cache_clear`.
    #[pg_extern]
    fn cache_expire_bucket(bucket: &str) {
        if !unsafe { pgrx::pg_sys::superuser() } {
            error!("only superusers may expire the object store cache");
        }

        expire_bucket(bucket);
    }

    /// The uri as the object store sees it: a `file://` uri rendered back as a plain path,
    /// everything else unchanged. Mostly useful for understanding an error message that
    /// quoted a uri you do not recognize.
    #[pg_extern]
    fn normalize_uri(uri: &str) -> String {
        let parsed = ParsedUri::try_from(uri).unwrap_or_else(|e| error!("{e}"));

        uri_as_string(&parsed.uri).unwrap_or_else(|e| error!("{e}"))
    }
}
