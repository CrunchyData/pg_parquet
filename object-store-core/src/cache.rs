use std::{
    cell::RefCell,
    collections::HashMap,
    hash::{Hash, Hasher},
    sync::atomic::{AtomicBool, Ordering},
    sync::Arc,
    time::SystemTime,
};

use object_store::{multipart::MultipartStore, path::Path, ObjectStore, ObjectStoreScheme};
use url::Url;

use crate::runtime::trace;
use crate::stores::{
    aws::create_s3_object_store, azure::create_azure_object_store, gcs::create_gcs_object_store,
    http::create_http_object_store, local_file::create_local_file_object_store,
    ObjectStoreWithExpiration,
};
use crate::uri::{ParsedUri, SUPPORTED_SCHEMES};
use crate::Result;

thread_local! {
    /// One object store per (scheme, bucket) for the life of the session, because building one
    /// resolves credentials, which for s3 can mean a round trip to the metadata service or to
    /// STS. Local paths are not cached: there is nothing to resolve, and a write has to
    /// make the destination's directories every time.
    ///
    /// This is per-thread rather than per-process so that the cache needs no locking. A
    /// Postgres backend is single-threaded, and a parallel worker is a separate process with
    /// its own cache, so in practice "per thread" and "per session" are the same thing.
    static OBJECT_STORE_CACHE: RefCell<ObjectStoreCache> = RefCell::new(ObjectStoreCache::new());
}

/// Whether the cache is consulted at all. A host that would rather pay to re-resolve
/// credentials on every access than risk using a credential that was revoked out from under it
/// turns this off; `pg_object_store` exposes it as `object_store.enable_store_cache`.
static CACHE_ENABLED: AtomicBool = AtomicBool::new(true);

pub fn set_cache_enabled(enabled: bool) {
    CACHE_ENABLED.store(enabled, Ordering::Relaxed);
}

pub fn cache_enabled() -> bool {
    CACHE_ENABLED.load(Ordering::Relaxed)
}

/// The store the uri names, along with the path of the object inside it.
pub fn get_or_create_object_store(
    uri: &ParsedUri,
    copy_from: bool,
) -> Result<(Arc<dyn ObjectStore>, Path)> {
    let (item, path) = get_or_create_object_store_item(uri, copy_from)?;

    Ok((item.object_store, path))
}

/// The multipart upload interface of the store the uri names, which is what lets several
/// processes upload parts of one upload. `None` for a store that does not implement it, e.g.
/// the local filesystem.
pub fn get_or_create_multipart_store(
    uri: &ParsedUri,
) -> Result<Option<(Arc<dyn MultipartStore>, Path)>> {
    let copy_from = false;
    let (item, path) = get_or_create_object_store_item(uri, copy_from)?;

    Ok(item.multipart_store.map(|store| (store, path)))
}

fn get_or_create_object_store_item(
    uri: &ParsedUri,
    copy_from: bool,
) -> Result<(ObjectStoreWithExpiration, Path)> {
    OBJECT_STORE_CACHE.with_borrow_mut(|cache| cache.get_or_create(uri, copy_from))
}

/// Drops every cached store. The next access rebuilds and so re-resolves credentials.
pub fn clear_cache() {
    OBJECT_STORE_CACHE.with_borrow_mut(|cache| cache.cache.clear());
}

/// Marks every store for a bucket as expired, so the next access rebuilds it. Exists for
/// tests, which need to prove that an expiring credential is in fact re-resolved without
/// waiting out a real expiry.
pub fn expire_bucket(bucket: &str) {
    OBJECT_STORE_CACHE.with_borrow_mut(|cache| {
        for (key, item) in cache.cache.iter_mut() {
            if key.bucket == bucket {
                item.expire_at = Some(SystemTime::UNIX_EPOCH);
            }
        }
    });
}

/// What is in the cache right now, as (scheme, bucket, expiry).
pub fn cache_items() -> Vec<(String, String, Option<SystemTime>)> {
    OBJECT_STORE_CACHE.with_borrow(|cache| {
        cache
            .cache
            .iter()
            .map(|(key, item)| {
                (
                    scheme_name(&key.scheme).to_string(),
                    key.bucket.clone(),
                    item.expire_at,
                )
            })
            .collect()
    })
}

/// A stable name for a scheme, for anything user-facing. `ObjectStoreScheme` has no `Display`
/// and its `Debug` is the Rust variant name.
pub fn scheme_name(scheme: &ObjectStoreScheme) -> &'static str {
    match scheme {
        ObjectStoreScheme::AmazonS3 => "s3",
        ObjectStoreScheme::MicrosoftAzure => "azure",
        ObjectStoreScheme::GoogleCloudStorage => "gcs",
        ObjectStoreScheme::Http => "http",
        ObjectStoreScheme::Local => "local",
        ObjectStoreScheme::Memory => "memory",
        _ => "unsupported",
    }
}

pub struct ObjectStoreCache {
    cache: HashMap<ObjectStoreCacheKey, ObjectStoreWithExpiration>,
}

impl Default for ObjectStoreCache {
    fn default() -> Self {
        Self::new()
    }
}

impl ObjectStoreCache {
    pub fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }

    fn get_or_create(
        &mut self,
        uri_info: &ParsedUri,
        copy_from: bool,
    ) -> Result<(ObjectStoreWithExpiration, Path)> {
        let uri = &uri_info.uri;
        let scheme = uri_info.scheme.clone();
        let path = uri_info.path.clone();

        // no need to cache local files, and an uncached host wants every access rebuilt
        if scheme == ObjectStoreScheme::Local || !cache_enabled() {
            return Ok((Self::create(scheme, uri, copy_from)?, path));
        }

        let bucket = uri_info
            .bucket
            .clone()
            .ok_or(format!("no bucket in uri {uri}"))?;

        let key = ObjectStoreCacheKey {
            scheme: scheme.clone(),
            bucket,
        };

        if let Some(item) = self.cache.get(&key) {
            if item.expired(&key.bucket) {
                self.cache.remove(&key);
            } else {
                return Ok((item.clone(), path));
            }
        }

        let item = Self::create(scheme, uri, copy_from)?;

        self.cache.insert(key, item.clone());

        Ok((item, path))
    }

    fn create(
        scheme: ObjectStoreScheme,
        uri: &Url,
        copy_from: bool,
    ) -> Result<ObjectStoreWithExpiration> {
        // the object_store crate recognizes a bunch of different schemes and paths, but we
        // only support local, s3, azure, https and gs with a subset of all supported paths.
        match scheme {
            ObjectStoreScheme::AmazonS3 => create_s3_object_store(uri),
            ObjectStoreScheme::MicrosoftAzure => create_azure_object_store(uri),
            ObjectStoreScheme::Http => create_http_object_store(uri),
            ObjectStoreScheme::GoogleCloudStorage => create_gcs_object_store(uri),
            ObjectStoreScheme::Local => create_local_file_object_store(uri, copy_from),
            _ => Err(format!(
                "unsupported scheme {} in uri {}. Supported: {}.",
                uri.scheme(),
                uri,
                SUPPORTED_SCHEMES
            )),
        }
    }
}

impl ObjectStoreWithExpiration {
    fn expired(&self, bucket: &str) -> bool {
        let Some(expire_at) = self.expire_at else {
            return false;
        };

        let expired = expire_at < SystemTime::now();

        if expired {
            trace(|| format!("credentials for {bucket} expired at {expire_at:?}"));
        }

        expired
    }
}

/// One object store per scheme and bucket.
#[derive(Clone, Eq, PartialEq)]
struct ObjectStoreCacheKey {
    scheme: ObjectStoreScheme,
    bucket: String,
}

impl Hash for ObjectStoreCacheKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // ObjectStoreScheme is a fieldless non_exhaustive enum, so its discriminant is all
        // there is to hash and it has no derived Hash of its own
        let scheme_tag = self.scheme.clone() as i32;
        scheme_tag.hash(state);
        self.bucket.hash(state);
    }
}
