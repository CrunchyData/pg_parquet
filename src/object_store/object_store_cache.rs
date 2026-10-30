use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    sync::Arc,
    time::SystemTime,
};

use object_store::{multipart::MultipartStore, path::Path, ObjectStore, ObjectStoreScheme};
use once_cell::sync::Lazy;
use pgrx::{ereport, PgLogLevel, PgSqlErrorCode};
use url::Url;

use crate::arrow_parquet::uri_utils::ParsedUriInfo;

use super::{
    aws::create_s3_object_store, azure::create_azure_object_store, gcs::create_gcs_object_store,
    http::create_http_object_store, local_file::create_local_file_object_store,
    profile::current_profile,
};

// OBJECT_STORE_CACHE is a global cache for object stores per Postgres session.
// It caches object stores based on the scheme, the bucket and the profile.
// Local paths are not cached.
static mut OBJECT_STORE_CACHE: Lazy<ObjectStoreCache> = Lazy::new(ObjectStoreCache::new);

pub(crate) fn get_or_create_object_store(
    uri_info: &ParsedUriInfo,
    copy_from: bool,
) -> (Arc<dyn ObjectStore>, Path) {
    let (item, path) = get_or_create_object_store_item(uri_info, copy_from);

    (item.object_store, path)
}

// get_or_create_multipart_store is the multipart upload interface of the store the uri names,
// which is what lets several processes upload parts of one upload. It is None for a store
// that does not implement it, e.g. the local filesystem.
pub(crate) fn get_or_create_multipart_store(
    uri_info: &ParsedUriInfo,
) -> Option<(Arc<dyn MultipartStore>, Path)> {
    let copy_from = false;
    let (item, path) = get_or_create_object_store_item(uri_info, copy_from);

    Some((item.multipart_store?, path))
}

fn get_or_create_object_store_item(
    uri_info: &ParsedUriInfo,
    copy_from: bool,
) -> (ObjectStoreWithExpiration, Path) {
    #[allow(static_mut_refs)]
    unsafe {
        OBJECT_STORE_CACHE.get_or_create(uri_info, copy_from)
    }
}

struct ObjectStoreCache {
    cache: HashMap<ObjectStoreCacheKey, ObjectStoreWithExpiration>,
}

impl ObjectStoreCache {
    fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }

    fn get_or_create(
        &mut self,
        uri_info: &ParsedUriInfo,
        copy_from: bool,
    ) -> (ObjectStoreWithExpiration, Path) {
        let uri = uri_info.uri.clone();
        let scheme = uri_info.scheme.clone();
        let bucket = uri_info.bucket.clone();
        let path = uri_info.path.clone();

        // no need to cache local files
        if scheme == ObjectStoreScheme::Local {
            let profile = None;
            let item = Self::create(scheme, &uri, copy_from, profile);
            return (item, path);
        }

        let bucket = bucket.expect("bucket is None");

        // the profile is read on every call rather than only when a store is created: it is
        // what authorizes the credentials the store was built with, so a session that is no
        // longer mapped to the profile is refused the store that is cached for it. It is a
        // part of the key as well, since two profiles name two sets of credentials and the
        // store of one must never answer the requests made with the other.
        let profile = current_profile(&scheme);

        let key = ObjectStoreCacheKey {
            scheme: scheme.clone(),
            bucket,
            profile: profile.clone(),
        };

        if let Some(item) = self.cache.get(&key) {
            if item.expired(&key.bucket) {
                self.cache.remove(&key);
            } else {
                return (item.clone(), path);
            }
        }

        let item = Self::create(scheme, &uri, copy_from, profile.as_deref());

        self.cache.insert(key, item.clone());

        (item, path)
    }

    fn create(
        scheme: ObjectStoreScheme,
        uri: &Url,
        copy_from: bool,
        profile: Option<&str>,
    ) -> ObjectStoreWithExpiration {
        // object_store crate can recognize a bunch of different schemes and paths, but we only support
        // local, s3, azure, https and gs schemes with a subset of all supported paths.
        match scheme {
            ObjectStoreScheme::AmazonS3 => create_s3_object_store(uri, profile),
            ObjectStoreScheme::MicrosoftAzure => create_azure_object_store(uri, profile),
            ObjectStoreScheme::Http => create_http_object_store(uri),
            ObjectStoreScheme::GoogleCloudStorage => create_gcs_object_store(uri, profile),
            ObjectStoreScheme::Local => create_local_file_object_store(uri, copy_from),
            _ => panic!(
                    "unsupported scheme {} in uri {}. pg_parquet supports local paths, https://, s3://, az:// or gs:// schemes.",
                    uri.scheme(),
                    uri
                ),
        }
    }
}

// ObjectStoreWithExpiration is a value for the object store cache map.
#[derive(Clone)]
pub(crate) struct ObjectStoreWithExpiration {
    pub(crate) object_store: Arc<dyn object_store::ObjectStore>,

    // the same store seen as a multipart upload interface, for a store that implements it
    pub(crate) multipart_store: Option<Arc<dyn MultipartStore>>,

    // expiration time (if not applicable, the object_store will never expire)
    pub(crate) expire_at: Option<SystemTime>,
}

impl ObjectStoreWithExpiration {
    fn expired(&self, bucket: &str) -> bool {
        if let Some(expire_at) = self.expire_at {
            let expired = expire_at < SystemTime::now();

            if expired {
                ereport!(
                    PgLogLevel::DEBUG2,
                    PgSqlErrorCode::ERRCODE_WARNING,
                    format!("credentials for {bucket} expired at {expire_at:?}"),
                );
            }

            expired
        } else {
            false
        }
    }
}

// ObjectStoreCacheKey is a key for the object store cache map
// We cache object stores based on the scheme, the bucket and the profile.
// i.e. 1 object store per scheme, bucket and profile.
#[derive(Clone, Eq, PartialEq)]
struct ObjectStoreCacheKey {
    scheme: ObjectStoreScheme,
    bucket: String,

    // the profile the credentials of the store were read from, None for the credentials of
    // the server's environment
    profile: Option<String>,
}

impl Hash for ObjectStoreCacheKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let schema_tag = self.scheme.clone() as i32;
        schema_tag.hash(state);
        self.bucket.hash(state);
        self.profile.hash(state);
    }
}

// The following udfs are only used for testing purposes.
#[cfg(feature = "pg_test")]
#[pgrx::pg_schema]
mod parquet_test {
    use std::time::UNIX_EPOCH;

    use pgrx::{iter::TableIterator, name, pg_extern, pg_sys::Timestamp};

    use super::OBJECT_STORE_CACHE;

    #[pg_extern]
    fn object_store_cache_clear() {
        #[allow(static_mut_refs)]
        unsafe {
            OBJECT_STORE_CACHE.cache.clear();
        }
    }

    #[pg_extern]
    fn object_store_cache_expire_bucket(bucket: &str) {
        #[allow(static_mut_refs)]
        let cache = unsafe { &mut super::OBJECT_STORE_CACHE.cache };

        cache.retain(|key, _| key.bucket != bucket);
    }

    #[pg_extern]
    fn object_store_cache_items() -> TableIterator<
        'static,
        (
            name!(scheme, String),
            name!(bucket, String),
            name!(profile, Option<String>),
            name!(expire_at, Option<Timestamp>),
        ),
    > {
        #[allow(static_mut_refs)]
        let cache = unsafe { &super::OBJECT_STORE_CACHE.cache };

        let rows = cache
            .iter()
            .map(|(key, value)| {
                let scheme = format!("{:?}", key.scheme);
                let bucket = key.bucket.clone();
                let profile = key.profile.clone();
                let expire_at = value
                    .expire_at
                    .map(|t| t.duration_since(UNIX_EPOCH).unwrap().as_micros() as Timestamp);

                (scheme, bucket, profile, expire_at)
            })
            .collect::<Vec<_>>();

        TableIterator::new(rows)
    }
}
