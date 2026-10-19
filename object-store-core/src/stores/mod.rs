//! One module per kind of store. Each exposes a `create_*_object_store` that turns a uri plus
//! whatever the environment says about credentials into a client, and — for the remote ones —
//! a `parse_*` that recovers the bucket or container name from the uri's many spellings.

pub mod aws;
pub mod azure;
pub mod gcs;
pub mod http;
pub mod local_file;

use std::sync::Arc;
use std::time::SystemTime;

use object_store::{multipart::MultipartStore, ObjectStore};

/// A built store, plus when its credentials stop working.
///
/// `multipart_store` is the same object as `object_store` whenever the backend supports
/// multipart uploads; it is a second handle rather than a downcast because `ObjectStore` and
/// `MultipartStore` are separate traits. The local filesystem has no multipart, so it is
/// `None` there, and callers that need to resume or stitch an upload have to say so.
#[derive(Clone)]
pub struct ObjectStoreWithExpiration {
    pub object_store: Arc<dyn ObjectStore>,
    pub multipart_store: Option<Arc<dyn MultipartStore>>,
    pub expire_at: Option<SystemTime>,
}
