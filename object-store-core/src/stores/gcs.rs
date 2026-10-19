use std::sync::Arc;

use object_store::gcp::GoogleCloudStorageBuilder;
use url::Url;

use super::ObjectStoreWithExpiration;
use crate::client_options::load_client_options;
use crate::Result;

/// Creates a GoogleCloudStorage object store for the bucket the uri names. Supported:
/// - GOOGLE_SERVICE_ACCOUNT_KEY
/// - GOOGLE_SERVICE_ACCOUNT_PATH
/// - GOOGLE_<HTTP CLIENT OPTION>, e.g. GOOGLE_TIMEOUT (object_store specific)
pub fn create_gcs_object_store(uri: &Url) -> Result<ObjectStoreWithExpiration> {
    let bucket_name = parse_gcs_bucket(uri).ok_or(format!("unsupported gcs uri: {uri}"))?;

    let mut gcs_builder = GoogleCloudStorageBuilder::new().with_bucket_name(bucket_name);

    let gcs_config = GoogleStorageConfig::load();

    // http client options. object_store allows http endpoints for gcs by default, which we
    // keep since the gcs config file is the only way to point the client at an alternative,
    // possibly local, endpoint
    gcs_builder = gcs_builder.with_client_options(load_client_options("GOOGLE", true));

    if let Some(service_account_key) = gcs_config.service_account_key {
        gcs_builder = gcs_builder.with_service_account_key(&service_account_key);
    }

    if let Some(service_account_path) = gcs_config.service_account_path {
        gcs_builder = gcs_builder.with_service_account_path(&service_account_path);
    }

    let object_store = Arc::new(gcs_builder.build().map_err(|e| e.to_string())?);

    Ok(ObjectStoreWithExpiration {
        multipart_store: Some(object_store.clone()),
        object_store,
        // object_store refreshes the bearer token itself, so there is nothing to expire here
        expire_at: None,
    })
}

pub fn parse_gcs_bucket(uri: &Url) -> Option<String> {
    let host = uri.host_str()?;

    // gs://{bucket}/key
    if uri.scheme() == "gs" {
        return Some(host.to_string());
    }

    None
}

/// The configuration used to build the Google Storage object store.
struct GoogleStorageConfig {
    service_account_key: Option<String>,
    service_account_path: Option<String>,
}

impl GoogleStorageConfig {
    fn load() -> Self {
        Self {
            service_account_key: std::env::var("GOOGLE_SERVICE_ACCOUNT_KEY").ok(),
            service_account_path: std::env::var("GOOGLE_SERVICE_ACCOUNT_PATH").ok(),
        }
    }
}
