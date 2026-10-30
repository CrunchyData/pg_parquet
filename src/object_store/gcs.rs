use std::sync::Arc;

use object_store::gcp::GoogleCloudStorageBuilder;
use url::Url;

use super::{
    client_options::load_client_options, object_store_cache::ObjectStoreWithExpiration,
    profile::gcs_service_account_path,
};

// create_gcs_object_store a GoogleCloudStorage object store from given uri.
// It is configured by environment variables. Currently, we only support
// following environment variables:
// - GOOGLE_SERVICE_ACCOUNT_KEY
// - GOOGLE_SERVICE_ACCOUNT_PATH
// - GOOGLE_<HTTP CLIENT OPTION>, e.g. GOOGLE_TIMEOUT (object_store specific)
// A session that named a profile reads the service account key of that profile instead,
// from the directory pg_parquet.gcs_credential_directory names.
pub(crate) fn create_gcs_object_store(
    uri: &Url,
    profile: Option<&str>,
) -> ObjectStoreWithExpiration {
    let bucket_name = parse_gcs_bucket(uri).unwrap_or_else(|| {
        panic!("unsupported gcs uri: {uri}");
    });

    let mut gcs_builder = GoogleCloudStorageBuilder::new().with_bucket_name(bucket_name);

    let gcs_config = GoogleStorageConfig::load(profile);

    // http client options. object_store allows http endpoints for gcs by default,
    // which we keep since the gcs config file is the only way to point the client at
    // an alternative, possibly local, endpoint
    gcs_builder = gcs_builder.with_client_options(load_client_options("GOOGLE", true));

    // service account key
    if let Some(service_account_key) = gcs_config.service_account_key {
        gcs_builder = gcs_builder.with_service_account_key(&service_account_key);
    }

    // service account path
    if let Some(service_account_path) = gcs_config.service_account_path {
        gcs_builder = gcs_builder.with_service_account_path(&service_account_path);
    }

    let object_store = gcs_builder.build().unwrap_or_else(|e| panic!("{}", e));

    // object store handles refreshing bearer token, so we do not need to handle expiry here
    let expire_at = None;

    let object_store = Arc::new(object_store);

    ObjectStoreWithExpiration {
        multipart_store: Some(object_store.clone()),
        object_store,
        expire_at,
    }
}

pub(crate) fn parse_gcs_bucket(uri: &Url) -> Option<String> {
    let host = uri.host_str()?;

    // gs://{bucket}/key
    if uri.scheme() == "gs" {
        return Some(host.to_string());
    }

    None
}

// GoogleStorageConfig is a struct that holds the configuration that is
// used to configure the Google Storage object store.
struct GoogleStorageConfig {
    service_account_key: Option<String>,
    service_account_path: Option<String>,
}

impl GoogleStorageConfig {
    // load loads the Google Storage configuration from the environment, or from the
    // service account key of the profile the session named. A profile is read on its own
    // rather than before the environment: the environment is what every session of the
    // server shares, so falling back to it would make a profile whose key is missing read
    // the credentials of the server rather than fail.
    fn load(profile: Option<&str>) -> Self {
        if let Some(profile) = profile {
            return Self {
                service_account_key: None,
                service_account_path: Some(gcs_service_account_path(profile)),
            };
        }

        Self {
            service_account_key: std::env::var("GOOGLE_SERVICE_ACCOUNT_KEY").ok(),
            service_account_path: std::env::var("GOOGLE_SERVICE_ACCOUNT_PATH").ok(),
        }
    }
}
