use std::sync::Arc;

use azure_storage::{ConnectionString, EndpointProtocol};
use home::home_dir;
use ini::Ini;
use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
use url::Url;

use super::ObjectStoreWithExpiration;
use crate::client_options::load_client_options;
use crate::Result;

/// Creates a MicrosoftAzure object store for the container the uri names. It is configured by
/// environment variables, with azure's own ini config file as the fallback. We read that file
/// ourselves because `object_store` has no way to read it. Supported:
/// - AZURE_STORAGE_ACCOUNT
/// - AZURE_STORAGE_KEY
/// - AZURE_STORAGE_CONNECTION_STRING
/// - AZURE_STORAGE_SAS_TOKEN
/// - AZURE_CONFIG_FILE (env var only, object_store specific)
/// - AZURE_STORAGE_ENDPOINT (env var only, object_store specific)
/// - AZURE_ALLOW_HTTP (env var only, object_store specific)
/// - AZURE_<HTTP CLIENT OPTION>, e.g. AZURE_TIMEOUT (env var only, object_store specific)
pub fn create_azure_object_store(uri: &Url) -> Result<ObjectStoreWithExpiration> {
    let container_name = parse_azure_blob_container(uri)
        .ok_or(format!("unsupported azure blob storage uri: {uri}"))?;

    let mut azure_builder = MicrosoftAzureBuilder::new().with_container_name(container_name);

    let azure_blob_config = AzureStorageConfig::load()?;

    // http client options, including allow http
    azure_builder = azure_builder
        .with_client_options(load_client_options("AZURE", azure_blob_config.allow_http));

    if let Some(endpoint) = azure_blob_config.endpoint {
        azure_builder = azure_builder.with_endpoint(endpoint);
    }

    if let Some(sas_token) = azure_blob_config.sas_token {
        azure_builder = azure_builder.with_config(AzureConfigKey::SasKey, sas_token);
    }

    if let Some(account_name) = azure_blob_config.account_name {
        azure_builder = azure_builder.with_account(account_name);
    }

    if let Some(account_key) = azure_blob_config.account_key {
        azure_builder = azure_builder.with_access_key(account_key);
    }

    if let Some(tenant_id) = azure_blob_config.tenant_id {
        azure_builder = azure_builder.with_tenant_id(tenant_id);
    }

    if let Some(client_id) = azure_blob_config.client_id {
        azure_builder = azure_builder.with_client_id(client_id);
    }

    if let Some(client_secret) = azure_blob_config.client_secret {
        azure_builder = azure_builder.with_client_secret(client_secret);
    }

    if let Some(federated_token_file) = azure_blob_config.federated_token_file {
        azure_builder = azure_builder.with_federated_token_file(federated_token_file);
    }

    let object_store = Arc::new(azure_builder.build().map_err(|e| e.to_string())?);

    Ok(ObjectStoreWithExpiration {
        multipart_store: Some(object_store.clone()),
        object_store,
        // object_store refreshes the bearer token itself, so there is nothing to expire here
        expire_at: None,
    })
}

/// Recovers the container name from the spellings of an azure blob uri.
pub fn parse_azure_blob_container(uri: &Url) -> Option<String> {
    let host = uri.host_str()?;

    // az(ure)://{container}/key
    if uri.scheme() == "az" || uri.scheme() == "azure" {
        return Some(host.to_string());
    }
    // https://{account}.blob.core.windows.net/{container}/key
    else if host.ends_with(".blob.core.windows.net") {
        let path_segments: Vec<&str> = uri.path_segments()?.collect();

        // Container name is the first part of the path
        return path_segments.first().map(|segment| segment.to_string());
    }

    None
}

/// The configuration used to build the Azure Blob Storage object store.
struct AzureStorageConfig {
    account_name: Option<String>,
    account_key: Option<String>,
    sas_token: Option<String>,
    tenant_id: Option<String>,
    client_id: Option<String>,
    client_secret: Option<String>,
    federated_token_file: Option<String>,
    endpoint: Option<String>,
    allow_http: bool,
}

impl AzureStorageConfig {
    /// Reads the azure config from the environment variables first and the config files as the
    /// fallback. There is no azure sdk config crate that reads the config files, so we read
    /// azure's ini config ourselves. See
    /// <https://learn.microsoft.com/en-us/cli/azure/azure-cli-configuration>
    fn load() -> Result<Self> {
        // ~/.azure/config
        let azure_config_file_path = match std::env::var("AZURE_CONFIG_FILE") {
            Ok(path) => path,
            Err(_) => home_dir()
                .ok_or("failed to get the home directory")?
                .join(".azure")
                .join("config")
                .to_string_lossy()
                .to_string(),
        };

        let azure_config_content = Ini::load_from_file(&azure_config_file_path).ok();

        let storage_setting = |key: &str| {
            azure_config_content
                .as_ref()
                .and_then(|ini| ini.section(Some("storage")))
                .and_then(|section| section.get(key))
                .map(|value| value.to_string())
        };

        let connection_string = std::env::var("AZURE_STORAGE_CONNECTION_STRING")
            .ok()
            .or_else(|| storage_setting("connection_string"));

        // a connection string overrides everything
        if let Some(connection_string) = connection_string {
            if let Ok(connection_string) = ConnectionString::new(&connection_string) {
                return Ok(connection_string.into());
            }
        }

        let account_name = std::env::var("AZURE_STORAGE_ACCOUNT")
            .ok()
            .or_else(|| storage_setting("account"));

        let account_key = std::env::var("AZURE_STORAGE_KEY")
            .ok()
            .or_else(|| storage_setting("key"));

        let sas_token = std::env::var("AZURE_STORAGE_SAS_TOKEN")
            .ok()
            .or_else(|| storage_setting("sas_token"));

        // allow http, object_store specific
        let allow_http = std::env::var("AZURE_ALLOW_HTTP")
            .ok()
            .and_then(|allow_http| allow_http.parse().ok())
            .unwrap_or(false);

        Ok(AzureStorageConfig {
            account_name,
            account_key,
            sas_token,
            // the rest are object_store specific and have no config file fallback
            tenant_id: std::env::var("AZURE_TENANT_ID").ok(),
            client_id: std::env::var("AZURE_CLIENT_ID").ok(),
            client_secret: std::env::var("AZURE_CLIENT_SECRET").ok(),
            federated_token_file: std::env::var("AZURE_FEDERATED_TOKEN_FILE").ok(),
            endpoint: std::env::var("AZURE_STORAGE_ENDPOINT").ok(),
            allow_http,
        })
    }
}

impl From<ConnectionString<'_>> for AzureStorageConfig {
    fn from(connection_string: ConnectionString) -> Self {
        let allow_http = matches!(
            connection_string.default_endpoints_protocol,
            Some(EndpointProtocol::Http)
        );

        AzureStorageConfig {
            account_name: connection_string.account_name.map(|name| name.to_string()),
            account_key: connection_string.account_key.map(|key| key.to_string()),
            sas_token: connection_string.sas.map(|sas| sas.to_string()),
            tenant_id: None,
            client_id: None,
            client_secret: None,
            federated_token_file: None,
            endpoint: connection_string
                .blob_endpoint
                .map(|endpoint| endpoint.to_string()),
            allow_http,
        }
    }
}
