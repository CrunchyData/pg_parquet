use std::sync::Arc;

use azure_storage::{ConnectionString, EndpointProtocol};
use home::home_dir;
use ini::Ini;
use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
use url::Url;

use super::{client_options::load_client_options, object_store_cache::ObjectStoreWithExpiration};

// AZURE_CONFIG_SECTION is the section of the azure config file the storage settings are
// read from. A profile is a section named after it, as [storage:<profile>].
const AZURE_CONFIG_SECTION: &str = "storage";

// create_azure_object_store creates a MicrosoftAzure object store with the given container name.
// It is configured by environment variables and azure config files as fallback method.
// We need to read the config files to make the fallback method work since object_store
// does not provide a way to read them. Currently, we only support following environment
// variables and config parameters:
// - AZURE_STORAGE_ACCOUNT
// - AZURE_STORAGE_KEY
// - AZURE_STORAGE_CONNECTION_STRING
// - AZURE_STORAGE_SAS_TOKEN
// - AZURE_CONFIG_FILE (env var only, object_store specific)
// - AZURE_STORAGE_ENDPOINT (env var only, object_store specific)
// - AZURE_ALLOW_HTTP (env var only, object_store specific)
// - AZURE_<HTTP CLIENT OPTION>, e.g. AZURE_TIMEOUT (env var only, object_store specific)
// A session that named a profile reads every one of the config parameters above from the
// [storage:<profile>] section of the config file instead.
pub(crate) fn create_azure_object_store(
    uri: &Url,
    profile: Option<&str>,
) -> ObjectStoreWithExpiration {
    let container_name = parse_azure_blob_container(uri).unwrap_or_else(|| {
        panic!("unsupported azure blob storage uri: {uri}");
    });

    let mut azure_builder = MicrosoftAzureBuilder::new().with_container_name(container_name);

    let azure_blob_config = AzureStorageConfig::load(profile);

    // http client options, including allow http
    azure_builder = azure_builder
        .with_client_options(load_client_options("AZURE", azure_blob_config.allow_http));

    // endpoint
    if let Some(endpoint) = azure_blob_config.endpoint {
        azure_builder = azure_builder.with_endpoint(endpoint);
    }

    // sas token
    if let Some(sas_token) = azure_blob_config.sas_token {
        azure_builder = azure_builder.with_config(AzureConfigKey::SasKey, sas_token);
    }

    // account name
    if let Some(account_name) = azure_blob_config.account_name {
        azure_builder = azure_builder.with_account(account_name);
    }

    // account key
    if let Some(account_key) = azure_blob_config.account_key {
        azure_builder = azure_builder.with_access_key(account_key);
    }

    // tenant id
    if let Some(tenant_id) = azure_blob_config.tenant_id {
        azure_builder = azure_builder.with_tenant_id(tenant_id);
    }

    // client id
    if let Some(client_id) = azure_blob_config.client_id {
        azure_builder = azure_builder.with_client_id(client_id);
    }

    // client secret
    if let Some(client_secret) = azure_blob_config.client_secret {
        azure_builder = azure_builder.with_client_secret(client_secret);
    }

    // federated token file
    if let Some(federated_token_file) = azure_blob_config.federated_token_file {
        azure_builder = azure_builder.with_federated_token_file(federated_token_file);
    }

    let object_store = azure_builder.build().unwrap_or_else(|e| panic!("{}", e));

    // object store handles refreshing bearer token, so we do not need to handle expiry here
    let expire_at = None;

    let object_store = Arc::new(object_store);

    ObjectStoreWithExpiration {
        multipart_store: Some(object_store.clone()),
        object_store,
        expire_at,
    }
}

pub(crate) fn parse_azure_blob_container(uri: &Url) -> Option<String> {
    let host = uri.host_str()?;

    // az(ure)://{container}/key
    if uri.scheme() == "az" || uri.scheme() == "azure" {
        return Some(host.to_string());
    }
    // https://{account}.blob.core.windows.net/{container}/key
    else if host.ends_with(".blob.core.windows.net") {
        let path_segments: Vec<&str> = uri.path_segments()?.collect();

        // Container name is the first part of the path
        return Some(
            path_segments
                .first()
                .expect("unexpected error during parsing azure blob uri")
                .to_string(),
        );
    }

    None
}

// AzureStorageConfig is a struct that holds the configuration that is
// used to configure the Azure Blob Storage object store. object_store does
// not provide a way to read the config files, so we need to read
// them ourselves via rust-ini and azure sdk.
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
    // load reads the azure config from the environment variables first and config files as fallback,
    // or from the section of the profile the session named.
    // There is no proper azure sdk config crate that can read the config files.
    // So, we need to read the config files manually from azure's ini config.
    // See https://learn.microsoft.com/en-us/cli/azure/azure-cli-configuration?view=azure-cli-latest
    fn load(profile: Option<&str>) -> Self {
        // ~/.azure/config
        let azure_config_file_path = std::env::var("AZURE_CONFIG_FILE").unwrap_or(
            home_dir()
                .expect("failed to get home directory")
                .join(".azure")
                .join("config")
                .to_str()
                .expect("failed to convert path to string")
                .to_string(),
        );

        let azure_config_content = Ini::load_from_file(&azure_config_file_path).ok();

        // a profile is a section of the config file of its own, named after it, and the
        // section every session shares is the one with no profile
        let section_name = match profile {
            Some(profile) => format!("{AZURE_CONFIG_SECTION}:{profile}"),
            None => AZURE_CONFIG_SECTION.to_string(),
        };

        let section = azure_config_content
            .as_ref()
            .and_then(|ini| ini.section(Some(section_name.as_str())));

        if profile.is_some() && section.is_none() {
            pgrx::error!(
                "the azure config file \"{azure_config_file_path}\" has no \
                 [{section_name}] section, so the profile names no credentials"
            );
        }

        let from_section = |key: &str| {
            section
                .and_then(|section| section.get(key))
                .map(str::to_string)
        };

        // a profile is read on its own rather than before the environment: the environment
        // is what every session of the server shares, so falling back to it would make a
        // profile whose section is missing a key read the credentials of the server
        let from_env_or_section = |variable: &str, key: &str| match profile {
            Some(_) => from_section(key),
            None => std::env::var(variable).ok().or_else(|| from_section(key)),
        };

        let connection_string =
            from_env_or_section("AZURE_STORAGE_CONNECTION_STRING", "connection_string");

        // connection string overrides everything
        if let Some(connection_string) = connection_string {
            if let Ok(connection_string) = ConnectionString::new(&connection_string) {
                return connection_string.into();
            }
        }

        let account_name = from_env_or_section("AZURE_STORAGE_ACCOUNT", "account");

        let account_key = from_env_or_section("AZURE_STORAGE_KEY", "key");

        let sas_token = from_env_or_section("AZURE_STORAGE_SAS_TOKEN", "sas_token");

        // the settings below are read from the environment for the session that named no
        // profile, which is what they have always been read from, and from the section of
        // the profile for a session that named one
        let from_env_or_profile_section = |variable: &str, key: &str| match profile {
            Some(_) => from_section(key),
            None => std::env::var(variable).ok(),
        };

        // endpoint, object_store specific
        let endpoint = from_env_or_profile_section("AZURE_STORAGE_ENDPOINT", "endpoint");

        // allow http, object_store specific
        let allow_http = from_env_or_profile_section("AZURE_ALLOW_HTTP", "allow_http")
            .map(|allow_http| allow_http.parse().unwrap_or(false))
            .unwrap_or(false);

        // tenant id, object_store specific
        let tenant_id = from_env_or_profile_section("AZURE_TENANT_ID", "tenant_id");

        // client id, object_store specific
        let client_id = from_env_or_profile_section("AZURE_CLIENT_ID", "client_id");

        // client secret, object_store specific
        let client_secret = from_env_or_profile_section("AZURE_CLIENT_SECRET", "client_secret");

        let federated_token_file =
            from_env_or_profile_section("AZURE_FEDERATED_TOKEN_FILE", "federated_token_file");

        AzureStorageConfig {
            account_name,
            account_key,
            sas_token,
            tenant_id,
            client_id,
            client_secret,
            federated_token_file,
            endpoint,
            allow_http,
        }
    }
}

impl From<ConnectionString<'_>> for AzureStorageConfig {
    fn from(connection_string: ConnectionString) -> Self {
        let account_name = connection_string
            .account_name
            .map(|account_name| account_name.to_string());

        let account_key = connection_string
            .account_key
            .map(|account_key| account_key.to_string());

        let sas_token = connection_string.sas.map(|sas| sas.to_string());

        let endpoint = connection_string
            .blob_endpoint
            .map(|blob_endpoint| blob_endpoint.to_string());

        let allow_http = matches!(
            connection_string.default_endpoints_protocol,
            Some(EndpointProtocol::Http)
        );

        AzureStorageConfig {
            account_name,
            account_key,
            sas_token,
            tenant_id: None,
            client_id: None,
            client_secret: None,
            federated_token_file: None,
            endpoint,
            allow_http,
        }
    }
}
