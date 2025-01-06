use home::home_dir;
use ini::Ini;
use object_store::azure::{AzureConfigKey, MicrosoftAzure, MicrosoftAzureBuilder};
use url::Url;

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
pub(crate) fn create_azure_object_store(uri: &Url) -> MicrosoftAzure {
    let container_name = parse_azure_blob_container(uri).unwrap_or_else(|| {
        panic!("unsupported azure blob storage uri: {}", uri);
    });

    let mut azure_builder = MicrosoftAzureBuilder::new().with_container_name(container_name);

    let azure_blob_config = AzureStorageConfig::load();

    // account name
    if let Some(account_name) = azure_blob_config.account_name {
        azure_builder = azure_builder.with_account(account_name);
    }

    // account key
    if let Some(account_key) = azure_blob_config.account_key {
        azure_builder = azure_builder.with_access_key(account_key);
    }

    // sas token
    if let Some(sas_token) = azure_blob_config.sas_token {
        azure_builder = azure_builder.with_config(AzureConfigKey::SasKey, sas_token);
    }

    // allow http
    azure_builder = azure_builder.with_allow_http(azure_blob_config.allow_http);

    // endpoint
    if let Some(endpoint) = azure_blob_config.endpoint {
        azure_builder = azure_builder.with_endpoint(endpoint);
    }

    azure_builder.build().unwrap_or_else(|e| panic!("{}", e))
}

fn parse_azure_blob_container(uri: &Url) -> Option<String> {
    let host = uri.host_str()?;

    // az(ure)://{container}/key
    if uri.scheme() == "az" || uri.scheme() == "azure" {
        return Some(host.to_string());
    }
    // https://{account}.blob.core.windows.net/{container}
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

// AzureStorageConfig represents the configuration for Azure Blob Storage.
// There is no proper azure sdk config crate that can read the config files.
// So, we need to read the config files manually from azure's ini config.
// See https://learn.microsoft.com/en-us/cli/azure/azure-cli-configuration?view=azure-cli-latest
struct AzureStorageConfig {
    account_name: Option<String>,
    account_key: Option<String>,
    sas_token: Option<String>,
    endpoint: Option<String>,
    allow_http: bool,
}

impl AzureStorageConfig {
    // load reads the azure config from the environment variables first and config files as fallback.
    fn load() -> Self {
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

        // connection string
        let connection_string = match std::env::var("AZURE_STORAGE_CONNECTION_STRING") {
            Ok(connection_string) => Some(connection_string),
            Err(_) => azure_config_content
                .as_ref()
                .and_then(|ini| ini.section(Some("storage")))
                .and_then(|section| section.get("connection_string"))
                .map(|connection_string| connection_string.to_string()),
        };

        // connection string has the highest priority
        if let Some(connection_string) = connection_string {
            return Self::from_connection_string(&connection_string);
        }

        // account name
        let account_name = match std::env::var("AZURE_STORAGE_ACCOUNT") {
            Ok(account) => Some(account),
            Err(_) => azure_config_content
                .as_ref()
                .and_then(|ini| ini.section(Some("storage")))
                .and_then(|section| section.get("account"))
                .map(|account| account.to_string()),
        };

        // account key
        let account_key = match std::env::var("AZURE_STORAGE_KEY") {
            Ok(key) => Some(key),
            Err(_) => azure_config_content
                .as_ref()
                .and_then(|ini| ini.section(Some("storage")))
                .and_then(|section| section.get("key"))
                .map(|key| key.to_string()),
        };

        // sas token
        let sas_token = match std::env::var("AZURE_STORAGE_SAS_TOKEN") {
            Ok(token) => Some(token),
            Err(_) => azure_config_content
                .as_ref()
                .and_then(|ini| ini.section(Some("storage")))
                .and_then(|section| section.get("sas_token"))
                .map(|token| token.to_string()),
        };

        // endpoint, object_store specific
        let endpoint = std::env::var("AZURE_STORAGE_ENDPOINT").ok();

        // allow http, object_store specific
        let allow_http = std::env::var("AZURE_ALLOW_HTTP")
            .ok()
            .map(|allow_http| allow_http.parse().unwrap_or(false))
            .unwrap_or(false);

        AzureStorageConfig {
            account_name,
            account_key,
            sas_token,
            endpoint,
            allow_http,
        }
    }

    // from_connection_string parses AzureBlobConfig from the given connection string.
    // See https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string#create-a-connection-string-for-an-explicit-storage-endpoint
    fn from_connection_string(connection_string: &str) -> Self {
        let mut account_name = None;
        let mut account_key = None;
        let mut sas_token = None;
        let mut endpoint = None;
        let mut allow_http = false;

        for pair in connection_string.trim_end_matches(';').split(';') {
            let (key, value) = pair
                .split_once('=')
                .expect("invalid azure connection string format");

            match key {
                "AccountName" => account_name = Some(value.to_string()),
                "AccountKey" => account_key = Some(value.to_string()),
                "SharedAccessSignature" => sas_token = Some(value.to_string()),
                "BlobEndpoint" => endpoint = Some(value.to_string()),
                "DefaultEndpointsProtocol" => {
                    allow_http = value.to_lowercase() == "http";
                }
                _ => {
                    panic!("unsupported config key in azure connection string: {}", key);
                }
            }
        }

        AzureStorageConfig {
            account_name,
            account_key,
            sas_token,
            endpoint,
            allow_http,
        }
    }
}
