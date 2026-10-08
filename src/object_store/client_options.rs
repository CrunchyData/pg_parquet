use std::str::FromStr;

use object_store::{ClientConfigKey, ClientOptions};

// load_client_options collects the http client options of an object store from the
// environment, e.g. AWS_TIMEOUT=5m or AZURE_PROXY_URL=http://localhost:3128. These are
// the same variables that object_store's own builder::from_env() would pick up, which
// we cannot use since not all of its configuration has a fallback to the config files.
// Variables that do not name an http client option are ignored, as object_store does.
pub(crate) fn load_client_options(env_prefix: &str, allow_http: bool) -> ClientOptions {
    let mut client_options = ClientOptions::new().with_allow_http(allow_http);

    let env_prefix = format!("{env_prefix}_");

    for (env_var, value) in std::env::vars() {
        let Some(option) = env_var.strip_prefix(&env_prefix) else {
            continue;
        };

        let Ok(config_key) = ClientConfigKey::from_str(&option.to_lowercase()) else {
            continue;
        };

        // "<prefix>_ALLOW_HTTP" is already handled by the caller, which parses it the
        // same way for all object stores, so we do not let object_store parse it again
        if matches!(config_key, ClientConfigKey::AllowHttp) {
            continue;
        }

        client_options = client_options.with_config(config_key, value);
    }

    client_options
}
