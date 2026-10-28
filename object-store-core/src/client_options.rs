use std::str::FromStr;

use object_store::{ClientConfigKey, ClientOptions};

/// Lets every `object_store` http client option be set from the environment, under a
/// per-vendor prefix: `AWS_TIMEOUT`, `AZURE_CONNECT_TIMEOUT`, `GOOGLE_USER_AGENT` and so on.
/// Anything that is not a client option is ignored, which is what makes this safe to run over
/// the whole environment.
///
/// `allow_http` is passed in rather than read here because each vendor spells its own env var
/// for it, and a connection string can decide it too.
pub fn load_client_options(env_prefix: &str, allow_http: bool) -> ClientOptions {
    let mut client_options = ClientOptions::new().with_allow_http(allow_http);

    let prefix = format!("{env_prefix}_");

    for (key, value) in std::env::vars() {
        let Some(option) = key.strip_prefix(&prefix) else {
            continue;
        };

        let Ok(config_key) = ClientConfigKey::from_str(&option.to_lowercase()) else {
            continue;
        };

        // allow_http is the caller's decision, see above
        if matches!(config_key, ClientConfigKey::AllowHttp) {
            continue;
        }

        client_options = client_options.with_config(config_key, value);
    }

    client_options
}

#[cfg(test)]
mod tests {
    use super::*;

    // one test rather than three, because every case here mutates the process environment and
    // cargo runs tests of a crate in threads of one process
    #[test]
    fn options_are_collected_per_prefix() {
        std::env::set_var("PGOSTEST_TIMEOUT", "5m");
        std::env::set_var("PGOSTEST_CONNECT_TIMEOUT", "1m");

        // belongs to another object store
        std::env::set_var("OTHERSTORE_READ_TIMEOUT", "2m");

        // does not name an http client option
        std::env::set_var("PGOSTEST_NOT_A_CLIENT_OPTION", "3m");

        let client_options = load_client_options("PGOSTEST", false);

        assert_eq!(
            client_options.get_config_value(&ClientConfigKey::Timeout),
            Some("5m".into())
        );
        assert_eq!(
            client_options.get_config_value(&ClientConfigKey::ConnectTimeout),
            Some("1m".into())
        );
        assert_eq!(
            client_options.get_config_value(&ClientConfigKey::ReadTimeout),
            None,
            "another store's prefix is not this store's option"
        );

        // allow_http is the caller's to decide: each vendor spells its own env var for it, so
        // the one in the environment must not win over the argument
        std::env::set_var("PGOSTEST_ALLOW_HTTP", "no");

        let client_options = load_client_options("PGOSTEST", true);

        assert_eq!(
            client_options.get_config_value(&ClientConfigKey::AllowHttp),
            Some("true".into())
        );

        for key in [
            "PGOSTEST_TIMEOUT",
            "PGOSTEST_CONNECT_TIMEOUT",
            "OTHERSTORE_READ_TIMEOUT",
            "PGOSTEST_NOT_A_CLIENT_OPTION",
            "PGOSTEST_ALLOW_HTTP",
        ] {
            std::env::remove_var(key);
        }
    }
}
