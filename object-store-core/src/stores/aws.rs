use std::{sync::Arc, time::SystemTime};

use aws_config::BehaviorVersion;
use aws_credential_types::provider::ProvideCredentials;
use object_store::aws::AmazonS3Builder;
use url::Url;

use super::ObjectStoreWithExpiration;
use crate::client_options::load_client_options;
use crate::runtime::block_on;
use crate::Result;

/// Creates an AmazonS3 object store for the bucket the uri names. It is configured by
/// environment variables, with the aws config files as the fallback. We read the config files
/// through the aws sdk because `object_store` has no way to read them itself. Supported:
/// - AWS_ACCESS_KEY_ID
/// - AWS_SECRET_ACCESS_KEY
/// - AWS_SESSION_TOKEN
/// - AWS_ENDPOINT_URL
/// - AWS_REGION
/// - AWS_SHARED_CREDENTIALS_FILE (env var only)
/// - AWS_CONFIG_FILE (env var only)
/// - AWS_PROFILE (env var only)
/// - AWS_ALLOW_HTTP (env var only, object_store specific)
/// - AWS_<HTTP CLIENT OPTION>, e.g. AWS_TIMEOUT (env var only, object_store specific)
pub fn create_s3_object_store(uri: &Url) -> Result<ObjectStoreWithExpiration> {
    let bucket_name = parse_s3_bucket(uri).ok_or(format!("unsupported s3 uri: {uri}"))?;

    // we do not use builder::from_env() here because not all environment variables have
    // a fallback to the config files
    let mut aws_s3_builder = AmazonS3Builder::new().with_bucket_name(bucket_name);

    let aws_s3_config = AwsS3Config::load()?;

    // http client options, including allow http
    aws_s3_builder =
        aws_s3_builder.with_client_options(load_client_options("AWS", aws_s3_config.allow_http));

    if let Some(access_key_id) = aws_s3_config.access_key_id {
        aws_s3_builder = aws_s3_builder.with_access_key_id(access_key_id);
    }

    if let Some(secret_access_key) = aws_s3_config.secret_access_key {
        aws_s3_builder = aws_s3_builder.with_secret_access_key(secret_access_key);
    }

    if let Some(session_token) = aws_s3_config.session_token {
        aws_s3_builder = aws_s3_builder.with_token(session_token);
    }

    if let Some(endpoint_url) = aws_s3_config.endpoint_url {
        aws_s3_builder = aws_s3_builder.with_endpoint(endpoint_url);
    }

    if let Some(region) = aws_s3_config.region {
        aws_s3_builder = aws_s3_builder.with_region(region);
    }

    let object_store = Arc::new(aws_s3_builder.build().map_err(|e| e.to_string())?);

    Ok(ObjectStoreWithExpiration {
        multipart_store: Some(object_store.clone()),
        object_store,
        expire_at: aws_s3_config.expire_at,
    })
}

/// Recovers the bucket name from the three spellings of an s3 uri.
pub fn parse_s3_bucket(uri: &Url) -> Option<String> {
    let host = uri.host_str()?;

    // s3(a)://{bucket}/key
    if uri.scheme() == "s3" {
        return Some(host.to_string());
    }
    // https://s3.amazonaws.com/{bucket}/key
    else if host == "s3.amazonaws.com" {
        let path_segments: Vec<&str> = uri.path_segments()?.collect();

        // Bucket name is the first part of the path
        return path_segments.first().map(|segment| segment.to_string());
    }
    // https://{bucket}.s3.amazonaws.com/key
    else if host.ends_with(".s3.amazonaws.com") {
        let bucket_name = host.split('.').next()?;
        return Some(bucket_name.to_string());
    }

    None
}

/// The configuration used to build the AmazonS3 object store, resolved from the environment
/// and the aws config files.
struct AwsS3Config {
    region: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    session_token: Option<String>,
    expire_at: Option<SystemTime>,
    endpoint_url: Option<String>,
    allow_http: bool,
}

impl AwsS3Config {
    fn load() -> Result<Self> {
        let allow_http = std::env::var("AWS_ALLOW_HTTP")
            .ok()
            .and_then(|allow_http| allow_http.parse().ok())
            .unwrap_or(false);

        // first tries environment variables and then the config files
        let sdk_config =
            block_on(async { aws_config::defaults(BehaviorVersion::latest()).load().await });

        let mut access_key_id = None;
        let mut secret_access_key = None;
        let mut session_token = None;
        let mut expire_at = None;

        if let Some(credential_provider) = sdk_config.credentials_provider() {
            let credentials = block_on(async { credential_provider.provide_credentials().await })
                .map_err(|e| format!("failed to load aws credentials: {e:?}"))?;

            access_key_id = Some(credentials.access_key_id().to_string());
            secret_access_key = Some(credentials.secret_access_key().to_string());
            session_token = credentials.session_token().map(|t| t.to_string());
            expire_at = credentials.expiry();
        }

        Ok(Self {
            region: sdk_config.region().map(|r| r.as_ref().to_string()),
            access_key_id,
            secret_access_key,
            session_token,
            expire_at,
            endpoint_url: sdk_config.endpoint_url().map(|u| u.to_string()),
            allow_http,
        })
    }
}
