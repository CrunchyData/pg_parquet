use aws_config::BehaviorVersion;
use aws_sdk_sts::config::ProvideCredentials;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use url::Url;

use super::PG_BACKEND_TOKIO_RUNTIME;

// create_s3_object_store creates an AmazonS3 object store with the given bucket name.
// It is configured by environment variables and aws config files as fallback method.
// We need to read the config files to make the fallback method work since object_store
// does not provide a way to read them. Currently, we only support following environment
// variables and config parameters:
// - AWS_ACCESS_KEY_ID
// - AWS_SECRET_ACCESS_KEY
// - AWS_SESSION_TOKEN
// - AWS_ENDPOINT_URL
// - AWS_REGION
// - AWS_SHARED_CREDENTIALS_FILE (env var only)
// - AWS_CONFIG_FILE (env var only)
// - AWS_PROFILE (env var only)
// - AWS_ALLOW_HTTP (env var only, object_store specific)
pub(crate) fn create_s3_object_store(uri: &Url) -> AmazonS3 {
    let bucket_name = parse_s3_bucket(uri).unwrap_or_else(|| {
        panic!("unsupported s3 uri: {}", uri);
    });

    // we do not use builder::from_env() here because not all environment variables have
    // a fallback to the config files
    let mut aws_s3_builder = AmazonS3Builder::new().with_bucket_name(bucket_name);

    if let Ok(allow_http) = std::env::var("AWS_ALLOW_HTTP") {
        aws_s3_builder = aws_s3_builder.with_allow_http(allow_http.parse().unwrap_or(false));
    }

    // first tries environment variables and then the config files
    let sdk_config = PG_BACKEND_TOKIO_RUNTIME.block_on(async {
        aws_config::defaults(BehaviorVersion::v2024_03_28())
            .load()
            .await
    });

    if let Some(credential_provider) = sdk_config.credentials_provider() {
        if let Ok(credentials) = PG_BACKEND_TOKIO_RUNTIME
            .block_on(async { credential_provider.provide_credentials().await })
        {
            // AWS_ACCESS_KEY_ID
            aws_s3_builder = aws_s3_builder.with_access_key_id(credentials.access_key_id());

            // AWS_SECRET_ACCESS_KEY
            aws_s3_builder = aws_s3_builder.with_secret_access_key(credentials.secret_access_key());

            if let Some(token) = credentials.session_token() {
                // AWS_SESSION_TOKEN
                aws_s3_builder = aws_s3_builder.with_token(token);
            }
        }
    }

    // AWS_ENDPOINT_URL
    if let Some(aws_endpoint_url) = sdk_config.endpoint_url() {
        aws_s3_builder = aws_s3_builder.with_endpoint(aws_endpoint_url);
    }

    // AWS_REGION
    if let Some(aws_region) = sdk_config.region() {
        aws_s3_builder = aws_s3_builder.with_region(aws_region.as_ref());
    }

    aws_s3_builder.build().unwrap_or_else(|e| panic!("{}", e))
}

fn parse_s3_bucket(uri: &Url) -> Option<String> {
    let host = uri.host_str()?;

    // s3(a)://{bucket}/key
    if uri.scheme() == "s3" {
        return Some(host.to_string());
    }
    // https://s3.amazonaws.com/{bucket}/key
    else if host == "s3.amazonaws.com" {
        let path_segments: Vec<&str> = uri.path_segments()?.collect();

        // Bucket name is the first part of the path
        return Some(
            path_segments
                .first()
                .expect("unexpected error during parsing s3 uri")
                .to_string(),
        );
    }
    // https://{bucket}.s3.amazonaws.com/key
    else if host.ends_with(".s3.amazonaws.com") {
        let bucket_name = host.split('.').next()?;
        return Some(bucket_name.to_string());
    }

    None
}
