use std::sync::Arc;

use object_store::http::HttpBuilder;
use url::Url;

use super::ObjectStoreWithExpiration;
use crate::client_options::load_client_options;
use crate::uri::base_uri;
use crate::Result;

/// Creates an http(s) object store rooted at the uri's scheme://host:port.
pub fn create_http_object_store(uri: &Url) -> Result<ObjectStoreWithExpiration> {
    let base_uri = parse_http_base_uri(uri).ok_or(format!("unsupported http uri: {uri}"))?;

    let allow_http = std::env::var("ALLOW_HTTP").is_ok();

    if let Some(refusal) = cleartext_refusal(uri, allow_http) {
        return Err(refusal);
    }

    // http client options, e.g. HTTP_TIMEOUT
    let client_options = load_client_options("HTTP", allow_http).with_allow_http2();

    let object_store = HttpBuilder::new()
        .with_url(base_uri)
        .with_client_options(client_options)
        .build()
        .map_err(|e| e.to_string())?;

    Ok(ObjectStoreWithExpiration {
        object_store: Arc::new(object_store),
        // an http store has no multipart upload
        multipart_store: None,
        expire_at: None,
    })
}

/// Why a uri cannot be reached as written, if it cannot.
///
/// The client refuses a cleartext url of its own accord, at request time, with a "builder
/// error" that names neither the decision nor where it is configured — so the refusal is made
/// here instead, while there is still something to say about it.
fn cleartext_refusal(uri: &Url, allow_http: bool) -> Option<String> {
    (uri.scheme() == "http" && !allow_http).then(|| {
        format!(
            "cannot reach {uri} over cleartext http. Use https, or set ALLOW_HTTP in the \
             server's environment to allow it."
        )
    })
}

/// For an http store the "bucket" is the base uri, since that is what a client is rooted at.
pub fn parse_http_base_uri(uri: &Url) -> Option<String> {
    base_uri(uri).ok()
}

#[cfg(test)]
mod tests {
    use super::cleartext_refusal;
    use url::Url;

    #[test]
    fn test_a_cleartext_uri_is_refused_by_name() {
        let cleartext = Url::parse("http://example.com/data.parquet").expect("a valid url");

        assert_eq!(
            cleartext_refusal(&cleartext, false).as_deref(),
            Some(
                "cannot reach http://example.com/data.parquet over cleartext http. Use https, \
                 or set ALLOW_HTTP in the server's environment to allow it."
            )
        );

        // allowed by the environment, and https never needed allowing
        assert_eq!(cleartext_refusal(&cleartext, true), None);

        let secure = Url::parse("https://example.com/data.parquet").expect("a valid url");

        assert_eq!(cleartext_refusal(&secure, false), None);
    }
}
