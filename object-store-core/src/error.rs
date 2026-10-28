use std::error::Error;

/// Digs the underlying `object_store::Error` out of an error's source chain and renders it,
/// since the wrappers above it ("Generic HTTP error", "External error") say nothing useful on
/// their own. A hint about proxies is appended when the response body looks like one.
pub fn object_store_error_message(error: impl Error + 'static) -> String {
    let message = match source_of_type::<object_store::Error>(&error) {
        Some(object_store_error) => object_store_error.to_string(),
        None => error.to_string(),
    };

    match proxy_hint(&message) {
        Some(hint) => format!("{message}. {hint}"),
        None => message,
    }
}

/// An object store that answers with an HTML page is almost never the object store: it is a
/// captive portal or a corporate proxy in front of it. The error that comes back is a parse
/// failure deep in the client, which is unactionable, so we name the likely cause.
pub fn proxy_hint(message: &str) -> Option<String> {
    let lowercased = message.to_lowercase();

    if lowercased.contains("<html") || lowercased.contains("<!doctype html") {
        return Some(
            "The response body is an HTML page, which suggests a proxy answered instead of the \
             object store. Check the HTTP(S)_PROXY and NO_PROXY environment variables of the \
             Postgres server process."
                .to_string(),
        );
    }

    None
}

/// Walks an error's source chain looking for a concrete error type.
pub fn source_of_type<'a, E: Error + 'static>(error: &'a (dyn Error + 'static)) -> Option<&'a E> {
    let mut source = Some(error);

    while let Some(current) = source {
        if let Some(downcasted) = current.downcast_ref::<E>() {
            return Some(downcasted);
        }

        source = current.source();
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Stands in for whatever a caller wraps an `object_store::Error` in — arrow and parquet
    /// both nest their own error around it, more than one layer deep.
    #[derive(Debug)]
    struct Wrapper(Box<dyn Error + Send + Sync + 'static>);

    impl std::fmt::Display for Wrapper {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "External error: {}", self.0)
        }
    }

    impl Error for Wrapper {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            Some(self.0.as_ref())
        }
    }

    #[test]
    fn unwraps_a_wrapped_object_store_error() {
        let object_store_error = object_store::Error::Generic {
            store: "S3",
            source: "404 Not Found".into(),
        };
        let expected = object_store_error.to_string();

        let error = Wrapper(Box::new(Wrapper(Box::new(object_store_error))));

        assert_eq!(object_store_error_message(error), expected);
    }

    #[test]
    fn keeps_an_error_that_is_not_an_object_store_error() {
        let error = Wrapper(Box::new(std::io::Error::other("not a store error")));
        let expected = error.to_string();

        assert_eq!(object_store_error_message(error), expected);
    }

    #[test]
    fn hints_at_a_proxy_only_for_an_html_body() {
        // the page haproxy answers a request it rejects with
        assert!(proxy_hint(
            "Generic S3 error: Server returned non-2xx status code: 400 Bad Request: \
             <html><body><h1>400 Bad request</h1></body></html>"
        )
        .is_some());

        assert!(proxy_hint("Error after 0 retries: <!DOCTYPE html><html></html>").is_some());

        // the object store's own error, which is XML
        assert!(proxy_hint(
            "Generic S3 error: Server returned non-2xx status code: 404 Not Found: \
             <?xml version=\"1.0\"?><Error><Code>NoSuchKey</Code></Error>"
        )
        .is_none());
    }

    #[test]
    fn the_hint_is_appended_to_the_unwrapped_message() {
        let error = Wrapper(Box::new(object_store::Error::Generic {
            store: "S3",
            source: "Server returned non-2xx status code: 400 Bad Request: \
                     <html><body><h1>400 Bad request</h1></body></html>"
                .into(),
        }));

        let message = object_store_error_message(error);

        assert!(message.starts_with("Generic S3 error: "), "{message}");
        assert!(
            message.contains("a proxy answered instead of the object store"),
            "{message}"
        );
    }
}
