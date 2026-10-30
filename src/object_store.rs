use std::error::Error;

use crate::arrow_parquet::parquet_single_upload::SINGLE_UPLOAD_BUFFER_SIZE;

pub(crate) mod aws;
pub(crate) mod azure;
pub(crate) mod client_options;
pub(crate) mod gcs;
pub(crate) mod http;
pub(crate) mod local_file;
pub(crate) mod object_store_cache;

// object_store_error_message describes an error that came from an object store.
// Both arrow and parquet wrap such errors, which repeats their own prefixes in the
// message, so we rather describe the error of the object store that we find in the
// source chain of the error.
pub(crate) fn object_store_error_message(error: impl Error + 'static) -> String {
    let error: &(dyn Error + 'static) = &error;

    let object_store_error = source_of_type::<object_store::Error>(error);

    let mut message = object_store_error.map_or_else(|| error.to_string(), ToString::to_string);

    if let Some(hint) = proxy_hint(&message) {
        message.push(' ');
        message.push_str(hint);
    }

    if let Some(hint) = object_store_error.and_then(no_multipart_upload_hint) {
        message.push(' ');
        message.push_str(&hint);
    }

    message
}

// proxy_hint describes an error message that carries an html response body. An object
// store answers with xml or json, so an html page means that something else answered,
// a proxy in front of the endpoint for instance, which is worth pointing out since the
// status code of such a response then says nothing about the request we made.
//
// The body is matched in the message since object_store only exposes it there, as the
// types that carry a failed response, RetryError and RequestError, are private to it.
pub(crate) fn proxy_hint(object_store_error_message: &str) -> Option<&'static str> {
    let message = object_store_error_message.to_lowercase();

    if message.contains("<html") || message.contains("<!doctype html") {
        return Some(
            "(the endpoint returned an html error page instead of an error from the object \
             store, so a proxy in front of the endpoint likely rejected the request)",
        );
    }

    None
}

// no_multipart_upload_hint describes the error a store that cannot continue an object into
// an upload of its own answers a copy with, which an `http(s)` destination does: such a
// destination takes a single request, so it holds the whole object in memory and fails once
// the copy outgrows that buffer. The size is what makes this worth a sentence, since the
// same command writing a smaller table succeeds and nothing of the destination says why.
pub(crate) fn no_multipart_upload_hint(error: &object_store::Error) -> Option<String> {
    let object_store::Error::NotImplemented { operation, .. } = error else {
        return None;
    };

    if !operation.contains("put_multipart") {
        return None;
    }

    Some(format!(
        "(the destination is written in a single request, so it takes an object of at most \
         {} MiB; a larger table is written to such a destination as a folder of files, with \
         a smaller file_size_bytes, and each file of that folder is then read back by its \
         own name since a uri pattern does not apply to an http(s) endpoint)",
        SINGLE_UPLOAD_BUFFER_SIZE / 1024 / 1024
    ))
}

// source_of_type returns the first error of the given type in the source chain of an error
fn source_of_type<'a, E: Error + 'static>(error: &'a (dyn Error + 'static)) -> Option<&'a E> {
    let mut error = Some(error);

    while let Some(current_error) = error {
        if let Some(current_error) = current_error.downcast_ref() {
            return Some(current_error);
        }

        error = current_error.source();
    }

    None
}
