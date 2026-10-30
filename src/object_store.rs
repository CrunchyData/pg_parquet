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

// write_error_message describes an error that failed a write into an object store. It is the
// message of the error, and a sentence about what it is not when the error is the one giving
// up an upload answers with: a writer that could neither finish nor give up the upload it
// started reports the error of giving it up rather than the error that failed the write.
pub(crate) fn write_error_message(error: impl Error + 'static) -> String {
    let mut message = object_store_error_message(error);

    if let Some(hint) = given_up_upload_hint(&message) {
        message.push(' ');
        message.push_str(hint);
    }

    message
}

// given_up_upload_hint describes a write that was answered with the error of giving up an
// upload, which is a DELETE of an upload id: object_store's writer gives the upload it
// started up when the write into it fails, and answers with the error of that abort when the
// abort fails as well, so the error that failed the write is reported nowhere. The error of
// an abort reads as if the copy was removing something, which is worth a sentence since
// what the copy was doing is writing, and the upload it wrote into is still there.
//
// The request is matched in the message since object_store only exposes it there, as the
// types that carry a failed request, RetryError and RequestError, are private to it.
pub(crate) fn given_up_upload_hint(object_store_error_message: &str) -> Option<&'static str> {
    let message = object_store_error_message.to_lowercase();

    if message.contains("performing delete") && message.contains("uploadid=") {
        return Some(
            "(this is the error of giving up the upload the COPY TO had started, which it gave \
             up because the write into it failed first; the error of that write is not reported \
             by the writer of the destination, and the upload is left unfinished, which only the \
             rule of the bucket for unfinished uploads removes)",
        );
    }

    None
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
