//! Reaching an object store, by calling tier 1 rather than by linking it.
//!
//! This is the consuming half of `pg_object_store`'s rendezvous vtable. The base deliberately
//! does *not* vendor `object-store-core`: a second copy would mean a second tokio runtime, a
//! second credential cache and a second set of connection pools, so a `COPY` to parquet and a
//! `COPY` to avro in the same session would resolve credentials twice. Going through the
//! vtable means one of each per backend no matter how many formats are installed.
//!
//! The ABI and the marshalling are `object-store-client`'s, which tier 1 publishes and every
//! dependent links, so there is one declaration of the struct rather than one per extension.
//! What is left here is the base's own two decisions: how tier 1 is found, and how loud a
//! failure is. Almost everything stops the statement; the few calls a provider's callback can
//! reach hand the message back as a value instead, because an `ereport` inside another
//! extension's frames would longjmp past its cleanup.

use std::ffi::CString;

use object_store_client::Client;
use pgrx::pg_sys::{find_rendezvous_variable, load_file, AsPgCStr};
use pgrx::prelude::*;

use object_store_client::PG_OBJECT_STORE_RENDEZVOUS_KEY;

/// A writer, under the base's name for it. Tier 1 holds the upload; dropping without `finish`
/// aborts it, which is what makes an error in the middle of a `COPY TO` leave no half-written
/// file behind.
pub(crate) use object_store_client::Writer as ClientWriter;

/// Resolves tier 1, loading it if this backend has not yet.
///
/// The load is lazy on purpose: requiring `pg_object_store` in
/// `shared_preload_libraries` would make installing a format extension a restart, and the
/// `requires` line in the control file already guarantees the *extension* is there. What it
/// does not guarantee is that the library is loaded in this particular backend, which is what
/// `load_file` is for — the same thing Postgres' own `CREATE EXTENSION` does for a C function.
fn client() -> Client {
    let key = CString::new(PG_OBJECT_STORE_RENDEZVOUS_KEY).expect("the key has no interior nul");

    let slot = unsafe { find_rendezvous_variable(key.as_ptr()) };

    let mut pointer = unsafe { *slot };

    if pointer.is_null() {
        unsafe { load_file("$libdir/pg_object_store".as_pg_cstr(), false) };
        pointer = unsafe { *slot };
    }

    if pointer.is_null() {
        ereport!(
            PgLogLevel::ERROR,
            PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
            "pg_object_store is not available",
            "pg_arrow_base reads and writes files through pg_object_store. Install it with \
             CREATE EXTENSION pg_object_store, or add it to shared_preload_libraries."
        );
    }

    // a magic or version mismatch: an ABI skew between two loaded libraries is a crash rather
    // than a link error, so it has to become a message while there is still a frame to report
    // it from
    unsafe { Client::from_rendezvous(pointer) }.unwrap_or_else(|message| error!("{message}"))
}

/// Whether the current user may reach a uri in a direction. The individual calls below enforce
/// this themselves inside tier 1, so this is for the case where the answer is needed *before*
/// doing any work — validating a statement, or deciding whether to plan a parallel read.
pub(crate) fn check_privilege(uri: &str, copy_from: bool) -> bool {
    client()
        .check_privilege(uri, copy_from)
        .unwrap_or_else(|message| error!("{message}"))
}

/// Whether tier 1 understands a uri at all, which is how recognition tells a uri only this
/// stack can reach from a file name core `COPY` handles itself. A parse and nothing more: the
/// answer is yes even when the access would be refused, because a refusal is ours to report.
pub(crate) fn recognizes(uri: &str) -> bool {
    client().check_privilege(uri, true).is_ok()
}

/// The size of an object, or `None` if there is none.
pub(crate) fn size(uri: &str) -> Option<i64> {
    client()
        .head(uri)
        .unwrap_or_else(|message| error!("{message}"))
}

/// The whole of an object, or `None` if there is none.
///
/// Two round trips — a stat and then a read — and the whole file in the backend's memory. Both
/// are the v1 whole-buffer limitation, not a property of the layering: the vtable has
/// `get_ranges` precisely so a format that can read a footer and then individual splits does
/// not have to come through here. See `MIGRATION.md`.
pub(crate) fn read_all(uri: &str) -> Option<bytes::Bytes> {
    client()
        .get_all(uri)
        .unwrap_or_else(|message| error!("{message}"))
}

/// The whole of an object, erroring rather than returning `None`. The error is the one almost
/// every caller wants, so it is written once here.
pub(crate) fn read_all_or_error(uri: &str) -> bytes::Bytes {
    read_all(uri).unwrap_or_else(|| {
        ereport!(
            PgLogLevel::ERROR,
            PgSqlErrorCode::ERRCODE_UNDEFINED_FILE,
            format!("there is no file at {uri}"),
            "Check the uri, and that the credentials in the server's environment can see it. \
             object_store.parse_uri() shows how the uri was understood."
        );

        unreachable!("ereport at ERROR does not return");
    })
}

/// Several byte ranges of one object, fetched concurrently inside tier 1.
///
/// Nothing here reports: this is what a provider's `get_ranges` callback ends up calling, and
/// a failure has to travel back through the provider's frames as a value.
pub(crate) fn try_read_ranges(
    uri: &str,
    ranges: &[(i64, i64)],
) -> Result<Vec<bytes::Bytes>, String> {
    client().get_ranges(uri, ranges)
}

/// An object being written incrementally, with the base's reporting policy on top of tier 1's
/// handle: the `try_` calls are the ones a provider's sink callback reaches, and the others
/// stop the statement.
pub(crate) struct Writer {
    writer: Option<ClientWriter>,
}

impl Writer {
    pub(crate) fn open(uri: &str) -> Writer {
        let writer = client().open_writer(uri).unwrap_or_else(|message| {
            ereport!(
                PgLogLevel::ERROR,
                PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
                message
            );

            unreachable!("ereport at ERROR does not return");
        });

        Writer {
            writer: Some(writer),
        }
    }

    /// Appends bytes, reporting a failure as a message rather than an `ereport`. This is the
    /// form a provider's sink callback reaches, since an `ereport` there would longjmp out of
    /// the provider's frames.
    pub(crate) fn try_write(&mut self, data: &[u8]) -> Result<(), String> {
        self.writer
            .as_mut()
            .expect("the writer is taken only by finishing it")
            .write(data)
    }

    /// Appends bytes, stopping the statement on failure.
    pub(crate) fn write(&mut self, data: &[u8]) {
        if let Err(message) = self.try_write(data) {
            error!("{message}");
        }
    }

    /// Completes the upload. The file becomes visible to a reader here and nowhere earlier,
    /// which is why this is a consuming call: there is nothing to do with the writer after.
    pub(crate) fn try_finish(mut self) -> Result<(), String> {
        self.writer
            .take()
            .expect("the writer is taken only by finishing it")
            .finish()
    }

    /// Completes the upload, stopping the statement on failure.
    pub(crate) fn finish(self) {
        if let Err(message) = self.try_finish() {
            error!("{message}");
        }
    }
}

/// Writes a whole object.
///
/// The other half of `read_all`: a provider whose container cannot stream hands back the
/// finished file and this puts it.
pub(crate) fn write_all(uri: &str, data: &[u8]) {
    client()
        .put(uri, data)
        .unwrap_or_else(|message| error!("{message}"))
}

/// The objects under a uri, treated as a prefix.
pub(crate) fn list(uri: &str) -> Vec<(String, i64)> {
    client()
        .list(uri)
        .unwrap_or_else(|message| error!("{message}"))
}

/// The objects one level under a uri, for a question that is about a single directory. Falls
/// back to the recursive form on a tier 1 that publishes no shallow list.
pub(crate) fn list_shallow(uri: &str) -> Vec<(String, i64)> {
    match client().list_shallow(uri) {
        Some(listed) => listed.unwrap_or_else(|message| error!("{message}")),
        None => list(uri),
    }
}

/// The objects one level under a uri, or `None` when they cannot be listed at all -- a store
/// with no listing, e.g. an http one, or a listing that failed. For the questions where not
/// knowing is an answer rather than a failure of its own, which is what a hint on an error path
/// needs.
pub(crate) fn try_list_shallow(uri: &str) -> Option<Vec<(String, i64)>> {
    match client().list_shallow(uri) {
        Some(listed) => listed.ok(),
        None => client().list(uri).ok(),
    }
}
