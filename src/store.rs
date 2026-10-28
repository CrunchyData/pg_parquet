//! Reading a parquet footer from a uri, through tier 1.
//!
//! `COPY` does not come through here — the base serves that, and what it hands this extension
//! is bytes. What is left needing a uri of its own is the parquet-only UDFs: `parquet.metadata`,
//! `parquet.file_metadata`, `parquet.schema` and `parquet.column_stats` all answer questions
//! about a file the caller names, and all of them are answered from the footer rather than from
//! the data.
//!
//! So this is small on purpose. It resolves `pg_object_store`'s vtable, which is the one
//! credential cache, client pool and tokio runtime in the backend, and reads ranges through it.
//! The marshalling is `object-store-client`'s and the footer parsing is upstream parquet's over
//! `parquet_format::SourceChunkReader` — the same adapter the provider's ranged reader uses, so
//! a UDF reads a file exactly the way a `COPY FROM` does.

use std::ffi::CString;
use std::sync::Arc;

use bytes::Bytes;
use format_registry::provider::ByteSource;
use format_registry::Result as ProviderResult;
use object_store_client::{Client, PG_OBJECT_STORE_RENDEZVOUS_KEY};
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use parquet::schema::types::SchemaDescriptor;
use parquet_format::source::SourceChunkReader;
use pgrx::pg_sys::{find_rendezvous_variable, load_file, AsPgCStr};
use pgrx::prelude::*;

/// Resolves tier 1, loading it if this backend has not yet.
///
/// Lazy on purpose: requiring `pg_object_store` in `shared_preload_libraries` would make
/// installing this extension a restart, and the `requires` line in the control file already
/// guarantees the *extension* is there. What it does not guarantee is that the library is
/// loaded in this particular backend, which is what `load_file` is for.
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
            "pg_parquet reads files through pg_object_store. Install it with CREATE EXTENSION \
             pg_object_store, or add it to shared_preload_libraries."
        );
    }

    unsafe { Client::from_rendezvous(pointer) }.unwrap_or_else(|message| error!("{message}"))
}

/// Refuses the statement unless the current user may read the uri.
///
/// Every read below is checked inside tier 1 too. This is for saying so before any work is
/// done, which is what a UDF wants: the message names the uri rather than a range of it.
pub(crate) fn ensure_read_privilege(uri: &str) {
    let copy_from = true;

    let allowed = client()
        .check_privilege(uri, copy_from)
        .unwrap_or_else(|message| error!("{message}"));

    if !allowed {
        ereport!(
            PgLogLevel::ERROR,
            PgSqlErrorCode::ERRCODE_INSUFFICIENT_PRIVILEGE,
            format!("permission denied to read {uri}"),
            "Reading a uri needs the object_store_read role, or pg_read_server_files for a \
             path on the server. Both are pg_object_store's."
        );
    }
}

/// A file at a uri, read in pieces through tier 1.
///
/// Holds the size it had when the read started rather than a client: the client is tier 1's and
/// is shared with every other format in the backend, and a parquet reader asks for the size
/// more than once.
struct UriSource {
    uri: String,
    size: i64,
}

impl ByteSource for UriSource {
    fn size(&self) -> ProviderResult<i64> {
        Ok(self.size)
    }

    fn get_ranges(&self, ranges: &[(i64, i64)]) -> ProviderResult<Vec<Bytes>> {
        if ranges.is_empty() {
            return Ok(Vec::new());
        }

        // one call for all of them: tier 1 fetches them concurrently, and a loop over the
        // singular form here would serialize a read upstream made parallel
        client().get_ranges(&self.uri, ranges)
    }
}

/// The file at a uri as something upstream parquet can read, privilege checked.
fn chunk_reader(uri: &str) -> SourceChunkReader {
    ensure_read_privilege(uri);

    let size = client()
        .head(uri)
        .unwrap_or_else(|message| error!("{message}"))
        .unwrap_or_else(|| {
            ereport!(
                PgLogLevel::ERROR,
                PgSqlErrorCode::ERRCODE_UNDEFINED_FILE,
                format!("there is no file at {uri}"),
                "Check the uri, and that the credentials in the server's environment can see \
                 it. object_store.parse_uri() shows how the uri was understood."
            );

            unreachable!("ereport at ERROR does not return");
        });

    let source = Arc::new(UriSource {
        uri: uri.to_string(),
        size,
    });

    SourceChunkReader::new(source).unwrap_or_else(|message| error!("{message}"))
}

/// The whole footer of the file at a uri: row groups, column chunks, statistics, key-value
/// metadata. Two or three round trips rather than a read of the file, which is the point of
/// doing this over ranges.
pub(crate) fn parquet_metadata_from_uri(uri: &str) -> Arc<ParquetMetaData> {
    let reader = chunk_reader(uri);

    // no page index: a UDF reports row group and column chunk metadata, and the index is a
    // read per column chunk on top of the footer
    let metadata = ParquetMetaDataReader::new()
        .parse_and_finish(&reader)
        .unwrap_or_else(|e| error!("could not read the parquet footer of {uri}: {e}"));

    Arc::new(metadata)
}

/// The parquet schema of the file at a uri, as the file itself describes it — physical types,
/// repetition and converted types included, which is what makes this different from
/// `arrow.schema`.
pub(crate) fn parquet_schema_from_uri(uri: &str) -> Arc<SchemaDescriptor> {
    parquet_metadata_from_uri(uri)
        .file_metadata()
        .schema_descr_ptr()
}
