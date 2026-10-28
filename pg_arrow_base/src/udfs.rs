//! The `arrow` schema: the questions about a file that have the same answer whatever format it
//! is in.
//!
//! `pg_parquet` has `parquet.schema`, `parquet.metadata`, `parquet.file_metadata`,
//! `parquet.kv_metadata` and `parquet.list`. Only the last is actually parquet-agnostic today,
//! and these are its generalization: one set of functions, dispatched on the registry, so that
//! installing `pg_avro` gets `arrow.schema('s3://b/x.avro')` for free rather than a second
//! `avro.schema` to learn.
//!
//! What stays format-specific is the detail no other format has: parquet's row group
//! statistics, its bloom filter settings, its column chunk offsets. Those belong in
//! `parquet.*`, and `arrow.file_info` is the common subset deliberately, not accidentally.

use format_registry::registry;
use pgrx::prelude::*;

use crate::batch_reader;
use crate::object_store;
use crate::providers;

#[pgrx::pg_schema]
mod arrow {
    use super::*;

    /// Every format this cluster has installed, the file extensions each claims, and what each
    /// one can do.
    ///
    /// Every installed provider is loaded first, so that this answers what is installed rather
    /// than what this session happens to have used: a format is of no use to somebody who
    /// cannot find out it exists.
    ///
    /// The two flags are not decoration: they are what this extension dispatches on.
    /// `reads_ranges` false means a `COPY FROM` buffers the whole file before decoding it, and
    /// `writes_streaming` false means a `COPY TO` buffers the whole file before uploading it.
    /// Which is to say they are the answer to "why is this statement using so much memory".
    #[pg_extern]
    fn formats() -> TableIterator<
        'static,
        (
            name!(format, String),
            name!(file_extensions, Vec<String>),
            name!(reads_ranges, bool),
            name!(writes_streaming, bool),
        ),
    > {
        providers::load_installed_providers();

        TableIterator::new(registry::registered())
    }

    /// The schema of a file, one row per top-level field.
    ///
    /// Nested fields are reported as the Arrow type of the whole struct or list, which is
    /// faithful: a `struct<a: int32, b: utf8>` is one field of the file.
    #[pg_extern]
    fn schema(
        uri: &str,
        format: default!(Option<&str>, "NULL"),
    ) -> TableIterator<
        'static,
        (
            name!(field_position, i32),
            name!(field_name, String),
            name!(arrow_type, String),
            name!(nullable, bool),
        ),
    > {
        let provider = providers::resolve(format, uri);
        let data = object_store::read_all_or_error(uri);

        let schema = provider
            .schema(&data)
            .unwrap_or_else(|message| error!("{message}"));

        let fields: Vec<_> = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(position, field)| {
                (
                    position as i32 + 1,
                    field.name().clone(),
                    field.data_type().to_string(),
                    field.is_nullable(),
                )
            })
            .collect();

        TableIterator::new(fields)
    }

    /// The file-level key/value metadata. `value` is NULL for a key stored without one, which
    /// parquet permits and which is not the same as an empty string.
    #[pg_extern]
    fn kv_metadata(
        uri: &str,
        format: default!(Option<&str>, "NULL"),
    ) -> TableIterator<'static, (name!(key, String), name!(value, Option<String>))> {
        let provider = providers::resolve(format, uri);
        let data = object_store::read_all_or_error(uri);

        let metadata = provider
            .kv_metadata(&data)
            .unwrap_or_else(|message| error!("{message}"));

        TableIterator::new(metadata)
    }

    /// What the file says about itself. Every column may be NULL: a format is allowed not to
    /// record a row count, or to have no file-level compression to report.
    // the row type is spelled out rather than aliased because pgrx reads the tuple out of the
    // signature to build the SQL
    #[allow(clippy::type_complexity)]
    #[pg_extern]
    fn file_info(
        uri: &str,
        format: default!(Option<&str>, "NULL"),
    ) -> TableIterator<
        'static,
        (
            name!(format, String),
            name!(size_bytes, Option<i64>),
            name!(num_rows, Option<i64>),
            name!(num_splits, Option<i64>),
            name!(split_label, Option<String>),
            name!(compression, Option<String>),
            name!(format_version, Option<String>),
        ),
    > {
        let provider = providers::resolve(format, uri);

        let size_bytes = object_store::size(uri);
        let data = object_store::read_all_or_error(uri);

        let info = provider
            .file_info(&data)
            .unwrap_or_else(|message| error!("{message}"));

        TableIterator::once((
            provider.name(),
            size_bytes,
            info.num_rows,
            info.num_splits,
            info.split_label,
            info.compression,
            info.format_version,
        ))
    }

    /// The files under a uri, with the format each would be read as.
    ///
    /// A uri with a `*` in it is the pattern a `COPY FROM` would expand, and the files listed
    /// are the ones that `COPY` would read; anything else is a prefix. `format` is NULL for a
    /// file whose extension nothing installed claims, which is the useful part: it is how to
    /// see that half a prefix is parquet and half of it is something that cannot be read here.
    #[pg_extern]
    fn list(
        uri: &str,
    ) -> TableIterator<
        'static,
        (
            name!(location, String),
            name!(size_bytes, i64),
            name!(format, Option<String>),
        ),
    > {
        providers::load_installed_providers();

        let listed = if uri.contains('*') {
            // tier 1 knows nothing about patterns and would take this one as a literal prefix,
            // so a listing of the uri a COPY FROM reads 3000 files from reported nothing
            batch_reader::matching_objects(uri).unwrap_or_else(|| {
                ereport!(
                    PgLogLevel::ERROR,
                    PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
                    format!("{uri} is not a valid pattern"),
                    "A * matches within one path segment; ** matches across segments."
                );

                unreachable!("ereport at ERROR does not return");
            })
        } else {
            object_store::list(uri)
        };

        let objects: Vec<_> = listed
            .into_iter()
            .map(|(location, size)| {
                let format = registry::lookup_by_path(&location).map(|provider| provider.name());

                (location, size, format)
            })
            .collect();

        TableIterator::new(objects)
    }
}
