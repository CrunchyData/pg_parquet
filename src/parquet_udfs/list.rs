use futures::StreamExt;
use glob::{MatchOptions, Pattern};
use object_store::path::Path;
use pgrx::{iter::TableIterator, name, pg_extern, pg_schema};

use crate::arrow_parquet::uri_utils::{ensure_access_privilege_to_uri, ParsedUriInfo};
use crate::object_store::object_store_cache::get_or_create_object_store;
use crate::PG_BACKEND_TOKIO_RUNTIME;

#[pg_schema]
mod parquet {
    use super::*;

    #[pg_extern]
    fn list(uri: String) -> TableIterator<'static, (name!(uri, String), name!(size, i64))> {
        let uri_info = ParsedUriInfo::try_from(uri.as_str()).unwrap_or_else(|e| {
            panic!("{}", e.to_string());
        });

        TableIterator::new(list_uri(&uri_info))
    }
}

fn prefix_from_pattern_path(pattern_path: &Path) -> Path {
    pattern_path
        .parts()
        .take_while(|part| !part.as_ref().contains("*") && !part.as_ref().contains("**"))
        .collect()
}

fn pattern_from_pattern_path(pattern_path: &Path) -> Pattern {
    Pattern::new(pattern_path.as_ref()).unwrap_or_else(|e| {
        panic!("{}", e);
    })
}

pub(crate) fn list_uri(uri_info: &ParsedUriInfo) -> Vec<(String, i64)> {
    let uri = uri_info.uri.clone();

    ensure_access_privilege_to_uri(&uri, true);

    let base_uri = uri_info.base_uri();

    let copy_from = true;
    let (parquet_object_store, location) = get_or_create_object_store(uri_info, copy_from);

    let prefix_location = prefix_from_pattern_path(&location);

    let pattern = pattern_from_pattern_path(&location);

    let mut objects_stream = parquet_object_store.list(Some(&prefix_location));

    let mut objects = vec![];

    // Collect all objects from the stream
    PG_BACKEND_TOKIO_RUNTIME.block_on(async {
        while let Some(meta) = objects_stream.next().await.transpose().unwrap_or_else(|e| {
            panic!("{}", e);
        }) {
            objects.push((meta.location.to_string(), meta.size as _));
        }
    });

    // Filter out objects that don't match the pattern
    objects
        .into_iter()
        .filter(|(object_uri, _)| {
            pattern.matches_path_with(
                std::path::Path::new(object_uri),
                MatchOptions {
                    case_sensitive: true,
                    require_literal_separator: true,
                    require_literal_leading_dot: false,
                },
            )
        })
        .map(|(object_uri, size)| {
            (
                std::path::Path::new(&base_uri)
                    .join(object_uri)
                    .to_str()
                    .expect("invalid path")
                    .to_string(),
                size,
            )
        })
        .collect::<Vec<_>>()
}
