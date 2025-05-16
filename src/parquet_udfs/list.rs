use futures::StreamExt;
use glob::{MatchOptions, Pattern};
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

pub(crate) fn list_uri(uri_info: &ParsedUriInfo) -> Vec<(String, i64)> {
    let uri = uri_info.uri.clone();

    ensure_access_privilege_to_uri(&uri, true);

    let base_uri = uri_info.base_uri();

    let copy_from = true;
    let (parquet_object_store, location) = get_or_create_object_store(uri_info, copy_from);

    // build the pattern before we start the stream to bail out early
    let pattern = Pattern::new(location.as_ref()).unwrap_or_else(|e| {
        panic!("{}", e);
    });

    // prefix is the part of the location that doesn't contain any wildcards
    let prefix = location
        .parts()
        .take_while(|part| !part.as_ref().contains("*") && !part.as_ref().contains("**"))
        .collect();

    // Collect all uris from the list stream
    let mut list_stream = parquet_object_store.list(Some(&prefix));

    let mut uris = vec![];

    PG_BACKEND_TOKIO_RUNTIME.block_on(async {
        while let Some(meta) = list_stream.next().await.transpose().unwrap_or_else(|e| {
            panic!("{}", e);
        }) {
            let uri = meta.location.to_string();
            let size = meta.size as _;

            uris.push((uri, size));
        }
    });

    // Filter out uris that don't match the pattern
    uris.into_iter()
        .filter(|(uri, _)| {
            pattern.matches_path_with(
                std::path::Path::new(uri),
                MatchOptions {
                    case_sensitive: true,
                    require_literal_separator: true,
                    require_literal_leading_dot: false,
                },
            )
        })
        .map(|(uri, size)| {
            (
                std::path::Path::new(&base_uri)
                    .join(uri)
                    .to_str()
                    .expect("invalid list uri path")
                    .to_string(),
                size,
            )
        })
        .collect::<Vec<_>>()
}
