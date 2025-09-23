use futures::StreamExt;
use glob::{MatchOptions, Pattern};
use pgrx::{iter::TableIterator, name, pg_extern, pg_schema};

use crate::arrow_parquet::uri_utils::{
    ensure_access_privilege_to_uri, object_store_base_uri, ParsedUriInfo,
};
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
    ensure_access_privilege_to_uri(uri_info, true);

    let base_uri = object_store_base_uri(&uri_info.uri);

    // build the pattern before we start the stream to bail out early
    let pattern = Pattern::try_from(uri_info).unwrap_or_else(|e| {
        panic!("{}", e);
    });

    let copy_from = true;
    let (parquet_object_store, location) = get_or_create_object_store(uri_info, copy_from);

    // prefix is the part of the location that doesn't contain any patterns
    let prefix = location
        .parts()
        .take_while(|part| !part.as_ref().contains('*'))
        .collect();

    // Collect all paths from the list stream
    let mut list_stream = parquet_object_store.list(Some(&prefix));

    let mut paths = vec![];

    PG_BACKEND_TOKIO_RUNTIME.block_on(async {
        while let Some(meta) = list_stream.next().await.transpose().unwrap_or_else(|e| {
            panic!("{}", e);
        }) {
            let path = meta.location.to_string();
            let size = meta.size as _;

            paths.push((path, size));
        }
    });

    // Filter out uris that don't match the pattern
    paths
        .into_iter()
        .filter(|(path, _)| {
            pattern.matches_path_with(
                std::path::Path::new(path),
                MatchOptions {
                    case_sensitive: true,
                    require_literal_separator: true,
                    require_literal_leading_dot: false,
                },
            )
        })
        .map(|(path, size)| {
            (
                std::path::Path::new(&base_uri)
                    .join(path)
                    .to_str()
                    .expect("invalid list uri path")
                    .to_string(),
                size,
            )
        })
        .collect::<Vec<_>>()
}

impl TryFrom<&ParsedUriInfo> for Pattern {
    type Error = String;

    fn try_from(uri_info: &ParsedUriInfo) -> Result<Self, Self::Error> {
        if uri_info.uri.scheme() == "http" || uri_info.uri.scheme() == "https" {
            return Err("list operation on http(s) object stores is not supported".into());
        }

        Self::new(uri_info.path.as_ref()).map_err(|e| e.to_string())
    }
}
