use futures::StreamExt;
use glob::{MatchOptions, Pattern};
use pgrx::{ereport, iter::TableIterator, name, pg_extern, pg_schema, PgLogLevel, PgSqlErrorCode};

use crate::arrow_parquet::uri_utils::{
    ensure_access_privilege_to_uri, object_store_base_uri, ParsedUriInfo,
};
use crate::block_on;
use crate::object_store::object_store_cache::get_or_create_object_store;
use crate::parquet_copy_hook::copy_cleanup::PART_URI_INFIX;

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

    // Collect the paths that match the pattern from the list stream. Only the matches are
    // kept, rather than the whole listing: the prefix the pattern is listed under is a
    // directory of its own in a bucket that has one, and the whole of a bucket in a pattern
    // that names no directory at all, which is a listing of millions of objects a pattern
    // matching one of them has no use for.
    let mut list_stream = parquet_object_store.list(Some(&prefix));

    let mut matched_uris = vec![];

    block_on(async {
        while let Some(meta) = list_stream.next().await.transpose().unwrap_or_else(|e| {
            panic!("{}", e);
        }) {
            let path = std::path::Path::new(meta.location.as_ref());

            let matches = pattern.matches_path_with(
                path,
                MatchOptions {
                    case_sensitive: true,
                    require_literal_separator: true,
                    require_literal_leading_dot: false,
                },
            );

            if !matches {
                continue;
            }

            let uri = std::path::Path::new(&base_uri)
                .join(path)
                .to_str()
                .expect("invalid list uri path")
                .to_string();

            matched_uris.push((uri, meta.size as i64));
        }
    });

    // a parallel COPY TO writes a part file per range next to its local destination, and the
    // parts of a copy whose backend did not survive it are left there. They hold the rows the
    // copy wrote, so a pattern that covers them reads those rows a second time.
    let (parts, matched_uris): (Vec<_>, Vec<_>) = matched_uris
        .into_iter()
        .partition(|(uri, _)| uri.contains(PART_URI_INFIX));

    report_parts_not_listed(&parts);

    // an object store also holds objects that are no data of their own: the zero byte marker a
    // browser writes for a folder it was asked to create, whose name ends at the separator,
    // and the "_SUCCESS" of a job that wrote the folder. A reader refuses either of them,
    // since a parquet file is its footer at the very least, so a pattern that covers one would
    // fail over it rather than read the files it matched next to it. They hold no row, so they
    // are reported rather than read, the way the parts of a copy are.
    let (empty_files, matched_uris): (Vec<_>, Vec<_>) =
        matched_uris.into_iter().partition(|(_, size)| *size == 0);

    report_empty_files_not_listed(&empty_files);

    matched_uris
}

// report_parts_not_listed describes the part files of a parallel COPY TO that a pattern
// covered. They are not listed, since they are not data, and a copy that is over left them
// behind for the user to remove, which is what the report is for.
fn report_parts_not_listed(parts: &[(String, i64)]) {
    let Some((part, _)) = parts.first() else {
        return;
    };

    ereport!(
        PgLogLevel::WARNING,
        PgSqlErrorCode::ERRCODE_WARNING,
        format!(
            "{} file(s) of a parallel COPY TO are not part of the data the pattern matches",
            parts.len()
        ),
        format!(
            "\"{part}\" and the files named like it hold the ranges of a copy rather than \
             data of their own. A copy that is still running takes its parts with it, and the \
             parts of a copy whose backend did not survive it can be removed."
        )
    );
}

// report_empty_files_not_listed describes the empty objects a pattern covered. They are not
// listed, since a file holding no byte holds no row either, and the report is what tells the
// user that the folder the pattern names holds a marker a reader would have refused.
fn report_empty_files_not_listed(empty_files: &[(String, i64)]) {
    let Some((empty_file, _)) = empty_files.first() else {
        return;
    };

    ereport!(
        PgLogLevel::WARNING,
        PgSqlErrorCode::ERRCODE_WARNING,
        format!(
            "{} empty file(s) that the pattern matches are not read",
            empty_files.len()
        ),
        format!(
            "\"{empty_file}\" holds no byte, and a parquet file is its footer at the very \
             least. The marker an object store browser writes for a folder it creates and the \
             \"_SUCCESS\" of a job that wrote one are empty this way."
        )
    );
}

impl TryFrom<&ParsedUriInfo> for Pattern {
    type Error = String;

    fn try_from(uri_info: &ParsedUriInfo) -> Result<Self, Self::Error> {
        if uri_info.uri.scheme() == "http" || uri_info.uri.scheme() == "https" {
            return Err("list operation on http(s) object stores is not supported".into());
        }

        // "*" and "**" are the pattern syntax of a uri, so the characters the glob crate takes
        // as syntax of its own, a "?" and the "[" and "]" of a class, are escaped into the
        // literals they are. An object whose name holds one of them is then matched as the
        // name it is: a pattern like "[a]*.parquet" reads the object named "[a].parquet"
        // rather than the "a.parquet" next to it, which a class of a single character matched.
        let pattern = uri_info
            .path
            .as_ref()
            .split('*')
            .map(Self::escape)
            .collect::<Vec<_>>()
            .join("*");

        Self::new(&pattern).map_err(|e| e.to_string())
    }
}
