//! `COPY TO '<uri>' WITH (file_size_bytes '...')`: one statement, several files.
//!
//! The uri becomes a folder and the files are written under it, a new one started as soon as the
//! one being written is large enough. Which is the only sane split for a file format: a
//! container has a header and a footer, so a 10 GB table that must come out in 1 GB pieces comes
//! out as ten whole files rather than as ten slices of one.
//!
//! Nothing here is format-specific — a threshold is a byte count, and a name is a name — which
//! is why the option is the base's rather than a provider's, and why a format gets it without
//! implementing anything.

use std::cell::RefCell;
use std::rc::Rc;

/// Below this a file is mostly header and footer, and a batch of rows is already a noticeable
/// fraction of it. The threshold is a *floor* and not a rounding: a file ends up slightly
/// larger, since the batch that crosses the line is written whole.
pub(crate) const MINIMUM_FILE_SIZE_BYTES: i64 = 1024 * 1024;

/// Where the ids of the files under a folder come from.
///
/// A statement that runs in one process numbers its own files, which is the count of the ones
/// it has finished. A parallel `COPY TO` fills one folder from several processes that know
/// nothing of each other, so it installs a claim that hands the ids out of shared memory
/// instead — see [`crate::parallel_copy_to`].
pub(crate) trait FileIdClaim {
    fn claim_file_id(&self) -> i64;
}

thread_local! {
    static INSTALLED_FILE_ID_CLAIM: RefCell<Option<Rc<dyn FileIdClaim>>> =
        const { RefCell::new(None) };
}

/// Makes `claim` the source of every folder file id written in this process until
/// [`uninstall_file_id_claim`].
pub(crate) fn install_file_id_claim(claim: Rc<dyn FileIdClaim>) {
    INSTALLED_FILE_ID_CLAIM.with_borrow_mut(|installed| *installed = Some(claim));
}

pub(crate) fn uninstall_file_id_claim() {
    INSTALLED_FILE_ID_CLAIM.with_borrow_mut(|installed| *installed = None);
}

/// The id to name the next file after: the installed claim's, or `local` when there is none.
pub(crate) fn claim_file_id(local: i64) -> i64 {
    INSTALLED_FILE_ID_CLAIM
        .with_borrow(|installed| installed.as_ref().map(|claim| claim.claim_file_id()))
        .unwrap_or(local)
}

/// A size as a statement may write it: a byte count, or a number with a `kB`, `MB` or `GB` unit.
///
/// The units are binary and the spelling is Postgres' own, as `shared_buffers` takes them,
/// which is what a user writing a `COPY` option is used to.
pub(crate) fn parse_file_size(size: &str) -> Result<i64, String> {
    let size = size.trim().to_ascii_uppercase();

    let digits = size
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(size.len());

    if digits == 0 {
        return Err(format!("file_size_bytes has no number in it: \"{size}\""));
    }

    let (number, unit) = (&size[..digits], size[digits..].trim());

    let multiplier = match unit {
        "" | "B" => 1,
        "KB" => 1024,
        "MB" => 1024 * 1024,
        "GB" => 1024 * 1024 * 1024,
        unit => return Err(format!("file_size_bytes has an unknown unit \"{unit}\"")),
    };

    let bytes = number
        .parse::<i64>()
        .ok()
        .and_then(|number| number.checked_mul(multiplier))
        .ok_or_else(|| format!("file_size_bytes is too large: \"{size}\""))?;

    if bytes < MINIMUM_FILE_SIZE_BYTES {
        return Err(format!(
            "file_size_bytes must be at least 1MB, and {bytes} bytes is less than that"
        ));
    }

    Ok(bytes)
}

/// What to call the `id`th file written under `uri`.
///
/// The names are `data_0`, `data_1`, … in the order they were written, so that a listing reads
/// back in the order the rows came out. The suffix is the one the statement wrote, because a
/// provider is told the name of the file it is writing and may read a format's own meaning out
/// of it — `orders.parquet.zst` is zstd, and a split of it has to stay zstd. A uri with no
/// suffix to copy gets the format's first extension instead.
pub(crate) fn child_uri(uri: &str, extensions: &[String], id: i64) -> String {
    let folder = uri.trim_end_matches('/');

    let name = folder
        .rsplit_once('/')
        .map(|(_, name)| name)
        .unwrap_or(folder);

    let suffix = match name.find('.') {
        Some(dot) => name[dot..].to_string(),
        None => extensions
            .first()
            .map(|extension| format!(".{extension}"))
            .unwrap_or_default(),
    };

    format!("{folder}/data_{id}{suffix}")
}

// no backend needed: a size is parsing and a name is string work
#[cfg(test)]
mod tests {
    use super::{child_uri, parse_file_size};

    #[test]
    fn test_a_file_size_is_parsed_with_its_unit() {
        assert_eq!(parse_file_size("1MB"), Ok(1024 * 1024));
        assert_eq!(parse_file_size(" 2 mb "), Ok(2 * 1024 * 1024));
        assert_eq!(parse_file_size("4096kB"), Ok(4096 * 1024));
        assert_eq!(parse_file_size("1GB"), Ok(1024 * 1024 * 1024));
        assert_eq!(parse_file_size("2097152"), Ok(2097152));
        assert_eq!(parse_file_size("2097152B"), Ok(2097152));

        assert!(parse_file_size("1TB").unwrap_err().contains("unknown unit"));
        assert!(parse_file_size("big").unwrap_err().contains("no number"));
        assert!(parse_file_size("99999999999GB")
            .unwrap_err()
            .contains("too large"));
        assert!(parse_file_size("99999999999999999999")
            .unwrap_err()
            .contains("too large"));
        assert!(parse_file_size("512kB")
            .unwrap_err()
            .contains("at least 1MB"));
    }

    #[test]
    fn test_a_child_keeps_the_suffix_the_statement_wrote() {
        let parquet = &["parquet".to_string()];

        assert_eq!(
            child_uri("s3://bucket/orders.parquet", parquet, 0),
            "s3://bucket/orders.parquet/data_0.parquet"
        );

        // a compound suffix is a format's to interpret, so all of it is kept
        assert_eq!(
            child_uri("/tmp/orders.parquet.zst", parquet, 7),
            "/tmp/orders.parquet.zst/data_7.parquet.zst"
        );

        // nothing to copy: the format's own extension, and a trailing slash is not a suffix
        assert_eq!(
            child_uri("/tmp/orders/", parquet, 1),
            "/tmp/orders/data_1.parquet"
        );

        // a format that claims no extension at all
        assert_eq!(child_uri("/tmp/orders", &[], 2), "/tmp/orders/data_2");
    }
}
