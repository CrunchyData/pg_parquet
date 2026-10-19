use object_store::{path::Path, ObjectStoreScheme};
use url::Url;

use crate::stores::{
    aws::parse_s3_bucket, azure::parse_azure_blob_container, gcs::parse_gcs_bucket,
    http::parse_http_base_uri,
};
use crate::Result;

/// The schemes this crate can reach, spelled the way a user would be told about them. Used in
/// every "unsupported scheme" message so they stay in one place.
pub const SUPPORTED_SCHEMES: &str = "local paths, https://, s3://, az:// or gs:// schemes";

/// Every spelling of those schemes, which is how a uri we can reach is told from one we cannot
/// when all we have is a failure to parse it.
const REACHABLE_SCHEMES: &[&str] = &[
    "file", "s3", "s3a", "gs", "az", "azure", "adl", "abfs", "abfss", "http", "https",
];

/// A uri resolved far enough to open a store against it: which kind of store, which bucket (or
/// container, or http base uri) it lives in, and the path of the object inside that bucket.
///
/// This is the Postgres-free half of what `pg_parquet` called `ParsedUriInfo`. The other half —
/// the temporary file behind `COPY ... TO STDOUT` and the pipe behind `COPY ... TO PROGRAM` —
/// is not about reaching an object store at all and stays with the COPY implementation.
#[derive(Debug, Clone)]
pub struct ParsedUri {
    pub uri: Url,
    pub bucket: Option<String>,
    pub path: Path,
    pub scheme: ObjectStoreScheme,
}

impl ParsedUri {
    /// True when the uri names a path on the server's own filesystem rather than a remote
    /// store. Callers need this because it decides which role governs access.
    pub fn is_local(&self) -> bool {
        matches!(self.scheme, ObjectStoreScheme::Local)
    }

    /// Renders the path of a listed object back as a uri in the form the prefix was written in,
    /// so that a listing can be fed straight back to a read. An `object_store` path is relative
    /// to the store's root and carries no scheme and no bucket, and for a local path not even
    /// the leading slash, so it is not a uri on its own.
    ///
    /// The prefix is rewritten rather than rebuilt from the scheme and the bucket, because more
    /// than one uri names the same bucket — `az://container/key` and
    /// `https://account.blob.core.windows.net/container/key` among them — and the caller should
    /// get back the one it asked with.
    pub fn location_as_uri(&self, location: &Path) -> Result<String> {
        let location = location.as_ref();

        let relative = location
            .strip_prefix(self.path.as_ref())
            .unwrap_or(location)
            .trim_start_matches('/');

        let prefix = uri_as_string(&self.uri)?;
        let prefix = prefix.trim_end_matches('/');

        if relative.is_empty() {
            return Ok(prefix.to_string());
        }

        Ok(format!("{prefix}/{relative}"))
    }

    fn try_parse_uri(uri: &str) -> Result<Url> {
        if !uri.contains("://") {
            // a bare path is a local file
            let path = resolve_parent_segments(uri)?;

            Url::from_file_path(&path).map_err(|_| format!("not a valid file path: {path}"))
        } else {
            Url::parse(uri).map_err(|e| e.to_string())
        }
    }

    fn try_parse_scheme(uri: &Url) -> Result<(ObjectStoreScheme, Path)> {
        ObjectStoreScheme::parse(uri).map_err(|e| {
            let shown = uri_as_string(uri).unwrap_or_else(|_| uri.to_string());

            // a scheme we reach whose path a store will not hold — a newline in a file name,
            // say — is a different mistake from naming a store we cannot reach, and a list of
            // schemes does not answer it. The parse error says which it was, but its enum is
            // private, so the scheme decides
            if REACHABLE_SCHEMES.contains(&uri.scheme()) {
                format!("cannot use {shown}: {e}")
            } else {
                format!("unrecognized uri {shown}. Supported: {SUPPORTED_SCHEMES}.")
            }
        })
    }

    fn try_parse_bucket(scheme: &ObjectStoreScheme, uri: &Url) -> Result<Option<String>> {
        match scheme {
            ObjectStoreScheme::AmazonS3 => parse_s3_bucket(uri)
                .ok_or(format!("unsupported s3 uri {uri}"))
                .map(Some),
            ObjectStoreScheme::MicrosoftAzure => parse_azure_blob_container(uri)
                .ok_or(format!("unsupported azure blob storage uri: {uri}"))
                .map(Some),
            ObjectStoreScheme::Http => parse_http_base_uri(uri)
                .ok_or(format!("unsupported http storage uri: {uri}"))
                .map(Some),
            ObjectStoreScheme::GoogleCloudStorage => parse_gcs_bucket(uri)
                .ok_or(format!("unsupported gcs uri {uri}"))
                .map(Some),
            ObjectStoreScheme::Local => Ok(None),
            _ => Err(format!(
                "unsupported scheme {} in uri {}. Supported: {}.",
                uri.scheme(),
                uri,
                SUPPORTED_SCHEMES
            )),
        }
    }
}

impl TryFrom<&str> for ParsedUri {
    type Error = String;

    fn try_from(uri: &str) -> Result<Self> {
        let uri = Self::try_parse_uri(uri)?;

        let (scheme, path) = Self::try_parse_scheme(&uri)?;

        let bucket = Self::try_parse_bucket(&scheme, &uri)?;

        Ok(ParsedUri {
            uri,
            bucket,
            path,
            scheme,
        })
    }
}

impl TryFrom<&Url> for ParsedUri {
    type Error = String;

    fn try_from(uri: &Url) -> Result<Self> {
        let (scheme, path) = Self::try_parse_scheme(uri)?;

        let bucket = Self::try_parse_bucket(&scheme, uri)?;

        Ok(ParsedUri {
            uri: uri.clone(),
            bucket,
            path,
            scheme,
        })
    }
}

/// Resolves the `..` segments of a local path, which an object store path cannot hold: a store
/// refuses `/data/../data/orders.parquet` outright, and core `COPY` writes it without comment.
///
/// Resolved against the filesystem rather than by rewriting the text, because `..` after a
/// symlink means the parent of what the link points at. The deepest ancestor that exists is
/// canonicalized and whatever is left — the file about to be written, usually — is appended to
/// it. A path with no `..` in it is left exactly as it was, symlinks and all.
fn resolve_parent_segments(path: &str) -> Result<String> {
    use std::path::{Component, PathBuf};

    let components: Vec<Component> = std::path::Path::new(path).components().collect();

    // a relative path is not ours to resolve: it is refused a moment later for being relative,
    // and resolving it here would do so against the server's working directory
    if !components.contains(&Component::ParentDir)
        || components.first() != Some(&Component::RootDir)
    {
        return Ok(path.to_string());
    }

    let mut resolved = PathBuf::new();

    for component in components {
        match component {
            // the path so far has to exist for a `..` to have a parent, which is what the
            // kernel requires of it too: `/missing/../tmp/x` is an error rather than `/tmp/x`
            Component::ParentDir => {
                resolved = resolved.canonicalize().map_err(|e| {
                    format!(
                        "cannot resolve the .. in {path}: {} {e}",
                        resolved.display()
                    )
                })?;

                resolved.pop();
            }
            Component::CurDir => {}
            other => resolved.push(other.as_os_str()),
        }
    }

    Ok(resolved.to_string_lossy().to_string())
}

/// Renders a uri the way a user wrote it: a `file://` uri goes back to being a plain path,
/// everything else is left alone.
pub fn uri_as_string(uri: &Url) -> Result<String> {
    if uri.scheme() == "file" {
        return uri
            .to_file_path()
            .map(|path| path.to_string_lossy().to_string())
            .map_err(|_| format!("invalid local path: {uri}"));
    }

    Ok(uri.to_string())
}

/// The part of a uri that identifies the *store* rather than the object: scheme, host and
/// port. A local path has no such part, so it gets the filesystem root.
pub fn base_uri(uri: &Url) -> Result<String> {
    if uri.scheme() == "file" {
        return Ok("/".to_string());
    }

    let scheme = uri.scheme();

    let host = uri
        .host_str()
        .ok_or_else(|| format!("missing host in uri {uri}"))?;

    let port = uri.port().map(|p| format!(":{p}")).unwrap_or_default();

    Ok(format!("{scheme}://{host}{port}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_bucket_out_of_every_supported_scheme() {
        for (uri, expected) in [
            ("s3://bucket/key", Some("bucket")),
            ("https://bucket.s3.amazonaws.com/key", Some("bucket")),
            ("https://s3.amazonaws.com/bucket/key", Some("bucket")),
            ("az://container/key", Some("container")),
            ("azure://container/key", Some("container")),
            (
                "https://account.blob.core.windows.net/container/key",
                Some("container"),
            ),
            ("gs://bucket/key", Some("bucket")),
            ("https://example.com/key", Some("https://example.com")),
            ("/tmp/local.file", None),
        ] {
            let parsed = ParsedUri::try_from(uri).unwrap_or_else(|e| panic!("{uri}: {e}"));
            assert_eq!(parsed.bucket.as_deref(), expected, "for uri {uri}");
        }
    }

    #[test]
    fn rejects_a_scheme_it_cannot_reach() {
        let error = ParsedUri::try_from("ftp://host/key").unwrap_err();
        assert!(error.contains("ftp"), "{error}");
    }

    #[test]
    fn resolves_the_parent_segments_of_a_local_path() {
        let dir = std::path::Path::new("/tmp/pg_object_store_dotdot/inner");

        std::fs::create_dir_all(dir).expect("failed to make the directories");

        // a store's path cannot hold a `..`, and core COPY takes one without comment, so the
        // two have to agree about where it points
        for (written, expected) in [
            (
                "/tmp/pg_object_store_dotdot/inner/../orders.bin",
                "/tmp/pg_object_store_dotdot/orders.bin",
            ),
            (
                "/tmp/pg_object_store_dotdot/../pg_object_store_dotdot/inner/orders.bin",
                "/tmp/pg_object_store_dotdot/inner/orders.bin",
            ),
            // the file itself need not exist; what has to is something to resolve against
            (
                "/tmp/pg_object_store_dotdot/inner/../missing/orders.bin",
                "/tmp/pg_object_store_dotdot/missing/orders.bin",
            ),
        ] {
            let parsed = ParsedUri::try_from(written).unwrap_or_else(|e| panic!("{written}: {e}"));

            assert_eq!(
                uri_as_string(&parsed.uri).unwrap(),
                expected,
                "for {written}"
            );
        }

        // and a `..` whose parent does not exist is refused, as the kernel refuses it: resolving
        // it as text would have written to /tmp/orders.bin, somewhere the user never named
        let error = ParsedUri::try_from("/tmp/pg_object_store_no_such_dir/../orders.bin")
            .expect_err("a .. under a directory that does not exist cannot be resolved");

        assert!(
            error.starts_with(
                "cannot resolve the .. in /tmp/pg_object_store_no_such_dir/../orders.bin:"
            ),
            "{error}"
        );

        std::fs::remove_dir_all("/tmp/pg_object_store_dotdot").ok();
    }

    #[test]
    fn says_what_is_wrong_with_a_path_a_store_will_not_take() {
        // a newline is legal in a file name and not in an object store path. The message has to
        // be about that, not a list of the schemes we support -- the scheme was fine
        let error = ParsedUri::try_from("/tmp/two\nlines.bin").unwrap_err();

        assert!(
            error.starts_with("cannot use /tmp/two\nlines.bin:"),
            "{error}"
        );
        assert!(!error.contains(SUPPORTED_SCHEMES), "{error}");
    }

    #[test]
    fn renders_a_listed_location_in_the_form_the_prefix_was_written_in() {
        // the location an object store hands back is relative to the store's root: no scheme,
        // no bucket, and for a local path not even the leading slash
        for (prefix, location, expected) in [
            (
                "/tmp/some/data.parquet",
                "tmp/some/data.parquet/part_0.parquet",
                "/tmp/some/data.parquet/part_0.parquet",
            ),
            (
                "/tmp/some/",
                "tmp/some/part_0.parquet",
                "/tmp/some/part_0.parquet",
            ),
            (
                "s3://bucket/dir",
                "dir/part_0.parquet",
                "s3://bucket/dir/part_0.parquet",
            ),
            (
                "s3://bucket",
                "part_0.parquet",
                "s3://bucket/part_0.parquet",
            ),
            // the bucket is in the path of a path-style uri, so rewriting the prefix keeps it
            // where rebuilding from the scheme and the bucket would have dropped it
            (
                "https://s3.amazonaws.com/bucket/dir",
                "dir/part_0.parquet",
                "https://s3.amazonaws.com/bucket/dir/part_0.parquet",
            ),
            // and the caller gets back the spelling it asked with, not the other one for the
            // same container
            (
                "az://container/dir",
                "dir/part_0.parquet",
                "az://container/dir/part_0.parquet",
            ),
            (
                "gs://bucket/dir",
                "dir/part_0.parquet",
                "gs://bucket/dir/part_0.parquet",
            ),
            (
                "https://example.com/dir",
                "dir/part_0.parquet",
                "https://example.com/dir/part_0.parquet",
            ),
        ] {
            let parsed = ParsedUri::try_from(prefix).unwrap_or_else(|e| panic!("{prefix}: {e}"));

            let rendered = parsed
                .location_as_uri(&Path::from(location))
                .unwrap_or_else(|e| panic!("{prefix}: {e}"));

            assert_eq!(rendered, expected, "for prefix {prefix}");
        }
    }

    #[test]
    fn renders_the_prefix_itself_as_the_prefix() {
        // listing a single object, rather than a directory, lists the object: the location is
        // the whole prefix and there is nothing left to append
        let parsed = ParsedUri::try_from("s3://bucket/dir/one.parquet").unwrap();

        assert_eq!(
            parsed
                .location_as_uri(&Path::from("dir/one.parquet"))
                .unwrap(),
            "s3://bucket/dir/one.parquet"
        );
    }

    #[test]
    fn renders_a_local_uri_back_as_a_path() {
        let parsed = ParsedUri::try_from("/tmp/local.file").unwrap();
        assert_eq!(uri_as_string(&parsed.uri).unwrap(), "/tmp/local.file");
        assert_eq!(base_uri(&parsed.uri).unwrap(), "/");
    }
}
