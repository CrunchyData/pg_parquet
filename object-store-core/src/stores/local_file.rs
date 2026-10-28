use std::sync::Arc;

use object_store::local::LocalFileSystem;
use url::Url;

use super::ObjectStoreWithExpiration;
use crate::uri::uri_as_string;
use crate::Result;

/// Creates a LocalFileSystem object store for a path on the server's own filesystem. Writing
/// creates the parent directories, because `LocalFileSystem` will not create a path that does
/// not exist, and proves the destination is writable while leaving it alone.
pub fn create_local_file_object_store(
    uri: &Url,
    copy_from: bool,
) -> Result<ObjectStoreWithExpiration> {
    if !copy_from {
        let path = uri_as_string(uri)?;

        let parent = std::path::Path::new(&path)
            .parent()
            .ok_or(format!("invalid parent for path: {path}"))?;

        std::fs::create_dir_all(parent).map_err(|e| {
            // a folder destination, which is what `file_size_bytes` writes, is the parent of the
            // files of the copy, so this is also how a path that is already a file is reported
            if parent.is_file() {
                return format!(
                    "\"{}\" is a file, so \"{path}\" cannot be created under it",
                    parent.display()
                );
            }

            format!("could not make the directories of {path}: {e}")
        })?;

        // the destination is opened for writing and closed again, without `truncate` and
        // without `create`: one that cannot be written is still reported here rather than part
        // way through the write, and one that can is not emptied by a statement that then
        // fails. Both the single `put` and the staged multipart upload write a temp file in the
        // parent and rename it over this path, so nothing needs the path to exist first
        if std::path::Path::new(&path).exists() {
            std::fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .map_err(|e| format!("{path} cannot be written: {e}"))?;
        } else {
            // and when there is no destination yet, what has to be writable is the directory
            // the temp file goes in. Proved the way the write itself will do it, because a
            // permission is not something to reason about from the mode bits
            let probe = parent.join(format!(".pg_object_store_probe_{}", std::process::id()));

            std::fs::File::create(&probe).map_err(|e| format!("{path} cannot be written: {e}"))?;

            std::fs::remove_file(&probe).ok();
        }
    }

    Ok(ObjectStoreWithExpiration {
        object_store: Arc::new(LocalFileSystem::new()),
        // the local filesystem has no multipart upload
        multipart_store: None,
        expire_at: None,
    })
}
