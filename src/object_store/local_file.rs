use std::{os::unix::fs::FileTypeExt, sync::Arc};

use object_store::local::LocalFileSystem;
use url::Url;

use crate::arrow_parquet::uri_utils::uri_as_string;

use super::object_store_cache::ObjectStoreWithExpiration;

// create_local_file_object_store creates a LocalFileSystem object store with the given path.
pub(crate) fn create_local_file_object_store(
    uri: &Url,
    copy_from: bool,
) -> ObjectStoreWithExpiration {
    if !copy_from {
        let path = uri_as_string(uri);

        // create parent folder if it doesn't exist
        let parent = std::path::Path::new(&path)
            .parent()
            .unwrap_or_else(|| panic!("invalid parent for path: {path}"));

        std::fs::create_dir_all(parent).unwrap_or_else(|e| {
            // a folder destination, which is what file_size_bytes writes, is the parent of the
            // files of the copy, so this is also how a path that is already a file is reported
            if parent.is_file() {
                panic!(
                    "\"{}\" is a file, so \"{path}\" cannot be created under it",
                    parent.display()
                );
            }

            panic!("failed to create the folder \"{}\": {e}", parent.display());
        });

        // a fifo or a socket is asked about before it is opened, because opening one to write
        // waits for something to read it and nothing interrupts that wait: the copy is not
        // cancelled by a statement_timeout and the backend is left in the open. It is no
        // destination either way, since the bytes are staged beside it and renamed onto it,
        // which replaces it with a file of its own.
        if let Ok(metadata) = std::fs::metadata(&path) {
            let file_type = metadata.file_type();

            if !file_type.is_file() && !file_type.is_dir() {
                let what = if file_type.is_fifo() {
                    "a fifo"
                } else if file_type.is_socket() {
                    "a socket"
                } else {
                    "a device"
                };

                panic!("\"{path}\" is {what}, which a parquet file cannot be written to");
            }
        }

        // create the local file, and say so under its own name rather than the "<path>#1" that
        // object_store stages the bytes in. Truncating it here would be the copy giving up the
        // rows already in it before writing one of its own, and the staged file is renamed onto
        // it at the end, so what is in it is what a copy that fails leaves behind.
        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .unwrap_or_else(|e| panic!("failed to create the file \"{path}\": {e}"));
    }

    let object_store = LocalFileSystem::new();
    let expire_at = None;

    ObjectStoreWithExpiration {
        object_store: Arc::new(object_store),
        multipart_store: None,
        expire_at,
    }
}
