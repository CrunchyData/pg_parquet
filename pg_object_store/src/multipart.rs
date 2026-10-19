//! The multi-process half of the vtable: several backends uploading parts of one object.
//!
//! `writer.rs` is one process writing one object, and it keeps the upload entirely on this side
//! of the boundary — the caller gets a handle, and the `BufWriter` behind it is never seen by
//! anyone else. A parallel `COPY TO` of a single file cannot work that way: the leader starts
//! the upload, every worker uploads parts of it, and the leader completes it, so the upload has
//! to be nameable in a process that did not start it.
//!
//! What crosses the boundary is therefore the store's own identifiers rather than a handle: an
//! upload id for the upload and a part id per part, both strings, both of them the store's to
//! define. There is no per-backend table here and nothing to sweep, because there is nothing
//! local to sweep — an upload is reachable from any backend that has its id, and whoever owns
//! the copy owns aborting it. A `COPY TO` that dies without aborting leaves a partial upload
//! for the bucket's own expiry rule, which is the same outcome as a dead single-process one.
//!
//! The ordering rule is the stores': parts are completed in the order their bytes have in the
//! object, and every part but the last has to reach [`MIN_PART_SIZE`]. Neither is enforced
//! here. The first cannot be — the part numbers are the caller's to assign, and it is the
//! caller that knows what order its bytes go in — and the second is checked by the store, which
//! is the only party that knows whether a given part is the last one.

use std::ffi::{c_char, CStr};

use object_store::multipart::PartId;
use object_store_core::error::object_store_error_message;
use object_store_core::get_or_create_multipart_store;
use object_store_core::runtime::block_on;
use object_store_core::uri::ParsedUri;

use crate::sync_core_settings;
use crate::vtable::{guard, palloc_cstring, parse, PGOS_NOT_FOUND, PGOS_OK};

pub use object_store_client::{
    PgObjectStoreExtV1, PG_OBJECT_STORE_EXT_MAGIC, PG_OBJECT_STORE_EXT_VERSION,
};

/// The second table, hung off the main one's first reserved slot.
pub(crate) static PG_OBJECT_STORE_EXT: PgObjectStoreExtV1 = PgObjectStoreExtV1 {
    magic: PG_OBJECT_STORE_EXT_MAGIC,
    version: PG_OBJECT_STORE_EXT_VERSION,
    multipart_begin: pgos_multipart_begin,
    multipart_put_part: pgos_multipart_put_part,
    multipart_complete: pgos_multipart_complete,
    multipart_abort: pgos_multipart_abort,
    delete: pgos_delete,
    list_shallow: Some(crate::vtable::pgos_list_shallow),
    reserved: [std::ptr::null(); 3],
};

/// The write-privilege check plus the parse, for the calls that do not go through
/// [`crate::vtable::open`]. Every entry point here is a write, so there is no direction to pass.
fn check_write(uri: *const c_char) -> Result<ParsedUri, String> {
    sync_core_settings();

    let parsed = parse(uri)?;

    let copy_from = false;

    if !crate::privilege::has_access_privilege_to_uri(&parsed, copy_from) {
        let uri = object_store_core::uri::uri_as_string(&parsed.uri)?;
        return Err(format!("permission denied to write {uri}"));
    }

    Ok(parsed)
}

/// The shared prologue of the multipart calls: the privilege check and the store, seen through
/// its multipart interface. `Ok(None)` is a store that has no multipart upload.
type MultipartTarget = Option<(
    std::sync::Arc<dyn object_store::multipart::MultipartStore>,
    object_store::path::Path,
)>;

fn open_multipart(uri: *const c_char) -> Result<MultipartTarget, String> {
    let parsed = check_write(uri)?;

    get_or_create_multipart_store(&parsed)
}

/// Reads an identifier the caller got from us in an earlier call back in.
fn c_str_arg(value: *const c_char, what: &str) -> Result<String, String> {
    if value.is_null() {
        return Err(format!("{what} must not be null"));
    }

    unsafe { CStr::from_ptr(value) }
        .to_str()
        .map(str::to_string)
        .map_err(|e| format!("{what} is not valid utf-8: {e}"))
}

/// Starts a multipart upload. A store with no multipart upload is `*upload_id` left null and
/// `PGOS_OK`: the caller has to plan around it, and a caller that cannot is better off saying
/// so in its own terms than reading ours.
unsafe extern "C-unwind" fn pgos_multipart_begin(
    uri: *const c_char,
    upload_id: *mut *mut c_char,
    error: *mut *mut c_char,
) -> i32 {
    guard(error, || {
        if upload_id.is_null() {
            return Err("the upload id out-parameter must not be null".to_string());
        }

        *upload_id = std::ptr::null_mut();

        let Some((store, path)) = open_multipart(uri)? else {
            return Ok(PGOS_OK);
        };

        let id = block_on(async { store.create_multipart(&path).await })
            .map_err(object_store_error_message)?;

        *upload_id = palloc_cstring(&id);

        Ok(PGOS_OK)
    })
}

/// Uploads one part. The part number is the caller's, and so is the guarantee that no two
/// processes claim the same one — this side has no way to tell a re-upload of a part from a
/// collision, and a store takes either.
unsafe extern "C-unwind" fn pgos_multipart_put_part(
    uri: *const c_char,
    upload_id: *const c_char,
    part: i32,
    data: *const u8,
    len: i64,
    part_id: *mut *mut c_char,
    error: *mut *mut c_char,
) -> i32 {
    guard(error, || {
        if part_id.is_null() {
            return Err("the part id out-parameter must not be null".to_string());
        }

        *part_id = std::ptr::null_mut();

        if part < 0 {
            return Err("part must not be negative".to_string());
        }

        if len < 0 {
            return Err("len must not be negative".to_string());
        }

        if data.is_null() && len > 0 {
            return Err("data must not be null".to_string());
        }

        let id = c_str_arg(upload_id, "upload id")?;

        let Some((store, path)) = open_multipart(uri)? else {
            return Err(
                "the store has no multipart upload, so it has no part to upload into".to_string(),
            );
        };

        let payload = if len == 0 {
            bytes::Bytes::new()
        } else {
            bytes::Bytes::copy_from_slice(std::slice::from_raw_parts(data, len as usize))
        };

        let uploaded = block_on(async {
            store
                .put_part(&path, &id, part as usize, payload.into())
                .await
        })
        .map_err(object_store_error_message)?;

        *part_id = palloc_cstring(&uploaded.content_id);

        Ok(PGOS_OK)
    })
}

/// Completes the upload. The part ids arrive in the order their bytes have in the object, which
/// is the caller's to get right: a store checks that the parts exist, not that they were meant
/// to be in that order.
unsafe extern "C-unwind" fn pgos_multipart_complete(
    uri: *const c_char,
    upload_id: *const c_char,
    n_parts: i32,
    part_ids: *const *const c_char,
    error: *mut *mut c_char,
) -> i32 {
    guard(error, || {
        if n_parts < 0 {
            return Err("n_parts must not be negative".to_string());
        }

        if part_ids.is_null() && n_parts > 0 {
            return Err("part_ids must not be null".to_string());
        }

        let id = c_str_arg(upload_id, "upload id")?;

        let parts = (0..n_parts as usize)
            .map(|at| {
                c_str_arg(*part_ids.add(at), &format!("part id {at}"))
                    .map(|content_id| PartId { content_id })
            })
            .collect::<Result<Vec<PartId>, String>>()?;

        let Some((store, path)) = open_multipart(uri)? else {
            return Err("the store has no multipart upload to complete".to_string());
        };

        block_on(async { store.complete_multipart(&path, &id, parts).await })
            .map_err(object_store_error_message)?;

        Ok(PGOS_OK)
    })
}

/// Discards the upload. Reports what went wrong rather than swallowing it: the caller is the
/// process that owns the upload, and it is the only one that can tell whether the partial
/// upload is actually gone.
unsafe extern "C-unwind" fn pgos_multipart_abort(
    uri: *const c_char,
    upload_id: *const c_char,
    error: *mut *mut c_char,
) -> i32 {
    guard(error, || {
        let id = c_str_arg(upload_id, "upload id")?;

        let Some((store, path)) = open_multipart(uri)? else {
            return Err("the store has no multipart upload to abort".to_string());
        };

        block_on(async { store.abort_multipart(&path, &id).await })
            .map_err(object_store_error_message)?;

        Ok(PGOS_OK)
    })
}

/// Deletes an object. `PGOS_NOT_FOUND` rather than an error when there is none, so that a
/// cleanup path can run twice.
unsafe extern "C-unwind" fn pgos_delete(uri: *const c_char, error: *mut *mut c_char) -> i32 {
    guard(error, || {
        let parsed = check_write(uri)?;

        // the privilege checked is the write one, but the store is built as a reader's: a
        // write store creates the parent directories of a local path, and a delete has no
        // business creating anything
        let copy_from = true;
        let (store, path) = object_store_core::get_or_create_object_store(&parsed, copy_from)?;

        match block_on(async {
            use object_store::ObjectStoreExt;
            store.delete(&path).await
        }) {
            Ok(()) => Ok(PGOS_OK),
            Err(object_store::Error::NotFound { .. }) => Ok(PGOS_NOT_FOUND),
            Err(e) => Err(object_store_error_message(e)),
        }
    })
}
