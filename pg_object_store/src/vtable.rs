//! The C ABI other extensions call.
//!
//! A pgrx extension is a `cdylib`, and Rust has no stable ABI, so nothing in this crate is
//! callable from another `.so` as Rust. `nm -D --defined-only` on a released pgrx extension
//! shows only its Postgres entry points; arrow, parquet, object_store and tokio are all
//! hidden. A dependent extension therefore has exactly two options: vendor
//! `object-store-core` and get its own copy of the runtime, the credential cache and the
//! connection pools, or call across a hand-written C boundary and share ours. This is that
//! boundary.
//!
//! It is published through `find_rendezvous_variable`, which is a Postgres-wide, per-backend
//! `void *` keyed by a string, available on every version from 13 on. A dependent looks the
//! key up, checks the magic and the version, and refuses to proceed on a mismatch — the
//! mismatch has to be caught here, because an ABI skew between two `.so`s is a crash rather
//! than a link error.
//!
//! ## The rules this boundary keeps
//!
//! - **Blocking.** The tokio runtime stays on this side. Not a regression: every entry point
//!   is already `block_on` internally.
//! - **`get_ranges` is plural.** A parquet reader issues coalesced concurrent range fetches,
//!   and that concurrency has to stay on the side of the boundary that owns the runtime. A
//!   singular-only ABI would quietly serialize every reader built on it.
//! - **Nothing unwinds across the edge.** Every function catches panics and turns them into an
//!   error code plus a message. Postgres' own `ereport` longjmp still passes through, as it
//!   does between any two loaded modules, but a Rust panic would be undefined behaviour.
//! - **Nothing ereports.** A function here returns a message and lets the caller decide how
//!   loud to be and in which error code. That is also what makes the functions usable from a
//!   caller that wants to recover.
//! - **Returned memory is palloc'd** in the caller's `CurrentMemoryContext`, so it is freed by
//!   the caller's context teardown and needs no explicit free function. A caller that wants a
//!   buffer to outlive the statement has to switch contexts before calling.
//! - **A write that does not fit in memory is a handle, not a buffer.** `put` takes a whole
//!   object; `put_begin`/`put_part`/`put_finish`/`put_abort` are for a `COPY TO` that produces
//!   bytes as it goes. The multipart upload stays on this side with the runtime.
//! - **A write several processes share is an id, not a handle.** A handle names something local
//!   to a backend, and a parallel `COPY TO` of one file is not local to any of them. Those
//!   entry points are in the second table; see `multipart.rs`.
//! - **`check_privilege` is in the vtable.** Every consumer has to make the same check, and an
//!   extension that reimplemented it and got it wrong would be a read-any-bucket primitive
//!   handed to any user who can call a function.

use std::ffi::{c_char, c_void, CStr};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use object_store::{path::Path, ObjectStore, ObjectStoreExt};
use object_store_core::error::object_store_error_message;
use object_store_core::get_or_create_object_store;
use object_store_core::runtime::block_on;
use object_store_core::uri::{uri_as_string, ParsedUri};
use pgrx::pg_sys::{find_rendezvous_variable, palloc};

use crate::privilege::has_access_privilege_to_uri;
use crate::sync_core_settings;

/// The ABI is declared in `object-store-client`, which is also what every dependent extension
/// links: one declaration means a layout skew between the publisher and a consumer is
/// impossible rather than merely checked. The constants are re-exported because this crate's
/// own tests and `publish` below are consumers of them too.
pub use object_store_client::{
    PgObjectStoreApiV1, PGOS_ERROR, PGOS_NOT_FOUND, PGOS_OK, PG_OBJECT_STORE_API_MAGIC,
    PG_OBJECT_STORE_API_VERSION, PG_OBJECT_STORE_RENDEZVOUS_KEY,
};

static PG_OBJECT_STORE_API: PgObjectStoreApiV1 = PgObjectStoreApiV1 {
    magic: PG_OBJECT_STORE_API_MAGIC,
    version: PG_OBJECT_STORE_API_VERSION,
    check_privilege: pgos_check_privilege,
    head: pgos_head,
    get_range: pgos_get_range,
    get_ranges: pgos_get_ranges,
    put: pgos_put,
    list: pgos_list,
    put_begin: Some(crate::writer::pgos_put_begin),
    put_part: Some(crate::writer::pgos_put_part),
    put_finish: Some(crate::writer::pgos_put_finish),
    put_abort: Some(crate::writer::pgos_put_abort),
    ext: &crate::multipart::PG_OBJECT_STORE_EXT as *const _ as *const c_void,
    reserved: [std::ptr::null(); 1],
};

/// Publishes the vtable. Called from `_PG_init`, which is the only point at which we know we
/// are loaded and before any dependent can look for us.
pub fn publish() {
    let key = std::ffi::CString::new(PG_OBJECT_STORE_RENDEZVOUS_KEY)
        .expect("the rendezvous key has no interior nul");

    unsafe {
        let slot = find_rendezvous_variable(key.as_ptr());

        // a non-null slot means a second copy of this extension is loaded in the same backend,
        // which can happen if one is preloaded and another is dlopened from a different
        // $libdir. Leaving the first in place keeps everyone on one credential cache.
        if (*slot).is_null() {
            *slot = &PG_OBJECT_STORE_API as *const PgObjectStoreApiV1 as *mut c_void;
        }
    }
}

/// The largest range this ABI can return: a fetched range is handed back in a palloc'd buffer,
/// and `palloc` refuses anything larger than 1GB. Checked here so that asking for more is a
/// sentence about the range rather than one about a memory allocation request size, and so that
/// a consumer can split the read instead of guessing why it failed.
pub(crate) const MAX_RANGE_BYTES: i64 = 1024 * 1024 * 1024 - 1;

/// palloc's a copy of a string as a C string in the caller's memory context.
pub(crate) fn palloc_cstring(message: &str) -> *mut c_char {
    let bytes = message.as_bytes();

    unsafe {
        let buffer = palloc(bytes.len() + 1) as *mut u8;
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), buffer, bytes.len());
        *buffer.add(bytes.len()) = 0;
        buffer as *mut c_char
    }
}

/// The message of a Postgres error that pgrx turned into a panic, with its detail when it has
/// one. Reported rather than rethrown: a consumer of this ABI is told what went wrong in the
/// same way for every failure, which is what lets one recover from any of them.
fn caught_error_message(payload: &Box<dyn std::any::Any + Send>) -> Option<String> {
    use pgrx::pg_sys::panic::CaughtError;

    let report = match payload.downcast_ref::<CaughtError>()? {
        CaughtError::PostgresError(report)
        | CaughtError::ErrorReport(report)
        | CaughtError::RustPanic {
            ereport: report, ..
        } => report,
    };

    Some(match report.detail() {
        Some(detail) => format!("{}: {detail}", report.message()),
        None => report.message().to_string(),
    })
}

/// Runs the body of a vtable function, turning both an `Err` and a panic into `PGOS_ERROR`
/// plus a palloc'd message. This is the only place the no-unwinding-across-the-edge rule is
/// enforced, so every entry point goes through it.
pub(crate) fn guard<F>(error: *mut *mut c_char, body: F) -> i32
where
    F: FnOnce() -> Result<i32, String>,
{
    let result = catch_unwind(AssertUnwindSafe(body)).unwrap_or_else(|payload| {
        let message = payload
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| payload.downcast_ref::<String>().cloned())
            // a Postgres error caught on this side of the edge: it carries its own message, and
            // flattening it to "panicked" is how a palloc of a range too large to palloc came
            // out as a sentence about the object store
            .or_else(|| caught_error_message(&payload))
            .unwrap_or_else(|| "panicked while reaching the object store".to_string());

        Err(message)
    });

    match result {
        Ok(code) => code,
        Err(message) => {
            if !error.is_null() {
                unsafe { *error = palloc_cstring(&message) };
            }

            PGOS_ERROR
        }
    }
}

/// A uri from C becomes a parsed uri. Reaches nothing and checks nothing, for the caller that
/// needs the parse on its own — rendering a listed object's path back as a uri, say.
pub(crate) fn parse(uri: *const c_char) -> Result<ParsedUri, String> {
    if uri.is_null() {
        return Err("uri must not be null".to_string());
    }

    let uri = unsafe { CStr::from_ptr(uri) }
        .to_str()
        .map_err(|e| format!("uri is not valid utf-8: {e}"))?;

    ParsedUri::try_from(uri)
}

/// The shared prologue: a uri from C becomes a parsed uri, a privilege check and a store.
pub(crate) fn open(
    uri: *const c_char,
    copy_from: bool,
) -> Result<(Arc<dyn ObjectStore>, Path), String> {
    sync_core_settings();

    let parsed = parse(uri)?;

    if !has_access_privilege_to_uri(&parsed, copy_from) {
        let direction = if copy_from { "read" } else { "write" };
        let uri = uri_as_string(&parsed.uri)?;
        return Err(format!("permission denied to {direction} {uri}"));
    }

    get_or_create_object_store(&parsed, copy_from)
}

unsafe extern "C-unwind" fn pgos_check_privilege(
    uri: *const c_char,
    copy_from: bool,
    allowed: *mut bool,
    error: *mut *mut c_char,
) -> i32 {
    guard(error, || {
        sync_core_settings();

        let uri = CStr::from_ptr(uri)
            .to_str()
            .map_err(|e| format!("uri is not valid utf-8: {e}"))?;

        let parsed = ParsedUri::try_from(uri)?;

        *allowed = has_access_privilege_to_uri(&parsed, copy_from);

        Ok(PGOS_OK)
    })
}

unsafe extern "C-unwind" fn pgos_head(
    uri: *const c_char,
    size: *mut i64,
    error: *mut *mut c_char,
) -> i32 {
    guard(error, || {
        let copy_from = true;
        let (store, path) = open(uri, copy_from)?;

        match block_on(async { store.head(&path).await }) {
            Ok(meta) => {
                *size = meta.size as i64;
                Ok(PGOS_OK)
            }
            Err(object_store::Error::NotFound { .. }) => Ok(PGOS_NOT_FOUND),
            Err(e) => Err(object_store_error_message(e)),
        }
    })
}

unsafe extern "C-unwind" fn pgos_get_range(
    uri: *const c_char,
    offset: i64,
    length: i64,
    data: *mut *mut u8,
    len: *mut i64,
    error: *mut *mut c_char,
) -> i32 {
    guard(error, || {
        if offset < 0 || length < 0 {
            return Err("offset and length must not be negative".to_string());
        }

        // a request for no bytes is answered without a request: a store rejects an empty range
        // as invalid, and the backends disagree about how (a 416, a generic error)
        if length == 0 {
            *data = palloc(1) as *mut u8;
            *len = 0;

            return Ok(PGOS_OK);
        }

        if length > MAX_RANGE_BYTES {
            return Err(format!(
                "a range of {length} bytes is more than the {MAX_RANGE_BYTES} a single read \
                 returns; read it in pieces"
            ));
        }

        let copy_from = true;
        let (store, path) = open(uri, copy_from)?;

        let range = (offset as u64)..(offset as u64 + length as u64);

        let bytes = match block_on(async { store.get_range(&path, range).await }) {
            Ok(bytes) => bytes,
            Err(object_store::Error::NotFound { .. }) => return Ok(PGOS_NOT_FOUND),
            Err(e) => return Err(object_store_error_message(e)),
        };

        let buffer = palloc(bytes.len().max(1)) as *mut u8;
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), buffer, bytes.len());

        *data = buffer;
        *len = bytes.len() as i64;

        Ok(PGOS_OK)
    })
}

unsafe extern "C-unwind" fn pgos_get_ranges(
    uri: *const c_char,
    n_ranges: i32,
    offsets: *const i64,
    lengths: *const i64,
    data: *mut *mut u8,
    lens: *mut i64,
    error: *mut *mut c_char,
) -> i32 {
    guard(error, || {
        if n_ranges < 0 {
            return Err("n_ranges must not be negative".to_string());
        }

        let n_ranges = n_ranges as usize;

        let offsets = std::slice::from_raw_parts(offsets, n_ranges);
        let lengths = std::slice::from_raw_parts(lengths, n_ranges);

        let mut ranges = Vec::with_capacity(n_ranges);
        // which caller range each fetched range answers; an empty one is answered without a
        // request, because a store rejects an empty range as invalid
        let mut wanted = Vec::with_capacity(n_ranges);

        for i in 0..n_ranges {
            if offsets[i] < 0 || lengths[i] < 0 {
                return Err("offsets and lengths must not be negative".to_string());
            }

            if lengths[i] > MAX_RANGE_BYTES {
                return Err(format!(
                    "a range of {} bytes is more than the {MAX_RANGE_BYTES} a single read \
                     returns; read it in pieces",
                    lengths[i]
                ));
            }

            *data.add(i) = palloc(1) as *mut u8;
            *lens.add(i) = 0;

            if lengths[i] > 0 {
                ranges.push((offsets[i] as u64)..(offsets[i] as u64 + lengths[i] as u64));
                wanted.push(i);
            }
        }

        if ranges.is_empty() {
            return Ok(PGOS_OK);
        }

        let copy_from = true;
        let (store, path) = open(uri, copy_from)?;

        // the whole point of the plural form: one call, concurrent fetches, on the side of the
        // boundary that owns the runtime
        let fetched = match block_on(async { store.get_ranges(&path, &ranges).await }) {
            Ok(fetched) => fetched,
            Err(object_store::Error::NotFound { .. }) => return Ok(PGOS_NOT_FOUND),
            Err(e) => return Err(object_store_error_message(e)),
        };

        for (bytes, i) in fetched.iter().zip(wanted) {
            let buffer = palloc(bytes.len().max(1)) as *mut u8;
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), buffer, bytes.len());

            *data.add(i) = buffer;
            *lens.add(i) = bytes.len() as i64;
        }

        Ok(PGOS_OK)
    })
}

unsafe extern "C-unwind" fn pgos_put(
    uri: *const c_char,
    data: *const u8,
    len: i64,
    error: *mut *mut c_char,
) -> i32 {
    guard(error, || {
        if len < 0 {
            return Err("len must not be negative".to_string());
        }

        let copy_from = false;
        let (store, path) = open(uri, copy_from)?;

        let payload = bytes::Bytes::copy_from_slice(std::slice::from_raw_parts(data, len as usize));

        block_on(async { store.put(&path, payload.into()).await })
            .map_err(object_store_error_message)?;

        Ok(PGOS_OK)
    })
}

unsafe extern "C-unwind" fn pgos_list(
    uri: *const c_char,
    n_objects: *mut i32,
    locations: *mut *mut *mut c_char,
    sizes: *mut *mut i64,
    error: *mut *mut c_char,
) -> i32 {
    listed(uri, n_objects, locations, sizes, error, |store, prefix| {
        block_on(async {
            use futures::TryStreamExt;
            store.list(Some(prefix)).try_collect::<Vec<_>>().await
        })
    })
}

/// The one-level form. The common prefixes a delimited list also returns are dropped: they
/// stand for a subdirectory, which is not an object anyone can read.
pub(crate) unsafe extern "C-unwind" fn pgos_list_shallow(
    uri: *const c_char,
    n_objects: *mut i32,
    locations: *mut *mut *mut c_char,
    sizes: *mut *mut i64,
    error: *mut *mut c_char,
) -> i32 {
    listed(uri, n_objects, locations, sizes, error, |store, prefix| {
        block_on(async { store.list_with_delimiter(Some(prefix)).await })
            .map(|result| result.objects)
    })
}

/// The shared body of both listing entry points: the privilege check, whichever listing the
/// caller asked for, and the palloc'd arrays it comes back in.
unsafe fn listed<F>(
    uri: *const c_char,
    n_objects: *mut i32,
    locations: *mut *mut *mut c_char,
    sizes: *mut *mut i64,
    error: *mut *mut c_char,
    list: F,
) -> i32
where
    F: FnOnce(
        &Arc<dyn ObjectStore>,
        &Path,
    ) -> Result<Vec<object_store::ObjectMeta>, object_store::Error>,
{
    guard(error, || {
        let copy_from = true;
        let parsed = parse(uri)?;
        let (store, prefix) = open(uri, copy_from)?;

        let objects = list(&store, &prefix).map_err(object_store_error_message)?;

        let count = objects.len();

        let locations_array =
            palloc(std::mem::size_of::<*mut c_char>() * count.max(1)) as *mut *mut c_char;
        let sizes_array = palloc(std::mem::size_of::<i64>() * count.max(1)) as *mut i64;

        for (i, meta) in objects.iter().enumerate() {
            *locations_array.add(i) = palloc_cstring(&parsed.location_as_uri(&meta.location)?);
            *sizes_array.add(i) = meta.size as i64;
        }

        *n_objects = count as i32;
        *locations = locations_array;
        *sizes = sizes_array;

        Ok(PGOS_OK)
    })
}
