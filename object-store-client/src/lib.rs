//! The consuming side of `pg_object_store`'s vtable.
//!
//! Tier 1 publishes a C vtable under a `find_rendezvous_variable` key because a pgrx extension
//! is a `cdylib` and Rust has no stable ABI: a dependent extension either vendors
//! `object-store-core` — and gets a second tokio runtime, a second credential cache and a
//! second set of connection pools — or calls across this boundary and shares ours. Every
//! dependent makes the same calls, so the marshalling is written once, here, rather than once
//! per extension.
//!
//! This crate is the single declaration of [`PgObjectStoreApiV1`]: tier 1 publishes *this*
//! struct and every consumer resolves *this* struct, which is what makes a layout skew
//! impossible rather than merely checked. It has no pgrx dependency and so builds once for
//! every Postgres major.
//!
//! What it deliberately does not do:
//!
//! - **It never reports.** Every call returns a `Result<_, String>` and the caller decides how
//!   loud to be. A consumer that wants an `ereport` writes one; a consumer inside a callback
//!   invoked from another extension's frames — where a longjmp would skip that extension's
//!   cleanup — hands the message back as a value instead.
//! - **It does not resolve the vtable.** Looking a rendezvous key up is `pg_sys`, which is the
//!   one thing a consumer has and this crate does not. A consumer passes the pointer it found
//!   to [`Client::from_rendezvous`] and gets the magic and version check for free.
//!
//! ```ignore
//! let slot = unsafe { find_rendezvous_variable(key.as_ptr()) };
//! let client = unsafe { Client::from_rendezvous(*slot) }.unwrap_or_else(|e| error!("{e}"));
//! let bytes = client.get_range("s3://bucket/orders.parquet", 0, 4096)?;
//! ```

use std::ffi::{c_char, c_void, CStr, CString};

use bytes::Bytes;

/// "POS1", so that a pointer left behind by an unrelated extension under a colliding key, or by
/// a future layout of this struct, is detected rather than called.
pub const PG_OBJECT_STORE_API_MAGIC: u32 = 0x504f_5331;

/// Bumped whenever the layout of [`PgObjectStoreApiV1`] changes in any way that is not adding a
/// field at the end in place of a reserved slot.
pub const PG_OBJECT_STORE_API_VERSION: u32 = 1;

/// The rendezvous key. Versioned, so that a v2 can be published alongside a v1 during a
/// transition instead of breaking every dependent at once.
pub const PG_OBJECT_STORE_RENDEZVOUS_KEY: &str = "pg_object_store.v1";

/// Returned by every function in the vtable.
pub const PGOS_OK: i32 = 0;
/// The object is not there. Distinct from an error so that a caller can probe without having to
/// parse a message.
pub const PGOS_NOT_FOUND: i32 = 1;
/// Something went wrong; `*error` has the message.
pub const PGOS_ERROR: i32 = -1;

/// What tier 1 publishes and a dependent extension resolves. Pointers are never null unless a
/// doc comment says so.
///
/// The streaming write entry points are `Option`, because they landed in four of the six slots
/// the first v1 reserved and a dependent built against that header has to keep working: adding
/// a field in place of a reserved slot does not change the layout, so it does not bump the
/// version, which in turn means a null check is the only thing that tells a caller whether the
/// copy it found has them.
///
/// The rules the boundary keeps, which are the same seen from either side:
///
/// - **Blocking.** The tokio runtime stays inside tier 1; every entry point is `block_on`
///   internally.
/// - **`get_ranges` is plural.** A parquet reader issues coalesced concurrent range fetches, and
///   that concurrency has to stay on the side that owns the runtime.
/// - **Nothing unwinds across the edge.** Tier 1 catches panics and turns them into a code plus
///   a message.
/// - **Nothing ereports.** A function returns a message and lets the caller decide.
/// - **Returned memory is palloc'd** in the caller's `CurrentMemoryContext`, so it is freed by
///   the caller's context teardown and there is no free function. This crate copies out of those
///   buffers for exactly that reason: an Arrow decoder holds bytes well past the context they
///   arrived in.
#[repr(C)]
pub struct PgObjectStoreApiV1 {
    pub magic: u32,
    pub version: u32,

    /// Whether the current user may reach `uri`. `copy_from` true means reading.
    /// `PGOS_OK` with `*allowed` set, or `PGOS_ERROR` if the uri does not parse.
    pub check_privilege: unsafe extern "C-unwind" fn(
        uri: *const c_char,
        copy_from: bool,
        allowed: *mut bool,
        error: *mut *mut c_char,
    ) -> i32,

    /// The size of the object at `uri`, in bytes. `PGOS_NOT_FOUND` if there is none.
    pub head: unsafe extern "C-unwind" fn(
        uri: *const c_char,
        size: *mut i64,
        error: *mut *mut c_char,
    ) -> i32,

    /// One byte range of the object at `uri`. `*data` is palloc'd and `*len` is how much of it
    /// was read, which can be short at the end of the object.
    pub get_range: unsafe extern "C-unwind" fn(
        uri: *const c_char,
        offset: i64,
        length: i64,
        data: *mut *mut u8,
        len: *mut i64,
        error: *mut *mut c_char,
    ) -> i32,

    /// Several byte ranges of one object, fetched concurrently. `data` and `lens` are arrays of
    /// `n_ranges` elements supplied by the caller; each `data[i]` is set to a palloc'd buffer.
    /// Either every range is fetched or none is.
    pub get_ranges: unsafe extern "C-unwind" fn(
        uri: *const c_char,
        n_ranges: i32,
        offsets: *const i64,
        lengths: *const i64,
        data: *mut *mut u8,
        lens: *mut i64,
        error: *mut *mut c_char,
    ) -> i32,

    /// Writes the object at `uri`, replacing anything already there.
    pub put: unsafe extern "C-unwind" fn(
        uri: *const c_char,
        data: *const u8,
        len: i64,
        error: *mut *mut c_char,
    ) -> i32,

    /// The objects under `uri`, treated as a prefix. `*locations` and `*sizes` are palloc'd
    /// arrays of `*n_objects` elements, and each `(*locations)[i]` is a palloc'd C string.
    pub list: unsafe extern "C-unwind" fn(
        uri: *const c_char,
        n_objects: *mut i32,
        locations: *mut *mut *mut c_char,
        sizes: *mut *mut i64,
        error: *mut *mut c_char,
    ) -> i32,

    /// Opens a streaming write to `uri` and sets `*handle` to an opaque, non-null handle.
    /// `*handle` is null whenever the call did not return `PGOS_OK`. Every handle has to end in
    /// `put_finish` or `put_abort`; one left open at transaction end is aborted, so the file is
    /// not written.
    pub put_begin: Option<
        unsafe extern "C-unwind" fn(
            uri: *const c_char,
            handle: *mut *mut c_void,
            error: *mut *mut c_char,
        ) -> i32,
    >,

    /// Appends `len` bytes to a writer. Anything other than `PGOS_OK` closes the handle and
    /// discards the upload: a multipart upload cannot resume past a missing part.
    pub put_part: Option<
        unsafe extern "C-unwind" fn(
            handle: *mut c_void,
            data: *const u8,
            len: i64,
            error: *mut *mut c_char,
        ) -> i32,
    >,

    /// Completes the upload and closes the handle. The object becomes visible to a reader here
    /// and nowhere earlier.
    pub put_finish:
        Option<unsafe extern "C-unwind" fn(handle: *mut c_void, error: *mut *mut c_char) -> i32>,

    /// Discards the upload and closes the handle.
    pub put_abort:
        Option<unsafe extern "C-unwind" fn(handle: *mut c_void, error: *mut *mut c_char) -> i32>,

    /// A [`PgObjectStoreExtV1`], or null on a tier 1 that publishes none. A second table rather
    /// than four more slots: what went in here is five entry points and the slots left were two,
    /// and an indirection costs one load on a call that is already a network round trip.
    pub ext: *const c_void,

    /// The spare, for the same reason the first four existed. Null; a dependent must check
    /// before calling.
    pub reserved: [*const c_void; 1],
}

/// "POS2", told apart from the main table's magic so that a pointer mistakenly stored in the
/// `ext` slot is detected rather than called.
pub const PG_OBJECT_STORE_EXT_MAGIC: u32 = 0x504f_5332;

/// Bumped whenever the layout of [`PgObjectStoreExtV1`] changes in any way that is not adding a
/// field at the end in place of a reserved slot. Independent of the main table's version: an
/// extension table that grows does not invalidate a consumer that only calls the main one.
pub const PG_OBJECT_STORE_EXT_VERSION: u32 = 1;

/// The second table, reached through [`PgObjectStoreApiV1::ext`].
///
/// `put_begin` and friends are one process writing one object, and they keep the upload on tier
/// 1's side precisely because nobody else needs to see it. These are the other case: several
/// *processes* uploading parts of a single object, which is what a parallel `COPY TO` of one
/// file is. The upload cannot live in any one of them, so what crosses the boundary is the
/// store's own identifiers — an upload id and a part id per part, both strings — and the
/// ordering of the parts is the caller's to decide and to hand back at completion.
///
/// Which is also why these are not `Writer`-shaped. A part is claimed by its number before its
/// bytes exist, parts are uploaded out of order by processes that never see each other, and the
/// one process that completes the upload may have uploaded none of them.
#[repr(C)]
pub struct PgObjectStoreExtV1 {
    pub magic: u32,
    pub version: u32,

    /// Starts a multipart upload of the object at `uri` and sets `*upload_id` to a palloc'd
    /// string identifying it. Sets `*upload_id` to null and returns `PGOS_OK` when the store
    /// behind `uri` has no multipart upload at all — the local filesystem and `http` — which is
    /// a thing to plan around rather than an error.
    pub multipart_begin: unsafe extern "C-unwind" fn(
        uri: *const c_char,
        upload_id: *mut *mut c_char,
        error: *mut *mut c_char,
    ) -> i32,

    /// Uploads `len` bytes as part number `part` of the upload, and sets `*part_id` to a
    /// palloc'd string the store identifies that part by. Parts may be uploaded in any order
    /// and from any process; what orders them in the finished object is `part`.
    pub multipart_put_part: unsafe extern "C-unwind" fn(
        uri: *const c_char,
        upload_id: *const c_char,
        part: i32,
        data: *const u8,
        len: i64,
        part_id: *mut *mut c_char,
        error: *mut *mut c_char,
    ) -> i32,

    /// Completes the upload with `n_parts` part ids, which must be in the order their bytes
    /// have in the object. The object becomes visible to a reader here and nowhere earlier.
    pub multipart_complete: unsafe extern "C-unwind" fn(
        uri: *const c_char,
        upload_id: *const c_char,
        n_parts: i32,
        part_ids: *const *const c_char,
        error: *mut *mut c_char,
    ) -> i32,

    /// Discards the upload and the parts already uploaded into it.
    pub multipart_abort: unsafe extern "C-unwind" fn(
        uri: *const c_char,
        upload_id: *const c_char,
        error: *mut *mut c_char,
    ) -> i32,

    /// Deletes the object at `uri`. `PGOS_NOT_FOUND` if there is none, so that removing
    /// something twice is not an error the second time.
    ///
    /// Here rather than in the main table because the main table has no destructive call at
    /// all: what needs one is the parallel `COPY TO` path that stages per-worker files and
    /// removes them once they are spliced together.
    pub delete: unsafe extern "C-unwind" fn(uri: *const c_char, error: *mut *mut c_char) -> i32,

    /// The objects immediately under `uri`, treated as a prefix: one level, no recursion, and
    /// the prefixes that stand for a subdirectory are not reported at all. Otherwise exactly
    /// [`PgObjectStoreApiV1::list`].
    ///
    /// Here because a recursive list is the wrong cost for a question about one directory. On a
    /// local filesystem it walks every subtree, so an unreadable directory nowhere near the
    /// prefix fails the call; on an object store it pages through every key under it.
    ///
    /// `None` on a tier 1 published before this entry point existed, which is why it is an
    /// `Option`: the slot it took was a reserved null.
    pub list_shallow: Option<
        unsafe extern "C-unwind" fn(
            uri: *const c_char,
            n_objects: *mut i32,
            locations: *mut *mut *mut c_char,
            sizes: *mut *mut i64,
            error: *mut *mut c_char,
        ) -> i32,
    >,

    /// Three spares, for the same reason the main table's were.
    pub reserved: [*const c_void; 3],
}

// SAFETY: as for the main table — plain integers and function pointers to `fn` items, immutable
// once published.
unsafe impl Sync for PgObjectStoreExtV1 {}

// SAFETY: every field is either a plain integer or a function pointer to a `fn` item, and the
// struct is immutable once published.
unsafe impl Sync for PgObjectStoreApiV1 {}

/// A resolved tier 1, and the calls a consumer makes on it.
///
/// Cheap to make and cheap to copy — it is a reference to a static the publishing library owns —
/// so a consumer may either hold one or resolve one per call.
#[derive(Clone, Copy)]
pub struct Client {
    api: &'static PgObjectStoreApiV1,
}

impl Client {
    /// Wraps the pointer found under [`PG_OBJECT_STORE_RENDEZVOUS_KEY`], checking that it is
    /// tier 1 and that it speaks the version this crate declares.
    ///
    /// The check is unconditional rather than a debug assertion: an ABI skew between two loaded
    /// `.so`s is a crash and not a link error, so it has to become a message while there is
    /// still a frame to report it from.
    ///
    /// # Safety
    ///
    /// `pointer` must be null or a `PgObjectStoreApiV1` published by a library that stays loaded
    /// for the life of the backend, which is what a rendezvous variable holds.
    pub unsafe fn from_rendezvous(pointer: *mut c_void) -> Result<Client, String> {
        if pointer.is_null() {
            return Err("pg_object_store is not loaded in this backend".to_string());
        }

        let api = unsafe { &*(pointer as *const PgObjectStoreApiV1) };

        if api.magic != PG_OBJECT_STORE_API_MAGIC || api.version != PG_OBJECT_STORE_API_VERSION {
            return Err(format!(
                "pg_object_store published magic 0x{:08x} version {}, this extension speaks \
                 0x{:08x} version {}",
                api.magic, api.version, PG_OBJECT_STORE_API_MAGIC, PG_OBJECT_STORE_API_VERSION
            ));
        }

        Ok(Client { api })
    }

    /// Whether the current user may reach a uri in a direction.
    ///
    /// Every call below enforces this inside tier 1, so this is for the case where the answer is
    /// needed *before* doing any work — validating a statement, or deciding whether to plan a
    /// parallel read.
    pub fn check_privilege(&self, uri: &str, copy_from: bool) -> Result<bool, String> {
        let uri = c_uri(uri)?;

        let mut allowed = false;
        let mut error = std::ptr::null_mut();

        let code = unsafe {
            (self.api.check_privilege)(uri.as_ptr(), copy_from, &mut allowed, &mut error)
        };

        if code != PGOS_OK {
            return Err(message(error, "could not check access to the uri"));
        }

        Ok(allowed)
    }

    /// The size of an object, or `None` if there is none.
    pub fn head(&self, uri: &str) -> Result<Option<i64>, String> {
        let c_uri = c_uri(uri)?;

        let mut size = 0;
        let mut error = std::ptr::null_mut();

        match unsafe { (self.api.head)(c_uri.as_ptr(), &mut size, &mut error) } {
            PGOS_OK => Ok(Some(size)),
            PGOS_NOT_FOUND => Ok(None),
            _ => Err(message(error, &format!("could not stat {uri}"))),
        }
    }

    /// One byte range of an object, or `None` if there is no such object. Short at the end of
    /// the object rather than an error, so a caller that asked for more than is there gets what
    /// is there.
    pub fn get_range(&self, uri: &str, offset: i64, length: i64) -> Result<Option<Bytes>, String> {
        let c_uri = c_uri(uri)?;

        let mut data = std::ptr::null_mut();
        let mut len = 0;
        let mut error = std::ptr::null_mut();

        let code = unsafe {
            (self.api.get_range)(
                c_uri.as_ptr(),
                offset,
                length,
                &mut data,
                &mut len,
                &mut error,
            )
        };

        match code {
            PGOS_OK => {}
            PGOS_NOT_FOUND => return Ok(None),
            _ => return Err(message(error, &format!("could not read {uri}"))),
        }

        Ok(Some(copy_out(data, len, uri)?))
    }

    /// The whole of an object, or `None` if there is none. Two round trips, and the whole file in
    /// the backend's memory: a format that can read a footer and then individual splits should be
    /// calling [`Client::get_ranges`] instead.
    pub fn get_all(&self, uri: &str) -> Result<Option<Bytes>, String> {
        let Some(length) = self.head(uri)? else {
            return Ok(None);
        };

        self.get_range(uri, 0, length)
    }

    /// Several byte ranges of one object, fetched concurrently inside tier 1.
    ///
    /// The plural form is the point: a format that reads a footer and then individual row groups
    /// issues coalesced concurrent fetches, and that concurrency has to stay on the side of the
    /// boundary that owns the runtime. Calling the singular form in a loop would serialize it.
    pub fn get_ranges(&self, uri: &str, ranges: &[(i64, i64)]) -> Result<Vec<Bytes>, String> {
        let c_uri = c_uri(uri)?;

        let offsets: Vec<i64> = ranges.iter().map(|(offset, _)| *offset).collect();
        let lengths: Vec<i64> = ranges.iter().map(|(_, length)| *length).collect();

        // tier 1 fills these in; they are the caller's arrays by contract
        let mut data: Vec<*mut u8> = vec![std::ptr::null_mut(); ranges.len()];
        let mut lens: Vec<i64> = vec![0; ranges.len()];
        let mut error = std::ptr::null_mut();

        let code = unsafe {
            (self.api.get_ranges)(
                c_uri.as_ptr(),
                ranges.len() as i32,
                offsets.as_ptr(),
                lengths.as_ptr(),
                data.as_mut_ptr(),
                lens.as_mut_ptr(),
                &mut error,
            )
        };

        match code {
            PGOS_OK => {}
            PGOS_NOT_FOUND => return Err(format!("there is no file at {uri}")),
            _ => return Err(message(error, &format!("could not read {uri}"))),
        }

        data.iter()
            .zip(lens.iter())
            .map(|(buffer, len)| copy_out(*buffer, *len, uri))
            .collect()
    }

    /// Writes a whole object, replacing anything already there.
    pub fn put(&self, uri: &str, data: &[u8]) -> Result<(), String> {
        let c_uri = c_uri(uri)?;

        let mut error = std::ptr::null_mut();

        let code =
            unsafe { (self.api.put)(c_uri.as_ptr(), data.as_ptr(), data.len() as i64, &mut error) };

        if code != PGOS_OK {
            return Err(message(error, &format!("could not write {uri}")));
        }

        Ok(())
    }

    /// The objects under a uri, treated as a prefix.
    pub fn list(&self, uri: &str) -> Result<Vec<(String, i64)>, String> {
        self.listed(uri, self.api.list)
    }

    /// The shared body of the two listing calls, which differ only in which entry point they
    /// are and so in how much of the store they walk.
    fn listed(
        &self,
        uri: &str,
        list: unsafe extern "C-unwind" fn(
            *const c_char,
            *mut i32,
            *mut *mut *mut c_char,
            *mut *mut i64,
            *mut *mut c_char,
        ) -> i32,
    ) -> Result<Vec<(String, i64)>, String> {
        let c_uri = c_uri(uri)?;

        let mut count = 0;
        let mut locations = std::ptr::null_mut();
        let mut sizes = std::ptr::null_mut();
        let mut error = std::ptr::null_mut();

        let code = unsafe {
            list(
                c_uri.as_ptr(),
                &mut count,
                &mut locations,
                &mut sizes,
                &mut error,
            )
        };

        if code != PGOS_OK {
            return Err(message(error, &format!("could not list {uri}")));
        }

        if count <= 0 || locations.is_null() || sizes.is_null() {
            return Ok(Vec::new());
        }

        Ok((0..count as usize)
            .map(|i| unsafe {
                let location = CStr::from_ptr(*locations.add(i))
                    .to_string_lossy()
                    .into_owned();

                (location, *sizes.add(i))
            })
            .collect())
    }

    /// The objects one level under a prefix uri, or `None` on a tier 1 that publishes no
    /// shallow list. A caller that only needs one directory should prefer this to
    /// [`Self::list`]; `None` means falling back to the recursive one.
    pub fn list_shallow(&self, uri: &str) -> Option<Result<Vec<(String, i64)>, String>> {
        let list_shallow = self.ext().ok()?.list_shallow?;

        Some(self.listed(uri, list_shallow))
    }

    /// Opens a streaming write. Errors if the resolved tier 1 is old enough not to have the
    /// entry points, which is the whole reason they are `Option`.
    pub fn open_writer(&self, uri: &str) -> Result<Writer, String> {
        let Some(put_begin) = self.api.put_begin else {
            return Err(
                "the installed pg_object_store cannot stream a write: it published no \
                 put_begin entry point. Upgrade it with ALTER EXTENSION pg_object_store \
                 UPDATE, and make sure the new library is the one this backend loaded."
                    .to_string(),
            );
        };

        let c_uri = c_uri(uri)?;

        let mut handle = std::ptr::null_mut();
        let mut error = std::ptr::null_mut();

        let code = unsafe { put_begin(c_uri.as_ptr(), &mut handle, &mut error) };

        if code != PGOS_OK {
            return Err(message(error, &format!("could not open {uri} for writing")));
        }

        if handle.is_null() {
            return Err(format!(
                "pg_object_store opened {uri} for writing but handed back no handle"
            ));
        }

        Ok(Writer {
            client: *self,
            handle,
            uri: uri.to_string(),
        })
    }

    /// The second table, or an error naming what is missing. Every call below starts here, so a
    /// tier 1 too old to have it says so once, in the caller's terms.
    fn ext(&self) -> Result<&'static PgObjectStoreExtV1, String> {
        if self.api.ext.is_null() {
            return Err(
                "the installed pg_object_store cannot share one upload between processes: it \
                 published no extension table. Upgrade it with ALTER EXTENSION pg_object_store \
                 UPDATE, and make sure the new library is the one this backend loaded."
                    .to_string(),
            );
        }

        let ext = unsafe { &*(self.api.ext as *const PgObjectStoreExtV1) };

        if ext.magic != PG_OBJECT_STORE_EXT_MAGIC || ext.version != PG_OBJECT_STORE_EXT_VERSION {
            return Err(format!(
                "pg_object_store published an extension table with magic 0x{:08x} version {}, \
                 this extension speaks 0x{:08x} version {}",
                ext.magic, ext.version, PG_OBJECT_STORE_EXT_MAGIC, PG_OBJECT_STORE_EXT_VERSION
            ));
        }

        Ok(ext)
    }

    /// Whether this tier 1 has the multi-process upload calls at all, for a caller that plans
    /// around their absence rather than failing on it.
    pub fn can_share_upload(&self) -> bool {
        self.ext().is_ok()
    }

    /// Starts a multipart upload, or `None` when the store behind the uri has no multipart
    /// upload — the local filesystem and `http`. A caller that needs several processes writing
    /// one object has to take a different path for those; see [`Client::multipart_put_part`].
    pub fn multipart_begin(&self, uri: &str) -> Result<Option<String>, String> {
        let ext = self.ext()?;
        let c_uri = c_uri(uri)?;

        let mut upload_id = std::ptr::null_mut();
        let mut error = std::ptr::null_mut();

        let code = unsafe { (ext.multipart_begin)(c_uri.as_ptr(), &mut upload_id, &mut error) };

        if code != PGOS_OK {
            return Err(message(
                error,
                &format!("could not start a multipart upload of {uri}"),
            ));
        }

        if upload_id.is_null() {
            return Ok(None);
        }

        Ok(Some(
            unsafe { CStr::from_ptr(upload_id) }
                .to_string_lossy()
                .into_owned(),
        ))
    }

    /// Uploads one part of an upload and returns the id the store gave it.
    ///
    /// `part` is what orders the parts in the finished object, not the order the calls are made
    /// in: that is the whole point, since the processes making them do not see each other. Every
    /// part but the one that ends the object has to be at least [`MIN_PART_SIZE`].
    pub fn multipart_put_part(
        &self,
        uri: &str,
        upload_id: &str,
        part: i32,
        data: &[u8],
    ) -> Result<String, String> {
        let ext = self.ext()?;
        let c_uri = c_uri(uri)?;
        let c_upload_id = c_string(upload_id, "upload id")?;

        let mut part_id = std::ptr::null_mut();
        let mut error = std::ptr::null_mut();

        let code = unsafe {
            (ext.multipart_put_part)(
                c_uri.as_ptr(),
                c_upload_id.as_ptr(),
                part,
                data.as_ptr(),
                data.len() as i64,
                &mut part_id,
                &mut error,
            )
        };

        if code != PGOS_OK {
            return Err(message(
                error,
                &format!("could not upload part {part} of {uri}"),
            ));
        }

        if part_id.is_null() {
            return Err(format!(
                "pg_object_store uploaded part {part} of {uri} but handed back no part id"
            ));
        }

        Ok(unsafe { CStr::from_ptr(part_id) }
            .to_string_lossy()
            .into_owned())
    }

    /// Completes an upload with the part ids in the order their bytes have in the object.
    pub fn multipart_complete(
        &self,
        uri: &str,
        upload_id: &str,
        part_ids: &[String],
    ) -> Result<(), String> {
        let ext = self.ext()?;
        let c_uri = c_uri(uri)?;
        let c_upload_id = c_string(upload_id, "upload id")?;

        let parts = part_ids
            .iter()
            .map(|part_id| c_string(part_id, "part id"))
            .collect::<Result<Vec<CString>, String>>()?;

        // the CStrings have to outlive the call, so the pointer array is built out of the vec
        // that owns them rather than from a temporary per element
        let pointers: Vec<*const c_char> = parts.iter().map(|part| part.as_ptr()).collect();

        let mut error = std::ptr::null_mut();

        let code = unsafe {
            (ext.multipart_complete)(
                c_uri.as_ptr(),
                c_upload_id.as_ptr(),
                pointers.len() as i32,
                pointers.as_ptr(),
                &mut error,
            )
        };

        if code != PGOS_OK {
            return Err(message(
                error,
                &format!("could not complete the multipart upload of {uri}"),
            ));
        }

        Ok(())
    }

    /// Discards an upload. Used on the error path of whichever process owns the upload, so it
    /// reports what went wrong rather than swallowing it.
    pub fn multipart_abort(&self, uri: &str, upload_id: &str) -> Result<(), String> {
        let ext = self.ext()?;
        let c_uri = c_uri(uri)?;
        let c_upload_id = c_string(upload_id, "upload id")?;

        let mut error = std::ptr::null_mut();

        let code =
            unsafe { (ext.multipart_abort)(c_uri.as_ptr(), c_upload_id.as_ptr(), &mut error) };

        if code != PGOS_OK {
            return Err(message(
                error,
                &format!("could not abort the multipart upload of {uri}"),
            ));
        }

        Ok(())
    }

    /// Deletes an object. `false` if there was none, so that removing it twice is not an error
    /// the second time.
    pub fn delete(&self, uri: &str) -> Result<bool, String> {
        let ext = self.ext()?;
        let c_uri = c_uri(uri)?;

        let mut error = std::ptr::null_mut();

        match unsafe { (ext.delete)(c_uri.as_ptr(), &mut error) } {
            PGOS_OK => Ok(true),
            PGOS_NOT_FOUND => Ok(false),
            _ => Err(message(error, &format!("could not delete {uri}"))),
        }
    }
}

/// The smallest a part of a multipart upload may be, which every part but the one that ends the
/// object has to reach. Declared here because it is the stores' rule and not a format's: a
/// caller that produces less than this for a part has to hand the bytes to whoever writes the
/// end of the object instead.
pub const MIN_PART_SIZE: usize = 5 * 1024 * 1024;

/// An object being written incrementally.
///
/// A `COPY TO` produces bytes as it goes and the file can be larger than the backend's memory,
/// so [`Client::put`] is not enough for it. Tier 1 holds the upload; what is here is a handle
/// and the rule that a handle has to end somewhere.
///
/// Dropping without [`Writer::finish`] aborts, which is what makes an error anywhere in the
/// middle of a `COPY TO` leave no half-written file behind. Tier 1 would sweep the upload at
/// transaction end anyway; doing it in `Drop` is what frees the remote resource at the end of
/// the statement rather than at the end of the transaction.
pub struct Writer {
    client: Client,
    handle: *mut c_void,
    uri: String,
}

impl Writer {
    /// The uri being written, for a caller that wants it in a message of its own.
    pub fn uri(&self) -> &str {
        &self.uri
    }

    /// Appends bytes.
    ///
    /// A failure has already discarded the upload inside tier 1, so the handle is forgotten here
    /// too: aborting a closed handle would be an error of its own, raised in place of the real
    /// one.
    pub fn write(&mut self, data: &[u8]) -> Result<(), String> {
        let Some(put_part) = self.client.api.put_part else {
            return Err("pg_object_store published put_begin without put_part".to_string());
        };

        let mut error = std::ptr::null_mut();

        let code = unsafe { put_part(self.handle, data.as_ptr(), data.len() as i64, &mut error) };

        if code != PGOS_OK {
            self.handle = std::ptr::null_mut();
            return Err(message(error, &format!("could not write to {}", self.uri)));
        }

        Ok(())
    }

    /// Completes the upload. The file becomes visible to a reader here and nowhere earlier,
    /// which is why this is a consuming call: there is nothing to do with the writer after.
    pub fn finish(mut self) -> Result<(), String> {
        let Some(put_finish) = self.client.api.put_finish else {
            return Err("pg_object_store published put_begin without put_finish".to_string());
        };

        let handle = std::mem::replace(&mut self.handle, std::ptr::null_mut());

        let mut error = std::ptr::null_mut();

        if unsafe { put_finish(handle, &mut error) } != PGOS_OK {
            return Err(message(
                error,
                &format!("could not finish writing {}", self.uri),
            ));
        }

        Ok(())
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        if self.handle.is_null() {
            return;
        }

        let Some(put_abort) = self.client.api.put_abort else {
            return;
        };

        let mut error = std::ptr::null_mut();

        // the result is dropped on purpose: this runs while unwinding from someone else's
        // failure, and tier 1's transaction-end sweep is the backstop if it does not land
        unsafe { put_abort(self.handle, &mut error) };

        self.handle = std::ptr::null_mut();
    }
}

fn c_uri(uri: &str) -> Result<CString, String> {
    CString::new(uri).map_err(|_| "uri must not contain a nul byte".to_string())
}

/// A store-supplied identifier on its way back across the boundary. Named in the message
/// because an upload id and a part id are opaque to us, so a caller that managed to produce an
/// unrepresentable one needs to know which of them it was.
fn c_string(value: &str, what: &str) -> Result<CString, String> {
    CString::new(value).map_err(|_| format!("{what} must not contain a nul byte"))
}

/// Reads the vtable's error out-parameter, falling back to a message of the caller's when tier 1
/// returned a code and no text.
fn message(error: *mut c_char, fallback: &str) -> String {
    if error.is_null() {
        return fallback.to_string();
    }

    unsafe { CStr::from_ptr(error) }
        .to_string_lossy()
        .into_owned()
}

/// Copies a palloc'd buffer out into memory this crate's caller owns. Tier 1 allocates in the
/// caller's `CurrentMemoryContext`, and an Arrow decoder holds the bytes long past it.
fn copy_out(data: *mut u8, len: i64, uri: &str) -> Result<Bytes, String> {
    if data.is_null() || len < 0 {
        return Err(format!(
            "pg_object_store read {uri} but handed back nothing"
        ));
    }

    let bytes = unsafe { std::slice::from_raw_parts(data, len as usize) };

    Ok(Bytes::copy_from_slice(bytes))
}
