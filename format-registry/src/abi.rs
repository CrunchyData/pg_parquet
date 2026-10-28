//! The C ABI a provider in its own `.so` registers over, and the adapter that turns one back
//! into a [`FormatProvider`].
//!
//! ## Why there has to be a C ABI
//!
//! Rust has no stable ABI and a pgrx extension is a `cdylib`: `nm -D --defined-only` on a built
//! extension shows its Postgres entry points and nothing else. `pg_arrow` cannot hand
//! `pg_arrow_base` a `Box<dyn FormatProvider>` even though both are Rust, both link the same
//! version of this crate, and the trait is right here. The vtable below is the same shape the
//! trait is, expressed in things that do have a stable layout: `#[repr(C)]` structs, function
//! pointers, and the Arrow C Data Interface.
//!
//! The data path is `ArrowArray` / `ArrowSchema` / `ArrowArrayStream` — the Arrow C Data
//! Interface, which is versioned by Arrow itself and stable across arrow-rs releases. That
//! matters more than it looks: it means `pg_arrow_base` and a provider may be built against
//! *different* arrow-rs versions. Nothing else here would survive that.
//!
//! ## Ownership
//!
//! - A `*const c_char` in [`PgArrowProviderV1`] itself (`name`, `file_extensions`) is a static
//!   string owned by the provider and read by the base.
//! - A `char **error` out-parameter, and every array behind a `***`, is palloc'd by the callee
//!   in the caller's `CurrentMemoryContext`. There is no free function: the caller's context
//!   teardown is the free. A caller that needs a value to outlive the statement copies it.
//! - An `ArrowArray` or `ArrowSchema` passed *in* is **consumed**: the callee owns it and must
//!   release it, exactly as the Arrow C Data Interface specifies. One filled in as an out
//!   parameter is handed to the caller on the same terms.
//!
//! ## Whole files, and the two ways around them
//!
//! `schema`, `kv_metadata`, `file_info` and `open_reader` take a whole file, and
//! `finish_writer` hands one back. That is right for a schema probe and wrong for a 40 GB
//! parquet file, so v2 adds the two entry points that avoid it, both optional:
//!
//! - `open_ranged_reader` is handed a [`PgArrowByteSourceV1`] instead of a buffer, and fetches
//!   what it needs — a footer, then the row groups it decided on. It can also be restricted to
//!   a half-open range of splits, which is what gives one worker of a parallel `COPY FROM` its
//!   share of the file.
//! - `open_streaming_writer` is handed a [`PgArrowByteSinkV1`] and writes the file into it as
//!   it goes, so a `COPY TO` of any size never materializes the output on either side.
//!
//! Both are `Option`: a format that cannot do either says so by leaving the slot null and is
//! driven whole. `can_read_ranges` / `can_stream_write` on the Rust trait are exactly these
//! null checks.
//!
//! Neither callback takes an error out-parameter, which is deliberate. Allocating a message to
//! hand *back* across the boundary would need the caller's allocator, and this crate has no
//! pgrx dependency and so no `palloc`. Instead the base keeps the message in the callback's own
//! context and the provider propagates only the code; the base then reports its own message,
//! which is the one that names the uri and the real failure, rather than the provider's
//! second-hand account of it.
//!
//! ## Reserved slots
//!
//! `reserved` is for what comes after: a `validate_options` taking the whole list at once, for
//! options that are only invalid in combination, and a multi-process multipart writer for a
//! parallel `COPY TO` of a single file.

use std::ffi::{c_char, c_void, CStr};
use std::mem::ManuallyDrop;
use std::ops::Range;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex};

use arrow::array::{Array, RecordBatch, StructArray};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::ffi::{to_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow::record_batch::RecordBatchReader;
use bytes::Bytes;

use crate::options::FormatOptions;
use crate::provider::{
    ByteSink, ByteSource, FileInfo, FormatProvider, RecordBatchSink, StreamingSink,
};
use crate::Result;

/// "PAB1".
pub const PG_ARROW_REGISTRY_MAGIC: u32 = 0x5041_4231;
/// "PAP1".
pub const PG_ARROW_PROVIDER_MAGIC: u32 = 0x5041_5031;

pub const PG_ARROW_REGISTRY_VERSION: u32 = 1;
/// 2 adds the ranged reader and the streaming writer. A bump rather than slot-filling because
/// nothing outside these repositories has ever registered a provider: every provider in a
/// cluster is built from the same source tree as the base, so making the new entry points
/// first-class fields is free, and the alternative — four `reserved` slots carrying meaning
/// only the comments record — is how an ABI becomes unreadable.
/// 3 adds `ranged_file_info`: the row count of a file that is read in ranges and therefore
/// never held whole, which is what `pg_stat_progress_copy` needs to report a `bytes_total` for
/// a `COPY FROM`. It also adds `streaming_writer_bytes_written`, for the other direction: a
/// provider's writer buffers, so the bytes the base has seen are not the bytes of the file.
pub const PG_ARROW_PROVIDER_VERSION: u32 = 3;

/// Where the base publishes its registry. A provider's `_PG_init` resolves this with
/// `find_rendezvous_variable`, falling back to `load_file("$libdir/pg_arrow_base")` if the slot
/// is null — which it will be unless the base is in `shared_preload_libraries`.
pub const PG_ARROW_REGISTRY_RENDEZVOUS_KEY: &str = "pg_arrow_base.registry.v1";

pub const PGARROW_OK: i32 = 0;
/// The format cannot answer this question at all — Arrow IPC has no file-level compression to
/// report, avro has no footer row count. Distinct from an error so the base can render NULL
/// instead of failing a query.
pub const PGARROW_UNSUPPORTED: i32 = 1;
pub const PGARROW_ERROR: i32 = -1;

/// The flat form of [`FileInfo`]. `has_` flags rather than sentinel values, because 0 rows and
/// "unknown rows" are different answers and a file with 0 rows is a real thing.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct PgArrowFileInfoV1 {
    pub has_num_rows: bool,
    pub num_rows: i64,
    pub has_num_splits: bool,
    pub num_splits: i64,
    /// Static, owned by the provider. Null if it has no better word than the default.
    pub split_label: *const c_char,
    /// palloc'd by the provider, or null.
    pub compression: *mut c_char,
    /// palloc'd by the provider, or null.
    pub format_version: *mut c_char,
}

impl Default for PgArrowFileInfoV1 {
    fn default() -> Self {
        Self {
            has_num_rows: false,
            num_rows: 0,
            has_num_splits: false,
            num_splits: 0,
            split_label: std::ptr::null(),
            compression: std::ptr::null_mut(),
            format_version: std::ptr::null_mut(),
        }
    }
}

/// Random access to a file the base holds. Installed by the base, called by a provider.
///
/// `get_ranges` fills caller-supplied arrays: `data` and `lens` have `n_ranges` elements, and
/// `(*data)[i]` is set to a buffer the *base* owns. Those buffers are valid only until the next
/// call on the same source, and the provider must not free them. A provider that holds bytes
/// for longer copies them, which is what [`CByteSource`] does.
///
/// A non-`PGARROW_OK` return means the read failed and the message is on the base's side. Stop
/// and propagate; do not retry.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct PgArrowByteSourceV1 {
    pub ctx: *mut c_void,

    /// The size of the whole file in bytes, or -1 on failure.
    pub size: unsafe extern "C-unwind" fn(ctx: *mut c_void) -> i64,

    pub get_ranges: unsafe extern "C-unwind" fn(
        ctx: *mut c_void,
        n_ranges: i32,
        offsets: *const i64,
        lengths: *const i64,
        data: *mut *mut u8,
        lens: *mut i64,
    ) -> i32,
}

/// Where a streaming writer's bytes go. Installed by the base, called by a provider.
///
/// Every call is a new chunk appended to the file; the base does not reorder, buffer
/// selectively or retry. A non-`PGARROW_OK` return means the upload failed.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct PgArrowByteSinkV1 {
    pub ctx: *mut c_void,

    pub write: unsafe extern "C-unwind" fn(ctx: *mut c_void, data: *const u8, len: i64) -> i32,
}

/// What a provider fills in and registers. Laid out to mirror [`FormatProvider`] one to one, so
/// that the adapter below is mechanical and a reader can check it against the trait.
#[repr(C)]
pub struct PgArrowProviderV1 {
    pub magic: u32,
    pub version: u32,

    /// The name in `COPY ... WITH (format 'x')`. Static, lower case.
    pub name: *const c_char,
    /// Comma-separated, no dots, lower case, e.g. `"arrow,arrows,feather"`. Static. May be
    /// empty but not null.
    pub file_extensions: *const c_char,

    pub validate_option: unsafe extern "C-unwind" fn(
        name: *const c_char,
        value: *const c_char,
        error: *mut *mut c_char,
    ) -> i32,

    /// Fills `out` with the file's schema. The caller owns it afterwards.
    pub schema: unsafe extern "C-unwind" fn(
        data: *const u8,
        len: i64,
        out: *mut FFI_ArrowSchema,
        error: *mut *mut c_char,
    ) -> i32,

    /// `*keys` and `*values` are palloc'd arrays of `*n` C strings; a null `(*values)[i]` is a
    /// key stored with no value, which is not the same as an empty one.
    pub kv_metadata: unsafe extern "C-unwind" fn(
        data: *const u8,
        len: i64,
        n: *mut i32,
        keys: *mut *mut *mut c_char,
        values: *mut *mut *mut c_char,
        error: *mut *mut c_char,
    ) -> i32,

    pub file_info: unsafe extern "C-unwind" fn(
        data: *const u8,
        len: i64,
        out: *mut PgArrowFileInfoV1,
        error: *mut *mut c_char,
    ) -> i32,

    /// Fills `out` with a stream over the file. The provider must not retain `data` past the
    /// end of the stream's life; the base keeps the buffer alive until the stream is dropped.
    pub open_reader: unsafe extern "C-unwind" fn(
        data: *const u8,
        len: i64,
        n_options: i32,
        option_names: *const *const c_char,
        option_values: *const *const c_char,
        out: *mut FFI_ArrowArrayStream,
        error: *mut *mut c_char,
    ) -> i32,

    /// `*handle` is an opaque provider-owned writer. The schema is consumed.
    pub open_writer: unsafe extern "C-unwind" fn(
        schema: *mut FFI_ArrowSchema,
        n_options: i32,
        option_names: *const *const c_char,
        option_values: *const *const c_char,
        handle: *mut *mut c_void,
        error: *mut *mut c_char,
    ) -> i32,

    /// Both the array and the schema are consumed.
    pub write_batch: unsafe extern "C-unwind" fn(
        handle: *mut c_void,
        array: *mut FFI_ArrowArray,
        schema: *mut FFI_ArrowSchema,
        error: *mut *mut c_char,
    ) -> i32,

    /// Finishes the file and hands back the whole of it, palloc'd. The handle is closed and
    /// must not be used again, whether this succeeds or fails.
    pub finish_writer: unsafe extern "C-unwind" fn(
        handle: *mut c_void,
        data: *mut *mut u8,
        len: *mut i64,
        error: *mut *mut c_char,
    ) -> i32,

    /// Discards a writer without finishing it, for a statement that errors partway.
    ///
    /// Exactly one of this and `finish_writer` runs for a handle. `finish_writer` closes the
    /// handle whether it succeeded or not and the base forgets it at that point, so a provider
    /// may free its allocation there; this is only ever called on a handle that was never
    /// finished. A null handle must be tolerated.
    pub close_writer: unsafe extern "C-unwind" fn(handle: *mut c_void),

    /// Fills `out` with a stream over a file the provider fetches through `source`, optionally
    /// restricted to splits `[split_begin, split_end)`. A negative `split_begin` means the
    /// whole file; `split_end` is ignored in that case.
    ///
    /// `source` is valid until the stream is released. Null slot means this format cannot read
    /// in ranges and must be handed a whole buffer.
    pub open_ranged_reader: Option<
        unsafe extern "C-unwind" fn(
            source: *const PgArrowByteSourceV1,
            split_begin: i64,
            split_end: i64,
            n_options: i32,
            option_names: *const *const c_char,
            option_values: *const *const c_char,
            out: *mut FFI_ArrowArrayStream,
            error: *mut *mut c_char,
        ) -> i32,
    >,

    /// `*handle` is an opaque provider-owned writer that writes the file into `sink` as it
    /// goes. The schema is consumed; `sink` is valid until the handle is closed. Batches go in
    /// through `write_batch`, the same as a buffered writer, and the handle ends in
    /// `finish_streaming_writer` or `close_writer`.
    ///
    /// Null slot means this format cannot write incrementally and must be buffered whole.
    pub open_streaming_writer: Option<
        unsafe extern "C-unwind" fn(
            schema: *mut FFI_ArrowSchema,
            n_options: i32,
            option_names: *const *const c_char,
            option_values: *const *const c_char,
            sink: *const PgArrowByteSinkV1,
            handle: *mut *mut c_void,
            error: *mut *mut c_char,
        ) -> i32,
    >,

    /// Writes the footer through the sink and closes the handle, which must not be used again
    /// whether this succeeded or failed. There is no buffer to hand back: the file is already
    /// in the sink.
    pub finish_streaming_writer:
        Option<unsafe extern "C-unwind" fn(handle: *mut c_void, error: *mut *mut c_char) -> i32>,

    /// Fills `out` from a file read through `source` rather than from a buffer. `source` is
    /// valid for the duration of the call only, so a provider must not retain it.
    ///
    /// Null slot, or `PGARROW_UNSUPPORTED`, means the format cannot say anything about a file
    /// it has not been handed whole — which is the honest answer unless the footer is enough,
    /// since fetching the file to answer would defeat the point of reading it in ranges.
    /// How many bytes of the file a streaming writer has produced so far, or a negative number
    /// for a writer that does not count. A plain counter, so there is no error channel: a
    /// provider that cannot answer says so with the sign rather than failing the `COPY`.
    pub streaming_writer_bytes_written:
        Option<unsafe extern "C-unwind" fn(handle: *mut c_void) -> i64>,

    pub ranged_file_info: Option<
        unsafe extern "C-unwind" fn(
            source: *const PgArrowByteSourceV1,
            out: *mut PgArrowFileInfoV1,
            error: *mut *mut c_char,
        ) -> i32,
    >,

    pub reserved: [*const c_void; 4],
}

/// What the base publishes for providers to find.
#[repr(C)]
pub struct PgArrowRegistryApiV1 {
    pub magic: u32,
    pub version: u32,

    /// Registers a provider for the life of the backend. The base keeps the pointer, so it has
    /// to point at something with static lifetime in the provider's `.so`.
    pub register_provider: unsafe extern "C-unwind" fn(
        provider: *const PgArrowProviderV1,
        error: *mut *mut c_char,
    ) -> i32,

    pub unregister_provider: unsafe extern "C-unwind" fn(name: *const c_char) -> bool,

    pub reserved: [*const c_void; 4],
}

// SAFETY: both structs are built once as a `static` — the base's registry on one side, a
// provider's descriptor on the other — and never mutated after publication. Every field is a
// plain integer, a function pointer to a `fn` item, or a pointer to a static string.
unsafe impl Sync for PgArrowRegistryApiV1 {}
unsafe impl Sync for PgArrowProviderV1 {}

/// A provider living in another `.so`, seen as a [`FormatProvider`].
///
/// Everything above the registry — the UDFs, the COPY hook, format inference — is written once
/// against the trait and does not know whether it is talking to this or to a statically linked
/// provider.
pub struct CProvider {
    api: *const PgArrowProviderV1,
    name: String,
    file_extensions: Vec<String>,
}

// SAFETY: `api` points at a static in a library that Postgres never unloads, and nothing here
// mutates through it. The provider's own functions are called from a backend or from a tokio
// worker owned by tier 1, and the trait requires them to tolerate that.
unsafe impl Send for CProvider {}
unsafe impl Sync for CProvider {}

impl CProvider {
    /// Checks the magic and the version before trusting a single other field. The check has to
    /// happen here: an ABI skew between two `.so`s is a crash, not a link error.
    ///
    /// # Safety
    ///
    /// `api` must be null or point at a `PgArrowProviderV1` that outlives the backend, since
    /// the registry keeps it. In practice that means a `static` in the provider's library.
    /// Everything else about it is checked rather than assumed.
    pub unsafe fn new(api: *const PgArrowProviderV1) -> Result<Self> {
        if api.is_null() {
            return Err("a format provider must not be null".to_string());
        }

        let provider = unsafe { &*api };

        if provider.magic != PG_ARROW_PROVIDER_MAGIC {
            return Err(format!(
                "format provider magic is 0x{:08x}, expected 0x{:08x}; the library is not a \
                 pg_arrow provider",
                provider.magic, PG_ARROW_PROVIDER_MAGIC
            ));
        }

        if provider.version != PG_ARROW_PROVIDER_VERSION {
            return Err(format!(
                "format provider speaks v{}, this pg_arrow_base speaks v{}",
                provider.version, PG_ARROW_PROVIDER_VERSION
            ));
        }

        let name = cstring(provider.name)
            .ok_or_else(|| "a format provider must have a name".to_string())?
            .to_ascii_lowercase();

        let file_extensions = cstring(provider.file_extensions)
            .unwrap_or_default()
            .split(',')
            .map(|extension| {
                extension
                    .trim()
                    .trim_start_matches('.')
                    .to_ascii_lowercase()
            })
            .filter(|extension| !extension.is_empty())
            .collect();

        Ok(Self {
            api,
            name,
            file_extensions,
        })
    }

    fn api(&self) -> &PgArrowProviderV1 {
        unsafe { &*self.api }
    }
}

/// Reads an out-parameter error message, or invents one, so that no failure crosses the
/// boundary without a reason attached.
fn error_message(error: *mut c_char, fallback: &str) -> String {
    cstring(error).unwrap_or_else(|| fallback.to_string())
}

fn cstring(pointer: *const c_char) -> Option<String> {
    if pointer.is_null() {
        return None;
    }

    unsafe { CStr::from_ptr(pointer) }
        .to_str()
        .ok()
        .map(str::to_string)
}

/// Options as the two parallel arrays the ABI wants. Returns the owning `CString`s too: they
/// have to outlive the call, and forgetting that is the obvious way to write a use-after-free
/// here.
fn c_options(
    options: &FormatOptions,
) -> (
    Vec<std::ffi::CString>,
    Vec<*const c_char>,
    Vec<*const c_char>,
) {
    let mut owned = Vec::with_capacity(options.len() * 2);
    let mut names = Vec::with_capacity(options.len());
    let mut values = Vec::with_capacity(options.len());

    for (name, value) in options.iter() {
        // an interior nul cannot come from the Postgres grammar, and a lossy conversion here
        // would silently change the option
        let name = std::ffi::CString::new(name).unwrap_or_default();
        let value = std::ffi::CString::new(value).unwrap_or_default();

        names.push(name.as_ptr());
        values.push(value.as_ptr());

        owned.push(name);
        owned.push(value);
    }

    (owned, names, values)
}

impl FormatProvider for CProvider {
    fn name(&self) -> String {
        self.name.clone()
    }

    fn file_extensions(&self) -> Vec<String> {
        self.file_extensions.clone()
    }

    fn validate_option(&self, name: &str, value: &str) -> Result<()> {
        let name = std::ffi::CString::new(name).map_err(|_| "option name has a nul".to_string())?;
        let value =
            std::ffi::CString::new(value).map_err(|_| "option value has a nul".to_string())?;

        let mut error = std::ptr::null_mut();

        let code =
            unsafe { (self.api().validate_option)(name.as_ptr(), value.as_ptr(), &mut error) };

        match code {
            PGARROW_OK => Ok(()),
            _ => Err(error_message(
                error,
                &format!("{} rejected an option", self.name),
            )),
        }
    }

    fn schema(&self, data: &[u8]) -> Result<SchemaRef> {
        let mut ffi_schema = FFI_ArrowSchema::empty();
        let mut error = std::ptr::null_mut();

        let code = unsafe {
            (self.api().schema)(
                data.as_ptr(),
                data.len() as i64,
                &mut ffi_schema,
                &mut error,
            )
        };

        if code != PGARROW_OK {
            return Err(error_message(
                error,
                &format!("{} could not read the file's schema", self.name),
            ));
        }

        let schema = Schema::try_from(&ffi_schema).map_err(|e| {
            format!(
                "{} returned a schema this arrow build cannot read: {e}",
                self.name
            )
        })?;

        Ok(SchemaRef::new(schema))
    }

    fn kv_metadata(&self, data: &[u8]) -> Result<Vec<(String, Option<String>)>> {
        let mut count = 0;
        let mut keys = std::ptr::null_mut();
        let mut values = std::ptr::null_mut();
        let mut error = std::ptr::null_mut();

        let code = unsafe {
            (self.api().kv_metadata)(
                data.as_ptr(),
                data.len() as i64,
                &mut count,
                &mut keys,
                &mut values,
                &mut error,
            )
        };

        match code {
            PGARROW_OK => {}
            PGARROW_UNSUPPORTED => return Ok(Vec::new()),
            _ => {
                return Err(error_message(
                    error,
                    &format!("{} could not read the file's metadata", self.name),
                ))
            }
        }

        if count <= 0 || keys.is_null() {
            return Ok(Vec::new());
        }

        let mut metadata = Vec::with_capacity(count as usize);

        for i in 0..count as usize {
            let key = unsafe { cstring(*keys.add(i)) };
            let value = if values.is_null() {
                None
            } else {
                unsafe { cstring(*values.add(i)) }
            };

            if let Some(key) = key {
                metadata.push((key, value));
            }
        }

        Ok(metadata)
    }

    fn file_info(&self, data: &[u8]) -> Result<FileInfo> {
        let mut out = PgArrowFileInfoV1::default();
        let mut error = std::ptr::null_mut();

        let code = unsafe {
            (self.api().file_info)(data.as_ptr(), data.len() as i64, &mut out, &mut error)
        };

        match code {
            PGARROW_OK => {}
            PGARROW_UNSUPPORTED => return Ok(FileInfo::default()),
            _ => {
                return Err(error_message(
                    error,
                    &format!("{} could not describe the file", self.name),
                ))
            }
        }

        Ok(FileInfo {
            num_rows: out.has_num_rows.then_some(out.num_rows),
            num_splits: out.has_num_splits.then_some(out.num_splits),
            split_label: cstring(out.split_label),
            compression: cstring(out.compression),
            format_version: cstring(out.format_version),
        })
    }

    fn ranged_file_info(&self, source: Arc<dyn ByteSource>) -> Result<FileInfo> {
        let Some(ranged_file_info) = self.api().ranged_file_info else {
            return Ok(FileInfo::default());
        };

        let mut out = PgArrowFileInfoV1::default();
        let mut error = std::ptr::null_mut();

        // only for the duration of the call: unlike the ranged reader, nothing the provider
        // hands back here holds on to the source
        let exported = ExportedByteSource::new(source);
        let c_source = exported.as_c();

        let code = unsafe { ranged_file_info(&c_source, &mut out, &mut error) };

        match code {
            PGARROW_OK => {}
            PGARROW_UNSUPPORTED => return Ok(FileInfo::default()),
            _ => {
                // the base's own message, if the read was what failed, is the one that names
                // the uri
                let message = exported.take_error().unwrap_or_else(|| {
                    error_message(error, &format!("{} could not describe the file", self.name))
                });

                return Err(message);
            }
        }

        Ok(FileInfo {
            num_rows: out.has_num_rows.then_some(out.num_rows),
            num_splits: out.has_num_splits.then_some(out.num_splits),
            split_label: cstring(out.split_label),
            compression: cstring(out.compression),
            format_version: cstring(out.format_version),
        })
    }

    fn open_reader(
        &self,
        data: Bytes,
        options: &FormatOptions,
    ) -> Result<Box<dyn RecordBatchReader + Send>> {
        let (_owned, names, values) = c_options(options);

        let mut stream = FFI_ArrowArrayStream::empty();
        let mut error = std::ptr::null_mut();

        let code = unsafe {
            (self.api().open_reader)(
                data.as_ptr(),
                data.len() as i64,
                names.len() as i32,
                names.as_ptr(),
                values.as_ptr(),
                &mut stream,
                &mut error,
            )
        };

        if code != PGARROW_OK {
            return Err(error_message(
                error,
                &format!("{} could not open the file for reading", self.name),
            ));
        }

        let reader = ArrowArrayStreamReader::try_new(stream).map_err(|e| {
            format!(
                "{} returned a stream this arrow build cannot read: {e}",
                self.name
            )
        })?;

        // the provider was told not to retain `data`, but it is holding a pointer into it for
        // the life of the stream, so the buffer has to outlive the reader
        Ok(Box::new(BufferBackedReader {
            reader,
            _data: data,
        }))
    }

    fn open_writer(
        &self,
        schema: SchemaRef,
        options: &FormatOptions,
    ) -> Result<Box<dyn RecordBatchSink>> {
        let (_owned, names, values) = c_options(options);

        let ffi_schema = FFI_ArrowSchema::try_from(schema.as_ref())
            .map_err(|e| format!("could not export the schema across the boundary: {e}"))?;

        // consumed by the callee, so it must not also be released here
        let mut ffi_schema = ManuallyDrop::new(ffi_schema);

        let mut handle = std::ptr::null_mut();
        let mut error = std::ptr::null_mut();

        let code = unsafe {
            (self.api().open_writer)(
                &mut *ffi_schema,
                names.len() as i32,
                names.as_ptr(),
                values.as_ptr(),
                &mut handle,
                &mut error,
            )
        };

        if code != PGARROW_OK {
            return Err(error_message(
                error,
                &format!("{} could not open the file for writing", self.name),
            ));
        }

        if handle.is_null() {
            return Err(format!("{} returned no writer", self.name));
        }

        Ok(Box::new(CSink {
            api: self.api,
            name: self.name.clone(),
            handle,
        }))
    }

    fn can_read_ranges(&self) -> bool {
        self.api().open_ranged_reader.is_some()
    }

    fn open_ranged_reader(
        &self,
        source: Arc<dyn ByteSource>,
        options: &FormatOptions,
        splits: Option<Range<i64>>,
    ) -> Result<Box<dyn RecordBatchReader + Send>> {
        let Some(open_ranged_reader) = self.api().open_ranged_reader else {
            return Err(format!(
                "{} cannot read a file in ranges; it has to be read whole",
                self.name
            ));
        };

        let (_owned, names, values) = c_options(options);

        // the exported source has to outlive the stream, so it is moved into the reader below
        let exported = ExportedByteSource::new(source);
        let c_source = exported.as_c();

        // a negative begin is the whole file, which is why the pair is not an Option across the
        // boundary: two i64s with a documented sentinel beat a third struct
        let (split_begin, split_end) = match splits {
            Some(range) => (range.start, range.end),
            None => (-1, -1),
        };

        let mut stream = FFI_ArrowArrayStream::empty();
        let mut error = std::ptr::null_mut();

        let code = unsafe {
            open_ranged_reader(
                &c_source,
                split_begin,
                split_end,
                names.len() as i32,
                names.as_ptr(),
                values.as_ptr(),
                &mut stream,
                &mut error,
            )
        };

        if code != PGARROW_OK {
            // the base's own message, if the failure was ours, is the one that names the uri
            let message = exported.take_error().unwrap_or_else(|| {
                error_message(
                    error,
                    &format!("{} could not open the file for reading", self.name),
                )
            });

            return Err(message);
        }

        let reader = ArrowArrayStreamReader::try_new(stream).map_err(|e| {
            format!(
                "{} returned a stream this arrow build cannot read: {e}",
                self.name
            )
        })?;

        Ok(Box::new(SourceBackedReader {
            reader,
            source: exported,
        }))
    }

    fn can_stream_write(&self) -> bool {
        self.api().open_streaming_writer.is_some() && self.api().finish_streaming_writer.is_some()
    }

    fn open_streaming_writer(
        &self,
        schema: SchemaRef,
        options: &FormatOptions,
        sink: Box<dyn ByteSink>,
    ) -> Result<Box<dyn StreamingSink>> {
        let (Some(open_streaming_writer), Some(_)) = (
            self.api().open_streaming_writer,
            self.api().finish_streaming_writer,
        ) else {
            return Err(format!(
                "{} cannot write a file incrementally; it has to be buffered whole",
                self.name
            ));
        };

        let (_owned, names, values) = c_options(options);

        let ffi_schema = FFI_ArrowSchema::try_from(schema.as_ref())
            .map_err(|e| format!("could not export the schema across the boundary: {e}"))?;

        // consumed by the callee, so it must not also be released here
        let mut ffi_schema = ManuallyDrop::new(ffi_schema);

        let exported = ExportedByteSink::new(sink);
        let c_sink = exported.as_c();

        let mut handle = std::ptr::null_mut();
        let mut error = std::ptr::null_mut();

        let code = unsafe {
            open_streaming_writer(
                &mut *ffi_schema,
                names.len() as i32,
                names.as_ptr(),
                values.as_ptr(),
                &c_sink,
                &mut handle,
                &mut error,
            )
        };

        if code != PGARROW_OK {
            let message = exported.take_error().unwrap_or_else(|| {
                error_message(
                    error,
                    &format!("{} could not open the file for writing", self.name),
                )
            });

            return Err(message);
        }

        if handle.is_null() {
            return Err(format!("{} returned no writer", self.name));
        }

        Ok(Box::new(CStreamingSink {
            api: self.api,
            name: self.name.clone(),
            handle,
            sink: exported,
        }))
    }
}

/// A stream from a provider, plus the buffer it is reading out of.
struct BufferBackedReader {
    reader: ArrowArrayStreamReader,
    _data: Bytes,
}

impl Iterator for BufferBackedReader {
    type Item = std::result::Result<RecordBatch, arrow::error::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.reader.next()
    }
}

impl RecordBatchReader for BufferBackedReader {
    fn schema(&self) -> SchemaRef {
        self.reader.schema()
    }
}

/// A writer in another `.so`, seen as a [`RecordBatchSink`].
struct CSink {
    api: *const PgArrowProviderV1,
    name: String,
    handle: *mut c_void,
}

// SAFETY: as for `CProvider`. The handle is only ever touched through the functions below.
unsafe impl Send for CSink {}

impl CSink {
    fn api(&self) -> &PgArrowProviderV1 {
        unsafe { &*self.api }
    }
}

impl RecordBatchSink for CSink {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let array = StructArray::from(batch.clone());

        let (ffi_array, ffi_schema) = to_ffi(&array.to_data())
            .map_err(|e| format!("could not export a batch across the boundary: {e}"))?;

        // both are consumed by the callee
        let mut ffi_array = ManuallyDrop::new(ffi_array);
        let mut ffi_schema = ManuallyDrop::new(ffi_schema);

        let mut error = std::ptr::null_mut();

        let code = unsafe {
            (self.api().write_batch)(self.handle, &mut *ffi_array, &mut *ffi_schema, &mut error)
        };

        match code {
            PGARROW_OK => Ok(()),
            _ => Err(error_message(
                error,
                &format!("{} could not write a batch", self.name),
            )),
        }
    }

    fn finish(mut self: Box<Self>) -> Result<Vec<u8>> {
        let mut data = std::ptr::null_mut();
        let mut len = 0;
        let mut error = std::ptr::null_mut();

        let code =
            unsafe { (self.api().finish_writer)(self.handle, &mut data, &mut len, &mut error) };

        // `finish_writer` closes the handle whether it succeeded or not, so `Drop` must not
        // close it a second time
        self.handle = std::ptr::null_mut();

        if code != PGARROW_OK {
            return Err(error_message(error, "could not finish the file"));
        }

        if data.is_null() || len < 0 {
            return Err("the provider finished the file but returned nothing".to_string());
        }

        // a copy, because the provider's buffer belongs to a memory context that is gone at the
        // end of the statement and the caller may well still be holding this
        Ok(unsafe { std::slice::from_raw_parts(data, len as usize) }.to_vec())
    }
}

impl Drop for CSink {
    fn drop(&mut self) {
        // null means `finish` already ran; anything else means the statement errored between
        // batches, which is the common case — an `ereport` longjmps and the sink is dropped
        // during cleanup
        if !self.handle.is_null() {
            unsafe { (self.api().close_writer)(self.handle) };
        }
    }
}

/// A Rust [`ByteSource`] seen from C, plus the state the thunks below need.
///
/// Two things live here that the C struct has nowhere to put. The first is the last set of
/// buffers handed out: the ABI says they stay valid until the next call, and something has to
/// own them until then. The second is the message from a failed fetch, which cannot be returned
/// across the boundary without an allocator, so it waits here until the base asks for it.
pub struct ExportedByteSource {
    ctx: Box<SourceCtx>,
}

struct SourceCtx {
    source: Arc<dyn ByteSource>,
    last: Mutex<Vec<Bytes>>,
    error: Mutex<Option<String>>,
}

impl ExportedByteSource {
    pub fn new(source: Arc<dyn ByteSource>) -> Self {
        Self {
            ctx: Box::new(SourceCtx {
                source,
                last: Mutex::new(Vec::new()),
                error: Mutex::new(None),
            }),
        }
    }

    /// The C view. Valid for as long as `self` is: the context pointer is the box's address, so
    /// moving `self` is fine and dropping it while a provider still holds the struct is not.
    pub fn as_c(&self) -> PgArrowByteSourceV1 {
        PgArrowByteSourceV1 {
            ctx: &*self.ctx as *const SourceCtx as *mut c_void,
            size: source_size,
            get_ranges: source_get_ranges,
        }
    }

    /// The message from the most recent failure, if the failure was on this side.
    pub fn take_error(&self) -> Option<String> {
        self.ctx.error.lock().ok()?.take()
    }
}

fn record_error(slot: &Mutex<Option<String>>, message: String) {
    if let Ok(mut slot) = slot.lock() {
        *slot = Some(message);
    }
}

/// Runs a callback body, turning a panic into a recorded message. Nothing may unwind into the
/// provider's frames: it is another `.so` built by another compiler invocation, and a panic
/// crossing that edge is undefined behaviour.
fn callback_guard<T, F>(error: &Mutex<Option<String>>, fallback: T, body: F) -> T
where
    F: FnOnce() -> std::result::Result<T, String>,
{
    match catch_unwind(AssertUnwindSafe(body)) {
        Ok(Ok(value)) => value,
        Ok(Err(message)) => {
            record_error(error, message);
            fallback
        }
        Err(payload) => {
            let message = payload
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "panicked inside pg_arrow_base".to_string());

            record_error(error, message);
            fallback
        }
    }
}

unsafe extern "C-unwind" fn source_size(ctx: *mut c_void) -> i64 {
    let ctx = unsafe { &*(ctx as *const SourceCtx) };

    callback_guard(&ctx.error, -1, || ctx.source.size())
}

unsafe extern "C-unwind" fn source_get_ranges(
    ctx: *mut c_void,
    n_ranges: i32,
    offsets: *const i64,
    lengths: *const i64,
    data: *mut *mut u8,
    lens: *mut i64,
) -> i32 {
    let ctx = unsafe { &*(ctx as *const SourceCtx) };

    callback_guard(&ctx.error, PGARROW_ERROR, || {
        if n_ranges < 0 {
            return Err("a provider asked for a negative number of ranges".to_string());
        }

        let n_ranges = n_ranges as usize;

        let offsets = unsafe { std::slice::from_raw_parts(offsets, n_ranges) };
        let lengths = unsafe { std::slice::from_raw_parts(lengths, n_ranges) };

        let ranges: Vec<(i64, i64)> = offsets
            .iter()
            .zip(lengths.iter())
            .map(|(offset, length)| (*offset, *length))
            .collect();

        let fetched = ctx.source.get_ranges(&ranges)?;

        if fetched.len() != n_ranges {
            return Err(format!(
                "asked for {n_ranges} ranges and got {}",
                fetched.len()
            ));
        }

        // the previous set stops being valid exactly here, which is the contract
        let mut last = ctx
            .last
            .lock()
            .map_err(|_| "the byte source's buffers are poisoned".to_string())?;

        *last = fetched;

        for (i, bytes) in last.iter().enumerate() {
            unsafe {
                // `*mut` because the ABI is C; a provider that writes through it is a provider
                // with a bug, which is why the doc comment on the struct says so
                *data.add(i) = bytes.as_ptr() as *mut u8;
                *lens.add(i) = bytes.len() as i64;
            }
        }

        Ok(PGARROW_OK)
    })
}

/// A Rust [`ByteSink`] seen from C. As with the source, the failure message waits here.
pub struct ExportedByteSink {
    ctx: Box<SinkCtx>,
}

struct SinkCtx {
    sink: Mutex<Box<dyn ByteSink>>,
    error: Mutex<Option<String>>,
}

impl ExportedByteSink {
    pub fn new(sink: Box<dyn ByteSink>) -> Self {
        Self {
            ctx: Box::new(SinkCtx {
                sink: Mutex::new(sink),
                error: Mutex::new(None),
            }),
        }
    }

    pub fn as_c(&self) -> PgArrowByteSinkV1 {
        PgArrowByteSinkV1 {
            ctx: &*self.ctx as *const SinkCtx as *mut c_void,
            write: sink_write,
        }
    }

    pub fn take_error(&self) -> Option<String> {
        self.ctx.error.lock().ok()?.take()
    }
}

unsafe extern "C-unwind" fn sink_write(ctx: *mut c_void, data: *const u8, len: i64) -> i32 {
    let ctx = unsafe { &*(ctx as *const SinkCtx) };

    callback_guard(&ctx.error, PGARROW_ERROR, || {
        if len < 0 {
            return Err("a provider wrote a negative number of bytes".to_string());
        }

        let bytes = if len == 0 {
            &[][..]
        } else {
            unsafe { std::slice::from_raw_parts(data, len as usize) }
        };

        ctx.sink
            .lock()
            .map_err(|_| "the byte sink is poisoned".to_string())?
            .write(bytes)?;

        Ok(PGARROW_OK)
    })
}

/// A stream from a provider, plus the source it is reading through.
struct SourceBackedReader {
    reader: ArrowArrayStreamReader,
    source: ExportedByteSource,
}

impl Iterator for SourceBackedReader {
    type Item = std::result::Result<RecordBatch, arrow::error::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let batch = self.reader.next()?;

        // a failure that started on our side of the boundary comes back as whatever the
        // provider made of it, which is never the useful message. The real one is here.
        if batch.is_err() {
            if let Some(message) = self.source.take_error() {
                return Some(Err(arrow::error::ArrowError::ExternalError(Box::new(
                    std::io::Error::other(message),
                ))));
            }
        }

        Some(batch)
    }
}

impl RecordBatchReader for SourceBackedReader {
    fn schema(&self) -> SchemaRef {
        self.reader.schema()
    }
}

/// A streaming writer in another `.so`, seen as a [`StreamingSink`].
struct CStreamingSink {
    api: *const PgArrowProviderV1,
    name: String,
    handle: *mut c_void,
    sink: ExportedByteSink,
}

// SAFETY: as for `CSink`. The handle is only ever touched through the functions below.
unsafe impl Send for CStreamingSink {}

impl CStreamingSink {
    fn api(&self) -> &PgArrowProviderV1 {
        unsafe { &*self.api }
    }
}

impl StreamingSink for CStreamingSink {
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let array = StructArray::from(batch.clone());

        let (ffi_array, ffi_schema) = to_ffi(&array.to_data())
            .map_err(|e| format!("could not export a batch across the boundary: {e}"))?;

        // both are consumed by the callee
        let mut ffi_array = ManuallyDrop::new(ffi_array);
        let mut ffi_schema = ManuallyDrop::new(ffi_schema);

        let mut error = std::ptr::null_mut();

        let code = unsafe {
            (self.api().write_batch)(self.handle, &mut *ffi_array, &mut *ffi_schema, &mut error)
        };

        if code == PGARROW_OK {
            return Ok(());
        }

        Err(self.sink.take_error().unwrap_or_else(|| {
            error_message(error, &format!("{} could not write a batch", self.name))
        }))
    }

    fn bytes_written(&self) -> Option<i64> {
        let bytes = unsafe { (self.api().streaming_writer_bytes_written?)(self.handle) };

        (bytes >= 0).then_some(bytes)
    }

    fn finish(mut self: Box<Self>) -> Result<()> {
        let finish = self
            .api()
            .finish_streaming_writer
            .ok_or_else(|| format!("{} has no streaming finish", self.name))?;

        let mut error = std::ptr::null_mut();

        let code = unsafe { finish(self.handle, &mut error) };

        // the handle is closed either way, so `Drop` must not close it again
        self.handle = std::ptr::null_mut();

        if code == PGARROW_OK {
            return Ok(());
        }

        Err(self.sink.take_error().unwrap_or_else(|| {
            error_message(error, &format!("{} could not finish the file", self.name))
        }))
    }
}

impl Drop for CStreamingSink {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe { (self.api().close_writer)(self.handle) };
        }
    }
}

/// The base's byte source, seen by a provider as a [`ByteSource`].
///
/// A provider does not have to use this — it is handed the `#[repr(C)]` struct and may call it
/// directly — but every provider wants the same three things out of it, so it is written here
/// once: the copy out of the base's buffers, the error code to message mapping, and the
/// `Send + Sync` the Arrow readers require.
pub struct CByteSource {
    source: PgArrowByteSourceV1,
}

// SAFETY: the base's source is reachable from whatever thread a provider's reader runs on; the
// callbacks are documented to tolerate that, and the context behind them is synchronized.
unsafe impl Send for CByteSource {}
unsafe impl Sync for CByteSource {}

impl CByteSource {
    /// # Safety
    ///
    /// `source` must be the struct the base passed in, and must outlive this. The base
    /// guarantees it lives until the stream is released.
    pub unsafe fn new(source: *const PgArrowByteSourceV1) -> Result<Self> {
        if source.is_null() {
            return Err("the base passed a null byte source".to_string());
        }

        Ok(Self {
            source: unsafe { *source },
        })
    }
}

impl ByteSource for CByteSource {
    fn size(&self) -> Result<i64> {
        let size = unsafe { (self.source.size)(self.source.ctx) };

        if size < 0 {
            return Err("pg_arrow_base could not determine the file's size".to_string());
        }

        Ok(size)
    }

    fn get_ranges(&self, ranges: &[(i64, i64)]) -> Result<Vec<Bytes>> {
        let offsets: Vec<i64> = ranges.iter().map(|(offset, _)| *offset).collect();
        let lengths: Vec<i64> = ranges.iter().map(|(_, length)| *length).collect();

        let mut data: Vec<*mut u8> = vec![std::ptr::null_mut(); ranges.len()];
        let mut lens: Vec<i64> = vec![0; ranges.len()];

        let code = unsafe {
            (self.source.get_ranges)(
                self.source.ctx,
                ranges.len() as i32,
                offsets.as_ptr(),
                lengths.as_ptr(),
                data.as_mut_ptr(),
                lens.as_mut_ptr(),
            )
        };

        if code != PGARROW_OK {
            return Err("pg_arrow_base could not read a range of the file".to_string());
        }

        // a copy: the base's buffers stop being valid at the next call on this source, and an
        // Arrow decoder holds what it is given for much longer than that
        Ok(data
            .iter()
            .zip(lens.iter())
            .map(|(buffer, len)| {
                if buffer.is_null() || *len <= 0 {
                    return Bytes::new();
                }

                Bytes::copy_from_slice(unsafe {
                    std::slice::from_raw_parts(*buffer, *len as usize)
                })
            })
            .collect())
    }
}

/// The base's upload, seen by a provider as a [`ByteSink`].
pub struct CByteSink {
    sink: PgArrowByteSinkV1,
}

unsafe impl Send for CByteSink {}

impl CByteSink {
    /// # Safety
    ///
    /// `sink` must be the struct the base passed in, and must outlive the writer handle it was
    /// passed with.
    pub unsafe fn new(sink: *const PgArrowByteSinkV1) -> Result<Self> {
        if sink.is_null() {
            return Err("the base passed a null byte sink".to_string());
        }

        Ok(Self {
            sink: unsafe { *sink },
        })
    }
}

impl ByteSink for CByteSink {
    fn write(&mut self, data: &[u8]) -> Result<()> {
        let code = unsafe { (self.sink.write)(self.sink.ctx, data.as_ptr(), data.len() as i64) };

        if code != PGARROW_OK {
            // deliberately vague: the base has the message that names the uri and the cause,
            // and it replaces this one before anybody sees it
            return Err("pg_arrow_base could not write to the destination".to_string());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    //! A provider written against the C ABI, in process, so that both callback directions can
    //! be driven without Postgres: the base exports a source and a sink, the "provider" calls
    //! them through [`CByteSource`] and [`CByteSink`], and [`CProvider`] is what the base sees.
    //!
    //! A real provider lives in another `.so`; the only thing that changes about these calls
    //! there is who compiled them.

    use std::cell::RefCell;

    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    thread_local! {
        /// Everything the fake provider recorded, so a test can assert on what crossed the
        /// boundary rather than only on what came back. Thread-local because the test harness
        /// runs these in parallel and every call the fake makes happens on the thread that
        /// started it — a shared static would have the tests reading each other's bytes.
        static OBSERVED: RefCell<Observed> = RefCell::new(Observed::default());
    }

    #[derive(Default)]
    struct Observed {
        /// the bytes the fake writer pushed into the base's sink
        written: Vec<u8>,
        /// what the fake reader saw through the base's source
        file_size: i64,
        read_back: Vec<u8>,
        splits: (i64, i64),
    }

    unsafe extern "C-unwind" fn validate_option(
        _name: *const c_char,
        _value: *const c_char,
        _error: *mut *mut c_char,
    ) -> i32 {
        PGARROW_OK
    }

    unsafe extern "C-unwind" fn unsupported_schema(
        _data: *const u8,
        _len: i64,
        _out: *mut FFI_ArrowSchema,
        _error: *mut *mut c_char,
    ) -> i32 {
        PGARROW_ERROR
    }

    unsafe extern "C-unwind" fn unsupported_kv(
        _data: *const u8,
        _len: i64,
        _n: *mut i32,
        _keys: *mut *mut *mut c_char,
        _values: *mut *mut *mut c_char,
        _error: *mut *mut c_char,
    ) -> i32 {
        PGARROW_UNSUPPORTED
    }

    unsafe extern "C-unwind" fn unsupported_file_info(
        _data: *const u8,
        _len: i64,
        _out: *mut PgArrowFileInfoV1,
        _error: *mut *mut c_char,
    ) -> i32 {
        PGARROW_UNSUPPORTED
    }

    unsafe extern "C-unwind" fn unsupported_open_reader(
        _data: *const u8,
        _len: i64,
        _n_options: i32,
        _option_names: *const *const c_char,
        _option_values: *const *const c_char,
        _out: *mut FFI_ArrowArrayStream,
        _error: *mut *mut c_char,
    ) -> i32 {
        PGARROW_ERROR
    }

    unsafe extern "C-unwind" fn unsupported_open_writer(
        _schema: *mut FFI_ArrowSchema,
        _n_options: i32,
        _option_names: *const *const c_char,
        _option_values: *const *const c_char,
        _handle: *mut *mut c_void,
        _error: *mut *mut c_char,
    ) -> i32 {
        PGARROW_ERROR
    }

    unsafe extern "C-unwind" fn unsupported_finish_writer(
        _handle: *mut c_void,
        _data: *mut *mut u8,
        _len: *mut i64,
        _error: *mut *mut c_char,
    ) -> i32 {
        PGARROW_ERROR
    }

    /// The fake's streaming writer: one line per batch, a footer at the end, straight into the
    /// base's sink.
    struct FakeWriter {
        sink: CByteSink,
        /// What the fake pretends it has buffered: ten more bytes than it has handed over, so
        /// that a test can tell the provider's count from the base's.
        written: i64,
    }

    unsafe extern "C-unwind" fn open_streaming_writer(
        schema: *mut FFI_ArrowSchema,
        _n_options: i32,
        _option_names: *const *const c_char,
        _option_values: *const *const c_char,
        sink: *const PgArrowByteSinkV1,
        handle: *mut *mut c_void,
        _error: *mut *mut c_char,
    ) -> i32 {
        // consumed, as the ABI says
        drop(unsafe { std::ptr::read(schema) });

        let mut sink = match unsafe { CByteSink::new(sink) } {
            Ok(sink) => sink,
            Err(_) => return PGARROW_ERROR,
        };

        if sink.write(b"FAKE1\n").is_err() {
            return PGARROW_ERROR;
        }

        unsafe {
            *handle = Box::into_raw(Box::new(FakeWriter { sink, written: 16 })) as *mut c_void
        };

        PGARROW_OK
    }

    unsafe extern "C-unwind" fn write_batch(
        handle: *mut c_void,
        array: *mut FFI_ArrowArray,
        schema: *mut FFI_ArrowSchema,
        _error: *mut *mut c_char,
    ) -> i32 {
        let array = unsafe { std::ptr::read(array) };
        let schema = unsafe { std::ptr::read(schema) };

        let data = match unsafe { arrow::ffi::from_ffi(array, &schema) } {
            Ok(data) => data,
            Err(_) => return PGARROW_ERROR,
        };

        let writer = unsafe { &mut *(handle as *mut FakeWriter) };

        writer.written += 16;

        match writer
            .sink
            .write(format!("rows {}\n", data.len()).as_bytes())
        {
            Ok(()) => PGARROW_OK,
            Err(_) => PGARROW_ERROR,
        }
    }

    unsafe extern "C-unwind" fn streaming_writer_bytes_written(handle: *mut c_void) -> i64 {
        unsafe { &*(handle as *mut FakeWriter) }.written
    }

    unsafe extern "C-unwind" fn finish_streaming_writer(
        handle: *mut c_void,
        _error: *mut *mut c_char,
    ) -> i32 {
        let mut writer = unsafe { Box::from_raw(handle as *mut FakeWriter) };

        match writer.sink.write(b"END\n") {
            Ok(()) => PGARROW_OK,
            Err(_) => PGARROW_ERROR,
        }
    }

    unsafe extern "C-unwind" fn close_writer(handle: *mut c_void) {
        if !handle.is_null() {
            drop(unsafe { Box::from_raw(handle as *mut FakeWriter) });
        }
    }

    /// The fake's ranged reader: asks the base for the whole file in two ranges, records what
    /// it got, and yields one batch holding the byte count.
    unsafe extern "C-unwind" fn open_ranged_reader(
        source: *const PgArrowByteSourceV1,
        split_begin: i64,
        split_end: i64,
        _n_options: i32,
        _option_names: *const *const c_char,
        _option_values: *const *const c_char,
        out: *mut FFI_ArrowArrayStream,
        _error: *mut *mut c_char,
    ) -> i32 {
        let source = match unsafe { CByteSource::new(source) } {
            Ok(source) => source,
            Err(_) => return PGARROW_ERROR,
        };

        let size = match source.size() {
            Ok(size) => size,
            Err(_) => return PGARROW_ERROR,
        };

        // two ranges rather than one, because the plural form is the point of the callback
        let head = size / 2;
        let fetched = match source.get_ranges(&[(0, head), (head, size - head)]) {
            Ok(fetched) => fetched,
            Err(_) => return PGARROW_ERROR,
        };

        let mut read_back = Vec::new();
        for bytes in &fetched {
            read_back.extend_from_slice(bytes);
        }

        OBSERVED.with_borrow_mut(|observed| {
            observed.file_size = size;
            observed.read_back = read_back.clone();
            observed.splits = (split_begin, split_end);
        });

        let schema = Arc::new(Schema::new(vec![Field::new(
            "bytes_read",
            DataType::Int64,
            false,
        )]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![read_back.len() as i64])) as ArrayRef],
        )
        .expect("the batch is well formed");

        let reader = arrow::array::RecordBatchIterator::new(vec![Ok(batch)], schema);

        unsafe { std::ptr::write(out, FFI_ArrowArrayStream::new(Box::new(reader))) };

        PGARROW_OK
    }

    /// The fake's ranged file info: the row count out of a "footer" it reads through the
    /// base's source, which is the only way a format that is never handed the file can answer.
    unsafe extern "C-unwind" fn ranged_file_info(
        source: *const PgArrowByteSourceV1,
        out: *mut PgArrowFileInfoV1,
        _error: *mut *mut c_char,
    ) -> i32 {
        let source = match unsafe { CByteSource::new(source) } {
            Ok(source) => source,
            Err(_) => return PGARROW_ERROR,
        };

        let size = match source.size() {
            Ok(size) => size,
            Err(_) => return PGARROW_ERROR,
        };

        // the fake's "footer" is the whole file, and its row count is its length
        unsafe {
            std::ptr::write(
                out,
                PgArrowFileInfoV1 {
                    has_num_rows: true,
                    num_rows: size,
                    ..PgArrowFileInfoV1::default()
                },
            )
        };

        PGARROW_OK
    }

    static FAKE: PgArrowProviderV1 = PgArrowProviderV1 {
        magic: PG_ARROW_PROVIDER_MAGIC,
        version: PG_ARROW_PROVIDER_VERSION,
        name: c"fake".as_ptr(),
        file_extensions: c"fake".as_ptr(),
        validate_option,
        schema: unsupported_schema,
        kv_metadata: unsupported_kv,
        file_info: unsupported_file_info,
        open_reader: unsupported_open_reader,
        open_writer: unsupported_open_writer,
        write_batch,
        finish_writer: unsupported_finish_writer,
        close_writer,
        open_ranged_reader: Some(open_ranged_reader),
        open_streaming_writer: Some(open_streaming_writer),
        finish_streaming_writer: Some(finish_streaming_writer),
        streaming_writer_bytes_written: Some(streaming_writer_bytes_written),
        ranged_file_info: Some(ranged_file_info),
        reserved: [std::ptr::null(); 4],
    };

    /// The base's side of the sink: appends to what the test can see.
    struct RecordingSink;

    impl ByteSink for RecordingSink {
        fn write(&mut self, data: &[u8]) -> Result<()> {
            OBSERVED.with_borrow_mut(|observed| observed.written.extend_from_slice(data));

            Ok(())
        }
    }

    /// The base's side of the source: a file in memory.
    struct MemorySource(Bytes);

    impl ByteSource for MemorySource {
        fn size(&self) -> Result<i64> {
            Ok(self.0.len() as i64)
        }

        fn get_ranges(&self, ranges: &[(i64, i64)]) -> Result<Vec<Bytes>> {
            ranges
                .iter()
                .map(|(offset, length)| {
                    let start = *offset as usize;
                    let end = start + *length as usize;

                    if end > self.0.len() {
                        return Err(format!("range {start}..{end} is past the end of the file"));
                    }

                    Ok(self.0.slice(start..end))
                })
                .collect()
        }
    }

    /// A failing sink, for the half of the design that is about where a message comes from.
    struct FailingSink;

    impl ByteSink for FailingSink {
        fn write(&mut self, _data: &[u8]) -> Result<()> {
            Err("the upload to s3://bucket/key was refused".to_string())
        }
    }

    fn provider() -> CProvider {
        OBSERVED.with_borrow_mut(|observed| *observed = Observed::default());

        unsafe { CProvider::new(&FAKE) }.expect("the fake is well formed")
    }

    fn one_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));

        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef],
        )
        .expect("the batch is well formed")
    }

    #[test]
    fn a_streaming_write_reaches_the_bases_sink_without_a_buffer() {
        let provider = provider();

        assert!(provider.can_stream_write());

        let batch = one_batch();

        let mut sink = provider
            .open_streaming_writer(
                batch.schema(),
                &FormatOptions::default(),
                Box::new(RecordingSink),
            )
            .expect("the fake can stream");

        sink.write_batch(&batch).expect("the batch is written");
        sink.write_batch(&batch).expect("and so is the second");
        sink.finish().expect("the file is finished");

        // the header, one line per batch, the footer: proof that bytes arrived as they were
        // produced rather than in one lump at the end
        OBSERVED.with_borrow(|observed| {
            assert_eq!(
                String::from_utf8_lossy(&observed.written),
                "FAKE1\nrows 3\nrows 3\nEND\n"
            )
        });
    }

    #[test]
    fn a_streaming_writer_reports_its_own_byte_count_across_the_abi() {
        let provider = provider();

        let batch = one_batch();

        let mut sink = provider
            .open_streaming_writer(
                batch.schema(),
                &FormatOptions::default(),
                Box::new(RecordingSink),
            )
            .expect("the fake can stream");

        // the fake counts 16 bytes per step while writing far fewer, which is the point: the
        // number the base reports is the provider's, not what it saw go past
        assert_eq!(Some(16), sink.bytes_written());

        sink.write_batch(&batch).expect("the batch is written");

        assert_eq!(Some(32), sink.bytes_written());

        sink.finish().expect("the file is finished");
    }

    #[test]
    fn a_failing_upload_reports_the_bases_message_and_not_the_providers() {
        let provider = provider();

        // the sink fails on the very first write, which the fake does inside open
        let error = provider
            .open_streaming_writer(
                one_batch().schema(),
                &FormatOptions::default(),
                Box::new(FailingSink),
            )
            .err()
            .expect("a failing sink fails the open");

        assert_eq!(error, "the upload to s3://bucket/key was refused");
    }

    #[test]
    fn a_ranged_read_fetches_through_the_bases_source() {
        let provider = provider();

        assert!(provider.can_read_ranges());

        let file = Bytes::from_static(b"a file the provider never holds whole");

        let mut reader = provider
            .open_ranged_reader(
                Arc::new(MemorySource(file.clone())),
                &FormatOptions::default(),
                Some(2..5),
            )
            .expect("the fake can read ranges");

        let batch = reader
            .next()
            .expect("one batch")
            .expect("and it is not an error");

        assert_eq!(batch.num_rows(), 1);

        OBSERVED.with_borrow(|observed| {
            assert_eq!(observed.file_size, file.len() as i64);
            assert_eq!(observed.read_back, file.as_ref());
            assert_eq!(observed.splits, (2, 5), "the split range crosses the ABI");
        });
    }

    #[test]
    fn the_whole_file_is_a_negative_split_begin() {
        let provider = provider();

        let _ = provider
            .open_ranged_reader(
                Arc::new(MemorySource(Bytes::from_static(b"xy"))),
                &FormatOptions::default(),
                None,
            )
            .expect("the fake can read ranges");

        OBSERVED.with_borrow(|observed| assert_eq!(observed.splits, (-1, -1)));
    }

    #[test]
    fn a_row_count_crosses_the_abi_for_a_file_that_is_never_held() {
        let provider = provider();

        let file = Bytes::from_static(b"0123456789");

        let info = provider
            .ranged_file_info(Arc::new(MemorySource(file.clone())))
            .expect("the fake reads its footer through the source");

        // the fake calls a file's length its row count; what is being asserted is that the
        // answer came back through the boundary at all, since a format read in ranges is the
        // only one whose row count the base cannot work out for itself
        assert_eq!(info.num_rows, Some(file.len() as i64));
    }
}
