//! The parquet provider as `pg_arrow_base` sees it: a `static PgArrowProviderV1` and the shims
//! behind its function pointers.
//!
//! This file is mechanical on purpose. The whole of the format lives in `parquet-format`, which
//! knows nothing about Postgres or about C; everything here is marshalling, and the rule is that
//! no decision about parquet is taken in this file. If a change needs to be made here and there,
//! the one here is a translation of the one there.
//!
//! The contract, which `pg_arrow_base.h` states in full and which the shims below are written
//! to keep:
//!
//! - Nothing ereports and nothing unwinds out. Every entry point is wrapped in [`guard`], so a
//!   `Result` becomes a code plus a palloc'd message and a panic becomes the same rather than a
//!   longjmp through two `.so`s.
//! - Everything handed back through an out-parameter is palloc'd in the caller's context. There
//!   is no free function; the caller's context teardown is the free.
//! - An `ArrowSchema` or `ArrowArray` passed *in* is consumed, so each is read out of its
//!   pointer into an owned value whose `Drop` calls the Arrow release callback.
//!
//! Parquet fills in both halves of ABI v2 — the ranged reader and the streaming writer — so a
//! `COPY` of a file larger than this backend's memory is range requests rather than a download,
//! and a `COPY TO` of any size costs the base's upload buffer and nothing more.

use std::ffi::{c_char, c_void, CStr};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use arrow::array::{RecordBatch, StructArray};
use arrow::datatypes::Schema;
use arrow::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use arrow::ffi_stream::FFI_ArrowArrayStream;
use bytes::Bytes;
use format_registry::abi::{
    CByteSink, CByteSource, PgArrowByteSinkV1, PgArrowByteSourceV1, PgArrowFileInfoV1,
    PgArrowProviderV1, PGARROW_ERROR, PGARROW_OK, PG_ARROW_PROVIDER_MAGIC,
    PG_ARROW_PROVIDER_VERSION,
};
use format_registry::options::FormatOptions;
use format_registry::panic_guard::panic_guarded;
use format_registry::provider::{
    check_exportable_schema, FileInfo, FormatProvider, RecordBatchSink, StreamingSink,
};
use format_registry::Result;
use parquet_format::{Parquet, FILE_EXTENSIONS, FORMAT_NAME, SPLIT_LABEL};
use pgrx::pg_sys::palloc;

/// The strings in the descriptor have to be static, so they are written out here rather than
/// built from `parquet-format`'s constants — `FORMAT_NAME` is a `&str` and the ABI wants a
/// nul-terminated pointer that outlives the backend. [`crate::register_provider`] checks the two
/// agree, since a silent disagreement would register the provider under the wrong name.
const NAME: &CStr = c"parquet";
const EXTENSIONS: &CStr = c"parquet";
const SPLIT: &CStr = c"row group";

/// What `pg_arrow_base` keeps a pointer to for the life of the backend.
pub(crate) static PROVIDER: PgArrowProviderV1 = PgArrowProviderV1 {
    magic: PG_ARROW_PROVIDER_MAGIC,
    version: PG_ARROW_PROVIDER_VERSION,
    name: NAME.as_ptr(),
    file_extensions: EXTENSIONS.as_ptr(),
    validate_option: parquet_validate_option,
    schema: parquet_schema,
    kv_metadata: parquet_kv_metadata,
    file_info: parquet_file_info,
    open_reader: parquet_open_reader,
    open_writer: parquet_open_writer,
    write_batch: parquet_write_batch,
    finish_writer: parquet_finish_writer,
    close_writer: parquet_close_writer,
    open_ranged_reader: Some(parquet_open_ranged_reader),
    open_streaming_writer: Some(parquet_open_streaming_writer),
    finish_streaming_writer: Some(parquet_finish_streaming_writer),
    streaming_writer_bytes_written: Some(parquet_streaming_writer_bytes_written),
    ranged_file_info: Some(parquet_ranged_file_info),
    reserved: [std::ptr::null(); 4],
};

/// Whether the descriptor's static strings still match the format they describe.
pub(crate) fn describes_the_same_format() -> bool {
    NAME.to_str() == Ok(FORMAT_NAME)
        && EXTENSIONS.to_str() == Ok(FILE_EXTENSIONS.join(",").as_str())
        && SPLIT.to_str() == Ok(SPLIT_LABEL)
}

fn palloc_bytes(data: &[u8]) -> *mut u8 {
    unsafe {
        // palloc(0) is legal and returns a usable pointer, so an empty file needs no special
        // case here — but the ABI's `len` is what the caller reads, not the pointer's validity
        let buffer = palloc(data.len().max(1)) as *mut u8;
        std::ptr::copy_nonoverlapping(data.as_ptr(), buffer, data.len());
        buffer
    }
}

fn palloc_cstring(value: &str) -> *mut c_char {
    let bytes = value.as_bytes();

    unsafe {
        let buffer = palloc(bytes.len() + 1) as *mut u8;
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), buffer, bytes.len());
        *buffer.add(bytes.len()) = 0;
        buffer as *mut c_char
    }
}

fn palloc_pointers(count: usize) -> *mut *mut c_char {
    unsafe { palloc(count * std::mem::size_of::<*mut c_char>()) as *mut *mut c_char }
}

/// Runs an entry point's body and turns anything that went wrong into a code and a message.
///
/// The `catch_unwind` is not defensive decoration: a panic here would otherwise unwind through
/// `pg_arrow_base`'s stack frames, and whether that is sound depends on how the other library
/// was built. A code and a message are sound however it was built.
unsafe fn guard<F>(error: *mut *mut c_char, body: F) -> i32
where
    F: FnOnce() -> Result<i32>,
{
    let result = catch_unwind(AssertUnwindSafe(body))
        .unwrap_or_else(|_| Err("the parquet provider panicked".to_string()));

    match result {
        Ok(code) => code,
        Err(message) => {
            if !error.is_null() {
                *error = palloc_cstring(&message);
            }

            PGARROW_ERROR
        }
    }
}

unsafe fn bytes<'a>(data: *const u8, len: i64) -> Result<&'a [u8]> {
    if data.is_null() || len < 0 {
        return Err("the parquet provider was handed no data".to_string());
    }

    Ok(std::slice::from_raw_parts(data, len as usize))
}

unsafe fn string(pointer: *const c_char) -> Option<String> {
    if pointer.is_null() {
        return None;
    }

    CStr::from_ptr(pointer).to_str().ok().map(str::to_string)
}

/// The two parallel arrays back into a [`FormatOptions`]. Order is preserved, because last one
/// wins is how core `COPY` treats a repeated option and the base relies on it.
unsafe fn options(
    count: i32,
    names: *const *const c_char,
    values: *const *const c_char,
) -> Result<FormatOptions> {
    let mut options = FormatOptions::new();

    if count <= 0 || names.is_null() {
        return Ok(options);
    }

    for i in 0..count as usize {
        let name = string(*names.add(i)).ok_or("an option with no name")?;

        let value = if values.is_null() {
            String::new()
        } else {
            string(*values.add(i)).unwrap_or_default()
        };

        options.push(name, value);
    }

    Ok(options)
}

/// A writer handle, in whichever of the two shapes it was opened in.
///
/// Both take batches through the same `write_batch` entry point, so the ABI has one handle type
/// and the shape is remembered here. The two finish differently and the base knows which it
/// asked for, but a mismatch is still worth a message rather than a wrong answer.
enum Writer {
    Buffered(Box<dyn RecordBatchSink>),
    Streaming(Box<dyn StreamingSink>),
}

/// `Box<Writer>` to get something that fits in the ABI's `void *`; the box is freed by
/// whichever of the finish entry points or `close_writer` runs.
unsafe fn writer<'a>(pointer: *mut c_void) -> Result<&'a mut Writer> {
    if pointer.is_null() {
        return Err("the parquet provider was handed a null writer".to_string());
    }

    Ok(&mut *(pointer as *mut Writer))
}

unsafe extern "C-unwind" fn parquet_validate_option(
    name: *const c_char,
    value: *const c_char,
    error: *mut *mut c_char,
) -> i32 {
    guard(error, || {
        let name = string(name).ok_or("an option must have a name")?;
        let value = string(value).unwrap_or_default();

        Parquet.validate_option(&name, &value)?;

        Ok(PGARROW_OK)
    })
}

unsafe extern "C-unwind" fn parquet_schema(
    data: *const u8,
    len: i64,
    out: *mut FFI_ArrowSchema,
    error: *mut *mut c_char,
) -> i32 {
    guard(error, || {
        if out.is_null() {
            return Err("the parquet provider was given nowhere to put the schema".to_string());
        }

        let schema = Parquet.schema(bytes(data, len)?)?;

        check_exportable_schema(&schema)?;

        let exported = FFI_ArrowSchema::try_from(schema.as_ref())
            .map_err(|e| format!("could not export the schema across the boundary: {e}"))?;

        // the caller passes an `FFI_ArrowSchema::empty()`, which owns nothing and has no
        // release callback, so writing over it without dropping it leaks nothing
        std::ptr::write(out, exported);

        Ok(PGARROW_OK)
    })
}

unsafe extern "C-unwind" fn parquet_kv_metadata(
    data: *const u8,
    len: i64,
    count: *mut i32,
    keys: *mut *mut *mut c_char,
    values: *mut *mut *mut c_char,
    error: *mut *mut c_char,
) -> i32 {
    guard(error, || {
        if count.is_null() || keys.is_null() || values.is_null() {
            return Err("the parquet provider was given nowhere to put the metadata".to_string());
        }

        let metadata = Parquet.kv_metadata(bytes(data, len)?)?;

        *count = metadata.len() as i32;

        if metadata.is_empty() {
            *keys = std::ptr::null_mut();
            *values = std::ptr::null_mut();

            return Ok(PGARROW_OK);
        }

        let key_array = palloc_pointers(metadata.len());
        let value_array = palloc_pointers(metadata.len());

        for (i, (key, value)) in metadata.iter().enumerate() {
            *key_array.add(i) = palloc_cstring(key);

            // parquet's footer permits a key with no value at all, which is not the same as one
            // with an empty string, and the ABI carries the difference
            *value_array.add(i) = match value {
                Some(value) => palloc_cstring(value),
                None => std::ptr::null_mut(),
            };
        }

        *keys = key_array;
        *values = value_array;

        Ok(PGARROW_OK)
    })
}

unsafe extern "C-unwind" fn parquet_file_info(
    data: *const u8,
    len: i64,
    out: *mut PgArrowFileInfoV1,
    error: *mut *mut c_char,
) -> i32 {
    guard(error, || {
        if out.is_null() {
            return Err("the parquet provider was given nowhere to put the file info".to_string());
        }

        let info = Parquet.file_info(bytes(data, len)?)?;

        std::ptr::write(out, flatten_file_info(info));

        Ok(PGARROW_OK)
    })
}

/// The same, for a file nobody is holding: the footer is fetched through the base's source.
unsafe extern "C-unwind" fn parquet_ranged_file_info(
    source: *const PgArrowByteSourceV1,
    out: *mut PgArrowFileInfoV1,
    error: *mut *mut c_char,
) -> i32 {
    guard(error, || {
        if out.is_null() {
            return Err("the parquet provider was given nowhere to put the file info".to_string());
        }

        let info = Parquet.ranged_file_info(Arc::new(CByteSource::new(source)?))?;

        std::ptr::write(out, flatten_file_info(info));

        Ok(PGARROW_OK)
    })
}

/// A [`FileInfo`] in the flat shape the ABI carries, with the strings palloc'd for the caller.
unsafe fn flatten_file_info(info: FileInfo) -> PgArrowFileInfoV1 {
    let mut flat = PgArrowFileInfoV1 {
        // static, so the caller may read it after this context is gone
        split_label: SPLIT.as_ptr(),
        ..PgArrowFileInfoV1::default()
    };

    if let Some(rows) = info.num_rows {
        flat.has_num_rows = true;
        flat.num_rows = rows;
    }

    if let Some(splits) = info.num_splits {
        flat.has_num_splits = true;
        flat.num_splits = splits;
    }

    if let Some(compression) = info.compression {
        flat.compression = palloc_cstring(&compression);
    }

    if let Some(version) = info.format_version {
        flat.format_version = palloc_cstring(&version);
    }

    flat
}

unsafe extern "C-unwind" fn parquet_open_reader(
    data: *const u8,
    len: i64,
    n_options: i32,
    option_names: *const *const c_char,
    option_values: *const *const c_char,
    out: *mut FFI_ArrowArrayStream,
    error: *mut *mut c_char,
) -> i32 {
    guard(error, || {
        if out.is_null() {
            return Err("the parquet provider was given nowhere to put the stream".to_string());
        }

        let options = options(n_options, option_names, option_values)?;

        // a copy of the whole file. The ABI permits borrowing the caller's buffer for the life
        // of the stream, but a borrow cannot be put in the `Bytes` the reader holds without
        // claiming a 'static lifetime this function cannot promise. A `COPY` does not come
        // through here at all: it takes the ranged reader below, which has no whole-file buffer
        // to copy.
        let reader = Parquet.open_reader(Bytes::copy_from_slice(bytes(data, len)?), &options)?;

        // checked here rather than left to the export, which reports a name it cannot put in a
        // C string from inside the stream, where the field it came from is no longer known
        check_exportable_schema(&reader.schema())?;

        // as for the schema: the caller's stream is an empty one that owns nothing
        // arrow's stream callbacks cannot carry a panic, so one in the reader would abort the
        // backend rather than fail the statement
        let reader = panic_guarded(reader)?;

        std::ptr::write(out, FFI_ArrowArrayStream::new(reader));

        Ok(PGARROW_OK)
    })
}

unsafe extern "C-unwind" fn parquet_open_ranged_reader(
    source: *const PgArrowByteSourceV1,
    split_begin: i64,
    split_end: i64,
    n_options: i32,
    option_names: *const *const c_char,
    option_values: *const *const c_char,
    out: *mut FFI_ArrowArrayStream,
    error: *mut *mut c_char,
) -> i32 {
    guard(error, || {
        if out.is_null() {
            return Err("the parquet provider was given nowhere to put the stream".to_string());
        }

        let options = options(n_options, option_names, option_values)?;

        // the footer first, then the row groups that are wanted, each through the base's source
        let source = Arc::new(CByteSource::new(source)?);

        // a negative begin is the whole file, and `split_end` says nothing in that case
        let splits = (split_begin >= 0).then_some(split_begin..split_end);

        let reader = Parquet.open_ranged_reader(source, &options, splits)?;

        check_exportable_schema(&reader.schema())?;

        // arrow's stream callbacks cannot carry a panic, so one in the reader would abort the
        // backend rather than fail the statement
        let reader = panic_guarded(reader)?;

        std::ptr::write(out, FFI_ArrowArrayStream::new(reader));

        Ok(PGARROW_OK)
    })
}

/// The schema a writer was opened with, read out of the pointer the base consumed it through.
unsafe fn schema_to_write(schema: *mut FFI_ArrowSchema) -> Result<Schema> {
    // consumed: reading it out takes ownership, so the release callback runs when `exported` is
    // dropped at the end of this function
    let exported = std::ptr::read(schema);

    Schema::try_from(&exported).map_err(|e| format!("could not read the schema to write: {e}"))
}

unsafe extern "C-unwind" fn parquet_open_writer(
    schema: *mut FFI_ArrowSchema,
    n_options: i32,
    option_names: *const *const c_char,
    option_values: *const *const c_char,
    out: *mut *mut c_void,
    error: *mut *mut c_char,
) -> i32 {
    guard(error, || {
        if schema.is_null() || out.is_null() {
            return Err("the parquet provider was handed no schema to write".to_string());
        }

        let options = options(n_options, option_names, option_values)?;
        let schema = schema_to_write(schema)?;

        let sink = Parquet.open_writer(Arc::new(schema), &options)?;

        *out = Box::into_raw(Box::new(Writer::Buffered(sink))) as *mut c_void;

        Ok(PGARROW_OK)
    })
}

unsafe extern "C-unwind" fn parquet_open_streaming_writer(
    schema: *mut FFI_ArrowSchema,
    n_options: i32,
    option_names: *const *const c_char,
    option_values: *const *const c_char,
    sink: *const PgArrowByteSinkV1,
    out: *mut *mut c_void,
    error: *mut *mut c_char,
) -> i32 {
    guard(error, || {
        if schema.is_null() || out.is_null() {
            return Err("the parquet provider was handed no schema to write".to_string());
        }

        let options = options(n_options, option_names, option_values)?;
        let schema = schema_to_write(schema)?;

        let sink = CByteSink::new(sink)?;

        let writer = Parquet.open_streaming_writer(Arc::new(schema), &options, Box::new(sink))?;

        *out = Box::into_raw(Box::new(Writer::Streaming(writer))) as *mut c_void;

        Ok(PGARROW_OK)
    })
}

unsafe extern "C-unwind" fn parquet_write_batch(
    handle: *mut c_void,
    array: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
    error: *mut *mut c_char,
) -> i32 {
    guard(error, || {
        if array.is_null() || schema.is_null() {
            return Err("the parquet provider was handed no batch to write".to_string());
        }

        // both consumed, as above
        let array = std::ptr::read(array);
        let exported = std::ptr::read(schema);

        let data = from_ffi(array, &exported)
            .map_err(|e| format!("could not read a batch across the boundary: {e}"))?;

        let batch = RecordBatch::from(StructArray::from(data));

        match writer(handle)? {
            Writer::Buffered(sink) => sink.write_batch(&batch)?,
            Writer::Streaming(sink) => sink.write_batch(&batch)?,
        }

        Ok(PGARROW_OK)
    })
}

unsafe extern "C-unwind" fn parquet_finish_writer(
    handle: *mut c_void,
    data: *mut *mut u8,
    len: *mut i64,
    error: *mut *mut c_char,
) -> i32 {
    guard(error, || {
        if handle.is_null() || data.is_null() || len.is_null() {
            return Err("the parquet provider was handed no writer to finish".to_string());
        }

        // taken back whether finishing works or not: the handle is closed either way, which is
        // why the caller must not call `close_writer` after this
        let writer: Box<Writer> = Box::from_raw(handle as *mut Writer);

        let Writer::Buffered(sink) = *writer else {
            return Err("a streaming parquet writer has no buffer to hand back".to_string());
        };

        let file = sink.finish()?;

        *data = palloc_bytes(&file);
        *len = file.len() as i64;

        Ok(PGARROW_OK)
    })
}

unsafe extern "C-unwind" fn parquet_finish_streaming_writer(
    handle: *mut c_void,
    error: *mut *mut c_char,
) -> i32 {
    guard(error, || {
        if handle.is_null() {
            return Err("the parquet provider was handed no writer to finish".to_string());
        }

        let writer: Box<Writer> = Box::from_raw(handle as *mut Writer);

        let Writer::Streaming(sink) = *writer else {
            return Err("a buffered parquet writer does not write through a sink".to_string());
        };

        // the footer goes into the base's sink; whether the object becomes visible is the
        // base's call, after this returns
        sink.finish()?;

        Ok(PGARROW_OK)
    })
}

/// How much of the file the streaming writer has produced. A negative number for anything
/// else, which is how the ABI says "this writer does not count".
unsafe extern "C-unwind" fn parquet_streaming_writer_bytes_written(handle: *mut c_void) -> i64 {
    if handle.is_null() {
        return -1;
    }

    match &*(handle as *mut Writer) {
        Writer::Streaming(sink) => sink.bytes_written().unwrap_or(-1),
        Writer::Buffered(_) => -1,
    }
}

unsafe extern "C-unwind" fn parquet_close_writer(handle: *mut c_void) {
    if handle.is_null() {
        return;
    }

    // no error channel, so a panic in a destructor has nowhere to go but here
    let _ = catch_unwind(AssertUnwindSafe(|| {
        drop(Box::from_raw(handle as *mut Writer));
    }));
}
