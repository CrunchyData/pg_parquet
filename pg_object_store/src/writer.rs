//! The streaming half of the vtable: a `COPY TO` writes a file it cannot hold in memory.
//!
//! `put` takes a whole object, which is all a schema probe or a small file needs. A `COPY TO`
//! of a wide table is not that: the writer produces bytes incrementally and the file can be
//! larger than the backend's memory. So the ABI grows four entry points — begin, part, finish,
//! abort — in the slots v1 reserved for exactly this, and the multipart upload itself stays on
//! this side of the boundary along with the runtime that drives it.
//!
//! The caller is handed an opaque handle rather than a pointer to a Rust object. A handle
//! crosses into another extension's code, comes back through error paths this crate does not
//! control, and may be presented twice or after a rollback; a stale or invented one has to
//! produce an error message rather than a jump into freed memory. So handles are keys into a
//! per-backend table, and every entry point starts by looking one up.
//!
//! What a caller still owes us is a terminator: every handle ends in `put_finish` or
//! `put_abort`. Forgetting is not a crash — a transaction end sweeps whatever is left, and so
//! does process exit, for a backend terminated mid-write — but a swept upload is an aborted
//! one, so a `COPY TO` that never finishes its writer writes no file. An upload that could not
//! be aborted is reported as a warning rather than left silent: its parts are storage the
//! session is paying for and only the bucket's expiry rule will collect.
//!
//! One thing deliberately not checked here: whether the backend can do multipart at all.
//! `BufWriter` buffers below its capacity and only starts a multipart upload above it, so a
//! small file lands with a single `PUT` on a store that has no multipart (the local
//! filesystem, which `object_store` emulates, and `http`). Refusing up front on
//! `multipart_store: None` would break the local filesystem, which is the most common write
//! target there is. An `http` target large enough to need multipart fails at the first part
//! instead, with that store's own message.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::ffi::{c_char, c_int, c_void, CStr};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::time::Duration;

use bytes::Bytes;
use object_store::buffered::BufWriter;
use object_store_core::error::object_store_error_message;
use object_store_core::runtime::{block_on, trace};
use pgrx::PgXactCallbackEvent;
use pgrx::{ereport, pg_sys, register_xact_callback, PgLogLevel, PgSqlErrorCode};
use tokio::io::AsyncWriteExt;

use crate::vtable::{guard, open, PGOS_OK};

/// How long the store has to give an unfinished upload up. The sweep runs while the backend is
/// exiting too, where holding the exit up is worse than leaving an upload for the bucket's own
/// expiry rule to collect.
const UPLOAD_ABORT_TIMEOUT: Duration = Duration::from_secs(5);

/// An upload in progress, plus the uri it is going to. The uri is kept only so that a failure
/// several parts in can say what it was writing: by then the caller may have nothing but a
/// handle left to report.
struct Writer {
    writer: BufWriter,
    uri: String,
}

thread_local! {
    /// Every writer this backend has open. Ids are never reused, so a handle presented after
    /// its writer is gone is reported as closed rather than matching a later one.
    static WRITERS: RefCell<HashMap<u64, Writer>> = RefCell::new(HashMap::new());

    static NEXT_HANDLE: Cell<u64> = const { Cell::new(1) };
}

/// Removes a writer from the table, by handle. Removing rather than borrowing is what keeps a
/// failed operation from leaving a half-written upload reachable: an entry point puts the
/// writer back only when it is still usable.
fn take(handle: *mut c_void) -> Result<Writer, String> {
    let id = handle as u64;

    if id == 0 {
        return Err("writer handle must not be null".to_string());
    }

    WRITERS
        .with_borrow_mut(|writers| writers.remove(&id))
        .ok_or_else(|| {
            format!(
            "writer handle {id} is not open: it was already finished or aborted, a part failed, \
             or it belongs to a transaction that has ended"
        )
        })
}

fn put_back(handle: *mut c_void, writer: Writer) {
    WRITERS.with_borrow_mut(|writers| writers.insert(handle as u64, writer));
}

/// Gives an upload up, and says what went wrong if it could not.
///
/// The writer is flushed first. A writer whose multipart upload is still being created gives
/// nothing up — the id of the upload is not known on this side yet — and that is the state a
/// write interrupted in the middle of one leaves it in, so the pending request is driven to the
/// end before the upload it created is aborted. A writer that created no upload has buffered
/// bytes and nothing to flush them to, so flushing them is not an error worth reporting either.
///
/// One window stays open: a creation the store accepted but whose response never arrived leaves
/// an upload whose id nothing here ever saw.
fn abort_upload(writer: &mut Writer) -> Result<(), String> {
    block_on(async {
        let _ = writer.writer.flush().await;

        tokio::time::timeout(UPLOAD_ABORT_TIMEOUT, writer.writer.abort()).await
    })
    .map_err(|_| {
        format!(
            "the store did not answer within {} seconds",
            UPLOAD_ABORT_TIMEOUT.as_secs()
        )
    })?
    .map_err(object_store_error_message)
}

/// Aborts a writer and lets nothing escape. Used on the paths where an error is already
/// unwinding or the backend is exiting: a part that failed, the transaction-end sweep and the
/// process-exit sweep.
///
/// What the upload still holds after a failed abort is storage the session is paying for and
/// nothing but the bucket's own expiry rule will collect, so the session is told about it —
/// as a warning, because the statement is over either way and the writer is gone.
fn abort_quietly(mut writer: Writer) {
    trace(|| format!("aborting the unfinished upload to {}", writer.uri));

    // `BufWriter::abort` panics if the writer was already shut down, which cannot happen for
    // one still in the table, and the network call can fail for all the usual reasons. Neither
    // may escape: this runs inside a transaction-end or process-exit callback, where a panic or
    // an `ereport(ERROR)` takes the whole backend down.
    let aborted = catch_unwind(AssertUnwindSafe(|| abort_upload(&mut writer)));

    let failure = match aborted {
        Ok(Ok(())) => return,
        Ok(Err(message)) => message,
        Err(_) => "the abort panicked".to_string(),
    };

    ereport!(
        PgLogLevel::WARNING,
        PgSqlErrorCode::ERRCODE_WARNING,
        format!(
            "the unfinished upload to \"{}\" could not be given up: {failure}",
            writer.uri
        ),
        "The parts it already holds are storage that only the bucket's own rule for incomplete \
         uploads will remove."
    );
}

/// Arranges for a writer the caller never terminated to be swept.
///
/// At transaction end, which is where an error or a cancel ends up: pgrx's xact callbacks are
/// per-transaction and cleared once they fire, so registering one per `put_begin` accumulates
/// nothing across statements. And at process exit, for a backend that is terminated in the
/// middle of a write — a `FATAL` unwinds nothing of the statement's own cleanup.
fn register_sweep(id: u64) {
    for event in [
        PgXactCallbackEvent::Abort,
        PgXactCallbackEvent::Commit,
        PgXactCallbackEvent::ParallelAbort,
        PgXactCallbackEvent::ParallelCommit,
    ] {
        register_xact_callback(event, move || sweep_writer(id));
    }

    register_proc_exit_sweep();
}

fn sweep_writer(id: u64) {
    if let Some(writer) = WRITERS.with_borrow_mut(|writers| writers.remove(&id)) {
        abort_quietly(writer);
    }
}

thread_local! {
    /// `on_proc_exit` callbacks are never unregistered, so one is enough for the backend.
    static PROC_EXIT_SWEEP_REGISTERED: Cell<bool> = const { Cell::new(false) };
}

fn register_proc_exit_sweep() {
    if PROC_EXIT_SWEEP_REGISTERED.replace(true) {
        return;
    }

    unsafe { pg_sys::on_proc_exit(Some(sweep_writers_at_proc_exit), pg_sys::Datum::from(0)) };
}

unsafe extern "C-unwind" fn sweep_writers_at_proc_exit(_code: c_int, _arg: pg_sys::Datum) {
    let writers = WRITERS.with_borrow_mut(std::mem::take);

    for (_id, writer) in writers {
        abort_quietly(writer);
    }
}

/// Opens a streaming write to `uri` and sets `*handle`. `*handle` is null on any failure, so a
/// caller that ignores the return code still has nothing callable.
pub(crate) unsafe extern "C-unwind" fn pgos_put_begin(
    uri: *const c_char,
    handle: *mut *mut c_void,
    error: *mut *mut c_char,
) -> i32 {
    guard(error, || {
        if handle.is_null() {
            return Err("the handle out-parameter must not be null".to_string());
        }

        *handle = std::ptr::null_mut();

        let copy_from = false;
        let (store, path) = open(uri, copy_from)?;

        // `open` has already proved this is a non-null, valid-utf8, parseable uri
        let uri = CStr::from_ptr(uri).to_string_lossy().into_owned();

        let id = NEXT_HANDLE.with(|next| {
            let id = next.get();
            next.set(id + 1);
            id
        });

        WRITERS.with_borrow_mut(|writers| {
            writers.insert(
                id,
                Writer {
                    writer: BufWriter::new(store, path),
                    uri,
                },
            )
        });

        register_sweep(id);

        *handle = id as *mut c_void;

        Ok(PGOS_OK)
    })
}

/// Appends `len` bytes to the writer. A failure here closes the handle: a multipart upload
/// with a missing part has no way forward, so the upload is aborted and `put_finish` on the
/// same handle reports it as closed rather than completing a short file.
pub(crate) unsafe extern "C-unwind" fn pgos_put_part(
    handle: *mut c_void,
    data: *const u8,
    len: i64,
    error: *mut *mut c_char,
) -> i32 {
    guard(error, || {
        if len < 0 {
            return Err("len must not be negative".to_string());
        }

        if data.is_null() && len > 0 {
            return Err("data must not be null".to_string());
        }

        let mut writer = take(handle)?;

        let payload = if len == 0 {
            Bytes::new()
        } else {
            Bytes::copy_from_slice(std::slice::from_raw_parts(data, len as usize))
        };

        // the writer is out of the table for the length of the call, so a panic out of the
        // store's own code would drop it — and drop the upload it started without giving it up
        let put = catch_unwind(AssertUnwindSafe(|| block_on(writer.writer.put(payload))));

        match put {
            Ok(Ok(())) => {
                put_back(handle, writer);
                Ok(PGOS_OK)
            }
            Ok(Err(e)) => {
                let message = format!(
                    "while writing {}: {}",
                    writer.uri,
                    object_store_error_message(e)
                );
                abort_quietly(writer);
                Err(message)
            }
            Err(panic) => {
                abort_quietly(writer);

                // the panic is `guard`'s to report, once the upload is given up
                std::panic::resume_unwind(panic)
            }
        }
    })
}

/// Completes the upload. The object only becomes visible to a reader here, which is what makes
/// a failed `COPY TO` leave no half-written file behind.
pub(crate) unsafe extern "C-unwind" fn pgos_put_finish(
    handle: *mut c_void,
    error: *mut *mut c_char,
) -> i32 {
    guard(error, || {
        let mut writer = take(handle)?;

        // shutdown is what flushes a buffered small file as a single put and what completes a
        // multipart upload. It has already moved past the point where `abort` is legal, so a
        // failure here just drops the writer.
        block_on(writer.writer.shutdown())
            .map_err(|e| format!("failed to finish writing {}: {e}", writer.uri))?;

        Ok(PGOS_OK)
    })
}

/// Discards the upload. Unlike the sweep, this one reports what went wrong, so a caller
/// unwinding from an error of its own can tell whether the partial upload is actually gone.
pub(crate) unsafe extern "C-unwind" fn pgos_put_abort(
    handle: *mut c_void,
    error: *mut *mut c_char,
) -> i32 {
    guard(error, || {
        let mut writer = take(handle)?;

        abort_upload(&mut writer)
            .map_err(|failure| format!("failed to abort writing {}: {failure}", writer.uri))?;

        Ok(PGOS_OK)
    })
}

/// How many writers this backend currently has open. For the tests, which have to prove that
/// finishing, aborting and the transaction-end sweep all actually close one.
#[cfg(any(test, feature = "pg_test"))]
pub(crate) fn open_writer_count() -> usize {
    WRITERS.with_borrow(|writers| writers.len())
}
