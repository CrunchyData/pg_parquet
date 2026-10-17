use std::{
    cell::{Cell, RefCell},
    ffi::c_int,
    path::Path,
    sync::Arc,
    time::Duration,
};

use object_store::{
    multipart::MultipartStore, path::Path as ObjectStorePath, Error as ObjectStoreError,
    MultipartId,
};
use pgrx::{ereport, pg_sys, PgLogLevel, PgSqlErrorCode};

use crate::{
    arrow_parquet::parquet_single_upload::SingleUpload, object_store::object_store_error_message,
    PG_BACKEND_TOKIO_RUNTIME,
};

// what a part of a parallel COPY TO is named after its prefix with, see part_uri
pub(crate) const PART_URI_INFIX: &str = ".pgpq-part-";

// how long the object store has to abort the upload of a finished or interrupted copy. The
// abort also runs while the backend is exiting, where holding the exit up is worse than
// leaving an unfinished upload behind for the bucket's own expiration to collect.
const UPLOAD_ABORT_TIMEOUT: Duration = Duration::from_secs(5);

// the writes of the copies running in this backend that are not finished yet. A copy the
// backend exits in the middle of dies with a FATAL, which unwinds none of the copy's own
// cleanup, so what it has not finished is given up at process exit as well.
thread_local! {
    static UNFINISHED_WRITES: RefCell<Vec<RegisteredWrite>> = const { RefCell::new(Vec::new()) };
    static CLEANUP_REGISTERED: Cell<bool> = const { Cell::new(false) };
    static NEXT_CLEANUP_ID: Cell<u64> = const { Cell::new(0) };
}

// CleanupId is the handle a copy gives the write it registered up by. Two copies running in
// the same backend are told apart by it, which a copy into the same destination as another
// one cannot be told apart by anything else.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CleanupId(u64);

struct RegisteredWrite {
    id: CleanupId,
    write: UnfinishedWrite,
}

// UnfinishedWrite is what a copy wrote and nothing else is going to remove. The objects of
// an object store destination are resolved when the copy starts, rather than where they are
// given up, because resolving one parses a uri and may build an object store client, and
// both of those report a failure by throwing, which the cleanup paths cannot afford: they
// run while an error is unwinding or while the backend is exiting.
pub(crate) enum UnfinishedWrite {
    // the part files of a parallel COPY TO into a local destination, which are removed by
    // sweeping the prefix they are named after
    PartFiles(String),
    // the destination's own upload of a parallel COPY TO, which the participants uploaded
    // the parts of and which holds the bytes of the copy until it is completed or aborted
    SharedUpload(Arc<dyn MultipartStore>, ObjectStorePath, MultipartId),
    // the upload object_store's writer started for a single process copy, which is given up
    // through the writer itself: the id of the upload it started is reachable nowhere else.
    WriterUpload(SingleUpload),
}

pub(crate) fn register_unfinished_write(write: UnfinishedWrite) -> CleanupId {
    // the callback is registered once and for all, because postgres holds a fixed number of
    // process exit callbacks and only cancels the one registered last
    if !CLEANUP_REGISTERED.replace(true) {
        unsafe { pg_sys::on_proc_exit(Some(give_up_unfinished_writes), pg_sys::Datum::from(0)) };
    }

    let id = CleanupId(NEXT_CLEANUP_ID.replace(NEXT_CLEANUP_ID.get() + 1));

    UNFINISHED_WRITES.with_borrow_mut(|unfinished| unfinished.push(RegisteredWrite { id, write }));

    id
}

// finish_unfinished_write gives the cleanup of the write up without running it, which a copy
// that turned what it wrote into the destination does: the bytes it wrote are the destination
// itself at that point, and giving the write up would be giving the destination up.
pub(crate) fn finish_unfinished_write(id: CleanupId) {
    UNFINISHED_WRITES.with_borrow_mut(|unfinished| unfinished.retain(|write| write.id != id));
}

// give_up_unfinished_write removes what the copy wrote and forgets it, so that the sweep at
// process exit does not go looking for it again. A copy that registered nothing, or that
// finished what it registered, has nothing given up here.
pub(crate) fn give_up_unfinished_write(id: CleanupId) {
    let write = UNFINISHED_WRITES.with_borrow_mut(|unfinished| {
        let at = unfinished.iter().position(|write| write.id == id)?;

        Some(unfinished.remove(at))
    });

    if let Some(write) = write {
        // the session that asked for the copy is still there to be told about a write the
        // copy could not give up
        let report_failures = true;
        give_up_write(&write.write, report_failures);
    }
}

// runs after the abort that terminates the workers of a parallel copy, which is what makes a
// part written after the sweep impossible
unsafe extern "C-unwind" fn give_up_unfinished_writes(_code: c_int, _arg: pg_sys::Datum) {
    let unfinished = UNFINISHED_WRITES.with_borrow_mut(std::mem::take);

    for write in unfinished {
        // the backend is exiting, which leaves nobody to report a failure to
        let report_failures = false;
        give_up_write(&write.write, report_failures);
    }
}

// give_up_write removes what the copy wrote, whether the copy was done with it or was
// interrupted in the middle of writing it. What it cannot remove stays where it was written
// for good, so a failure to remove something is reported as a warning when the caller asks
// for one, and only as a warning, because the copy itself is done with it either way. It
// throws nothing: it runs while an error is unwinding as well.
fn give_up_write(write: &UnfinishedWrite, report_failures: bool) {
    match write {
        UnfinishedWrite::PartFiles(part_prefix) => remove_part_files(part_prefix, report_failures),
        UnfinishedWrite::SharedUpload(store, path, upload_id) => {
            abort_upload(store, path, upload_id, report_failures)
        }
        UnfinishedWrite::WriterUpload(upload) => abort_writer_upload(upload, report_failures),
    }
}

// abort_upload gives the destination's upload up, which is what releases the bytes that were
// already uploaded into it. It throws nothing: it runs while an error is unwinding as well.
pub(crate) fn abort_upload(
    store: &Arc<dyn MultipartStore>,
    path: &ObjectStorePath,
    upload_id: &MultipartId,
    report_failures: bool,
) {
    // the abort is given a deadline, since an object store that stopped answering must not
    // hold a backend that is exiting up
    let result = PG_BACKEND_TOKIO_RUNTIME.block_on(async {
        tokio::time::timeout(UPLOAD_ABORT_TIMEOUT, store.abort_multipart(path, upload_id)).await
    });

    if report_failures {
        report_abort_failure(path.as_ref(), result);
    }
}

// abort_writer_upload gives the upload object_store's writer started for a single process
// copy up, if it started one: a copy that stayed inside the writer's buffer was put as a
// single object instead, and has no upload to give up. It throws nothing: it runs while an
// error is unwinding as well.
fn abort_writer_upload(upload: &SingleUpload, report_failures: bool) {
    let result = PG_BACKEND_TOKIO_RUNTIME
        .block_on(async { tokio::time::timeout(UPLOAD_ABORT_TIMEOUT, upload.abort()).await });

    if report_failures {
        report_abort_failure(upload.destination(), result);
    }
}

// report_abort_failure tells the session that the upload of its copy is neither completed nor
// aborted, which the parts it holds are storage the session did not ask for and which only
// the bucket's own rule for unfinished uploads removes
fn report_abort_failure(
    destination: &str,
    result: Result<Result<(), ObjectStoreError>, tokio::time::error::Elapsed>,
) {
    let failure = match result {
        Err(_) => Some(format!(
            "it was not aborted within {} seconds",
            UPLOAD_ABORT_TIMEOUT.as_secs()
        )),
        Ok(Ok(())) | Ok(Err(ObjectStoreError::NotFound { .. })) => None,
        Ok(Err(error)) => Some(object_store_error_message(error)),
    };

    if let Some(failure) = failure {
        ereport!(
            PgLogLevel::WARNING,
            PgSqlErrorCode::ERRCODE_WARNING,
            format!("failed to abort the upload of the COPY TO: {failure}"),
            format!("The upload of \"{destination}\" is left unfinished.")
        );
    }
}

// report_parts_left_behind tells the session that the parts of its copy are still in the
// destination's directory, which is storage it did not ask for and which nothing else is
// going to remove
fn report_parts_left_behind(part_prefix: &str, failure: &str) {
    ereport!(
        PgLogLevel::WARNING,
        PgSqlErrorCode::ERRCODE_WARNING,
        format!("failed to remove the parts of the parallel COPY TO: {failure}"),
        format!("The parts named after \"{part_prefix}{PART_URI_INFIX}\" are left in place.")
    );
}

// remove_part_files removes the part files of a local copy. The whole prefix is swept, rather
// than the part file of each range, because the writer stages a part file in a sibling file
// it names after it, which is left behind when the write does not finish. The prefix holds
// the leader pid, so the sweep cannot remove the parts of another copy.
fn remove_part_files(part_prefix: &str, report_failures: bool) {
    let part_path = Path::new(part_prefix);

    let (Some(part_dir), Some(part_name)) = (part_path.parent(), part_path.file_name()) else {
        return;
    };

    let Ok(part_dir) = std::fs::read_dir(part_dir) else {
        return;
    };

    let part_name = format!("{}{PART_URI_INFIX}", part_name.to_string_lossy());

    let mut failure = None;

    for part in part_dir.flatten() {
        if part.file_name().to_string_lossy().starts_with(&part_name) {
            // a part that is already gone is not a part that was left behind
            match std::fs::remove_file(part.path()) {
                Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
                    failure = failure.or_else(|| Some(error.to_string()));
                }
                _ => {}
            }
        }
    }

    if let (true, Some(failure)) = (report_failures, failure) {
        report_parts_left_behind(part_prefix, &failure);
    }
}
