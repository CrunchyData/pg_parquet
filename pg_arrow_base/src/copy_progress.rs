//! `pg_stat_progress_copy` for a direction Postgres cannot see.
//!
//! `COPY FROM` is reported by Postgres itself, because a reader is pulled through its copy
//! callback API and core counts what it pulls. `COPY TO` goes through our own dest receiver,
//! which core knows nothing about, so the rows and bytes are reported from here.

use pgrx::pg_sys::{
    pgstat_progress_end_command, pgstat_progress_start_command, pgstat_progress_update_multi_param,
    pgstat_progress_update_param, Oid, ProgressCommandType, PROGRESS_COPY_BYTES_PROCESSED,
    PROGRESS_COPY_BYTES_TOTAL, PROGRESS_COPY_COMMAND, PROGRESS_COPY_COMMAND_TO,
    PROGRESS_COPY_TUPLES_PROCESSED, PROGRESS_COPY_TYPE, PROGRESS_COPY_TYPE_FILE,
    PROGRESS_COPY_TYPE_PIPE, PROGRESS_COPY_TYPE_PROGRAM,
};

use crate::endpoint::Endpoint;

/// Starts a progress command for a `COPY TO`. Must be paired with [`end`].
///
/// The type is the end the statement named, by the same reading core gives it: a uri is a
/// `FILE` however remote it is, since what the view distinguishes is where the bytes leave the
/// backend rather than which store holds them.
pub(crate) fn start_copy_to(relation_oid: Oid, endpoint: &Endpoint) {
    let copy_type = match endpoint {
        Endpoint::Uri(_) => PROGRESS_COPY_TYPE_FILE,
        Endpoint::Program(_) => PROGRESS_COPY_TYPE_PROGRAM,
        Endpoint::Stdio => PROGRESS_COPY_TYPE_PIPE,
    };

    unsafe {
        pgstat_progress_start_command(ProgressCommandType::PROGRESS_COMMAND_COPY, relation_oid);

        let indexes = [PROGRESS_COPY_COMMAND as i32, PROGRESS_COPY_TYPE as i32];
        let values = [PROGRESS_COPY_COMMAND_TO as i64, copy_type as i64];

        pgstat_progress_update_multi_param(indexes.len() as _, indexes.as_ptr(), values.as_ptr());
    }
}

/// The rows the dest receiver has taken from the executor, which is what the view's
/// `tuples_processed` means for a `COPY TO`: a row this extension is holding in the batch it is
/// about to convert has still been processed, and reporting only whole batches would make the
/// count jump a thousand at a time.
pub(crate) fn update_copy_to_tuples(tuples_processed: i64) {
    unsafe {
        pgstat_progress_update_param(PROGRESS_COPY_TUPLES_PROCESSED as i32, tuples_processed)
    };
}

/// The rows the dest receiver has taken and the bytes that have reached tier 1.
pub(crate) fn update_copy_to(tuples_processed: i64, bytes_processed: i64) {
    unsafe {
        let indexes = [
            PROGRESS_COPY_TUPLES_PROCESSED as i32,
            PROGRESS_COPY_BYTES_PROCESSED as i32,
        ];
        let values = [tuples_processed, bytes_processed];

        pgstat_progress_update_multi_param(indexes.len() as _, indexes.as_ptr(), values.as_ptr());
    }
}

/// The size of the binary copy stream a reader feeds to Postgres. Core leaves it at 0 for a
/// callback source, since only the callback can know how much is left.
pub(crate) fn update_copy_from_bytes_total(bytes_total: i64) {
    unsafe { pgstat_progress_update_param(PROGRESS_COPY_BYTES_TOTAL as i32, bytes_total) };
}

pub(crate) fn end() {
    unsafe { pgstat_progress_end_command() };
}
