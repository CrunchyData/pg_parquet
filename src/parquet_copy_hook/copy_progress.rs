use pgrx::{
    pg_sys::{
        pgstat_progress_end_command, pgstat_progress_start_command,
        pgstat_progress_update_multi_param, pgstat_progress_update_param, InvalidOid, PlannedStmt,
        ProgressCommandType, PROGRESS_COPY_BYTES_PROCESSED, PROGRESS_COPY_BYTES_TOTAL,
        PROGRESS_COPY_COMMAND, PROGRESS_COPY_COMMAND_TO, PROGRESS_COPY_TUPLES_PROCESSED,
        PROGRESS_COPY_TYPE, PROGRESS_COPY_TYPE_FILE, PROGRESS_COPY_TYPE_PIPE,
        PROGRESS_COPY_TYPE_PROGRAM,
    },
    PgBox,
};

use super::copy_utils::{
    copy_stmt_has_relation, copy_stmt_is_std_inout, copy_stmt_program, copy_stmt_relation_oid,
};

// Postgres reports the COPY FROM progress itself since it pulls our parquet reader through its
// copy callback api, but COPY TO goes through our own dest receiver, which Postgres knows
// nothing about. That direction is reported to pg_stat_progress_copy by the functions here.

// start_copy_to_progress starts a COPY progress command for the ongoing COPY TO.
// It must be paired with end_copy_progress.
pub(crate) fn start_copy_to_progress(p_stmt: &PgBox<PlannedStmt>) {
    let relation_oid = if copy_stmt_has_relation(p_stmt) {
        copy_stmt_relation_oid(p_stmt)
    } else {
        // COPY (SELECT ...) TO has no relation to report
        InvalidOid
    };

    let copy_type = if copy_stmt_program(p_stmt).is_some() {
        PROGRESS_COPY_TYPE_PROGRAM
    } else if copy_stmt_is_std_inout(p_stmt) {
        PROGRESS_COPY_TYPE_PIPE
    } else {
        PROGRESS_COPY_TYPE_FILE
    };

    unsafe {
        pgstat_progress_start_command(ProgressCommandType::PROGRESS_COMMAND_COPY, relation_oid);

        let indexes = [PROGRESS_COPY_COMMAND as i32, PROGRESS_COPY_TYPE as i32];
        let values = [PROGRESS_COPY_COMMAND_TO as i64, copy_type as i64];

        pgstat_progress_update_multi_param(indexes.len() as _, indexes.as_ptr(), values.as_ptr());
    }
}

// update_copy_to_progress reports the tuples that our dest receiver collected and the bytes
// that it already wrote to the parquet file(s).
pub(crate) fn update_copy_to_progress(tuples_processed: i64, bytes_processed: i64) {
    unsafe {
        let indexes = [
            PROGRESS_COPY_TUPLES_PROCESSED as i32,
            PROGRESS_COPY_BYTES_PROCESSED as i32,
        ];
        let values = [tuples_processed, bytes_processed];

        pgstat_progress_update_multi_param(indexes.len() as _, indexes.as_ptr(), values.as_ptr());
    }
}

// update_copy_to_tuples_progress reports only the tuples that our dest receiver collected.
pub(crate) fn update_copy_to_tuples_progress(tuples_processed: i64) {
    unsafe {
        pgstat_progress_update_param(PROGRESS_COPY_TUPLES_PROCESSED as i32, tuples_processed)
    };
}

// update_copy_from_bytes_total reports the size of the binary copy stream that our parquet
// reader feeds to Postgres. Postgres leaves it at 0 for a callback source since only the
// callback can know how much data is left.
pub(crate) fn update_copy_from_bytes_total(bytes_total: i64) {
    unsafe { pgstat_progress_update_param(PROGRESS_COPY_BYTES_TOTAL as i32, bytes_total) };
}

// end_copy_progress ends the COPY progress command started by start_copy_to_progress.
pub(crate) fn end_copy_progress() {
    unsafe { pgstat_progress_end_command() };
}
