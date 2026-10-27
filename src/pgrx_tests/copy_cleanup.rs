#[pgrx::pg_schema]
mod tests {
    use std::{
        io::Write,
        process::{Child, Command, Stdio},
        time::Instant,
    };

    use pgrx::{pg_test, Spi};

    use crate::pgrx_tests::common::{
        copy_session_output, create_interruptible_table, drop_interruptible_table, test_pg_port,
        FileCleanup, COPY_START_TIMEOUT, INTERRUPTED_TABLE_ROWS,
    };

    // every test here writes a destination of its own, since a copy that was interrupted on
    // purpose leaves the staged upload of a test that failed where the next test would find it
    const CONTROL_TEST_FILE_PATH: &str = "/tmp/pg_parquet_test_cleanup_control.parquet";
    const FAILED_TEST_FILE_PATH: &str = "/tmp/pg_parquet_test_cleanup_failed.parquet";
    const CANCELLED_TEST_FILE_PATH: &str = "/tmp/pg_parquet_test_cleanup_cancelled.parquet";
    const TERMINATED_TEST_FILE_PATH: &str = "/tmp/pg_parquet_test_cleanup_terminated.parquet";
    const CONTROL_TEST_FOLDER_PATH: &str = "/tmp/pg_parquet_test_cleanup_control_folder";
    const FAILED_TEST_FOLDER_PATH: &str = "/tmp/pg_parquet_test_cleanup_failed_folder";
    const CANCELLED_TEST_FOLDER_PATH: &str = "/tmp/pg_parquet_test_cleanup_cancelled_folder";
    const UNREMOVABLE_TEST_FOLDER_PATH: &str = "/tmp/pg_parquet_test_cleanup_unremovable_folder";

    // object_store writes a destination with a single put until the copy of it is larger than
    // the 10 MiB it buffers, and starts an upload the rest of the copy is staged into past
    // that point. Only the staged upload is left behind by an interrupted copy, so the copies
    // below write their row groups small enough for the staging to begin well before they are
    // interrupted: the buffer fills over the whole copy and not over a single row group.
    const INTERRUPTED_COPY_ROW_GROUP_SIZE: i64 = 20_000;
    const OBJECT_STORE_BUFFER_SIZE: u64 = 10 * 1024 * 1024;

    // a copy with file_size_bytes cuts its rows into the files of a folder, and only cuts them
    // where a row group was written, so the split copies below write their row groups well
    // under the smallest file size the option takes. The table they copy is large enough to
    // fill several of those files before the copy reaches the row it fails on.
    const SPLIT_COPY_ROW_GROUP_SIZE: i64 = 1_000;
    const SPLIT_COPY_FILE_SIZE_BYTES: &str = "1MB";
    const SPLIT_COPY_TABLE_ROWS: i64 = 40_000;

    // how many files a split copy is let finish before it is interrupted, so that the files it
    // finished are what the interruption leaves to give up and not only the file it is in the
    // middle of
    const FINISHED_FOLDER_FILES: usize = 2;

    // staged_upload_files are the files object_store stages the upload of a local destination
    // into, which it names after the destination, and which a copy that completed its upload
    // has renamed to the destination itself
    fn staged_upload_files(destination: &str) -> Vec<String> {
        let path = std::path::Path::new(destination);
        let directory = path.parent().expect("the destination has no directory");
        let staged_prefix = format!(
            "{}#",
            path.file_name()
                .expect("the destination has no file name")
                .to_string_lossy()
        );

        let mut staged = std::fs::read_dir(directory)
            .expect("failed to read the directory of the destination")
            .map(|entry| {
                entry
                    .expect("failed to read an entry of the directory of the destination")
                    .file_name()
                    .to_string_lossy()
                    .to_string()
            })
            .filter(|file_name| file_name.starts_with(&staged_prefix))
            .collect::<Vec<_>>();

        staged.sort();
        staged
    }

    // clean_destination removes what an earlier run of the test left of the destination, the
    // staged files of its upload included: a stale one would be taken for the upload of the
    // copy the test interrupts. The destination itself is removed again when the test ends.
    fn clean_destination(destination: &str) -> FileCleanup {
        let directory = std::path::Path::new(destination)
            .parent()
            .expect("the destination has no directory");

        for staged in staged_upload_files(destination) {
            std::fs::remove_file(directory.join(staged)).expect("failed to remove a staged file");
        }

        FileCleanup::new(destination)
    }

    // assert_copy_is_staged pins down what the assertions of the tests below rest on: that a
    // copy of the given table is larger than the buffer object_store fills before it starts
    // staging an upload. A copy that fits in the buffer leaves nothing behind whatever the
    // cleanup does, which would leave every assertion below holding for the wrong reason.
    fn assert_copy_is_staged(table: &str) {
        let _control_file = FileCleanup::new(CONTROL_TEST_FILE_PATH);

        Spi::run(&format!(
            "COPY {table} TO '{CONTROL_TEST_FILE_PATH}'
             WITH (format parquet, row_group_size {INTERRUPTED_COPY_ROW_GROUP_SIZE});"
        ))
        .unwrap();

        let copy_size = std::fs::metadata(CONTROL_TEST_FILE_PATH)
            .expect("the control copy wrote no destination")
            .len();

        assert!(
            copy_size > OBJECT_STORE_BUFFER_SIZE,
            "a copy of {table} is {copy_size} bytes, which is not past the {OBJECT_STORE_BUFFER_SIZE} \
             bytes object_store buffers before it stages an upload, so an interrupted copy of it \
             has no staged upload to leave behind: a larger {table} would fix this"
        );
    }

    // assert_destination_is_empty is what a copy that did not finish leaves of a local
    // destination: pg_parquet truncates the destination when the copy starts, and object_store
    // only writes it when the upload it staged the copy into is completed, so the destination of
    // an interrupted copy is the empty file the copy began with.
    fn assert_destination_is_empty(destination: &str, interruption: &str) {
        let destination_size = std::fs::metadata(destination)
            .expect("the destination of the copy is not there at all")
            .len();

        assert_eq!(
            destination_size, 0,
            "the copy that was {interruption} wrote {destination_size} bytes of its destination"
        );
    }

    // create_failing_table creates the table the failing copy below reads in the session that
    // copies it, rather than in one of its own, so that the table goes away with the test: a
    // table another session created cannot be dropped again while the transaction of the copy
    // is still holding it.
    fn create_failing_table(table: &str) {
        Spi::run(&format!(
            "CREATE TABLE {table} (id bigint, payload text);
             INSERT INTO {table}
             SELECT g, md5(random()::text) || md5(random()::text) ||
                       md5(random()::text) || md5(random()::text) ||
                       md5(random()::text) || md5(random()::text) ||
                       md5(random()::text) || md5(random()::text)
             FROM generate_series(1, {INTERRUPTED_TABLE_ROWS}) g;"
        ))
        .unwrap();
    }

    // copy_in_another_session starts a COPY TO of the given table without parallel workers in
    // a session of its own, and waits until object_store has staged part of it. It returns the
    // session together with the pid of the backend running the copy, which is what an
    // interruption is sent to.
    //
    // The commands are written to a session that stays open afterwards, rather than passed as
    // arguments to a psql that exits once it is done with them, because a session that exits
    // gives up the unfinished writes of its copies at process exit whatever the copy itself
    // did: what an interrupted copy gave up on its own is only visible while its session is
    // still there.
    fn copy_in_another_session(table: &str, uri: &str) -> (Child, i32) {
        let port = test_pg_port();

        let mut copy = Command::new("psql")
            .args(["-h", "localhost", "-p", &port, "-d", "pgrx_tests", "-At"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to run psql");

        let stdin = copy.stdin.as_mut().expect("psql took no input");
        writeln!(stdin, "SET pg_parquet.max_parallel_workers TO 0;")
            .and_then(|()| {
                writeln!(
                    stdin,
                    "COPY {table} TO '{uri}'
                     WITH (format parquet, row_group_size {INTERRUPTED_COPY_ROW_GROUP_SIZE});"
                )
            })
            .and_then(|()| stdin.flush())
            .expect("failed to send the copy to psql");

        let copy_pid = copy_pid(table);
        wait_for_staged_upload(uri);

        (copy, copy_pid)
    }

    // copy_pid is the pid of the backend running the COPY TO of the given table
    fn copy_pid(table: &str) -> i32 {
        let deadline = Instant::now() + COPY_START_TIMEOUT;

        loop {
            // the state of the other backends is read once per transaction and then cached,
            // and a test runs in a single transaction, so every look drops the cache first
            Spi::run("SELECT pg_stat_clear_snapshot();").unwrap();

            let copy_pid = Spi::get_one::<i32>(&format!(
                "SELECT coalesce(max(pid), 0) FROM pg_stat_activity
                 WHERE pid <> pg_backend_pid() AND query LIKE 'COPY {table} TO%'"
            ))
            .unwrap()
            .unwrap();

            if copy_pid != 0 {
                return copy_pid;
            }

            assert!(
                Instant::now() < deadline,
                "no COPY {table} TO was running after {} seconds",
                COPY_START_TIMEOUT.as_secs()
            );

            Spi::run("SELECT pg_sleep(0.01);").unwrap();
        }
    }

    // wait_for_staged_upload waits until the copy is past the point there is an upload of the
    // destination to give up, which is where interrupting it is worth something
    fn wait_for_staged_upload(destination: &str) {
        let deadline = Instant::now() + COPY_START_TIMEOUT;

        while staged_upload_files(destination).is_empty() {
            assert!(
                Instant::now() < deadline,
                "the copy staged nothing of \"{destination}\" after {} seconds",
                COPY_START_TIMEOUT.as_secs()
            );

            Spi::run("SELECT pg_sleep(0.01);").unwrap();
        }
    }

    // wait_for_session_state waits until the backend running the copy is in the given state,
    // which is 'gone' once the backend is no longer there at all
    // the sweep of a backend that was terminated is not over when its session is gone from
    // pg_stat_activity: Postgres forgets the session before it runs the callbacks the sweep
    // hangs off, so what the copy staged is waited on rather than looked at once
    fn wait_for_no_staged_upload(destination: &str) {
        let deadline = Instant::now() + COPY_START_TIMEOUT;

        loop {
            let staged = staged_upload_files(destination);

            if staged.is_empty() {
                return;
            }

            assert!(
                Instant::now() < deadline,
                "the copy left {staged:?} of \"{destination}\" behind after {} seconds",
                COPY_START_TIMEOUT.as_secs()
            );

            Spi::run("SELECT pg_sleep(0.01);").unwrap();
        }
    }

    fn wait_for_session_state(copy_pid: i32, state: &str) {
        let deadline = Instant::now() + COPY_START_TIMEOUT;

        loop {
            Spi::run("SELECT pg_stat_clear_snapshot();").unwrap();

            let session_state = Spi::get_one::<String>(&format!(
                "SELECT coalesce(max(state), 'gone') FROM pg_stat_activity WHERE pid = {copy_pid}"
            ))
            .unwrap()
            .unwrap();

            if session_state == state {
                return;
            }

            assert!(
                Instant::now() < deadline,
                "the backend running the copy was {session_state} rather than {state} after {} \
                 seconds",
                COPY_START_TIMEOUT.as_secs()
            );

            Spi::run("SELECT pg_sleep(0.01);").unwrap();
        }
    }

    // a copy that fails in the middle has an upload of its destination to give up: object
    // store destinations are staged into an upload once the copy of them is large enough,
    // and a local destination is where that is visible from outside the session that ran it.
    #[pg_test]
    fn test_copy_to_gives_up_its_staged_upload_when_it_fails() {
        let _test_file = clean_destination(FAILED_TEST_FILE_PATH);

        let table = "failing_copy_table";
        create_failing_table(table);
        assert_copy_is_staged(table);

        // the last row of the table carries a numeric the parquet schema cannot hold, so the
        // copy fails at the end of the table, by which point it has staged most of it
        Spi::run(&format!(
            "ALTER TABLE {table} ADD COLUMN unwritable numeric;
             INSERT INTO {table} (id, unwritable) VALUES (-1, repeat('9', 40)::numeric);"
        ))
        .unwrap();

        // the error is caught in plpgsql rather than by the test, so that the session that ran
        // the copy is still there when what it left behind is looked at
        Spi::run(&format!(
            "CREATE FUNCTION failing_copy_to() RETURNS text
             LANGUAGE plpgsql AS $$
             BEGIN
                 BEGIN
                     EXECUTE 'COPY {table} TO ''{FAILED_TEST_FILE_PATH}''
                              WITH (format parquet, row_group_size {INTERRUPTED_COPY_ROW_GROUP_SIZE})';

                     RETURN 'no error';
                 EXCEPTION WHEN others THEN
                     RETURN SQLERRM;
                 END;
             END;
             $$;"
        ))
        .unwrap();

        let error = Spi::get_one::<String>("SELECT failing_copy_to()")
            .unwrap()
            .unwrap();

        assert!(
            error.contains("exceeds max allowed integral digits"),
            "the copy reported \"{error}\" instead"
        );

        assert_eq!(
            staged_upload_files(FAILED_TEST_FILE_PATH),
            Vec::<String>::new()
        );

        assert_destination_is_empty(FAILED_TEST_FILE_PATH, "failed");
    }

    // a cancel is the interruption a running copy is most likely to meet, and the cleanup of
    // the copy runs while the error of it unwinds
    #[pg_test]
    fn test_copy_to_gives_up_its_staged_upload_after_a_cancel() {
        let _test_file = clean_destination(CANCELLED_TEST_FILE_PATH);

        let table = "cancelled_sequential_copy_table";
        create_interruptible_table(table);

        let (copy, copy_pid) = copy_in_another_session(table, CANCELLED_TEST_FILE_PATH);

        let cancelled = Spi::get_one::<bool>(&format!("SELECT pg_cancel_backend({copy_pid})"))
            .unwrap()
            .unwrap();
        assert!(cancelled, "the backend running the copy took no cancel");

        // the session is still there while what the copy staged is looked at, so what gave the
        // upload up is the copy's own cleanup rather than the sweep a session runs when it exits
        wait_for_session_state(copy_pid, "idle");

        assert_eq!(
            staged_upload_files(CANCELLED_TEST_FILE_PATH),
            Vec::<String>::new()
        );

        assert_destination_is_empty(CANCELLED_TEST_FILE_PATH, "cancelled");

        let output = copy_session_output(copy);
        drop_interruptible_table(table);

        assert!(
            output.contains("canceling statement due to user request"),
            "the copy was not cancelled while it was still running: {output}"
        );
    }

    // a terminated backend dies with a FATAL, which unwinds none of the copy's own cleanup, so
    // the upload of a copy interrupted this way is given up at process exit instead
    #[pg_test]
    fn test_copy_to_gives_up_its_staged_upload_after_a_terminate() {
        let _test_file = clean_destination(TERMINATED_TEST_FILE_PATH);

        let table = "terminated_sequential_copy_table";
        create_interruptible_table(table);

        let (copy, copy_pid) = copy_in_another_session(table, TERMINATED_TEST_FILE_PATH);

        let terminated = Spi::get_one::<bool>(&format!("SELECT pg_terminate_backend({copy_pid})"))
            .unwrap()
            .unwrap();
        assert!(terminated, "the backend running the copy took no terminate");

        wait_for_session_state(copy_pid, "gone");

        wait_for_no_staged_upload(TERMINATED_TEST_FILE_PATH);

        assert_destination_is_empty(TERMINATED_TEST_FILE_PATH, "terminated");

        let output = copy_session_output(copy);
        drop_interruptible_table(table);

        assert!(
            output.contains("terminating connection due to administrator command"),
            "the copy was not terminated while it was still running: {output}"
        );
    }

    // folder_files are the files the copy wrote into its folder destination, which is every
    // file in it: the folder is cleaned before the copy that fills it runs
    fn folder_files(folder: &str) -> Vec<String> {
        // a copy that created no file at all left no folder behind either
        let Ok(entries) = std::fs::read_dir(folder) else {
            return Vec::new();
        };

        let mut files = entries
            .map(|entry| {
                entry
                    .expect("failed to read an entry of the folder destination")
                    .file_name()
                    .to_string_lossy()
                    .to_string()
            })
            .collect::<Vec<_>>();

        files.sort();
        files
    }

    // clean_folder removes what an earlier run of the test left in the folder destination,
    // which a stale file would be taken for one the copy of this test left behind. The folder
    // is removed again when the test ends.
    fn clean_folder(folder: &str) -> FileCleanup {
        let _ = std::fs::remove_dir_all(folder);

        FileCleanup::new(folder)
    }

    // create_split_copy_table creates the table the split copies below read. Its payload is
    // random, since a payload the compression of the parquet writer shrinks away would leave
    // the copy of it too small to be cut into several files.
    fn create_split_copy_table(table: &str) {
        Spi::run(&format!(
            "CREATE TABLE {table} (id bigint, payload text, unwritable numeric);
             INSERT INTO {table} (id, payload)
             SELECT g, md5(random()::text) || md5(random()::text) ||
                       md5(random()::text) || md5(random()::text) ||
                       md5(random()::text) || md5(random()::text) ||
                       md5(random()::text) || md5(random()::text)
             FROM generate_series(1, {SPLIT_COPY_TABLE_ROWS}) g;"
        ))
        .unwrap();
    }

    // a copy with file_size_bytes writes the files of a folder, and the folder is what it was
    // asked to write rather than any file in it, so a copy that fails in the middle has every
    // file it created to give up, the ones it finished writing included
    #[pg_test]
    fn test_split_copy_to_gives_up_its_folder_when_it_fails() {
        let _control_folder = clean_folder(CONTROL_TEST_FOLDER_PATH);
        let _test_folder = clean_folder(FAILED_TEST_FOLDER_PATH);

        let table = "failing_split_copy_table";
        create_split_copy_table(table);

        // the participants of a parallel copy fill the files of a folder without knowing of
        // each other, and how far each of them got before the copy failed is up to the
        // scheduler, so the copies below are pinned to a single process
        Spi::run("SET pg_parquet.max_parallel_workers TO 0;").unwrap();

        // what the assertion at the end rests on: a copy of this table fills several files of
        // the folder, so that the failing copy has files it finished writing to give up and
        // not only the file it was in the middle of
        Spi::run(&format!(
            "COPY {table} TO '{CONTROL_TEST_FOLDER_PATH}'
             WITH (format parquet, row_group_size {SPLIT_COPY_ROW_GROUP_SIZE},
                   file_size_bytes '{SPLIT_COPY_FILE_SIZE_BYTES}');"
        ))
        .unwrap();

        let control_files = folder_files(CONTROL_TEST_FOLDER_PATH);

        assert!(
            control_files.len() > 1,
            "a copy of {table} wrote {control_files:?}, which is not several files: a larger \
             table or a smaller file_size_bytes would fix this"
        );

        // the last row of the table carries a numeric the parquet schema cannot hold, so the
        // copy fails at the end of the table, by which point it has filled most of the folder
        Spi::run(&format!(
            "INSERT INTO {table} (id, unwritable) VALUES (-1, repeat('9', 40)::numeric);"
        ))
        .unwrap();

        // the error is caught in plpgsql rather than by the test, so that the session that ran
        // the copy is still there when what it left behind is looked at
        Spi::run(&format!(
            "CREATE FUNCTION failing_split_copy_to() RETURNS text
             LANGUAGE plpgsql AS $$
             BEGIN
                 BEGIN
                     EXECUTE 'COPY {table} TO ''{FAILED_TEST_FOLDER_PATH}''
                              WITH (format parquet,
                                    row_group_size {SPLIT_COPY_ROW_GROUP_SIZE},
                                    file_size_bytes ''{SPLIT_COPY_FILE_SIZE_BYTES}'')';

                     RETURN 'no error';
                 EXCEPTION WHEN others THEN
                     RETURN SQLERRM;
                 END;
             END;
             $$;"
        ))
        .unwrap();

        let error = Spi::get_one::<String>("SELECT failing_split_copy_to()")
            .unwrap()
            .unwrap();

        assert!(
            error.contains("exceeds max allowed integral digits"),
            "the copy reported \"{error}\" instead"
        );

        assert_eq!(
            folder_files(FAILED_TEST_FOLDER_PATH),
            Vec::<String>::new(),
            "the copy that failed left its files in the folder it was writing"
        );
    }

    // split_copy_in_another_session starts a COPY TO of the given table into the given folder
    // in a session of its own, and waits until the copy has files it finished writing. It
    // returns the session together with the pid of the backend running the copy, which is what
    // an interruption is sent to.
    fn split_copy_in_another_session(table: &str, folder: &str) -> (Child, i32) {
        let port = test_pg_port();

        let mut copy = Command::new("psql")
            .args(["-h", "localhost", "-p", &port, "-d", "pgrx_tests", "-At"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to run psql");

        let stdin = copy.stdin.as_mut().expect("psql took no input");
        writeln!(stdin, "SET pg_parquet.max_parallel_workers TO 0;")
            .and_then(|()| {
                writeln!(
                    stdin,
                    "COPY {table} TO '{folder}'
                     WITH (format parquet, row_group_size {SPLIT_COPY_ROW_GROUP_SIZE},
                           file_size_bytes '{SPLIT_COPY_FILE_SIZE_BYTES}');"
                )
            })
            .and_then(|()| stdin.flush())
            .expect("failed to send the copy to psql");

        let copy_pid = copy_pid(table);
        wait_for_folder_files(folder, FINISHED_FOLDER_FILES);

        (copy, copy_pid)
    }

    // wait_for_folder_files waits until the copy has cut the given number of files out of its
    // rows, which is where interrupting it is worth something
    fn wait_for_folder_files(folder: &str, wanted: usize) {
        let deadline = Instant::now() + COPY_START_TIMEOUT;

        loop {
            let files = folder_files(folder);

            if files.len() >= wanted {
                return;
            }

            assert!(
                Instant::now() < deadline,
                "the copy wrote {files:?} into \"{folder}\" after {} seconds",
                COPY_START_TIMEOUT.as_secs()
            );

            Spi::run("SELECT pg_sleep(0.01);").unwrap();
        }
    }

    // a cancel is the interruption a running copy is most likely to meet, and the folder of a
    // cancelled copy holds as little of what it wrote as the folder of a failed one
    #[pg_test]
    fn test_split_copy_to_gives_up_its_folder_after_a_cancel() {
        let _test_folder = clean_folder(CANCELLED_TEST_FOLDER_PATH);

        let table = "cancelled_split_copy_table";
        create_interruptible_table(table);

        let (copy, copy_pid) = split_copy_in_another_session(table, CANCELLED_TEST_FOLDER_PATH);

        let cancelled = Spi::get_one::<bool>(&format!("SELECT pg_cancel_backend({copy_pid})"))
            .unwrap()
            .unwrap();
        assert!(cancelled, "the backend running the copy took no cancel");

        // the session is still there while the folder is looked at, so what emptied it is the
        // copy's own cleanup rather than the sweep a session runs when it exits
        wait_for_session_state(copy_pid, "idle");

        assert_eq!(
            folder_files(CANCELLED_TEST_FOLDER_PATH),
            Vec::<String>::new(),
            "the copy that was cancelled left its files in the folder it was writing"
        );

        let output = copy_session_output(copy);
        drop_interruptible_table(table);

        assert!(
            output.contains("canceling statement due to user request"),
            "the copy was not cancelled while it was still running: {output}"
        );
    }

    // a file the cleanup cannot remove stays in the folder for good, so the session that asked
    // for the copy is told which file that is rather than left to find it
    #[pg_test]
    fn test_split_copy_to_reports_a_file_it_cannot_remove() {
        let _test_folder = clean_folder(UNREMOVABLE_TEST_FOLDER_PATH);

        let table = "unremovable_split_copy_table";
        create_interruptible_table(table);

        let (copy, copy_pid) = split_copy_in_another_session(table, UNREMOVABLE_TEST_FOLDER_PATH);

        // the first file is finished by now, since the copy is already past it, so a folder is
        // put in its place: the cleanup removes a file of the copy and a folder is not one
        let unremovable = "data_0.parquet";
        let unremovable_path = format!("{UNREMOVABLE_TEST_FOLDER_PATH}/{unremovable}");
        std::fs::remove_file(&unremovable_path).expect("the copy finished no first file");
        std::fs::create_dir(&unremovable_path).expect("failed to take the place of the file");

        let cancelled = Spi::get_one::<bool>(&format!("SELECT pg_cancel_backend({copy_pid})"))
            .unwrap()
            .unwrap();
        assert!(cancelled, "the backend running the copy took no cancel");

        wait_for_session_state(copy_pid, "idle");

        // the file it could not remove is the only thing the cleanup left, so a failure on one
        // file does not stop it from removing the rest
        assert_eq!(
            folder_files(UNREMOVABLE_TEST_FOLDER_PATH),
            vec![unremovable.to_string()]
        );

        let output = copy_session_output(copy);
        drop_interruptible_table(table);

        assert!(
            output.contains("failed to remove the files of the COPY TO"),
            "the cleanup did not report the file it could not remove: {output}"
        );
        assert!(
            output.contains(unremovable),
            "the report of the cleanup does not name {unremovable}: {output}"
        );
    }
}
