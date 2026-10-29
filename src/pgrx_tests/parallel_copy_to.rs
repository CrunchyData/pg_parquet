#[pgrx::pg_schema]
mod tests {
    use std::{
        fs::File,
        io::Write,
        process::{Child, Command, Stdio},
        time::Instant,
    };

    use pgrx::{pg_sys, pg_test, Spi};

    use object_store::{ObjectStoreExt, PutPayload};
    use parquet::{
        bloom_filter::Sbbf,
        file::{
            metadata::{
                ColumnChunkMetaData, PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader,
            },
            statistics::Statistics,
        },
    };

    use crate::{
        arrow_parquet::uri_utils::{
            parquet_metadata_from_uri, ParquetObjectStoreReader, ParsedUriInfo,
        },
        object_store::object_store_cache::{
            get_or_create_multipart_store, get_or_create_object_store,
        },
        parquet_copy_hook::copy_cleanup::{abort_upload, PART_URI_INFIX},
        pgrx_tests::common::{
            copy_session_output, create_interruptible_table, drop_interruptible_table,
            extension_exists, extension_version, geospatial_crs_by_column_of, test_pg_port,
            FileCleanup, COPY_START_TIMEOUT, INTERRUPTED_TABLE_ROWS, LOCAL_TEST_FILE_PATH,
        },
        PG_BACKEND_TOKIO_RUNTIME,
    };

    const SEQUENTIAL_TEST_FILE_PATH: &str = "/tmp/pg_parquet_test_sequential.parquet";
    const STDOUT_TEST_FILE_PATH: &str = "/tmp/pg_parquet_test_parallel_stdout.parquet";
    const GEO_TEST_FILE_PATH: &str = "/tmp/pg_parquet_test_parallel_geo.parquet";
    const GEO_SEQUENTIAL_TEST_FILE_PATH: &str =
        "/tmp/pg_parquet_test_parallel_geo_sequential.parquet";

    // pg_parquet only splits a COPY TO into ctid ranges above a minimum number of pages, so
    // the test table has to be wide enough to get there
    const WIDE_TABLE_ROWS: i64 = 100_000;
    const MIN_PARALLEL_BLOCKS: i64 = 1024;

    fn create_wide_table() {
        Spi::run(
            "CREATE TABLE wide_table (
                 id bigint,
                 name text,
                 amount numeric(12,4),
                 created_at timestamp,
                 flag bool,
                 payload bytea
             );",
        )
        .unwrap();

        Spi::run(&format!(
            "INSERT INTO wide_table
             SELECT g,
                    CASE WHEN g % 10 = 0 THEN NULL ELSE 'name-' || g || repeat('x', g % 50) END,
                    CASE WHEN g % 11 = 0 THEN NULL ELSE (g % 100000)::numeric / 10000 END,
                    CASE WHEN g % 13 = 0 THEN NULL
                         ELSE timestamp '2024-01-01' + (g || ' seconds')::interval END,
                    CASE WHEN g % 7 = 0 THEN NULL ELSE g % 2 = 0 END,
                    CASE WHEN g % 17 = 0 THEN NULL ELSE decode(lpad(to_hex(g), 8, '0'), 'hex') END
             FROM generate_series(1, {WIDE_TABLE_ROWS}) g;"
        ))
        .unwrap();

        let nblocks = Spi::get_one::<i64>("SELECT pg_relation_size('wide_table') / 8192")
            .unwrap()
            .unwrap();

        assert!(
            nblocks >= MIN_PARALLEL_BLOCKS,
            "the test table has {nblocks} pages, which is too few to be copied in parallel"
        );
    }

    // pg_parquet only splits a COPY TO into ctid ranges above a minimum number of pages, so
    // the geometry table has to be wide enough to get there too
    const GEO_TABLE_ROWS: i64 = 200_000;

    fn create_geo_table() {
        Spi::run(
            "CREATE TABLE geo_table (
                 id bigint,
                 no_srid geometry,
                 lon_lat geometry(point, 4326),
                 web_mercator geometry(point, 3857),
                 geog geography
             );",
        )
        .unwrap();

        // the geometries of a column spread over the whole range, and are of more than one
        // geometry type, so that a range of the heap cannot hold all of the bounding box or
        // all of the geometry types of the column by itself
        Spi::run(&format!(
            "INSERT INTO geo_table
             SELECT g,
                    CASE WHEN g % 10 = 0 THEN NULL
                         WHEN g % 3 = 0 THEN ST_MakeLine(ST_MakePoint(g % 180, g % 90),
                                                         ST_MakePoint(-(g % 180), -(g % 90)))
                         ELSE ST_MakePoint(g % 180, g % 90) END,
                    CASE WHEN g % 11 = 0 THEN NULL
                         ELSE ST_SetSRID(ST_MakePoint(g % 180, g % 90), 4326) END,
                    CASE WHEN g % 13 = 0 THEN NULL
                         ELSE ST_SetSRID(ST_MakePoint(g % 1000, g % 1000), 3857) END,
                    CASE WHEN g % 17 = 0 THEN NULL
                         ELSE ST_MakePoint(g % 180, g % 90)::geography END
             FROM generate_series(1, {GEO_TABLE_ROWS}) g;"
        ))
        .unwrap();

        let nblocks = Spi::get_one::<i64>("SELECT pg_relation_size('geo_table') / 8192")
            .unwrap()
            .unwrap();

        assert!(
            nblocks >= MIN_PARALLEL_BLOCKS,
            "the geometry table has {nblocks} pages, which is too few to be copied in parallel"
        );
    }

    fn row_groups(uri: &str) -> i64 {
        Spi::get_one::<i64>(&format!(
            "SELECT count(DISTINCT row_group_id) FROM parquet.metadata('{uri}')"
        ))
        .unwrap()
        .unwrap()
    }

    fn max_row_group_rows(uri: &str) -> i64 {
        Spi::get_one::<i64>(&format!(
            "SELECT max(row_group_num_rows) FROM parquet.metadata('{uri}')"
        ))
        .unwrap()
        .unwrap()
    }

    // bounding_box is the box that covers the bounding boxes of every row group of the column
    fn bounding_box(uri: &str, column: &str) -> Option<String> {
        Spi::get_one::<String>(&format!(
            "SELECT min((stats_geospatial->'bbox'->>'xmin')::float8)::text || ' ' ||
                    max((stats_geospatial->'bbox'->>'xmax')::float8)::text || ' ' ||
                    min((stats_geospatial->'bbox'->>'ymin')::float8)::text || ' ' ||
                    max((stats_geospatial->'bbox'->>'ymax')::float8)::text
             FROM parquet.metadata('{uri}')
             WHERE path_in_schema = '{column}'"
        ))
        .unwrap()
    }

    // geospatial_types is the geometry types that the row groups of the column report
    fn geospatial_types(uri: &str, column: &str) -> Option<String> {
        Spi::get_one::<String>(&format!(
            "SELECT array_agg(DISTINCT t::int ORDER BY t::int)::text
             FROM parquet.metadata('{uri}') m,
                  jsonb_array_elements_text(m.stats_geospatial->'geospatial_types') t
             WHERE m.path_in_schema = '{column}'"
        ))
        .unwrap()
    }

    // rows_not_in_other_file reads both files into a table of the geometry table's own shape
    // and counts the rows of each that the other does not have
    fn rows_not_in_other_file(left_uri: &str, right_uri: &str) -> (i64, i64) {
        for (table, uri) in [("geo_left", left_uri), ("geo_right", right_uri)] {
            Spi::run(&format!("CREATE TABLE {table} (LIKE geo_table);")).unwrap();
            Spi::run(&format!("COPY {table} FROM '{uri}' WITH (format parquet);")).unwrap();
        }

        let not_in_right = Spi::get_one::<i64>(
            "SELECT count(*) FROM (SELECT * FROM geo_left
                                   EXCEPT ALL
                                   SELECT * FROM geo_right) q",
        )
        .unwrap()
        .unwrap();

        let not_in_left = Spi::get_one::<i64>(
            "SELECT count(*) FROM (SELECT * FROM geo_right
                                   EXCEPT ALL
                                   SELECT * FROM geo_left) q",
        )
        .unwrap()
        .unwrap();

        Spi::run("DROP TABLE geo_left, geo_right;").unwrap();

        (not_in_right, not_in_left)
    }

    fn file_metadata(uri: &str) -> (i64, String) {
        Spi::get_two::<i64, String>(&format!(
            "SELECT num_rows, format_version FROM parquet.file_metadata('{uri}')"
        ))
        .map(|(num_rows, format_version)| (num_rows.unwrap(), format_version.unwrap()))
        .unwrap()
    }

    fn kv_metadata_fingerprint(uri: &str) -> String {
        Spi::get_one::<String>(&format!(
            "SELECT md5(string_agg(key || coalesce(value, ''), '|' ORDER BY key))
             FROM parquet.kv_metadata('{uri}')"
        ))
        .unwrap()
        .unwrap()
    }

    fn schema_fingerprint(uri: &str) -> String {
        Spi::get_one::<String>(&format!(
            "SELECT md5(string_agg(name || coalesce(type_name, '') ||
                                   coalesce(repetition_type, '') ||
                                   coalesce(logical_type, '') ||
                                   coalesce(scale::text, '') ||
                                   coalesce(precision::text, ''), '|'))
             FROM parquet.schema('{uri}')"
        ))
        .unwrap()
        .unwrap()
    }

    // rows_not_in_file is the number of rows of the table that the parquet file does not
    // have, and the other way round. Both have to be 0 for the file to hold exactly the
    // rows of the table.
    fn rows_not_in_file(uri: &str) -> (i64, i64) {
        Spi::run("CREATE TABLE wide_table_from_file (LIKE wide_table);").unwrap();

        Spi::run(&format!(
            "COPY wide_table_from_file FROM '{uri}' WITH (format parquet);"
        ))
        .unwrap();

        let missing_from_file = Spi::get_one::<i64>(
            "SELECT count(*) FROM (SELECT * FROM wide_table
                                   EXCEPT ALL
                                   SELECT * FROM wide_table_from_file) q",
        )
        .unwrap()
        .unwrap();

        let extra_in_file = Spi::get_one::<i64>(
            "SELECT count(*) FROM (SELECT * FROM wide_table_from_file
                                   EXCEPT ALL
                                   SELECT * FROM wide_table) q",
        )
        .unwrap()
        .unwrap();

        Spi::run("DROP TABLE wide_table_from_file;").unwrap();

        (missing_from_file, extra_in_file)
    }

    fn compressed_size(uri: &str) -> i64 {
        Spi::get_one::<i64>(&format!(
            "SELECT sum(total_compressed_size)::bigint FROM parquet.metadata('{uri}')"
        ))
        .unwrap()
        .unwrap()
    }

    // part_files is the part files a parallel COPY TO a local uri left behind next to it. The
    // part file name holds the pid of the leader between the uri and the part infix.
    fn part_files(uri: &str) -> Vec<String> {
        let uri = std::path::Path::new(uri);
        let parent = uri.parent().unwrap();
        let prefix = format!("{}.", uri.file_name().unwrap().to_string_lossy());

        std::fs::read_dir(parent)
            .unwrap()
            .filter_map(|entry| {
                let entry = entry.unwrap();
                let name = entry.file_name().to_string_lossy().to_string();

                (name.starts_with(&prefix) && name.contains(".pgpq-part-"))
                    .then(|| entry.path().display().to_string())
            })
            .collect()
    }

    // split_file_names is the names of the files a copy with file_size_bytes wrote into the
    // folder it was given. A copy that wrote no file left no folder behind either.
    fn split_file_names(folder: &str) -> Vec<String> {
        let Ok(entries) = std::fs::read_dir(folder) else {
            return vec![];
        };

        let mut names = entries
            .map(|entry| entry.unwrap().file_name().to_string_lossy().to_string())
            .collect::<Vec<_>>();

        names.sort();

        names
    }

    // split_folder_rows is the number of rows the files of a folder destination hold together
    fn split_folder_rows(folder: &str) -> i64 {
        split_file_names(folder)
            .iter()
            .map(|name| file_metadata(&format!("{folder}/{name}")).0)
            .sum()
    }

    // folder_file_names is what a folder destination holds, as paths relative to the folder,
    // which is how a folder on an object store is read as well as a local one. It descends
    // into the folder, since only the direct children of it are the files of a copy.
    fn folder_file_names(folder: &str) -> Vec<String> {
        let uris = Spi::get_one::<String>(&format!(
            "SELECT coalesce(string_agg(uri, ' ' ORDER BY uri), '')
             FROM parquet.list('{folder}/**')"
        ))
        .unwrap()
        .unwrap();

        uris.split_whitespace()
            .map(|uri| {
                uri.trim_start_matches(folder)
                    .trim_start_matches('/')
                    .to_string()
            })
            .collect()
    }

    // folder_rows is the number of rows the files of a folder destination hold together,
    // including those of an object store folder
    fn folder_rows(folder: &str) -> i64 {
        folder_file_names(folder)
            .iter()
            .map(|name| file_metadata(&format!("{folder}/{name}")).0)
            .sum()
    }

    // failing_copy_to runs a COPY TO that is expected to fail and gives back what it
    // reported, caught in plpgsql so that the test can go on asking what the copy left in its
    // destination
    fn failing_copy_to(copy: &str) -> String {
        Spi::run(&format!(
            "CREATE OR REPLACE FUNCTION failing_copy_to() RETURNS text
             LANGUAGE plpgsql AS $body$
             BEGIN
                 EXECUTE $copy${copy}$copy$;

                 RETURN 'no error';
             EXCEPTION WHEN others THEN
                 RETURN SQLERRM;
             END;
             $body$;"
        ))
        .unwrap();

        Spi::get_one::<String>("SELECT failing_copy_to()")
            .unwrap()
            .unwrap()
    }

    // empty_folder removes everything a folder destination holds, which a test that writes a
    // folder of an object store needs before it starts: the bucket is not emptied between
    // the runs of the suite the way a local folder is
    fn empty_folder(folder: &str) {
        for name in folder_file_names(folder) {
            delete_object(&format!("{folder}/{name}"));
        }
    }

    // object_store_parts is the part objects a parallel COPY TO the given object store uri
    // left behind next to it. The parts of this session's copies are what it looks for, since
    // a part is named after the pid of the backend that led the copy, and a part another
    // session left behind is not this test's business.
    fn object_store_parts(uri: &str) -> Vec<String> {
        object_store_parts_of(uri, unsafe { pg_sys::MyProcPid })
    }

    // object_store_parts_of is the part objects the copy led by the given backend left next to
    // the given object store uri
    fn object_store_parts_of(uri: &str, leader_pid: i32) -> Vec<String> {
        let (dir_uri, _) = uri.rsplit_once('/').expect("uri names no directory");

        let parts = Spi::get_one::<String>(&format!(
            "SELECT coalesce(string_agg(uri, ' '), '') FROM parquet.list('{dir_uri}/*')
             WHERE uri LIKE '%.{leader_pid}.pgpq-part-%'"
        ))
        .unwrap()
        .unwrap();

        parts.split_whitespace().map(str::to_string).collect()
    }

    // object_exists tells whether the given uri holds an object a reader can find, which
    // the destination of a copy that did not complete its upload does not.
    fn object_exists(uri: &str) -> bool {
        let uri_info = ParsedUriInfo::try_from(uri).unwrap();

        let copy_from = true;
        let (store, location) = get_or_create_object_store(&uri_info, copy_from);

        PG_BACKEND_TOKIO_RUNTIME.block_on(async move { store.head(&location).await.is_ok() })
    }

    // delete_object removes the object the given uri names, if it is there at all
    fn delete_object(uri: &str) {
        let uri_info = ParsedUriInfo::try_from(uri).unwrap();

        let copy_from = false;
        let (store, location) = get_or_create_object_store(&uri_info, copy_from);

        let _ = PG_BACKEND_TOKIO_RUNTIME.block_on(async move { store.delete(&location).await });
    }

    // failing_copy_to_error runs a parallel COPY TO the given uri that fails in the middle of
    // a range, and returns what it reported. The last row of the relation carries a numeric
    // the parquet schema cannot hold, so the range that covers the end of the relation fails
    // while the other ranges have already written parts of their own.
    fn failing_copy_to_error(uri: &str) -> String {
        create_wide_table();

        Spi::run(
            "ALTER TABLE wide_table ADD COLUMN unwritable numeric;
             INSERT INTO wide_table (id, unwritable) VALUES (-1, repeat('9', 40)::numeric);",
        )
        .unwrap();

        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();

        // the error is caught in plpgsql rather than by the test, so that what the copy left
        // behind can be looked at in the same session that ran it: the parts of a copy carry
        // the pid of the backend that led it
        Spi::run(&format!(
            "CREATE FUNCTION failing_copy_to() RETURNS text
             LANGUAGE plpgsql AS $$
             BEGIN
                 BEGIN
                     EXECUTE 'COPY wide_table TO ''{uri}'' WITH (format parquet)';

                     RETURN 'no error';
                 EXCEPTION WHEN others THEN
                     RETURN SQLERRM;
                 END;
             END;
             $$;"
        ))
        .unwrap();

        Spi::get_one::<String>("SELECT failing_copy_to()")
            .unwrap()
            .unwrap()
    }

    // a copy that fails in the middle of its ranges has to take its parts with it: the parts
    // of a local destination are files next to it, and a copy that never reached its splice
    // is a copy whose destination nothing is supposed to find either.
    #[pg_test]
    fn test_parallel_copy_to_removes_its_parts_when_a_range_fails() {
        let _parallel_file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

        let error = failing_copy_to_error(LOCAL_TEST_FILE_PATH);

        assert!(
            error.contains("exceeds max allowed integral digits"),
            "the parallel copy reported \"{error}\" instead"
        );

        assert_eq!(part_files(LOCAL_TEST_FILE_PATH), Vec::<String>::new());

        assert!(
            !std::path::Path::new(LOCAL_TEST_FILE_PATH).exists(),
            "the copy that failed wrote its destination anyway"
        );
    }

    // the participants of a copy to an object store write into the destination's own upload,
    // so a copy that fails has no part object to remove: what it has to give up is the upload
    // itself. Whether the upload was aborted is not something the object store api answers,
    // since it lists objects and not uploads, so what this pins down is the part a reader can
    // see: an upload that was not completed leaves no destination behind.
    #[pg_test]
    fn test_s3_parallel_copy_to_gives_up_its_upload_when_a_range_fails() {
        let test_bucket_name =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let uri = format!("s3://{test_bucket_name}/parallel_copy_to/failed.parquet");

        // a destination an earlier run left behind would pass the assertion below for the
        // wrong reason
        delete_object(&uri);

        let error = failing_copy_to_error(&uri);

        assert!(
            error.contains("exceeds max allowed integral digits"),
            "the parallel copy reported \"{error}\" instead"
        );

        assert_eq!(object_store_parts(&uri), Vec::<String>::new());

        assert!(
            !object_exists(&uri),
            "the copy that failed completed its upload anyway"
        );
    }

    // copy_in_another_session starts a parallel COPY TO of the given table in a session of
    // its own and waits until the copy is running with workers of its own, which is past the
    // point the destination's upload is opened: the upload is opened before the workers are
    // launched. It returns the session together with the pid of the backend leading the copy,
    // which is what an interruption is sent to.
    //
    // The commands are written to a session that stays open afterwards, rather than passed
    // as arguments to a psql that exits once it is done with them, because a session that
    // exits removes the parts of its copies at process exit whatever the copy itself did:
    // what a cancelled copy cleaned up on its own is only visible while its session is there.
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
        writeln!(stdin, "SET pg_parquet.max_parallel_workers TO 4;")
            .and_then(|()| writeln!(stdin, "COPY {table} TO '{uri}' WITH (format parquet);"))
            .and_then(|()| stdin.flush())
            .expect("failed to send the copy to psql");

        (copy, copy_leader_pid(table))
    }

    // wait_for_idle_copy_session waits until the backend leading the copy is done with it,
    // which an interrupted copy is once the backend takes commands again. The cleanup of the
    // copy runs while the error of the interruption unwinds, so it is done by then as well.
    fn wait_for_idle_copy_session(leader_pid: i32) {
        let deadline = Instant::now() + COPY_START_TIMEOUT;

        loop {
            Spi::run("SELECT pg_stat_clear_snapshot();").unwrap();

            let state = Spi::get_one::<String>(&format!(
                "SELECT coalesce(max(state), 'gone') FROM pg_stat_activity WHERE pid = {leader_pid}"
            ))
            .unwrap()
            .unwrap();

            if state != "active" {
                return;
            }

            assert!(
                Instant::now() < deadline,
                "the backend leading the copy was still running it after {} seconds",
                COPY_START_TIMEOUT.as_secs()
            );

            Spi::run("SELECT pg_sleep(0.01);").unwrap();
        }
    }

    // copy_leader_pid is the pid of the backend running the COPY TO of the given table, taken
    // once the copy has parallel workers of its own
    fn copy_leader_pid(table: &str) -> i32 {
        let deadline = Instant::now() + COPY_START_TIMEOUT;

        loop {
            // the state of the other backends is read once per transaction and then cached,
            // and a test runs in a single transaction, so every look drops the cache first
            Spi::run("SELECT pg_stat_clear_snapshot();").unwrap();

            let leader_pid = Spi::get_one::<i32>(&format!(
                "SELECT coalesce(max(leader.pid), 0) FROM pg_stat_activity leader
                 WHERE leader.pid <> pg_backend_pid()
                   AND leader.query LIKE 'COPY {table} TO%'
                   AND EXISTS (SELECT 1 FROM pg_stat_activity worker
                               WHERE worker.leader_pid = leader.pid)"
            ))
            .unwrap()
            .unwrap();

            if leader_pid != 0 {
                return leader_pid;
            }

            assert!(
                Instant::now() < deadline,
                "no parallel COPY {table} TO was running after {} seconds",
                COPY_START_TIMEOUT.as_secs()
            );

            Spi::run("SELECT pg_sleep(0.01);").unwrap();
        }
    }

    // a cancel is the interruption a running copy is most likely to meet, and a local
    // destination is where the whole of the cleanup is visible from outside the session that
    // ran it: the part files are either still there or not.
    #[pg_test]
    fn test_parallel_copy_to_removes_its_part_files_after_a_cancel() {
        let _parallel_file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

        let table = "cancelled_copy_table";
        create_interruptible_table(table);

        let (copy, leader_pid) = copy_in_another_session(table, LOCAL_TEST_FILE_PATH);

        let cancelled = Spi::get_one::<bool>(&format!("SELECT pg_cancel_backend({leader_pid})"))
            .unwrap()
            .unwrap();
        assert!(cancelled, "the backend leading the copy took no cancel");

        // the session is still there while its parts are looked at, so what removed them is
        // the copy's own cleanup rather than the sweep a session runs when it exits
        wait_for_idle_copy_session(leader_pid);

        assert_eq!(part_files(LOCAL_TEST_FILE_PATH), Vec::<String>::new());

        assert!(
            !std::path::Path::new(LOCAL_TEST_FILE_PATH).exists(),
            "the cancelled copy wrote its destination anyway"
        );

        let output = copy_session_output(copy);
        drop_interruptible_table(table);

        assert!(
            output.contains("canceling statement due to user request"),
            "the copy was not cancelled while it was still running, which a larger \
             {table} would fix: {output}"
        );
    }

    // a part the sweep cannot remove is reported rather than passed over, and the parts it can
    // remove still go. A directory of the copy's own part name is a part remove_file fails on,
    // which is the only failure of a removal a test can arrange for.
    #[pg_test]
    fn test_parallel_copy_to_reports_the_part_it_could_not_remove() {
        let _parallel_file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

        let table = "parts_left_behind_table";
        create_interruptible_table(table);

        let (copy, leader_pid) = copy_in_another_session(table, LOCAL_TEST_FILE_PATH);

        let unremovable_part = format!("{LOCAL_TEST_FILE_PATH}.{leader_pid}.pgpq-part-dir.parquet");
        std::fs::create_dir(&unremovable_part).expect("failed to create the unremovable part");

        let cancelled = Spi::get_one::<bool>(&format!("SELECT pg_cancel_backend({leader_pid})"))
            .unwrap()
            .unwrap();
        assert!(cancelled, "the backend leading the copy took no cancel");

        wait_for_idle_copy_session(leader_pid);

        assert_eq!(
            part_files(LOCAL_TEST_FILE_PATH),
            vec![unremovable_part.clone()],
            "the copy kept the parts it could remove"
        );

        std::fs::remove_dir(&unremovable_part).expect("failed to remove the unremovable part");

        let output = copy_session_output(copy);
        drop_interruptible_table(table);

        assert!(
            output.contains("failed to remove the parts of the parallel COPY TO"),
            "the part that could not be removed was not reported: {output}"
        );
        assert!(
            output.contains(&format!(
                "The parts named after \"{LOCAL_TEST_FILE_PATH}.{leader_pid}.pgpq-part-\" are left in place."
            )),
            "the report did not name the parts: {output}"
        );
    }

    // a terminated backend dies with a FATAL, which unwinds none of the copy's own cleanup,
    // so the parts of a copy interrupted this way are removed at process exit instead. A
    // local destination is where that is visible from outside the session that ran it.
    #[pg_test]
    fn test_parallel_copy_to_removes_its_part_files_after_a_terminate() {
        let _parallel_file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

        let table = "terminated_copy_table";
        create_interruptible_table(table);

        let (copy, leader_pid) = copy_in_another_session(table, LOCAL_TEST_FILE_PATH);

        // the terminate is waited for, and what it waits for is the backend's process to be
        // gone, which is only after the cleanup that runs at process exit is done
        let terminated = Spi::get_one::<bool>(&format!(
            "SELECT pg_terminate_backend({leader_pid}, {})",
            COPY_START_TIMEOUT.as_millis()
        ))
        .unwrap()
        .unwrap();
        assert!(terminated, "the backend leading the copy did not exit");

        let output = copy_session_output(copy);
        drop_interruptible_table(table);

        assert!(
            output.contains("terminating connection due to administrator command"),
            "the copy was not terminated while it was still running, which a larger \
             {table} would fix: {output}"
        );

        assert_eq!(part_files(LOCAL_TEST_FILE_PATH), Vec::<String>::new());

        assert!(
            !std::path::Path::new(LOCAL_TEST_FILE_PATH).exists(),
            "the terminated copy wrote its destination anyway"
        );
    }

    // a cancelled copy to an object store has an upload of the destination's own to give up,
    // which is what keeps the destination from being there and the bytes the participants
    // already uploaded from being storage nobody asked for
    #[pg_test]
    fn test_s3_parallel_copy_to_cleans_up_after_a_cancel() {
        let test_bucket_name =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let uri = format!("s3://{test_bucket_name}/parallel_copy_to/cancelled.parquet");

        // a destination an earlier run left behind would pass the assertions below for the
        // wrong reason
        delete_object(&uri);

        let table = "cancelled_s3_copy_table";
        create_interruptible_table(table);

        let (copy, leader_pid) = copy_in_another_session(table, &uri);

        let cancelled = Spi::get_one::<bool>(&format!("SELECT pg_cancel_backend({leader_pid})"))
            .unwrap()
            .unwrap();
        assert!(cancelled, "the backend leading the copy took no cancel");

        // the session is still there while the destination is looked at, so what gave the
        // upload up is the copy's own cleanup rather than the sweep a session runs when it
        // exits
        wait_for_idle_copy_session(leader_pid);

        assert_eq!(
            object_store_parts_of(&uri, leader_pid),
            Vec::<String>::new()
        );

        assert!(
            !object_exists(&uri),
            "the cancelled copy completed its upload anyway"
        );

        let output = copy_session_output(copy);
        drop_interruptible_table(table);

        assert!(
            output.contains("canceling statement due to user request"),
            "the copy was not cancelled while it was still running, which a larger \
             {table} would fix: {output}"
        );
    }

    // a terminated backend dies with a FATAL, which unwinds none of the copy's own cleanup,
    // so the upload of a copy interrupted this way is given up at process exit instead
    #[pg_test]
    fn test_s3_parallel_copy_to_cleans_up_after_a_terminate() {
        let test_bucket_name =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let uri = format!("s3://{test_bucket_name}/parallel_copy_to/terminated.parquet");

        delete_object(&uri);

        let table = "terminated_s3_copy_table";
        create_interruptible_table(table);

        let (copy, leader_pid) = copy_in_another_session(table, &uri);

        // the terminate is waited for, and what it waits for is the backend's process to be
        // gone, which is only after the cleanup that runs at process exit is done
        let terminated = Spi::get_one::<bool>(&format!(
            "SELECT pg_terminate_backend({leader_pid}, {})",
            COPY_START_TIMEOUT.as_millis()
        ))
        .unwrap()
        .unwrap();
        assert!(terminated, "the backend leading the copy did not exit");

        let output = copy_session_output(copy);
        drop_interruptible_table(table);

        assert!(
            output.contains("terminating connection due to administrator command"),
            "the copy was not terminated while it was still running, which a larger \
             {table} would fix: {output}"
        );

        assert_eq!(
            object_store_parts_of(&uri, leader_pid),
            Vec::<String>::new()
        );

        assert!(
            !object_exists(&uri),
            "the terminated copy completed its upload anyway"
        );
    }

    // the upload of an interrupted copy is aborted rather than left for the bucket's own rule
    // for unfinished uploads to find. That an upload is gone is not something the object store
    // api answers by listing, since it lists objects and not uploads, so the store itself is
    // asked instead: an upload that was aborted takes no part of its own any more and can no
    // longer be completed, which is also what releases the bytes its parts hold.
    #[pg_test]
    fn test_s3_parallel_copy_to_abort_releases_the_upload() {
        let test_bucket_name =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let uri = format!("s3://{test_bucket_name}/parallel_copy_to/aborted_upload.parquet");

        delete_object(&uri);

        let uri_info = ParsedUriInfo::try_from(uri.as_str()).unwrap();
        let (store, path) =
            get_or_create_multipart_store(&uri_info).expect("s3 takes a multipart upload");

        // an upload holding a part of its own, which is the state the upload of a copy
        // interrupted in the middle is in. The part is one the store would take in a
        // completed upload, so that what refuses the completion below is the abort itself.
        let (upload_id, part) = PG_BACKEND_TOKIO_RUNTIME.block_on(async {
            let upload_id = store.create_multipart(&path).await.unwrap();

            let payload = PutPayload::from(vec![b'p'; 5 * 1024 * 1024]);
            let part = store.put_part(&path, &upload_id, 0, payload).await.unwrap();

            (upload_id, part)
        });

        let report_failures = true;
        abort_upload(&store, &path, &upload_id, report_failures);

        let completed = PG_BACKEND_TOKIO_RUNTIME.block_on(async {
            store
                .complete_multipart(&path, &upload_id, vec![part])
                .await
        });

        assert!(
            completed.is_err(),
            "the upload was completed after it was aborted"
        );

        assert!(
            !object_exists(&uri),
            "the aborted upload left its bytes behind as the destination"
        );
    }

    #[pg_test]
    fn test_parallel_copy_to_matches_sequential() {
        let _parallel_file = FileCleanup::new(LOCAL_TEST_FILE_PATH);
        let _sequential_file = FileCleanup::new(SEQUENTIAL_TEST_FILE_PATH);

        create_wide_table();

        Spi::run("SET pg_parquet.max_parallel_workers TO 0;").unwrap();
        Spi::run(&format!(
            "COPY wide_table TO '{SEQUENTIAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();

        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();
        Spi::run(&format!(
            "COPY wide_table TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();

        // the parallel file holds the same rows, with the same schema and the same key value
        // metadata, as the one the single process path writes
        assert_eq!(
            file_metadata(LOCAL_TEST_FILE_PATH),
            file_metadata(SEQUENTIAL_TEST_FILE_PATH)
        );
        assert_eq!(
            schema_fingerprint(LOCAL_TEST_FILE_PATH),
            schema_fingerprint(SEQUENTIAL_TEST_FILE_PATH)
        );
        assert_eq!(
            kv_metadata_fingerprint(LOCAL_TEST_FILE_PATH),
            kv_metadata_fingerprint(SEQUENTIAL_TEST_FILE_PATH)
        );

        assert_eq!(file_metadata(LOCAL_TEST_FILE_PATH).0, WIDE_TABLE_ROWS);
        assert_eq!(rows_not_in_file(LOCAL_TEST_FILE_PATH), (0, 0));

        // the part files are temporary and must not outlive the command
        assert_eq!(part_files(LOCAL_TEST_FILE_PATH), Vec::<String>::new());
    }

    // the parts of a parallel COPY TO are named next to its destination, and a copy whose
    // backend did not survive it leaves them there. They hold the rows that copy wrote, so a
    // pattern that covers them must not read those rows a second time.
    #[pg_test]
    fn test_parts_left_behind_are_not_read_as_data() {
        let folder = "/tmp/pg_parquet_test_parts_left_behind";
        let _folder = FileCleanup::new(folder);

        std::fs::create_dir_all(folder).unwrap();

        let destination = format!("{folder}/out.parquet");

        Spi::run("CREATE TABLE parts_table(a int); CREATE TABLE parts_result(a int);").unwrap();
        Spi::run("INSERT INTO parts_table SELECT i FROM generate_series(1, 1000) i;").unwrap();
        Spi::run(&format!(
            "COPY parts_table TO '{destination}' WITH (format parquet);"
        ))
        .unwrap();

        // the part of a copy that is over, which only the backend that led it would have
        // removed. The pid it is named after is of no process of this test.
        let part = format!("{destination}.1{PART_URI_INFIX}0.parquet");
        std::fs::copy(&destination, &part).unwrap();

        // the files the pattern covers are the data of the destination alone
        let listed = Spi::get_one::<i64>(&format!(
            "SELECT count(*) FROM parquet.list('{folder}/*.parquet')"
        ))
        .unwrap()
        .unwrap();
        assert_eq!(listed, 1);

        Spi::run(&format!(
            "COPY parts_result FROM '{folder}/*.parquet' WITH (format parquet);"
        ))
        .unwrap();

        assert_eq!(
            Spi::get_one::<i64>("SELECT count(*) FROM parts_result")
                .unwrap()
                .unwrap(),
            1000
        );

        // a part named outright is still a parquet file, so the rows of a copy that was
        // interrupted are there to be read by whoever wants them
        Spi::run("TRUNCATE parts_result;").unwrap();
        Spi::run(&format!(
            "COPY parts_result FROM '{part}' WITH (format parquet);"
        ))
        .unwrap();

        assert_eq!(
            Spi::get_one::<i64>("SELECT count(*) FROM parts_result")
                .unwrap()
                .unwrap(),
            1000
        );
    }

    #[pg_test]
    fn test_parallel_copy_to_file_size_bytes() {
        let sequential_folder = "/tmp/pg_parquet_test_split_sequential";
        let parallel_folder = "/tmp/pg_parquet_test_split_parallel";

        let _sequential_folder = FileCleanup::new(sequential_folder);
        let _parallel_folder = FileCleanup::new(parallel_folder);

        create_wide_table();

        Spi::run("SET pg_parquet.max_parallel_workers TO 0;").unwrap();
        Spi::run(&format!(
            "COPY wide_table TO '{sequential_folder}'
             WITH (format parquet, file_size_bytes '1MB');"
        ))
        .unwrap();

        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();
        Spi::run(&format!(
            "COPY wide_table TO '{parallel_folder}'
             WITH (format parquet, file_size_bytes '1MB');"
        ))
        .unwrap();

        // the folder holds the rows of the table, whatever the participants of the copy cut
        // them into, and the files are written the way the single process path writes them
        assert_eq!(split_folder_rows(parallel_folder), WIDE_TABLE_ROWS);
        assert_eq!(
            rows_not_in_file(&format!("{parallel_folder}/*.parquet")),
            (0, 0)
        );
        assert_eq!(
            schema_fingerprint(&format!("{parallel_folder}/data_0.parquet")),
            schema_fingerprint(&format!("{sequential_folder}/data_0.parquet"))
        );

        // every file is named after an id of its own, so no participant overwrote a file
        // another one wrote, and the ids they took together are the ids of one copy
        let parallel_files = split_file_names(parallel_folder);
        let mut parallel_file_ids = parallel_files
            .iter()
            .map(|name| {
                name.trim_start_matches("data_")
                    .trim_end_matches(".parquet")
                    .parse::<usize>()
                    .unwrap_or_else(|_| panic!("the folder holds a file named {name}"))
            })
            .collect::<Vec<_>>();
        parallel_file_ids.sort();

        assert_eq!(
            parallel_file_ids,
            (0..parallel_files.len()).collect::<Vec<_>>()
        );

        // a range finishes the file it is in the middle of before it ends, so a copy of
        // several ranges leaves more, smaller files behind than the single process path
        let sequential_files = split_file_names(sequential_folder);
        assert!(
            parallel_files.len() > sequential_files.len(),
            "the parallel copy wrote {} files and the single process path {}",
            parallel_files.len(),
            sequential_files.len()
        );

        // a file is created by the row that goes into it, so none of them holds no rows
        for name in &parallel_files {
            assert!(
                file_metadata(&format!("{parallel_folder}/{name}")).0 > 0,
                "{name} holds no rows"
            );
        }
    }

    // a range that finds no row of its own writes no file, so a folder destination never holds
    // a file of no rows. The rows of the table are deleted by the transaction of the test, so
    // the blocks that held them are still there for the ranges to claim while no range finds a
    // row it can see.
    #[pg_test]
    fn test_parallel_copy_to_file_size_bytes_empty_ranges() {
        let sequential_folder = "/tmp/pg_parquet_test_split_empty_sequential";
        let parallel_folder = "/tmp/pg_parquet_test_split_empty_parallel";

        let _sequential_folder = FileCleanup::new(sequential_folder);
        let _parallel_folder = FileCleanup::new(parallel_folder);
        let _file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

        create_wide_table();

        Spi::run("DELETE FROM wide_table;").unwrap();

        Spi::run("SET pg_parquet.max_parallel_workers TO 0;").unwrap();
        Spi::run(&format!(
            "COPY wide_table TO '{sequential_folder}'
             WITH (format parquet, file_size_bytes '1MB');"
        ))
        .unwrap();

        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();
        Spi::run(&format!(
            "COPY wide_table TO '{parallel_folder}'
             WITH (format parquet, file_size_bytes '1MB');"
        ))
        .unwrap();

        assert_eq!(split_file_names(sequential_folder), Vec::<String>::new());
        assert_eq!(split_file_names(parallel_folder), Vec::<String>::new());

        // a copy that writes a single file still writes it, since that file is the destination
        // the command names rather than a file one of its ranges needed
        Spi::run(&format!(
            "COPY wide_table TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();

        assert_eq!(file_metadata(LOCAL_TEST_FILE_PATH).0, 0);
    }

    // a folder destination holds the rows of the copy that wrote it last, rather than those
    // rows merged with the tail of an earlier and larger copy, whose files a read of the
    // folder would otherwise count as data of the copy that did not write them
    #[pg_test]
    fn test_copy_to_file_size_bytes_replaces_a_previous_copy() {
        let sequential_folder = "/tmp/pg_parquet_test_split_replace_sequential";
        let parallel_folder = "/tmp/pg_parquet_test_split_replace_parallel";

        let _sequential_folder = FileCleanup::new(sequential_folder);
        let _parallel_folder = FileCleanup::new(parallel_folder);

        create_wide_table();

        let folders = [(sequential_folder, 0), (parallel_folder, 4)];

        let mut files_of_the_first_copy = vec![];

        for (folder, workers) in folders {
            Spi::run(&format!(
                "SET pg_parquet.max_parallel_workers TO {workers};"
            ))
            .unwrap();

            Spi::run(&format!(
                "COPY wide_table TO '{folder}'
                 WITH (format parquet, file_size_bytes '1MB', row_group_size 1000);"
            ))
            .unwrap();

            let files = split_file_names(folder);
            assert!(
                files.len() > 1,
                "the copy wrote {} file(s), so a smaller copy leaves no tail behind",
                files.len()
            );

            files_of_the_first_copy.push(files.len());
        }

        // the rows are deleted by the transaction of the test, so the second copy writes far
        // fewer files while the relation keeps the blocks the ranges of a parallel copy claim
        Spi::run("DELETE FROM wide_table WHERE id > 100;").unwrap();

        for ((folder, workers), first_copy_files) in folders.iter().zip(files_of_the_first_copy) {
            Spi::run(&format!(
                "SET pg_parquet.max_parallel_workers TO {workers};"
            ))
            .unwrap();

            Spi::run(&format!(
                "COPY wide_table TO '{folder}'
                 WITH (format parquet, file_size_bytes '1MB', row_group_size 1000);"
            ))
            .unwrap();

            let files = split_file_names(folder);
            assert!(
                files.len() < first_copy_files,
                "the second copy wrote {} of the {first_copy_files} files of the first one",
                files.len()
            );

            // the ids of the files are the ids of the second copy alone, so nothing of the
            // first one is left in the folder
            let mut expected_files = (0..files.len())
                .map(|file_id| format!("data_{file_id}.parquet"))
                .collect::<Vec<_>>();
            expected_files.sort();

            assert_eq!(files, expected_files);
            assert_eq!(split_folder_rows(folder), 100);
        }
    }

    // a folder can hold files that no copy with file_size_bytes wrote, e.g. the marker a
    // reader of the folder leaves behind or a folder of its own, and the copy that replaces
    // the files of a previous copy is not a copy of those
    #[pg_test]
    fn test_copy_to_file_size_bytes_keeps_what_no_copy_wrote() {
        let folder = "/tmp/pg_parquet_test_split_replace_keeps";
        let _folder = FileCleanup::new(folder);

        create_wide_table();

        Spi::run("SET pg_parquet.max_parallel_workers TO 0;").unwrap();

        Spi::run(&format!(
            "COPY wide_table TO '{folder}'
             WITH (format parquet, file_size_bytes '1MB', row_group_size 1000);"
        ))
        .unwrap();

        assert!(
            split_file_names(folder).len() > 1,
            "the copy wrote a single file, so a smaller copy leaves no tail behind"
        );

        // a file whose name is not the name of a file of a copy, and one that is but is no
        // direct child of the folder
        Spi::run(&format!(
            "COPY (SELECT 1 AS a) TO '{folder}/kept.parquet' WITH (format parquet);
             COPY (SELECT 1 AS a) TO '{folder}/nested/data_99.parquet' WITH (format parquet);"
        ))
        .unwrap();

        Spi::run("DELETE FROM wide_table WHERE id > 100;").unwrap();

        Spi::run(&format!(
            "COPY wide_table TO '{folder}'
             WITH (format parquet, file_size_bytes '1MB', row_group_size 1000);"
        ))
        .unwrap();

        assert_eq!(
            folder_file_names(folder),
            vec!["data_0.parquet", "kept.parquet", "nested/data_99.parquet"]
        );
    }

    // the files of a previous copy are removed once the copy has written its own rather than
    // before it starts, so a copy that fails leaves the files of the previous copy that it
    // did not write over, rather than a folder it emptied on its way out. The files it did
    // write over are gone with the ones it gave up, which is a folder that is neither copy:
    // writing a folder twice is only safe for a copy that gets through its rows.
    #[pg_test]
    fn test_copy_to_file_size_bytes_failed_copy_keeps_what_it_did_not_write() {
        let folder = "/tmp/pg_parquet_test_split_replace_failed";
        let _folder = FileCleanup::new(folder);

        create_wide_table();

        Spi::run("SET pg_parquet.max_parallel_workers TO 0;").unwrap();

        Spi::run(&format!(
            "COPY wide_table TO '{folder}'
             WITH (format parquet, file_size_bytes '1MB', row_group_size 1000);"
        ))
        .unwrap();

        let files_of_the_first_copy = split_file_names(folder);
        assert!(
            files_of_the_first_copy.len() > 1,
            "the copy wrote a single file, so a failing copy writes over all of them"
        );

        let rows_of_the_first_copy = files_of_the_first_copy
            .iter()
            .map(|name| (name.clone(), file_metadata(&format!("{folder}/{name}")).0))
            .collect::<Vec<_>>();

        // fails_above is raised by the row above the given id, so a copy of the whole table
        // fails once it is that far through its rows
        Spi::run(
            "CREATE FUNCTION fails_above(id bigint, last bigint) RETURNS bigint
             LANGUAGE plpgsql AS $$
             BEGIN
                 IF id > last THEN
                     RAISE EXCEPTION 'no row above % of this copy', last;
                 END IF;

                 RETURN id;
             END;
             $$;",
        )
        .unwrap();

        // the copy fails on its very first row, so it has created no file of its own: the
        // folder is the previous copy, whole, which emptying it up front would have lost
        let failed_at_the_first_row = failing_copy_to(&format!(
            "COPY (SELECT fails_above(id, 0) AS id, name FROM wide_table) TO '{folder}'
             WITH (format parquet, file_size_bytes '1MB', row_group_size 1000)"
        ));

        assert_eq!(failed_at_the_first_row, "no row above 0 of this copy");
        assert_eq!(split_file_names(folder), files_of_the_first_copy);
        assert_eq!(split_folder_rows(folder), WIDE_TABLE_ROWS);

        // a copy that fails halfway has written some of the files of the folder, and gives
        // exactly those up
        let failed_halfway = failing_copy_to(&format!(
            "COPY (SELECT fails_above(id, {rows}) AS id, name FROM wide_table) TO '{folder}'
             WITH (format parquet, file_size_bytes '1MB', row_group_size 1000)",
            rows = WIDE_TABLE_ROWS / 2
        ));

        assert_eq!(
            failed_halfway,
            format!("no row above {} of this copy", WIDE_TABLE_ROWS / 2)
        );

        let kept_files = split_file_names(folder);

        assert!(
            !kept_files.is_empty() && kept_files.len() < files_of_the_first_copy.len(),
            "the failed copy left {} of the {} files of the previous copy",
            kept_files.len(),
            files_of_the_first_copy.len()
        );

        // the files it left are the last files of the previous copy, the ones it did not
        // reach, and they hold what that copy wrote into them
        assert_eq!(
            kept_files,
            files_of_the_first_copy[files_of_the_first_copy.len() - kept_files.len()..]
        );
        assert_eq!(
            split_folder_rows(folder),
            rows_of_the_first_copy
                .iter()
                .filter(|(name, _)| kept_files.contains(name))
                .map(|(_, rows)| rows)
                .sum::<i64>()
        );
    }

    // a folder on an object store is a prefix rather than a directory, so the files of a
    // previous copy are told apart from everything else under that prefix by their names
    // alone, and are removed one request at a time rather than by emptying a directory
    #[pg_test]
    fn test_s3_copy_to_file_size_bytes_replaces_a_previous_copy() {
        let test_bucket_name =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let sequential_folder =
            format!("s3://{test_bucket_name}/parallel_copy_to/replace_sequential");
        let parallel_folder = format!("s3://{test_bucket_name}/parallel_copy_to/replace_parallel");

        // the bucket outlives the test, so a folder of an earlier run is no part of this one
        empty_folder(&sequential_folder);
        empty_folder(&parallel_folder);

        create_wide_table();

        let folders = [(&sequential_folder, 0), (&parallel_folder, 4)];

        let mut files_of_the_first_copy = vec![];

        for (folder, workers) in folders {
            Spi::run(&format!(
                "SET pg_parquet.max_parallel_workers TO {workers};"
            ))
            .unwrap();

            Spi::run(&format!(
                "COPY wide_table TO '{folder}'
                 WITH (format parquet, file_size_bytes '1MB', row_group_size 1000);"
            ))
            .unwrap();

            let files = folder_file_names(folder);
            assert!(
                files.len() > 1,
                "the copy wrote {} file(s), so a smaller copy leaves no tail behind",
                files.len()
            );

            files_of_the_first_copy.push(files.len());
        }

        Spi::run("DELETE FROM wide_table WHERE id > 100;").unwrap();

        for ((folder, workers), first_copy_files) in folders.iter().zip(files_of_the_first_copy) {
            Spi::run(&format!(
                "SET pg_parquet.max_parallel_workers TO {workers};"
            ))
            .unwrap();

            Spi::run(&format!(
                "COPY wide_table TO '{folder}'
                 WITH (format parquet, file_size_bytes '1MB', row_group_size 1000);"
            ))
            .unwrap();

            let files = folder_file_names(folder);
            assert!(
                files.len() < first_copy_files,
                "the second copy wrote {} of the {first_copy_files} files of the first one",
                files.len()
            );

            let mut expected_files = (0..files.len())
                .map(|file_id| format!("data_{file_id}.parquet"))
                .collect::<Vec<_>>();
            expected_files.sort();

            assert_eq!(files, expected_files);
            assert_eq!(folder_rows(folder), 100);
        }
    }

    #[pg_test]
    fn test_parallel_copy_to_with_options() {
        let _parallel_file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

        create_wide_table();

        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();
        Spi::run(&format!(
            "COPY wide_table TO '{LOCAL_TEST_FILE_PATH}'
             WITH (format parquet, compression zstd, compression_level 19,
                   parquet_version v2, row_group_size 10000,
                   row_group_size_bytes 1000000, dictionary 'all');"
        ))
        .unwrap();

        // the options of the command have to survive the splice of the part files
        assert_eq!(
            file_metadata(LOCAL_TEST_FILE_PATH),
            (WIDE_TABLE_ROWS, "2".to_string())
        );

        let codecs = Spi::get_one::<String>(&format!(
            "SELECT string_agg(DISTINCT split_part(compression, '(', 1), ',')
             FROM parquet.metadata('{LOCAL_TEST_FILE_PATH}')"
        ))
        .unwrap()
        .unwrap();
        assert_eq!(codecs, "ZSTD");

        // a row group never holds more rows than row_group_size asks for
        let max_row_group_rows = Spi::get_one::<i64>(&format!(
            "SELECT max(row_group_num_rows) FROM parquet.metadata('{LOCAL_TEST_FILE_PATH}')"
        ))
        .unwrap()
        .unwrap();
        assert!(
            max_row_group_rows <= 10000,
            "a row group holds {max_row_group_rows} rows, more than row_group_size"
        );

        assert_eq!(rows_not_in_file(LOCAL_TEST_FILE_PATH), (0, 0));

        // parquet only records the codec, not the level it was given, so the level is only
        // visible in how well the file compressed
        let parallel_compressed_size = compressed_size(LOCAL_TEST_FILE_PATH);

        let _sequential_file = FileCleanup::new(SEQUENTIAL_TEST_FILE_PATH);
        Spi::run(&format!(
            "COPY wide_table TO '{SEQUENTIAL_TEST_FILE_PATH}'
             WITH (format parquet, compression zstd, compression_level 1,
                   parquet_version v2, row_group_size 10000,
                   row_group_size_bytes 1000000, dictionary 'all');"
        ))
        .unwrap();

        assert!(
            parallel_compressed_size < compressed_size(SEQUENTIAL_TEST_FILE_PATH),
            "compression_level did not reach the parallel workers"
        );
    }

    #[pg_test]
    fn test_parallel_copy_to_with_attribute_list() {
        let _parallel_file = FileCleanup::new(LOCAL_TEST_FILE_PATH);
        let _sequential_file = FileCleanup::new(SEQUENTIAL_TEST_FILE_PATH);

        create_wide_table();

        Spi::run("SET pg_parquet.max_parallel_workers TO 0;").unwrap();
        Spi::run(&format!(
            "COPY wide_table (id, amount, payload) TO '{SEQUENTIAL_TEST_FILE_PATH}'
             WITH (format parquet);"
        ))
        .unwrap();

        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();
        Spi::run(&format!(
            "COPY wide_table (id, amount, payload) TO '{LOCAL_TEST_FILE_PATH}'
             WITH (format parquet);"
        ))
        .unwrap();

        assert_eq!(
            schema_fingerprint(LOCAL_TEST_FILE_PATH),
            schema_fingerprint(SEQUENTIAL_TEST_FILE_PATH)
        );
        assert_eq!(file_metadata(LOCAL_TEST_FILE_PATH).0, WIDE_TABLE_ROWS);

        Spi::run(
            "CREATE TABLE wide_table_subset (id bigint, amount numeric(12,4), payload bytea);",
        )
        .unwrap();
        Spi::run(&format!(
            "COPY wide_table_subset FROM '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();

        let diff = Spi::get_one::<i64>(
            "SELECT count(*) FROM (
                 SELECT id, amount, payload FROM wide_table
                 EXCEPT ALL
                 SELECT id, amount, payload FROM wide_table_subset
             ) q",
        )
        .unwrap()
        .unwrap();
        assert_eq!(diff, 0);
    }

    // the per range statement leaves placeholders for the range bounds and the part uri,
    // which the participant that claims the range fills in by replacing their text. A name
    // or an option value of the command that holds the text of a placeholder is replaced
    // along with them unless the copy keeps the single process path.
    #[pg_test]
    fn test_parallel_copy_to_names_holding_a_placeholder() {
        create_wide_table();

        Spi::run("ALTER TABLE wide_table RENAME COLUMN payload TO \"@PART_URI@\";").unwrap();

        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();

        // the attribute list holds the text of a placeholder
        {
            let _file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

            Spi::run(&format!(
                "COPY wide_table (\"@PART_URI@\") TO '{LOCAL_TEST_FILE_PATH}'
                 WITH (format parquet);"
            ))
            .unwrap();

            assert_eq!(file_metadata(LOCAL_TEST_FILE_PATH).0, WIDE_TABLE_ROWS);
            assert_eq!(part_files(LOCAL_TEST_FILE_PATH), Vec::<String>::new());
        }

        // an option value holds the text of a placeholder, while the command names no column
        {
            let _file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

            Spi::run(&format!(
                "COPY wide_table TO '{LOCAL_TEST_FILE_PATH}'
                 WITH (format parquet,
                       field_ids '{{\"id\": 1, \"name\": 2, \"amount\": 3, \"created_at\": 4,
                                    \"flag\": 5, \"@PART_URI@\": 6}}');"
            ))
            .unwrap();

            assert_eq!(file_metadata(LOCAL_TEST_FILE_PATH).0, WIDE_TABLE_ROWS);
            assert_eq!(part_files(LOCAL_TEST_FILE_PATH), Vec::<String>::new());
        }

        // the schema and the relation hold the text of a placeholder
        {
            let _file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

            Spi::run(
                "CREATE SCHEMA \"s@LAST_BLOCK@\";
                 ALTER TABLE wide_table SET SCHEMA \"s@LAST_BLOCK@\";
                 ALTER TABLE \"s@LAST_BLOCK@\".wide_table RENAME TO \"t@FIRST_BLOCK@\";",
            )
            .unwrap();

            Spi::run(&format!(
                "COPY \"s@LAST_BLOCK@\".\"t@FIRST_BLOCK@\" TO '{LOCAL_TEST_FILE_PATH}'
                 WITH (format parquet);"
            ))
            .unwrap();

            assert_eq!(file_metadata(LOCAL_TEST_FILE_PATH).0, WIDE_TABLE_ROWS);
            assert_eq!(part_files(LOCAL_TEST_FILE_PATH), Vec::<String>::new());
        }
    }

    // a COPY TO that the parallel path cannot handle still has to write every row, through
    // the single process path
    #[pg_test]
    fn test_parallel_copy_to_fallbacks() {
        create_wide_table();

        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();

        // a table below the minimum number of pages
        {
            let _file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

            Spi::run("CREATE TABLE narrow_table AS SELECT g id FROM generate_series(1, 100) g;")
                .unwrap();
            Spi::run(&format!(
                "COPY narrow_table TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
            ))
            .unwrap();

            assert_eq!(file_metadata(LOCAL_TEST_FILE_PATH).0, 100);
        }

        // a query rather than a relation
        {
            let _file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

            Spi::run(&format!(
                "COPY (SELECT id FROM wide_table WHERE id % 1000 = 0) TO '{LOCAL_TEST_FILE_PATH}'
                 WITH (format parquet);"
            ))
            .unwrap();

            assert_eq!(
                file_metadata(LOCAL_TEST_FILE_PATH).0,
                WIDE_TABLE_ROWS / 1000
            );
        }

        // a temporary relation, which a parallel worker cannot see
        {
            let _file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

            Spi::run("CREATE TEMP TABLE temp_wide_table AS SELECT * FROM wide_table;").unwrap();
            Spi::run(&format!(
                "COPY temp_wide_table TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
            ))
            .unwrap();

            assert_eq!(file_metadata(LOCAL_TEST_FILE_PATH).0, WIDE_TABLE_ROWS);
            assert_eq!(part_files(LOCAL_TEST_FILE_PATH), Vec::<String>::new());
        }

        // a relation with row level security, whose scan is rewritten into a subquery that a
        // ctid range qual can no longer be pushed into
        {
            let _file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

            Spi::run("ALTER TABLE wide_table ENABLE ROW LEVEL SECURITY;").unwrap();

            Spi::run(&format!(
                "COPY wide_table TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
            ))
            .unwrap();

            Spi::run("ALTER TABLE wide_table DISABLE ROW LEVEL SECURITY;").unwrap();

            assert_eq!(file_metadata(LOCAL_TEST_FILE_PATH).0, WIDE_TABLE_ROWS);
            assert_eq!(part_files(LOCAL_TEST_FILE_PATH), Vec::<String>::new());
        }

        // an option without an argument, which the per range COPY statement cannot render
        // back
        {
            let _file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

            Spi::run(&format!(
                "COPY wide_table TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet, freeze);"
            ))
            .unwrap();

            assert_eq!(file_metadata(LOCAL_TEST_FILE_PATH).0, WIDE_TABLE_ROWS);
            assert_eq!(part_files(LOCAL_TEST_FILE_PATH), Vec::<String>::new());
        }

        // an option the per range COPY statement does not know how to render
        {
            let _file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

            Spi::run(&format!(
                "COPY wide_table TO '{LOCAL_TEST_FILE_PATH}'
                 WITH (format parquet, freeze true);"
            ))
            .unwrap();

            assert_eq!(file_metadata(LOCAL_TEST_FILE_PATH).0, WIDE_TABLE_ROWS);
            assert_eq!(part_files(LOCAL_TEST_FILE_PATH), Vec::<String>::new());
        }

        // a role that holds select on the columns it copies but not on the relation itself,
        // which the ctid of the per range statement would be refused
        {
            let _file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

            Spi::run(
                "CREATE ROLE column_select_only;
                 GRANT pg_write_server_files TO column_select_only;
                 GRANT SELECT (id) ON wide_table TO column_select_only;
                 SET ROLE column_select_only;",
            )
            .unwrap();

            Spi::run(&format!(
                "COPY wide_table (id) TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
            ))
            .unwrap();

            Spi::run("RESET ROLE;").unwrap();

            assert_eq!(file_metadata(LOCAL_TEST_FILE_PATH).0, WIDE_TABLE_ROWS);
            assert_eq!(part_files(LOCAL_TEST_FILE_PATH), Vec::<String>::new());
        }
    }

    // an http uri takes no multipart upload, which is what the splice is streamed into, so
    // it falls back to the single process path. The write then fails the way the single
    // process path fails it, because nothing answers the uri.
    #[pg_test]
    #[should_panic(
        expected = "failed to finish the parquet writer of uri http://localhost:1/pg_parquet_test.parquet"
    )]
    fn test_parallel_copy_to_http_destination() {
        create_wide_table();

        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();

        Spi::run(
            "COPY wide_table TO 'http://localhost:1/pg_parquet_test.parquet'
                  WITH (format parquet);",
        )
        .unwrap();
    }

    // a copy with file_size_bytes writes the files of a folder rather than the uri it was
    // given, so the error of a write names the file of the folder it could not write
    #[pg_test]
    #[should_panic(expected = "uri http://localhost:1/pg_parquet_test_folder/data_0.parquet")]
    fn test_copy_to_file_size_bytes_write_error_names_the_file() {
        create_wide_table();

        Spi::run("SET pg_parquet.max_parallel_workers TO 0;").unwrap();

        Spi::run(
            "COPY wide_table TO 'http://localhost:1/pg_parquet_test_folder'
             WITH (format parquet, file_size_bytes '1MB');",
        )
        .unwrap();
    }

    // a COPY TO STDOUT writes a temporary file under the data directory that the single
    // process path streams to the client once the copy is over, so the parallel path has to
    // leave the destination alone rather than splice the parts into that file and return.
    // The copy runs in a session of its own because the client is what receives the rows.
    #[pg_test]
    fn test_parallel_copy_to_stdout_streams_the_rows() {
        let table = "stdout_table";
        create_interruptible_table(table);

        let _file = FileCleanup::new(STDOUT_TEST_FILE_PATH);
        let stdout_file =
            File::create(STDOUT_TEST_FILE_PATH).expect("failed to create the stdout file");

        let port = test_pg_port();
        let copy = Command::new("psql")
            .args(["-h", "localhost", "-p", &port, "-d", "pgrx_tests", "-At"])
            .args(["-c", "SET pg_parquet.max_parallel_workers TO 4;"])
            .args([
                "-c",
                &format!("COPY {table} TO STDOUT WITH (format parquet);"),
            ])
            .stdout(Stdio::from(stdout_file))
            .output()
            .expect("failed to run the copy to stdout");

        assert!(
            copy.status.success(),
            "copy to stdout failed: {}",
            String::from_utf8_lossy(&copy.stderr)
        );

        drop_interruptible_table(table);

        assert_eq!(
            file_metadata(STDOUT_TEST_FILE_PATH).0,
            INTERRUPTED_TABLE_ROWS
        );
    }

    // the leader copies every ctid range itself when no parallel worker can be launched
    #[pg_test]
    fn test_parallel_copy_to_without_workers() {
        let _file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

        create_wide_table();

        Spi::run("SET max_parallel_workers TO 0;").unwrap();
        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();

        Spi::run(&format!(
            "COPY wide_table TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();

        assert_eq!(file_metadata(LOCAL_TEST_FILE_PATH).0, WIDE_TABLE_ROWS);
        assert_eq!(rows_not_in_file(LOCAL_TEST_FILE_PATH), (0, 0));
    }

    // a COPY TO an object store uri is spliced straight into a multipart upload, so this
    // also covers that the splice reaches the object store rather than local disk
    #[pg_test]
    fn test_s3_parallel_copy_to_matches_sequential() {
        let test_bucket_name =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let sequential_uri = format!("s3://{test_bucket_name}/parallel_copy_to/sequential.parquet");
        let parallel_uri = format!("s3://{test_bucket_name}/parallel_copy_to/parallel.parquet");

        create_wide_table();

        Spi::run("SET pg_parquet.max_parallel_workers TO 0;").unwrap();
        Spi::run(&format!(
            "COPY wide_table TO '{sequential_uri}' WITH (format parquet);"
        ))
        .unwrap();

        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();
        Spi::run(&format!(
            "COPY wide_table TO '{parallel_uri}' WITH (format parquet);"
        ))
        .unwrap();

        assert_eq!(file_metadata(&parallel_uri), file_metadata(&sequential_uri));
        assert_eq!(
            schema_fingerprint(&parallel_uri),
            schema_fingerprint(&sequential_uri)
        );
        assert_eq!(
            kv_metadata_fingerprint(&parallel_uri),
            kv_metadata_fingerprint(&sequential_uri)
        );

        assert_eq!(file_metadata(&parallel_uri).0, WIDE_TABLE_ROWS);
        assert_eq!(rows_not_in_file(&parallel_uri), (0, 0));

        // the parts of an object store destination are objects of the same store, and they
        // must not outlive the command
        assert_eq!(object_store_parts(&parallel_uri), Vec::<String>::new());
    }

    // the splice is streamed into a multipart upload, which hands a part over as soon as it
    // is full, so a destination larger than a single upload part is written while the splice
    // that feeds it is still running
    // metadata_with_page_index is the footer of the given uri together with the page indexes
    // it points at, which are read from where it says they are
    fn metadata_with_page_index(uri: &str) -> ParquetMetaData {
        let uri_info = ParsedUriInfo::try_from(uri).unwrap();

        let copy_from = true;
        let (store, location) = get_or_create_object_store(&uri_info, copy_from);

        PG_BACKEND_TOKIO_RUNTIME.block_on(async move {
            let object = store.head(&location).await.unwrap();

            let mut reader = ParquetObjectStoreReader::new(store, location, object.size);

            ParquetMetaDataReader::new()
                .with_page_index_policy(PageIndexPolicy::Required)
                .load_and_finish(&mut reader, object.size)
                .await
                .unwrap()
        })
    }

    // the page index of a range is written past the last row group of that range, so it ends
    // up in another part of the destination than the rows it belongs to. The footer has to
    // point every column chunk at where its own index was uploaded, which a reader that seeks
    // to it is what proves.
    #[pg_test]
    fn test_s3_parallel_copy_to_page_index_points_into_the_destination() {
        let test_bucket_name =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let sequential_uri =
            format!("s3://{test_bucket_name}/parallel_copy_to/page_index_sequential.parquet");
        let parallel_uri =
            format!("s3://{test_bucket_name}/parallel_copy_to/page_index_parallel.parquet");

        create_wide_table();

        Spi::run("SET pg_parquet.max_parallel_workers TO 0;").unwrap();
        Spi::run(&format!(
            "COPY wide_table TO '{sequential_uri}' WITH (format parquet);"
        ))
        .unwrap();

        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();
        Spi::run(&format!(
            "COPY wide_table TO '{parallel_uri}' WITH (format parquet);"
        ))
        .unwrap();

        let sequential = metadata_with_page_index(&sequential_uri);
        let parallel = metadata_with_page_index(&parallel_uri);

        let sequential_index = sequential.page_index().expect("no page index was written");
        let parallel_index = parallel.page_index().expect("no page index was written");

        // whatever the sequential path writes an index for, the parallel path writes one for
        assert_eq!(
            parallel_index.has_column_indexes(),
            sequential_index.has_column_indexes()
        );
        assert_eq!(
            parallel_index.has_offset_indexes(),
            sequential_index.has_offset_indexes()
        );

        assert!(!parallel.row_groups().is_empty());

        for (at, row_group) in parallel.row_groups().iter().enumerate() {
            for (column_at, column) in row_group.columns().iter().enumerate() {
                let offset_index = parallel_index
                    .offset_index(at, column_at)
                    .expect("a column chunk of the destination has no offset index");

                let pages = offset_index.page_locations();

                // the index of a column chunk describes that chunk, which is what reading it
                // from where the footer says it is has to land on
                assert_eq!(
                    pages.first().expect("a column chunk has no page").offset,
                    column.data_page_offset(),
                    "the offset index of column {column_at} of row group {at} is not the one \
                     of that column chunk"
                );

                let last = pages.last().expect("a column chunk has no page");

                assert!(
                    last.offset + last.compressed_page_size as i64
                        <= column.data_page_offset() + column.compressed_size(),
                    "the offset index of column {column_at} of row group {at} runs past that \
                     column chunk"
                );
            }
        }
    }

    #[pg_test]
    fn test_s3_parallel_copy_to_several_upload_parts() {
        let test_bucket_name =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let uri = format!("s3://{test_bucket_name}/parallel_copy_to/several_upload_parts.parquet");

        // every hash of the payload is drawn for the row it lands in, so that the
        // compression and the dictionary the writer would otherwise find a repeat in have
        // nothing to work with and the rows take their whole size in the destination
        Spi::run("CREATE TABLE random_table (id bigint, payload text);").unwrap();
        Spi::run(&format!(
            "INSERT INTO random_table
             SELECT g, md5(random()::text) || md5(random()::text) ||
                       md5(random()::text) || md5(random()::text) ||
                       md5(random()::text) || md5(random()::text) ||
                       md5(random()::text) || md5(random()::text)
             FROM generate_series(1, {WIDE_TABLE_ROWS}) g;"
        ))
        .unwrap();

        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();
        Spi::run(&format!(
            "COPY random_table TO '{uri}' WITH (format parquet);"
        ))
        .unwrap();

        assert_eq!(file_metadata(&uri).0, WIDE_TABLE_ROWS);

        // an upload part is 8MiB, and a destination that stopped running over one would no
        // longer write more than a single one of them
        let (dir_uri, _) = uri.rsplit_once('/').expect("uri names no directory");
        let size = Spi::get_one::<i64>(&format!(
            "SELECT size FROM parquet.list('{dir_uri}/*') WHERE uri = '{uri}'"
        ))
        .unwrap()
        .unwrap();
        assert!(
            size > 8 * 1024 * 1024,
            "the destination holds {size} bytes, which a single upload part covers"
        );

        assert_eq!(object_store_parts(&uri), Vec::<String>::new());
    }

    // the parts of an object store destination are objects of the same store rather than
    // local files, so a user who may write to the object store gets the fan-out without the
    // privilege to write the server's local files, and nothing of the copy touches the disk
    #[pg_test]
    fn test_s3_parallel_copy_to_without_local_file_privileges() {
        let test_bucket_name =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let parallel_uri =
            format!("s3://{test_bucket_name}/parallel_copy_to/object_store_only.parquet");

        create_wide_table();

        Spi::run(
            "CREATE USER object_store_only;
             GRANT parquet_object_store_write TO object_store_only;
             GRANT SELECT ON wide_table TO object_store_only;",
        )
        .unwrap();

        // the role the copy runs as may write to the object store and nothing else
        assert!(!Spi::get_one::<bool>(
            "SELECT pg_has_role('object_store_only', 'pg_write_server_files', 'USAGE')"
        )
        .unwrap()
        .unwrap());

        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();

        Spi::run("SET SESSION AUTHORIZATION object_store_only;").unwrap();

        Spi::run(&format!(
            "COPY wide_table TO '{parallel_uri}' WITH (format parquet);"
        ))
        .unwrap();

        Spi::run("RESET SESSION AUTHORIZATION;").unwrap();

        // a row group per ctid range is what the fan-out leaves behind, where the single
        // process path writes this table into a single row group
        assert!(
            row_groups(&parallel_uri) > 1,
            "the copy was not run in parallel"
        );

        assert_eq!(file_metadata(&parallel_uri).0, WIDE_TABLE_ROWS);
        assert_eq!(rows_not_in_file(&parallel_uri), (0, 0));

        // a local part file would need the privilege the role does not have, and a part
        // object left in the store would be read back by the next copy
        assert_eq!(object_store_parts(&parallel_uri), Vec::<String>::new());
    }

    // a geospatial column is written through a crs that the column's postgis type carries and
    // through per row group bounding box statistics, both of which the leader has to splice out
    // of the part files, so the parallel path is checked against the single process path for a
    // table of geometries too
    #[pg_test]
    fn test_parallel_copy_to_geospatial_matches_sequential() {
        if !extension_exists("postgis") || *extension_version("postgis") < *"3.4" {
            return;
        }
        Spi::run("DROP EXTENSION IF EXISTS postgis CASCADE; CREATE EXTENSION postgis;").unwrap();

        let _parallel_file = FileCleanup::new(GEO_TEST_FILE_PATH);
        let _sequential_file = FileCleanup::new(GEO_SEQUENTIAL_TEST_FILE_PATH);

        create_geo_table();

        Spi::run("SET pg_parquet.max_parallel_workers TO 0;").unwrap();
        Spi::run(&format!(
            "COPY geo_table TO '{GEO_SEQUENTIAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();

        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();
        Spi::run(&format!(
            "COPY geo_table TO '{GEO_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();

        // the ranges are written by their own writers, so the parallel file is the one with
        // more row groups. without this the rest would also hold for a silent fallback to the
        // single process path
        assert!(
            row_groups(GEO_TEST_FILE_PATH) > row_groups(GEO_SEQUENTIAL_TEST_FILE_PATH),
            "the copy did not run in parallel"
        );

        assert_eq!(
            file_metadata(GEO_TEST_FILE_PATH),
            file_metadata(GEO_SEQUENTIAL_TEST_FILE_PATH)
        );
        assert_eq!(
            schema_fingerprint(GEO_TEST_FILE_PATH),
            schema_fingerprint(GEO_SEQUENTIAL_TEST_FILE_PATH)
        );
        assert_eq!(
            kv_metadata_fingerprint(GEO_TEST_FILE_PATH),
            kv_metadata_fingerprint(GEO_SEQUENTIAL_TEST_FILE_PATH)
        );
        assert_eq!(file_metadata(GEO_TEST_FILE_PATH).0, GEO_TABLE_ROWS);

        // the crs of each column survives the splice, and is the one the column's type says
        let crs_by_column = geospatial_crs_by_column_of(GEO_TEST_FILE_PATH);
        assert_eq!(
            crs_by_column,
            geospatial_crs_by_column_of(GEO_SEQUENTIAL_TEST_FILE_PATH)
        );
        assert_eq!(
            crs_by_column.get("no_srid"),
            Some(&Some("srid:0".to_string()))
        );
        assert_eq!(crs_by_column.get("lon_lat"), Some(&None));
        assert_eq!(
            crs_by_column.get("web_mercator"),
            Some(&Some("EPSG:3857".to_string()))
        );
        assert_eq!(crs_by_column.get("geog"), Some(&None));

        // the bounding box of a column covers the same area, and the same geometry types are
        // seen, however the rows are split into row groups
        for column in ["no_srid", "lon_lat", "web_mercator", "geog"] {
            assert_eq!(
                bounding_box(GEO_TEST_FILE_PATH, column),
                bounding_box(GEO_SEQUENTIAL_TEST_FILE_PATH, column),
                "the bounding box of {column} does not match"
            );
            assert_eq!(
                geospatial_types(GEO_TEST_FILE_PATH, column),
                geospatial_types(GEO_SEQUENTIAL_TEST_FILE_PATH, column),
                "the geometry types of {column} do not match"
            );
            assert!(
                bounding_box(GEO_TEST_FILE_PATH, column).is_some(),
                "{column} has no bounding box statistics"
            );
        }

        // a column that is not geospatial has no such statistics, which is what tells the
        // asserts above apart from a file that lost them
        assert_eq!(bounding_box(GEO_TEST_FILE_PATH, "id"), None);
        assert_eq!(geospatial_types(GEO_TEST_FILE_PATH, "id"), None);

        // both files read back as the same rows
        assert_eq!(
            rows_not_in_other_file(GEO_TEST_FILE_PATH, GEO_SEQUENTIAL_TEST_FILE_PATH),
            (0, 0)
        );

        assert_eq!(part_files(GEO_TEST_FILE_PATH), Vec::<String>::new());
    }

    #[pg_test]
    fn test_parallel_copy_to_reaches_requested_row_group_size() {
        let _parallel_file = FileCleanup::new(LOCAL_TEST_FILE_PATH);
        let _sequential_file = FileCleanup::new(SEQUENTIAL_TEST_FILE_PATH);

        create_wide_table();

        // the ranges are sized from the relation's statistics, which an analyze puts there
        Spi::run("ANALYZE wide_table;").unwrap();

        // a tenth of the rows, so the relation holds ten row groups of the asked for size
        let row_group_size = WIDE_TABLE_ROWS / 10;

        Spi::run("SET pg_parquet.max_parallel_workers TO 0;").unwrap();
        Spi::run(&format!(
            "COPY wide_table TO '{SEQUENTIAL_TEST_FILE_PATH}'
             WITH (format parquet, row_group_size {row_group_size});"
        ))
        .unwrap();

        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();
        Spi::run(&format!(
            "COPY wide_table TO '{LOCAL_TEST_FILE_PATH}'
             WITH (format parquet, row_group_size {row_group_size});"
        ))
        .unwrap();

        assert_eq!(file_metadata(LOCAL_TEST_FILE_PATH).0, WIDE_TABLE_ROWS);
        assert_eq!(rows_not_in_file(LOCAL_TEST_FILE_PATH), (0, 0));

        // a range is written into a part file of its own, so a range holding fewer rows than
        // the asked for row group size would leave every row group of the copy short of it
        assert_eq!(max_row_group_rows(LOCAL_TEST_FILE_PATH), row_group_size);
        assert_eq!(
            max_row_group_rows(LOCAL_TEST_FILE_PATH),
            max_row_group_rows(SEQUENTIAL_TEST_FILE_PATH)
        );

        // the rows are still spread over ranges rather than written by the leader alone
        assert!(row_groups(LOCAL_TEST_FILE_PATH) > 1);

        assert_eq!(part_files(LOCAL_TEST_FILE_PATH), Vec::<String>::new());
    }

    #[pg_test]
    fn test_parallel_copy_to_row_group_size_the_relation_cannot_fill() {
        let _parallel_file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

        create_wide_table();

        Spi::run("ANALYZE wide_table;").unwrap();

        // the relation cannot give a participant a row group of this size, so the copy writes
        // row groups short of it rather than giving up the fan-out for it
        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();
        Spi::run(&format!(
            "COPY wide_table TO '{LOCAL_TEST_FILE_PATH}'
             WITH (format parquet, row_group_size {WIDE_TABLE_ROWS});"
        ))
        .unwrap();

        // a row group never runs over the asked for size, which is what the option bounds
        assert!(max_row_group_rows(LOCAL_TEST_FILE_PATH) <= WIDE_TABLE_ROWS);

        // the rows are still spread over ranges rather than written by the leader alone
        assert!(row_groups(LOCAL_TEST_FILE_PATH) > 1);

        assert_eq!(file_metadata(LOCAL_TEST_FILE_PATH).0, WIDE_TABLE_ROWS);
        assert_eq!(rows_not_in_file(LOCAL_TEST_FILE_PATH), (0, 0));
        assert_eq!(part_files(LOCAL_TEST_FILE_PATH), Vec::<String>::new());
    }

    #[pg_test]
    fn test_parallel_copy_to_directory_destination_errors_like_sequential() {
        create_wide_table();

        let directory = "/tmp/pg_parquet_test_parallel_dir";
        std::fs::create_dir_all(directory).unwrap();

        Spi::run(&format!(
            "CREATE FUNCTION copy_to_directory_error(workers int) RETURNS text
             LANGUAGE plpgsql AS $$
             BEGIN
                 EXECUTE 'SET pg_parquet.max_parallel_workers TO ' || workers;

                 BEGIN
                     EXECUTE 'COPY wide_table TO ''{directory}'' WITH (format parquet)';

                     RETURN 'no error';
                 EXCEPTION WHEN others THEN
                     RETURN SQLERRM;
                 END;
             END;
             $$;"
        ))
        .unwrap();

        let sequential_error = Spi::get_one::<String>("SELECT copy_to_directory_error(0)")
            .unwrap()
            .unwrap();
        let parallel_error = Spi::get_one::<String>("SELECT copy_to_directory_error(4)")
            .unwrap()
            .unwrap();

        // the splice reports a destination it cannot create in its own words, so a
        // destination it cannot create at all is left to the single process path
        assert_eq!(parallel_error, sequential_error);
        assert!(
            sequential_error.contains("Is a directory"),
            "the single process path reported \"{sequential_error}\" instead"
        );

        std::fs::remove_dir_all(directory).unwrap();
    }

    // column_stats_fingerprint is what parquet.column_stats says about the columns of a file.
    // Those statistics are written per row group and aggregated over the whole file by the
    // function, so they are comparable between two files that hold the same rows in a
    // different number of row groups, which the parallel and the single process path do.
    fn column_stats_fingerprint(uri: &str) -> String {
        Spi::get_one::<String>(&format!(
            "SELECT md5(string_agg(column_id || '|' ||
                                   coalesce(field_id::text, '<none>') || '|' ||
                                   coalesce(stats_min, '<none>') || '|' ||
                                   coalesce(stats_max, '<none>') || '|' ||
                                   coalesce(stats_null_count::text, '<none>'),
                                   E'\\n' ORDER BY column_id))
             FROM parquet.column_stats('{uri}')"
        ))
        .unwrap()
        .unwrap()
    }

    // field_ids_of is the field id the file carries for each of its columns, in schema order
    fn field_ids_of(uri: &str) -> Vec<String> {
        let field_ids = Spi::get_one::<String>(&format!(
            "SELECT coalesce(string_agg(name || '=' || coalesce(field_id::text, '<none>'), ' '), '')
             FROM parquet.schema('{uri}')"
        ))
        .unwrap()
        .unwrap();

        field_ids.split_whitespace().map(str::to_string).collect()
    }

    // the statistics of a column are what a reader skips row groups with, so a wrong minimum
    // or maximum in the footer of a spliced file is a file that reads back short rather than
    // a file that fails to read. The aggregate over the row groups has to be the aggregate
    // the single process path writes.
    #[pg_test]
    fn test_parallel_copy_to_column_stats_match_sequential() {
        let _parallel_file = FileCleanup::new(LOCAL_TEST_FILE_PATH);
        let _sequential_file = FileCleanup::new(SEQUENTIAL_TEST_FILE_PATH);

        create_wide_table();

        Spi::run("SET pg_parquet.max_parallel_workers TO 0;").unwrap();
        Spi::run(&format!(
            "COPY wide_table TO '{SEQUENTIAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();

        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();
        Spi::run(&format!(
            "COPY wide_table TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();

        // the parallel file is the one written in ranges, which is what makes the comparison
        // below one between an aggregate of many row groups and one of few
        assert!(row_groups(LOCAL_TEST_FILE_PATH) > row_groups(SEQUENTIAL_TEST_FILE_PATH));

        assert_eq!(
            column_stats_fingerprint(LOCAL_TEST_FILE_PATH),
            column_stats_fingerprint(SEQUENTIAL_TEST_FILE_PATH)
        );

        // every column of the table but its id carries nulls, and a column whose statistics
        // were lost in the splice would report none of them
        let columns_with_nulls = Spi::get_one::<i64>(&format!(
            "SELECT count(*) FROM parquet.column_stats('{LOCAL_TEST_FILE_PATH}')
             WHERE stats_null_count > 0"
        ))
        .unwrap()
        .unwrap();
        assert_eq!(
            columns_with_nulls, 5,
            "{columns_with_nulls} columns of the parallel file report nulls of their own"
        );
    }

    // a field id is written into the schema of the file rather than into a row group, so what
    // has to carry it through is the schema the leader writes the footer with
    #[pg_test]
    fn test_parallel_copy_to_field_ids() {
        let _parallel_file = FileCleanup::new(LOCAL_TEST_FILE_PATH);
        let _sequential_file = FileCleanup::new(SEQUENTIAL_TEST_FILE_PATH);

        create_wide_table();

        for field_ids in ["auto", "none", r#"{"id": 7, "name": 9}"#] {
            Spi::run("SET pg_parquet.max_parallel_workers TO 0;").unwrap();
            Spi::run(&format!(
                "COPY wide_table TO '{SEQUENTIAL_TEST_FILE_PATH}'
                 WITH (format parquet, field_ids '{field_ids}');"
            ))
            .unwrap();

            Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();
            Spi::run(&format!(
                "COPY wide_table TO '{LOCAL_TEST_FILE_PATH}'
                 WITH (format parquet, field_ids '{field_ids}');"
            ))
            .unwrap();

            assert!(row_groups(LOCAL_TEST_FILE_PATH) > 1);

            assert_eq!(
                field_ids_of(LOCAL_TEST_FILE_PATH),
                field_ids_of(SEQUENTIAL_TEST_FILE_PATH),
                "field_ids '{field_ids}' did not survive the splice"
            );
            assert_eq!(file_metadata(LOCAL_TEST_FILE_PATH).0, WIDE_TABLE_ROWS);
        }

        // the ids the explicit mapping asked for are the ids the file ends up with, so that
        // the comparison above is not one between two files that both carry none
        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();
        Spi::run(&format!(
            "COPY wide_table TO '{LOCAL_TEST_FILE_PATH}'
             WITH (format parquet, field_ids '{{\"id\": 7, \"name\": 9}}');"
        ))
        .unwrap();

        let ids = field_ids_of(LOCAL_TEST_FILE_PATH);
        assert!(
            ids.contains(&"id=7".to_string()) && ids.contains(&"name=9".to_string()),
            "the parallel file carries the field ids {ids:?}"
        );
    }

    // columns_matching_every_row_group is the leaf columns of the file that satisfy the given
    // predicate in every one of its row groups. A spliced file has a row group per range, and
    // an option that only reached the leader would show up in one of them rather than all.
    fn columns_matching_every_row_group(
        uri: &str,
        predicate: fn(&ColumnChunkMetaData) -> bool,
    ) -> Vec<String> {
        let uri_info = ParsedUriInfo::try_from(uri).unwrap();
        let metadata = parquet_metadata_from_uri(&uri_info);

        let mut matching: Option<Vec<String>> = None;

        for row_group in metadata.row_groups() {
            let mut columns = row_group
                .columns()
                .iter()
                .filter(|column| predicate(column))
                .map(|column| column.column_path().string())
                .collect::<Vec<_>>();
            columns.sort();

            match &matching {
                None => matching = Some(columns),
                Some(first) => assert_eq!(
                    *first, columns,
                    "the row groups of {uri} do not agree on which columns match"
                ),
            }
        }

        matching.expect("the file holds no row group")
    }

    // a bloom filter is written after the rows of its own row group, so every filter of a
    // spliced file sits at a different offset than the part file it came out of. The footer
    // has to point every column chunk at where its filter ended up, which is only provable by
    // reading the filter back from where the footer says it is and asking it about a value the
    // row group holds.
    #[pg_test]
    fn test_parallel_copy_to_bloom_filter() {
        let _parallel_file = FileCleanup::new(LOCAL_TEST_FILE_PATH);
        let _sequential_file = FileCleanup::new(SEQUENTIAL_TEST_FILE_PATH);

        create_wide_table();

        Spi::run("SET pg_parquet.max_parallel_workers TO 0;").unwrap();
        Spi::run(&format!(
            "COPY wide_table TO '{SEQUENTIAL_TEST_FILE_PATH}'
             WITH (format parquet, bloom_filter 'all', bloom_filter_fpp 0.001);"
        ))
        .unwrap();

        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();
        Spi::run(&format!(
            "COPY wide_table TO '{LOCAL_TEST_FILE_PATH}'
             WITH (format parquet, bloom_filter 'all', bloom_filter_fpp 0.001);"
        ))
        .unwrap();

        assert!(row_groups(LOCAL_TEST_FILE_PATH) > 1);

        // whichever columns the single process path writes a filter for, every row group of
        // the parallel file carries one for
        let with_filter = |uri| {
            columns_matching_every_row_group(uri, |column| column.bloom_filter_offset().is_some())
        };
        assert_eq!(
            with_filter(LOCAL_TEST_FILE_PATH),
            with_filter(SEQUENTIAL_TEST_FILE_PATH)
        );
        assert!(!with_filter(LOCAL_TEST_FILE_PATH).is_empty());

        assert_filters_answer_for_their_own_row_groups(LOCAL_TEST_FILE_PATH);

        // a column the option leaves out gets no filter, in any row group
        Spi::run(&format!(
            "COPY wide_table TO '{LOCAL_TEST_FILE_PATH}'
             WITH (format parquet, bloom_filter '{{\"id\": true, \"name\": true}}');"
        ))
        .unwrap();
        assert_eq!(
            columns_matching_every_row_group(LOCAL_TEST_FILE_PATH, |column| column
                .bloom_filter_offset()
                .is_some()),
            vec!["id", "name"]
        );

        Spi::run(&format!(
            "COPY wide_table TO '{LOCAL_TEST_FILE_PATH}'
             WITH (format parquet, bloom_filter 'none');"
        ))
        .unwrap();
        assert_eq!(
            columns_matching_every_row_group(LOCAL_TEST_FILE_PATH, |column| column
                .bloom_filter_offset()
                .is_some()),
            Vec::<String>::new()
        );
    }

    // assert_filters_answer_for_their_own_row_groups reads the bloom filter of the id column
    // of every row group of the file from the offset the footer gives for it, and asks it
    // about the ids that row group holds. A filter the footer points at the wrong offset
    // either fails to parse or answers for another row group's ids.
    fn assert_filters_answer_for_their_own_row_groups(uri: &str) {
        let uri_info = ParsedUriInfo::try_from(uri).unwrap();
        let metadata = parquet_metadata_from_uri(&uri_info);

        let file = File::open(uri).expect("the destination cannot be read");

        let id_column = metadata
            .file_metadata()
            .schema_descr()
            .columns()
            .iter()
            .position(|column| column.name() == "id")
            .expect("the file has no id column");

        for (at, row_group) in metadata.row_groups().iter().enumerate() {
            let column = row_group.column(id_column);

            let filter = Sbbf::read_from_column_chunk(column, &file)
                .unwrap_or_else(|e| panic!("the filter of row group {at} cannot be read: {e}"))
                .unwrap_or_else(|| panic!("row group {at} carries no filter for its id column"));

            // the id column holds the generated series, so the bounds of the row group are
            // ids it holds and every id outside them is an id it does not
            let statistics = column
                .statistics()
                .unwrap_or_else(|| panic!("row group {at} carries no statistics"));

            let (min, max) = match statistics {
                Statistics::Int64(stats) => (
                    *stats.min_opt().expect("no minimum"),
                    *stats.max_opt().expect("no maximum"),
                ),
                other => panic!("the id column of row group {at} has {other:?}"),
            };

            assert!(
                filter.check(&min) && filter.check(&max),
                "the filter of row group {at} does not know the ids {min} and {max} of its \
                 own rows, so the footer points it at another row group's filter"
            );

            // an id no row of the file holds, which only a false positive of the filter
            // would claim: the fpp the copy was given makes that unlikely enough to assert
            assert!(
                !filter.check(&(WIDE_TABLE_ROWS + 1_000_000 + at as i64)),
                "the filter of row group {at} claims an id the table never held"
            );
        }
    }

    // a dictionary is written per column chunk, so the option has to reach the participant
    // that writes the chunk rather than only the leader that writes the footer
    #[pg_test]
    fn test_parallel_copy_to_dictionary() {
        let _parallel_file = FileCleanup::new(LOCAL_TEST_FILE_PATH);
        let _sequential_file = FileCleanup::new(SEQUENTIAL_TEST_FILE_PATH);

        create_wide_table();

        let with_dictionary = |uri| {
            columns_matching_every_row_group(uri, |column| {
                column.dictionary_page_offset().is_some()
            })
        };

        for dictionary in ["all", "none"] {
            Spi::run("SET pg_parquet.max_parallel_workers TO 0;").unwrap();
            Spi::run(&format!(
                "COPY wide_table TO '{SEQUENTIAL_TEST_FILE_PATH}'
                 WITH (format parquet, dictionary '{dictionary}');"
            ))
            .unwrap();

            Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();
            Spi::run(&format!(
                "COPY wide_table TO '{LOCAL_TEST_FILE_PATH}'
                 WITH (format parquet, dictionary '{dictionary}');"
            ))
            .unwrap();

            assert!(row_groups(LOCAL_TEST_FILE_PATH) > 1);

            assert_eq!(
                with_dictionary(LOCAL_TEST_FILE_PATH),
                with_dictionary(SEQUENTIAL_TEST_FILE_PATH),
                "dictionary '{dictionary}' did not reach the participants of the copy"
            );

            if dictionary == "none" {
                assert_eq!(with_dictionary(LOCAL_TEST_FILE_PATH), Vec::<String>::new());
            } else {
                assert!(!with_dictionary(LOCAL_TEST_FILE_PATH).is_empty());
            }

            assert_eq!(file_metadata(LOCAL_TEST_FILE_PATH).0, WIDE_TABLE_ROWS);
            assert_eq!(rows_not_in_file(LOCAL_TEST_FILE_PATH), (0, 0));
        }
    }

    // azure takes a multipart upload of its own kind, with its own rule for how a part is
    // named and how the parts are committed, so the object store path of the copy is run
    // against a second store rather than against s3 alone
    #[pg_test]
    #[cfg(not(rhel8))]
    fn test_azure_parallel_copy_to_matches_sequential() {
        let test_container_name = std::env::var("AZURE_TEST_CONTAINER_NAME")
            .expect("AZURE_TEST_CONTAINER_NAME not found");

        let sequential_uri =
            format!("az://{test_container_name}/parallel_copy_to/sequential.parquet");
        let parallel_uri = format!("az://{test_container_name}/parallel_copy_to/parallel.parquet");

        create_wide_table();

        Spi::run("SET pg_parquet.max_parallel_workers TO 0;").unwrap();
        Spi::run(&format!(
            "COPY wide_table TO '{sequential_uri}' WITH (format parquet);"
        ))
        .unwrap();

        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();
        Spi::run(&format!(
            "COPY wide_table TO '{parallel_uri}' WITH (format parquet);"
        ))
        .unwrap();

        assert!(row_groups(&parallel_uri) > row_groups(&sequential_uri));

        assert_eq!(file_metadata(&parallel_uri), file_metadata(&sequential_uri));
        assert_eq!(
            schema_fingerprint(&parallel_uri),
            schema_fingerprint(&sequential_uri)
        );
        assert_eq!(
            kv_metadata_fingerprint(&parallel_uri),
            kv_metadata_fingerprint(&sequential_uri)
        );
        assert_eq!(
            column_stats_fingerprint(&parallel_uri),
            column_stats_fingerprint(&sequential_uri)
        );

        assert_eq!(file_metadata(&parallel_uri).0, WIDE_TABLE_ROWS);
        assert_eq!(rows_not_in_file(&parallel_uri), (0, 0));

        assert_eq!(object_store_parts(&parallel_uri), Vec::<String>::new());
    }

    // the upload of a cancelled copy is given up for whatever store the destination is in,
    // which is checked against azure as well as against s3 because each store aborts an
    // upload in its own way
    #[pg_test]
    #[cfg(not(rhel8))]
    fn test_azure_parallel_copy_to_cleans_up_after_a_cancel() {
        let test_container_name = std::env::var("AZURE_TEST_CONTAINER_NAME")
            .expect("AZURE_TEST_CONTAINER_NAME not found");

        let uri = format!("az://{test_container_name}/parallel_copy_to/cancelled.parquet");

        // a destination an earlier run left behind would pass the assertions below for the
        // wrong reason
        delete_object(&uri);

        let table = "cancelled_azure_copy_table";
        create_interruptible_table(table);

        let (copy, leader_pid) = copy_in_another_session(table, &uri);

        let cancelled = Spi::get_one::<bool>(&format!("SELECT pg_cancel_backend({leader_pid})"))
            .unwrap()
            .unwrap();
        assert!(cancelled, "the backend leading the copy took no cancel");

        // the session is still there while the destination is looked at, so what gave the
        // upload up is the copy's own cleanup rather than the sweep a session runs when it
        // exits
        wait_for_idle_copy_session(leader_pid);

        assert_eq!(
            object_store_parts_of(&uri, leader_pid),
            Vec::<String>::new()
        );

        assert!(
            !object_exists(&uri),
            "the cancelled copy completed its upload anyway"
        );

        let output = copy_session_output(copy);
        drop_interruptible_table(table);

        assert!(
            output.contains("canceling statement due to user request"),
            "the copy was not cancelled while it was still running, which a larger \
             {table} would fix: {output}"
        );
    }
}
