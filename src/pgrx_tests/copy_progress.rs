#[pgrx::pg_schema]
mod tests {
    use pgrx::{pg_test, Spi};

    use crate::pgrx_tests::common::{FileCleanup, LOCAL_TEST_FILE_PATH};

    // the sampler collects a row of pg_stat_progress_copy per copied tuple into
    // copy_progress_samples, so that the progress of the ongoing COPY can be asserted
    fn create_copy_progress_sampler() {
        Spi::run(
            "CREATE TABLE copy_progress_samples (
                 sample_id bigserial,
                 command text,
                 copy_type text,
                 bytes_processed bigint,
                 bytes_total bigint,
                 tuples_processed bigint
             );

             CREATE FUNCTION sample_copy_progress() RETURNS void AS $$
             BEGIN
                 -- the backend status snapshot is cached until the end of the transaction,
                 -- so it needs to be discarded to see the progress of the ongoing COPY
                 PERFORM pg_stat_clear_snapshot();

                 INSERT INTO copy_progress_samples
                     (command, copy_type, bytes_processed, bytes_total, tuples_processed)
                 SELECT command, type, bytes_processed, bytes_total, tuples_processed
                 FROM pg_stat_progress_copy
                 WHERE pid = pg_backend_pid();
             END;
             $$ LANGUAGE plpgsql;

             -- sampler for COPY TO, which is called by the copied query for every nth row.
             -- It is volatile, hence parallel unsafe, which also keeps the plan serial.
             CREATE FUNCTION sample_copy_progress_at(val bigint, nth bigint)
             RETURNS bigint AS $$
             BEGIN
                 IF val % nth = 0 THEN
                     PERFORM sample_copy_progress();
                 END IF;

                 RETURN val;
             END;
             $$ LANGUAGE plpgsql VOLATILE;

             -- sampler for COPY FROM, which is called for every inserted row
             CREATE FUNCTION sample_copy_progress_trigger() RETURNS trigger AS $$
             BEGIN
                 PERFORM sample_copy_progress();

                 RETURN NEW;
             END;
             $$ LANGUAGE plpgsql;",
        )
        .unwrap();
    }

    fn sample_count() -> i64 {
        Spi::get_one::<i64>("SELECT count(*) FROM copy_progress_samples")
            .unwrap()
            .unwrap()
    }

    fn distinct_sampled(column: &str) -> Vec<String> {
        Spi::connect(|client| {
            let query =
                format!("SELECT DISTINCT {column}::text FROM copy_progress_samples ORDER BY 1");

            client
                .select(&query, None, &[])
                .unwrap()
                .map(|row| row.get::<String>(1).unwrap().unwrap())
                .collect()
        })
    }

    fn max_sampled(column: &str) -> i64 {
        Spi::get_one::<i64>(&format!("SELECT max({column}) FROM copy_progress_samples"))
            .unwrap()
            .unwrap()
    }

    // the reported bytes may only grow during a COPY
    fn assert_sampled_bytes_never_decrease() {
        let decreasing_samples = Spi::get_one::<i64>(
            "SELECT count(*) FROM (
                 SELECT bytes_processed - lag(bytes_processed) OVER (ORDER BY sample_id) AS diff
                 FROM copy_progress_samples
             ) diffs
             WHERE diff < 0",
        )
        .unwrap()
        .unwrap();

        assert_eq!(0, decreasing_samples);
    }

    fn assert_copy_progress_ended() {
        let active_copies = Spi::get_one::<i64>(
            "SELECT pg_stat_clear_snapshot();
             SELECT count(*) FROM pg_stat_progress_copy WHERE pid = pg_backend_pid()",
        )
        .unwrap()
        .unwrap();

        assert_eq!(0, active_copies);
    }

    #[pg_test]
    fn test_copy_to_progress() {
        let _file_cleanup = FileCleanup::new(LOCAL_TEST_FILE_PATH);

        create_copy_progress_sampler();

        let total_rows = 2000;

        let copy_to_command = format!(
            "COPY (SELECT sample_copy_progress_at(i, 1) FROM generate_series(1, {total_rows}) i)
             TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet, row_group_size 100)"
        );
        Spi::run(&copy_to_command).unwrap();

        assert_eq!(total_rows, sample_count());

        assert_eq!(vec!["COPY TO".to_string()], distinct_sampled("command"));
        assert_eq!(vec!["FILE".to_string()], distinct_sampled("copy_type"));

        // the sampler runs before the tuple reaches our dest receiver
        assert_eq!(total_rows - 1, max_sampled("tuples_processed"));

        // the row groups are flushed to the file during the COPY
        assert!(max_sampled("bytes_processed") > 0);
        assert_sampled_bytes_never_decrease();

        // the size of the parquet file is not known before the COPY ends
        assert_eq!(0, max_sampled("bytes_total"));

        assert_copy_progress_ended();
    }

    #[pg_test]
    fn test_copy_to_progress_with_split_files() {
        const SPLIT_FOLDER: &str = "/tmp/pg_parquet_progress_test";
        const FILE_SIZE_BYTES: i64 = 1024 * 1024;

        let _file_cleanup = FileCleanup::new(SPLIT_FOLDER);

        create_copy_progress_sampler();

        let copy_to_command = format!(
            "COPY (SELECT sample_copy_progress_at(i, 10000),
                          repeat(md5(i::text), 4) AS payload
                   FROM generate_series(1, 300000) i)
             TO '{SPLIT_FOLDER}' WITH (format parquet, file_size_bytes '1MB')"
        );
        Spi::run(&copy_to_command).unwrap();

        // the copy is split into multiple files
        let file_count = std::fs::read_dir(SPLIT_FOLDER).unwrap().count();
        assert!(file_count > 1);

        // the bytes of the files that are already written are kept in the reported total
        assert!(max_sampled("bytes_processed") > FILE_SIZE_BYTES);
        assert_sampled_bytes_never_decrease();

        assert_copy_progress_ended();
    }

    #[pg_test]
    fn test_copy_from_progress() {
        let _file_cleanup = FileCleanup::new(LOCAL_TEST_FILE_PATH);

        let total_rows = 2000;

        let copy_to_command = format!(
            "COPY (SELECT i::int4 AS a FROM generate_series(1, {total_rows}) i)
             TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet)"
        );
        Spi::run(&copy_to_command).unwrap();

        create_copy_progress_sampler();

        Spi::run(
            "CREATE TABLE test_result (a int4);

             CREATE TRIGGER sample_copy_progress BEFORE INSERT ON test_result
             FOR EACH ROW EXECUTE FUNCTION sample_copy_progress_trigger();",
        )
        .unwrap();

        let copy_from_command =
            format!("COPY test_result FROM '{LOCAL_TEST_FILE_PATH}' WITH (format parquet)");
        Spi::run(&copy_from_command).unwrap();

        assert_eq!(total_rows, sample_count());

        assert_eq!(vec!["COPY FROM".to_string()], distinct_sampled("command"));
        assert_eq!(vec!["CALLBACK".to_string()], distinct_sampled("copy_type"));

        // the binary copy stream of a single int4 column is a 19 byte header, a 2 byte
        // trailer and 10 bytes per row (2 byte attribute count, 4 byte length, 4 byte value).
        // The trailer is only appended after the last row is consumed by PG, so the samples
        // see the total without it.
        let expected_bytes_total = 19 + total_rows * 10;
        assert_eq!(
            vec![expected_bytes_total.to_string()],
            distinct_sampled("bytes_total")
        );

        assert!(max_sampled("bytes_processed") <= expected_bytes_total);
        assert_sampled_bytes_never_decrease();

        assert_copy_progress_ended();
    }

    #[pg_test]
    fn test_copy_to_program_progress() {
        let _file_cleanup = FileCleanup::new(LOCAL_TEST_FILE_PATH);

        create_copy_progress_sampler();

        let copy_to_command = format!(
            "COPY (SELECT sample_copy_progress_at(i, 1) FROM generate_series(1, 10) i)
             TO PROGRAM 'cat > {LOCAL_TEST_FILE_PATH}' WITH (format parquet)"
        );
        Spi::run(&copy_to_command).unwrap();

        assert_eq!(vec!["COPY TO".to_string()], distinct_sampled("command"));
        assert_eq!(vec!["PROGRAM".to_string()], distinct_sampled("copy_type"));

        assert_copy_progress_ended();
    }
}
