#[pgrx::pg_schema]
mod tests {
    use std::fs::File;
    use std::process::{Child, Command, Stdio};
    use std::time::Instant;

    use pgrx::{pg_test, Spi};

    use crate::pgrx_tests::common::{psql, test_pg_port, FileCleanup, COPY_START_TIMEOUT};

    // the tests of this module keep to their own paths rather than the file path the rest of
    // the suite shares, because a parallel worker reads the file while the command runs and
    // another test removing it in between turns into a confusing failure
    const LOCAL_TEST_FILE_PATH: &str = "/tmp/pg_parquet_test_parallel_from.parquet";

    const WIDE_TABLE_ROWS: i64 = 100_000;

    // pg_parquet only splits a COPY FROM between the row groups of the file, so the test file
    // has to hold more than one of them
    const ROW_GROUP_SIZE: i64 = 10_000;

    // the function a test holds a slot of the worker pool with, and how long it holds it
    // for, which only has to outlast the copy running next to it
    const SLOT_HOLDER_FUNCTION: &str = "slot_holder_sleep";
    const SLOT_HOLD_SECONDS: i64 = 60;

    // the parameter that puts a query under a parallel worker whatever the cost estimates
    // say, which was renamed in pg16
    #[cfg(pre_pg16)]
    const FORCE_PARALLEL_PLAN: &str = "force_parallel_mode";
    #[cfg(not(pre_pg16))]
    const FORCE_PARALLEL_PLAN: &str = "debug_parallel_query";

    const WIDE_TABLE_COLUMNS: &str = "(
         id bigint,
         name text,
         amount numeric(12,4),
         created_at timestamp,
         flag bool,
         payload bytea,
         tags int[],
         doc jsonb
     )";

    fn create_wide_table(name: &str) {
        Spi::run(&format!("CREATE TABLE {name} {WIDE_TABLE_COLUMNS};")).unwrap();
    }

    // write_wide_file writes the file that the COPY FROM tests read, and returns the number of
    // row groups it holds
    fn write_wide_file() -> i64 {
        write_wide_file_at(LOCAL_TEST_FILE_PATH)
    }

    fn write_wide_file_at(uri: &str) -> i64 {
        write_wide_file_rows_at(uri, WIDE_TABLE_ROWS)
    }

    fn write_wide_file_rows_at(uri: &str, rows: i64) -> i64 {
        create_wide_table("wide_table");
        insert_wide_rows("wide_table", rows);

        Spi::run(&format!(
            "COPY wide_table TO '{uri}'
             WITH (format parquet, row_group_size {ROW_GROUP_SIZE});"
        ))
        .unwrap();

        let num_row_groups = Spi::get_one::<i64>(&format!(
            "SELECT count(*) FROM parquet.metadata('{uri}')
             WHERE column_id = 0"
        ))
        .unwrap()
        .unwrap();

        assert!(
            num_row_groups > 1,
            "the test file holds {num_row_groups} row groups, which is too few to be copied \
             in parallel"
        );

        num_row_groups
    }

    fn insert_wide_rows(table: &str, rows: i64) {
        Spi::run(&format!(
            "INSERT INTO {table}
             SELECT g,
                    CASE WHEN g % 10 = 0 THEN NULL ELSE 'name-' || g || repeat('x', g % 50) END,
                    CASE WHEN g % 11 = 0 THEN NULL ELSE (g % 100000)::numeric / 10000 END,
                    CASE WHEN g % 13 = 0 THEN NULL
                         ELSE timestamp '2024-01-01' + (g || ' seconds')::interval END,
                    CASE WHEN g % 7 = 0 THEN NULL ELSE g % 2 = 0 END,
                    CASE WHEN g % 17 = 0 THEN NULL ELSE decode(lpad(to_hex(g), 8, '0'), 'hex') END,
                    CASE WHEN g % 19 = 0 THEN NULL ELSE ARRAY[g, g + 1, g + 2] END,
                    CASE WHEN g % 23 = 0 THEN NULL ELSE jsonb_build_object('k', g) END
             FROM generate_series(1, {rows}) g;"
        ))
        .unwrap();
    }

    fn count(relation: &str) -> i64 {
        Spi::get_one::<i64>(&format!("SELECT count(*) FROM {relation}"))
            .unwrap()
            .unwrap()
    }

    // rows_not_in is the number of rows of the first relation that the second one does not
    // have, and the other way round. Both have to be 0 for the two to hold exactly the same
    // rows.
    fn rows_not_in(left: &str, right: &str) -> (i64, i64) {
        let missing = Spi::get_one::<i64>(&format!(
            "SELECT count(*) FROM (SELECT * FROM {left} EXCEPT ALL SELECT * FROM {right}) q"
        ))
        .unwrap()
        .unwrap();

        let extra = Spi::get_one::<i64>(&format!(
            "SELECT count(*) FROM (SELECT * FROM {right} EXCEPT ALL SELECT * FROM {left}) q"
        ))
        .unwrap()
        .unwrap();

        (missing, extra)
    }

    // rows_in_different_order is how many rows the two relations disagree on when both are
    // read in the order they were written in. It is 0 when the two copies inserted the rows
    // of the same files in the same order.
    fn rows_in_different_order(left: &str, right: &str) -> i64 {
        Spi::get_one::<i64>(&format!(
            "SELECT count(*) FROM
                 (SELECT row_number() OVER (ORDER BY ctid) rn, id FROM {left}) l
                 JOIN
                 (SELECT row_number() OVER (ORDER BY ctid) rn, id FROM {right}) r
                 USING (rn)
             WHERE l.id IS DISTINCT FROM r.id"
        ))
        .unwrap()
        .unwrap()
    }

    #[pg_test]
    fn test_parallel_copy_from_matches_sequential() {
        let _file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

        write_wide_file();

        create_wide_table("sequential_table");
        Spi::run("SET pg_parquet.max_parallel_workers TO 0;").unwrap();
        Spi::run(&format!(
            "COPY sequential_table FROM '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();

        create_wide_table("parallel_table");
        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();
        Spi::run(&format!(
            "COPY parallel_table FROM '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();

        assert_eq!(count("parallel_table"), WIDE_TABLE_ROWS);
        assert_eq!(rows_not_in("sequential_table", "parallel_table"), (0, 0));

        // the rows of the file are inserted in the order the file holds them, the same order
        // the single process path inserts them in, even though several processes decode them
        assert_eq!(
            rows_in_different_order("sequential_table", "parallel_table"),
            0
        );
    }

    // a uri with a wildcard stands for several files, whose row groups are one list that the
    // workers split between themselves
    #[pg_test]
    fn test_parallel_copy_from_several_files() {
        let folder = "/tmp/pg_parquet_test_parallel_from_files";
        let _folder = FileCleanup::new(folder);

        create_wide_table("wide_table");
        insert_wide_rows("wide_table", WIDE_TABLE_ROWS);

        // the files hold different numbers of row groups, so the row groups a worker owns are
        // spread over the files rather than lining up with their boundaries
        for (name, first, last) in [
            ("a_file", 1, 25_000),
            ("b_file", 25_001, 60_000),
            ("c_file", 60_001, WIDE_TABLE_ROWS),
        ] {
            Spi::run(&format!(
                "COPY (SELECT * FROM wide_table WHERE id BETWEEN {first} AND {last} ORDER BY id)
                 TO '{folder}/{name}.parquet'
                 WITH (format parquet, row_group_size {ROW_GROUP_SIZE});"
            ))
            .unwrap();
        }

        create_wide_table("sequential_files_table");
        Spi::run("SET pg_parquet.max_parallel_workers TO 0;").unwrap();
        Spi::run(&format!(
            "COPY sequential_files_table FROM '{folder}/*.parquet' WITH (format parquet);"
        ))
        .unwrap();

        create_wide_table("parallel_files_table");
        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();
        Spi::run(&format!(
            "COPY parallel_files_table FROM '{folder}/*.parquet' WITH (format parquet);"
        ))
        .unwrap();

        assert_eq!(count("parallel_files_table"), WIDE_TABLE_ROWS);
        assert_eq!(rows_not_in("wide_table", "parallel_files_table"), (0, 0));

        // the rows arrive in the order of the row groups of the files, taking the files in
        // the order the single process path takes them, which is the order they are listed in
        assert_eq!(
            rows_in_different_order("sequential_files_table", "parallel_files_table"),
            0
        );
    }

    #[pg_test]
    fn test_parallel_copy_from_with_attribute_list() {
        let _file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

        write_wide_file();

        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();

        Spi::run("CREATE TABLE subset_table (id bigint, amount numeric(12,4), doc jsonb);")
            .unwrap();
        Spi::run(&format!(
            "COPY subset_table (id, amount, doc) FROM '{LOCAL_TEST_FILE_PATH}'
             WITH (format parquet, match_by 'name');"
        ))
        .unwrap();

        assert_eq!(count("subset_table"), WIDE_TABLE_ROWS);

        let diff = Spi::get_one::<i64>(
            "SELECT count(*) FROM (
                 SELECT id, amount, doc FROM wide_table
                 EXCEPT ALL
                 SELECT id, amount, doc FROM subset_table
             ) q",
        )
        .unwrap()
        .unwrap();
        assert_eq!(diff, 0);
    }

    // the WHERE clause of the command filters the rows the parallel workers decode, the same
    // way it filters the ones the single process path decodes
    #[pg_test]
    fn test_parallel_copy_from_with_where_clause() {
        let _file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

        write_wide_file();

        create_wide_table("filtered_table");
        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();
        Spi::run(&format!(
            "COPY filtered_table FROM '{LOCAL_TEST_FILE_PATH}'
             WITH (format parquet) WHERE id % 100 = 0;"
        ))
        .unwrap();

        assert_eq!(count("filtered_table"), WIDE_TABLE_ROWS / 100);
        assert_eq!(
            rows_not_in(
                "filtered_table",
                "(SELECT * FROM wide_table WHERE id % 100 = 0) w"
            ),
            (0, 0)
        );
    }

    // a COPY FROM that the parallel path cannot handle still has to insert every row, through
    // the single process path
    #[pg_test]
    fn test_parallel_copy_from_fallbacks() {
        let _file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

        write_wide_file();

        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();

        // a file with a single row group, which there is nothing to split
        {
            let single_row_group_file =
                "/tmp/pg_parquet_test_parallel_from_single_row_group.parquet";
            let _file = FileCleanup::new(single_row_group_file);

            Spi::run("CREATE TABLE narrow_table AS SELECT g id FROM generate_series(1, 100) g;")
                .unwrap();
            Spi::run(&format!(
                "COPY narrow_table TO '{single_row_group_file}' WITH (format parquet);"
            ))
            .unwrap();

            Spi::run("TRUNCATE narrow_table;").unwrap();
            Spi::run(&format!(
                "COPY narrow_table FROM '{single_row_group_file}' WITH (format parquet);"
            ))
            .unwrap();

            assert_eq!(count("narrow_table"), 100);
        }

        // a temporary relation, which a parallel worker cannot see
        {
            create_wide_table("temp_holder");
            Spi::run("CREATE TEMP TABLE temp_table (LIKE temp_holder);").unwrap();
            Spi::run(&format!(
                "COPY temp_table FROM '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
            ))
            .unwrap();

            assert_eq!(count("temp_table"), WIDE_TABLE_ROWS);
        }

        // a relation with a row trigger, which has to see every row it is inserted
        {
            create_wide_table("trigger_table");
            Spi::run(
                "CREATE FUNCTION count_rows() RETURNS trigger AS $$
                 BEGIN
                     INSERT INTO trigger_counter VALUES (1);
                     RETURN NEW;
                 END
                 $$ LANGUAGE plpgsql;",
            )
            .unwrap();
            Spi::run("CREATE TABLE trigger_counter (n int);").unwrap();
            Spi::run(
                "CREATE TRIGGER count_rows BEFORE INSERT ON trigger_table
                 FOR EACH ROW EXECUTE FUNCTION count_rows();",
            )
            .unwrap();

            Spi::run(&format!(
                "COPY trigger_table FROM '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
            ))
            .unwrap();

            assert_eq!(count("trigger_table"), WIDE_TABLE_ROWS);
            assert_eq!(count("trigger_counter"), WIDE_TABLE_ROWS);
        }

        // a relation with a default expression for an attribute the command does not name,
        // which only the process that holds the command can evaluate
        {
            create_wide_table("default_table");
            Spi::run("ALTER TABLE default_table ADD COLUMN extra int DEFAULT 7;").unwrap();

            Spi::run(&format!(
                "COPY default_table (id, name, amount, created_at, flag, payload, tags, doc)
                 FROM '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
            ))
            .unwrap();

            assert_eq!(count("default_table"), WIDE_TABLE_ROWS);
            assert_eq!(count("default_table WHERE extra = 7"), WIDE_TABLE_ROWS);
        }

        // a CHECK constraint whose function runs statements of its own, which the process that
        // holds the command evaluates for every row it writes and cannot run in parallel mode
        {
            Spi::run(
                "CREATE FUNCTION volatile_positive(n bigint) RETURNS bool
                 LANGUAGE plpgsql VOLATILE AS $$
                 BEGIN
                     PERFORM 1;
                     RETURN n > 0;
                 END
                 $$;",
            )
            .unwrap();

            create_wide_table("check_table");
            Spi::run("ALTER TABLE check_table ADD CHECK (volatile_positive(id));").unwrap();

            Spi::run(&format!(
                "COPY check_table FROM '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
            ))
            .unwrap();

            assert_eq!(count("check_table"), WIDE_TABLE_ROWS);
        }

        // the same constraint added NOT VALID, which the rows already in the relation were not
        // checked against but the rows of the copy still are
        {
            create_wide_table("not_valid_check_table");
            Spi::run(
                "ALTER TABLE not_valid_check_table ADD CHECK (volatile_positive(id)) NOT VALID;",
            )
            .unwrap();

            Spi::run(&format!(
                "COPY not_valid_check_table FROM '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
            ))
            .unwrap();

            assert_eq!(count("not_valid_check_table"), WIDE_TABLE_ROWS);
        }

        // an attribute whose type is a domain carrying such a constraint, which is evaluated
        // the same way but is reached through the type rather than through the relation
        {
            Spi::run("CREATE DOMAIN positive_id AS bigint CHECK (volatile_positive(VALUE));")
                .unwrap();
            Spi::run("CREATE TABLE domain_table (id positive_id, name text);").unwrap();

            Spi::run(&format!(
                "COPY domain_table (id, name) FROM '{LOCAL_TEST_FILE_PATH}'
                 WITH (format parquet, match_by 'name');"
            ))
            .unwrap();

            assert_eq!(count("domain_table"), WIDE_TABLE_ROWS);
        }

        // a WHERE clause calling such a function, which the process that holds the command
        // evaluates for every row the workers hand it
        {
            create_wide_table("volatile_where_table");

            Spi::run(&format!(
                "COPY volatile_where_table FROM '{LOCAL_TEST_FILE_PATH}'
                 WITH (format parquet) WHERE volatile_positive(id);"
            ))
            .unwrap();

            assert_eq!(count("volatile_where_table"), WIDE_TABLE_ROWS);
        }

        // a WHERE clause taking a value from a sequence, which parallel mode refuses outright
        // rather than through the command counter
        {
            create_wide_table("nextval_where_table");
            Spi::run("CREATE SEQUENCE where_sequence;").unwrap();

            Spi::run(&format!(
                "COPY nextval_where_table FROM '{LOCAL_TEST_FILE_PATH}'
                 WITH (format parquet) WHERE nextval('where_sequence') > 0;"
            ))
            .unwrap();

            assert_eq!(count("nextval_where_table"), WIDE_TABLE_ROWS);
        }

        // a uri with a wildcard that matches no file at all, which is an error of the single
        // process path to report
        {
            create_wide_table("no_match_table");

            Spi::run(
                "CREATE FUNCTION copy_from_no_match() RETURNS text LANGUAGE plpgsql AS $$
                 BEGIN
                     EXECUTE 'COPY no_match_table
                              FROM ''/tmp/pg_parquet_test_parallel_no_match/*.parquet''
                              WITH (format parquet)';

                     RETURN 'no error';
                 EXCEPTION WHEN others THEN
                     RETURN SQLERRM;
                 END;
                 $$;",
            )
            .unwrap();

            let error = Spi::get_one::<String>("SELECT copy_from_no_match()")
                .unwrap()
                .unwrap();

            assert!(
                error.contains("no files found that match the pattern"),
                "the copy reported \"{error}\" instead"
            );
        }
    }

    // a constraint and a filter that the process holding the command can evaluate in parallel
    // mode keep the copy on the parallel path, rather than cost it the parallel path for
    // carrying a constraint or a filter at all. The copy runs in a session of its own, because
    // the workers of a copy are only there while it is still running and a session cannot look
    // at a copy it is itself waiting for.
    #[pg_test]
    fn test_parallel_copy_from_with_ordinary_constraint_and_filter() {
        let _file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

        write_wide_file();

        let port = test_pg_port();
        let table = "ordinary_constraint_table";

        // the table is created in a session of its own, so that the session that copies into it
        // can see it, and is dropped the same way at the end. Its constraint calls a stable
        // function, which plpgsql runs the statements of read only, so it asks for none of the
        // command counter increments parallel mode forbids.
        psql(
            &port,
            &[&format!(
                "DROP TABLE IF EXISTS {table};
                 CREATE TABLE {table} {WIDE_TABLE_COLUMNS};
                 CREATE OR REPLACE FUNCTION stable_positive(n bigint) RETURNS bool
                     LANGUAGE plpgsql STABLE AS 'BEGIN PERFORM 1; RETURN n > 0; END';
                 ALTER TABLE {table} ADD CHECK (stable_positive(id));"
            )],
        );

        let filter = "id % 2 = 0";

        let copy = copy_from_in_another_session(table, LOCAL_TEST_FILE_PATH, 4, filter);
        let (_, nworkers) = backend_with_workers(&format!("COPY {table} FROM"));

        let copy = copy.wait_with_output().expect("failed to wait for psql");

        assert!(
            copy.status.success(),
            "the copy did not succeed: {}",
            String::from_utf8_lossy(&copy.stderr)
        );

        assert!(
            nworkers > 0,
            "the copy ran with no parallel worker of its own, so the constraint or the filter \
             took it off the parallel path"
        );

        // the rows are read in the session that drops the table, rather than in this one, because
        // a transaction that read it would still be holding it when the drop asked for it
        let copied = psql(
            &port,
            &[
                &format!("SELECT count(*) FROM {table};"),
                &format!("DROP TABLE {table}; DROP FUNCTION stable_positive(bigint);"),
            ],
        );

        assert_eq!(
            copied.lines().next(),
            Some((WIDE_TABLE_ROWS / 2).to_string().as_str())
        );
    }

    // the leader decodes every row group itself when no parallel worker can be launched
    #[pg_test]
    fn test_parallel_copy_from_without_workers() {
        let _file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

        write_wide_file();

        create_wide_table("no_worker_table");

        Spi::run("SET max_parallel_workers TO 0;").unwrap();
        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();

        Spi::run(&format!(
            "COPY no_worker_table FROM '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();

        assert_eq!(count("no_worker_table"), WIDE_TABLE_ROWS);
        assert_eq!(rows_not_in("wide_table", "no_worker_table"), (0, 0));
    }

    // a request above the server's own max_parallel_workers is capped to it, so that the
    // command asks for no more workers than the server would ever hand it
    #[pg_test]
    fn test_parallel_copy_from_above_server_max_parallel_workers() {
        let _file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

        write_wide_file();

        create_wide_table("capped_worker_table");

        Spi::run("SET max_parallel_workers TO 1;").unwrap();
        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();

        Spi::run(&format!(
            "COPY capped_worker_table FROM '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();

        assert_eq!(count("capped_worker_table"), WIDE_TABLE_ROWS);
        assert_eq!(rows_not_in("wide_table", "capped_worker_table"), (0, 0));
    }

    // a COPY FROM STDIN hands its rows over on the connection to the client, which no
    // worker can read a row group of its own out of, so it falls back to the single process
    // path. It is driven from a psql session, because Spi has no stdin to hand rows on.
    #[pg_test]
    fn test_parallel_copy_from_stdin_source() {
        let _file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

        write_wide_file();

        let port = test_pg_port();

        // the table is created in a session of its own, so that the session that copies into
        // it can see it, and is dropped the same way at the end
        psql(
            &port,
            &[&format!(
                "DROP TABLE IF EXISTS stdin_table; CREATE TABLE stdin_table {WIDE_TABLE_COLUMNS};"
            )],
        );

        let file = File::open(LOCAL_TEST_FILE_PATH).expect("failed to open the test file");

        let output = Command::new("psql")
            .args(["-h", "localhost", "-p", &port, "-d", "pgrx_tests"])
            .args(["-c", "SET pg_parquet.max_parallel_workers TO 4;"])
            .args(["-c", "COPY stdin_table FROM STDIN WITH (format parquet);"])
            .stdin(Stdio::from(file))
            .output()
            .expect("failed to run psql");
        assert!(
            output.status.success(),
            "the COPY FROM STDIN did not succeed: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let copied = psql(&port, &["SELECT count(*) FROM stdin_table"]);
        psql(&port, &["DROP TABLE stdin_table"]);

        assert_eq!(copied.trim(), WIDE_TABLE_ROWS.to_string());
    }

    // the whole worker pool is a request the server cannot meet in full, since a background
    // worker of its own holds one of the slots the pool is handed out of, and the rows a
    // command short of a worker reads are the rows of the file all the same
    #[pg_test]
    fn test_parallel_copy_from_whole_worker_pool() {
        let _file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

        write_wide_file();

        create_wide_table("whole_pool_table");

        let pool = Spi::get_one::<String>("SELECT current_setting('max_worker_processes')")
            .unwrap()
            .unwrap();

        Spi::run(&format!("SET max_parallel_workers TO {pool};")).unwrap();
        Spi::run(&format!("SET pg_parquet.max_parallel_workers TO {pool};")).unwrap();

        Spi::run(&format!(
            "COPY whole_pool_table FROM '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();

        assert_eq!(count("whole_pool_table"), WIDE_TABLE_ROWS);
        assert_eq!(rows_not_in("wide_table", "whole_pool_table"), (0, 0));
        assert_eq!(rows_in_different_order("wide_table", "whole_pool_table"), 0);
    }

    // a command given fewer workers than it asked for reads the file with the ones it was
    // given, rather than leave them idle and read all of it in the leader. The copy runs in
    // a session of its own, because the workers of a copy are only there while it is still
    // running and a session cannot look at a copy it is itself waiting for.
    #[pg_test]
    fn test_parallel_copy_from_fewer_workers_than_requested() {
        let _file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

        write_wide_file();

        let port = test_pg_port();
        let table = "fewer_workers_table";
        let sequential_table = "fewer_workers_sequential_table";

        // the tables are created in a session of its own, so that the sessions that copy into
        // them can see them, and are dropped the same way at the end. The second one is what
        // the copy short of workers is held against: it is read by the single process path.
        psql(
            &port,
            &[&format!(
                "DROP TABLE IF EXISTS {table};
                 DROP TABLE IF EXISTS {sequential_table};
                 CREATE TABLE {table} {WIDE_TABLE_COLUMNS};
                 CREATE TABLE {sequential_table} {WIDE_TABLE_COLUMNS};"
            )],
        );

        psql(
            &port,
            &[
                "SET pg_parquet.max_parallel_workers TO 0;",
                &format!(
                    "COPY {sequential_table} FROM '{LOCAL_TEST_FILE_PATH}' \
                     WITH (format parquet);"
                ),
            ],
        );

        let pool = Spi::get_one::<i32>("SELECT current_setting('max_worker_processes')::int")
            .unwrap()
            .unwrap();

        // with a slot of the pool held elsewhere, a copy that asks for the whole of it is
        // short of workers whatever else the server is up to
        let (holder, holder_pid) = hold_a_worker_slot();

        let copy = copy_from_in_another_session(table, LOCAL_TEST_FILE_PATH, pool, "");
        let (_, nworkers) = backend_with_workers(&format!("COPY {table} FROM"));

        let copy = copy.wait_with_output().expect("failed to wait for psql");
        release_worker_slot(holder, holder_pid);

        assert!(
            copy.status.success(),
            "the copy did not succeed: {}",
            String::from_utf8_lossy(&copy.stderr)
        );

        assert!(
            nworkers < pool as i64,
            "the copy ran with all {pool} workers of the pool, which leaves the shortfall \
             this test is about untested"
        );

        // how many rows the copy read, how many of them the single process path did not read
        // and the other way round, and how many of them the two paths read in a different
        // place of the relation. The tables are read in the session that drops them, rather
        // than in this one, because a transaction that read them would still be holding them
        // when the drop asked for them.
        let copied = psql(
            &port,
            &[
                &format!(
                    "SELECT (SELECT count(*) FROM {table}),
                            (SELECT count(*) FROM (SELECT * FROM {table}
                                                   EXCEPT ALL
                                                   SELECT * FROM {sequential_table}) q),
                            (SELECT count(*) FROM (SELECT * FROM {sequential_table}
                                                   EXCEPT ALL
                                                   SELECT * FROM {table}) q),
                            (SELECT count(*) FROM
                                 (SELECT row_number() OVER (ORDER BY ctid) rn, id
                                  FROM {table}) l
                                 JOIN
                                 (SELECT row_number() OVER (ORDER BY ctid) rn, id
                                  FROM {sequential_table}) r
                                 USING (rn)
                             WHERE l.id IS DISTINCT FROM r.id);"
                ),
                &format!("DROP TABLE {table}; DROP TABLE {sequential_table};"),
            ],
        );

        assert_eq!(
            copied.lines().next(),
            Some(format!("{WIDE_TABLE_ROWS}|0|0|0").as_str())
        );
    }

    // copy_from_in_another_session starts a parallel COPY FROM into the given table in a
    // session of its own, asking for the given number of workers and filtering the rows it
    // reads by the given clause, which is left out when it is empty
    fn copy_from_in_another_session(table: &str, uri: &str, nworkers: i32, filter: &str) -> Child {
        let port = test_pg_port();

        let filter = if filter.is_empty() {
            String::new()
        } else {
            format!(" WHERE {filter}")
        };

        Command::new("psql")
            .args(["-h", "localhost", "-p", &port, "-d", "pgrx_tests", "-At"])
            .args([
                "-c",
                &format!(
                    "SET max_parallel_workers TO {nworkers};
                     SET pg_parquet.max_parallel_workers TO {nworkers};"
                ),
            ])
            .args([
                "-c",
                &format!("COPY {table} FROM '{uri}' WITH (format parquet){filter};"),
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to run psql")
    }

    // hold_a_worker_slot takes a slot of the worker pool in a session of its own and keeps it
    // until the slot is released again, so that a command asking for the whole pool meanwhile
    // cannot be given all of it. The slot is held by a sleeping function that the server is
    // told to run under a parallel worker rather than plan on its own, since a plan chosen on
    // cost estimates is a plan a server version may choose differently. The pid the session
    // is leading the query with is what releases the slot.
    fn hold_a_worker_slot() -> (Child, i32) {
        let port = test_pg_port();

        let holder = Command::new("psql")
            .args(["-h", "localhost", "-p", &port, "-d", "pgrx_tests", "-At"])
            .args([
                "-c",
                &format!(
                    "CREATE OR REPLACE FUNCTION {SLOT_HOLDER_FUNCTION}() RETURNS int
                     LANGUAGE plpgsql PARALLEL SAFE AS 'BEGIN
                         PERFORM pg_sleep({SLOT_HOLD_SECONDS}); RETURN 1;
                     END';"
                ),
            ])
            .args([
                "-c",
                &format!(
                    "SET max_parallel_workers_per_gather TO 1;
                     SET {FORCE_PARALLEL_PLAN} TO on;"
                ),
            ])
            .args(["-c", &format!("SELECT {SLOT_HOLDER_FUNCTION}();")])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to run psql");

        let (holder_pid, _) = backend_with_workers(&format!("SELECT {SLOT_HOLDER_FUNCTION}"));

        (holder, holder_pid)
    }

    // release_worker_slot ends the query holding the slot, which the session it is running in
    // is told to stop rather than left to sleep its way out of, and takes the table and the
    // function it was holding it with away again
    fn release_worker_slot(holder: Child, holder_pid: i32) {
        Spi::run(&format!("SELECT pg_terminate_backend({holder_pid});")).unwrap();

        holder.wait_with_output().expect("failed to wait for psql");

        let port = test_pg_port();
        psql(
            &port,
            &[&format!(
                "DROP FUNCTION IF EXISTS {SLOT_HOLDER_FUNCTION}();"
            )],
        );
    }

    // backend_with_workers waits until a backend other than this one is running a statement
    // that starts with the given text with parallel workers of its own, and returns its pid
    // together with how many workers it is running with
    fn backend_with_workers(statement: &str) -> (i32, i64) {
        let deadline = Instant::now() + COPY_START_TIMEOUT;

        loop {
            // the state of the other backends is read once per transaction and then cached,
            // and a test runs in a single transaction, so every look drops the cache first
            Spi::run("SELECT pg_stat_clear_snapshot();").unwrap();

            let (leader_pid, nworkers) = Spi::get_two::<i32, i64>(&format!(
                "SELECT coalesce(max(leader.pid), 0), count(worker.pid)
                 FROM pg_stat_activity leader
                 JOIN pg_stat_activity worker ON worker.leader_pid = leader.pid
                 WHERE leader.pid <> pg_backend_pid()
                   AND leader.query LIKE '{statement}%'"
            ))
            .unwrap();

            let leader_pid = leader_pid.unwrap_or(0);

            if leader_pid != 0 {
                return (leader_pid, nworkers.unwrap_or(0));
            }

            if Instant::now() >= deadline {
                let running = Spi::get_one::<String>(
                    "SELECT coalesce(string_agg(format('%s(leader %s): %s', pid,
                                                       coalesce(leader_pid::text, '-'),
                                                       left(query, 60)), '; '), 'nothing')
                     FROM pg_stat_activity
                     WHERE pid <> pg_backend_pid() AND query <> ''",
                )
                .unwrap()
                .unwrap_or_default();

                panic!(
                    "no \"{statement}\" was running with parallel workers after {} seconds, \
                     the other backends were running {running}",
                    COPY_START_TIMEOUT.as_secs()
                );
            }

            Spi::run("SELECT pg_sleep(0.01);").unwrap();
        }
    }

    // an error a parallel worker raises has to reach the client rather than leave the leader
    // waiting for rows that are never coming
    #[pg_test]
    #[should_panic(expected = "column count mismatch between table and parquet file")]
    fn test_parallel_copy_from_worker_error() {
        let _file = FileCleanup::new(LOCAL_TEST_FILE_PATH);

        write_wide_file();

        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();
        Spi::run("CREATE TABLE mismatched_table (a text, b text);").unwrap();

        Spi::run(&format!(
            "COPY mismatched_table FROM '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();
    }

    // a worker of a parallel COPY FROM an object store uri reads its own row groups out of
    // the uri itself, so this covers that a worker reaches the object store at all, and that
    // the rows several workers read out of one object still land in the file's own order
    #[pg_test]
    fn test_s3_parallel_copy_from_matches_sequential() {
        let test_bucket_name =
            std::env::var("AWS_S3_TEST_BUCKET").expect("AWS_S3_TEST_BUCKET not found");

        let s3_uri = format!("s3://{test_bucket_name}/parallel_copy_from/wide.parquet");

        write_wide_file_at(&s3_uri);

        create_wide_table("sequential_table");
        Spi::run("SET pg_parquet.max_parallel_workers TO 0;").unwrap();
        Spi::run(&format!(
            "COPY sequential_table FROM '{s3_uri}' WITH (format parquet);"
        ))
        .unwrap();

        create_wide_table("parallel_table");
        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();
        Spi::run(&format!(
            "COPY parallel_table FROM '{s3_uri}' WITH (format parquet);"
        ))
        .unwrap();

        assert_eq!(count("parallel_table"), WIDE_TABLE_ROWS);
        assert_eq!(rows_not_in("sequential_table", "parallel_table"), (0, 0));

        let out_of_order = Spi::get_one::<i64>(
            "SELECT count(*) FROM
                 (SELECT row_number() OVER (ORDER BY ctid) rn, id FROM sequential_table) s
                 JOIN
                 (SELECT row_number() OVER (ORDER BY ctid) rn, id FROM parallel_table) p
                 USING (rn)
             WHERE s.id IS DISTINCT FROM p.id",
        )
        .unwrap()
        .unwrap();
        assert_eq!(out_of_order, 0);
    }

    // a worker reads its own row groups out of the uri itself, with an object store of its
    // own, so every store a uri can name has to be reachable from a worker rather than only
    // from the backend that resolved the credentials of the command
    #[pg_test]
    #[cfg(not(rhel8))]
    fn test_azure_parallel_copy_from_matches_sequential() {
        let test_container_name = std::env::var("AZURE_TEST_CONTAINER_NAME")
            .expect("AZURE_TEST_CONTAINER_NAME not found");

        let azure_uri = format!("az://{test_container_name}/parallel_copy_from/wide.parquet");

        write_wide_file_at(&azure_uri);

        create_wide_table("sequential_table");
        Spi::run("SET pg_parquet.max_parallel_workers TO 0;").unwrap();
        Spi::run(&format!(
            "COPY sequential_table FROM '{azure_uri}' WITH (format parquet);"
        ))
        .unwrap();

        create_wide_table("parallel_table");
        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();
        Spi::run(&format!(
            "COPY parallel_table FROM '{azure_uri}' WITH (format parquet);"
        ))
        .unwrap();

        assert_eq!(count("parallel_table"), WIDE_TABLE_ROWS);
        assert_eq!(rows_not_in("sequential_table", "parallel_table"), (0, 0));

        // the rows of a row group stay together and in order, whichever worker read them
        let out_of_order = Spi::get_one::<i64>(
            "SELECT count(*) FROM
                 (SELECT row_number() OVER (ORDER BY ctid) rn, id FROM sequential_table) s
                 JOIN
                 (SELECT row_number() OVER (ORDER BY ctid) rn, id FROM parallel_table) p
                 USING (rn)
             WHERE s.id IS DISTINCT FROM p.id",
        )
        .unwrap()
        .unwrap();
        assert_eq!(out_of_order, 0);
    }

    // an http(s) endpoint serves a range of a file the same way an object store does, which
    // is all a worker needs to read its own row groups out of the uri itself. The file is
    // kept smaller than the rest of the module's because object_store writes an http
    // destination with a single put rather than with a multipart upload.
    #[pg_test]
    fn test_http_parallel_copy_from_matches_sequential() {
        let http_endpoint = std::env::var("HTTP_ENDPOINT").expect("HTTP_ENDPOINT not found");

        let http_uri = format!("{http_endpoint}/parallel_copy_from_wide.parquet");

        let rows = ROW_GROUP_SIZE * 3;
        write_wide_file_rows_at(&http_uri, rows);

        create_wide_table("sequential_table");
        Spi::run("SET pg_parquet.max_parallel_workers TO 0;").unwrap();
        Spi::run(&format!(
            "COPY sequential_table FROM '{http_uri}' WITH (format parquet);"
        ))
        .unwrap();

        create_wide_table("parallel_table");
        Spi::run("SET pg_parquet.max_parallel_workers TO 4;").unwrap();
        Spi::run(&format!(
            "COPY parallel_table FROM '{http_uri}' WITH (format parquet);"
        ))
        .unwrap();

        assert_eq!(count("parallel_table"), rows);
        assert_eq!(rows_not_in("sequential_table", "parallel_table"), (0, 0));

        let out_of_order = Spi::get_one::<i64>(
            "SELECT count(*) FROM
                 (SELECT row_number() OVER (ORDER BY ctid) rn, id FROM sequential_table) s
                 JOIN
                 (SELECT row_number() OVER (ORDER BY ctid) rn, id FROM parallel_table) p
                 USING (rn)
             WHERE s.id IS DISTINCT FROM p.id",
        )
        .unwrap()
        .unwrap();
        assert_eq!(out_of_order, 0);
    }
}
