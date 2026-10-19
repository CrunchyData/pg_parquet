//! Where a `COPY` of this format can read from and write to.
//!
//! The endpoints are tier 2's and tier 1's, not this crate's, so what is under test is that
//! naming `format 'arrow'` reaches all of them — the same set `format 'parquet'` reaches — and
//! not that any one of them works. A format that could only be written to a file would be a
//! second-class one, and that is the thing these would catch.

#[pgrx::pg_schema]
mod tests {
    use std::process::{Command, Stdio};

    use pgrx::{pg_test, Spi};

    use crate::pgrx_tests::common::{test_file, test_pg_port, FileCleanup};

    /// Runs a script in a second session, through psql, because `COPY ... TO STDOUT` has no
    /// meaning inside SPI: the destination is the client's connection and SPI is not a client.
    ///
    /// The script goes in on stdin rather than through `-c`, which takes one statement and no
    /// backslash commands.
    fn psql(script: &str) -> String {
        let mut psql = Command::new("psql")
            .args([
                "-p",
                &test_pg_port(),
                "-h",
                "localhost",
                "-d",
                "pgrx_tests",
                "-v",
                "ON_ERROR_STOP=1",
                "-At",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to run psql");

        use std::io::Write;

        psql.stdin
            .take()
            .expect("psql has a stdin")
            .write_all(script.as_bytes())
            .expect("failed to hand psql its script");

        let output = psql.wait_with_output().expect("psql did not finish");

        assert!(
            output.status.success(),
            "psql failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        String::from_utf8_lossy(&output.stdout).into_owned()
    }

    #[pg_test]
    fn test_a_file_is_written_and_read_back() {
        let uri = test_file("endpoint_file.arrow");
        let _cleanup = FileCleanup::new(&uri);

        Spi::run(&format!(
            "CREATE TABLE written AS SELECT i AS a FROM generate_series(1, 3) i;
             COPY written TO '{uri}' WITH (format 'arrow');
             CREATE TABLE came_back (a int);
             COPY came_back FROM '{uri}' WITH (format 'arrow');"
        ))
        .unwrap();

        assert_eq!(
            Spi::get_one::<i64>("SELECT sum(a) FROM came_back").unwrap(),
            Some(6)
        );
    }

    #[pg_test]
    fn test_stdout_and_stdin_are_the_same_file() {
        // the format has to survive a destination that is neither seekable nor re-readable,
        // which for an IPC file means the footer is written last and nothing is rewritten
        let uri = test_file("endpoint_stdout.arrow");
        let _cleanup = FileCleanup::new(&uri);

        psql(&format!(
            "CREATE TABLE stdout_source AS SELECT i AS a, i::text AS b FROM generate_series(1, 3) i;
             \\copy (SELECT * FROM stdout_source) TO '{uri}' WITH (format 'arrow')
             CREATE TABLE stdin_target (a int, b text);
             \\copy stdin_target FROM '{uri}' WITH (format 'arrow')"
        ));

        let rows = psql("SELECT string_agg(a || ':' || b, ',' ORDER BY a) FROM stdin_target");

        assert_eq!(rows.trim(), "1:1,2:2,3:3");
    }

    #[pg_test]
    fn test_a_program_on_both_sides() {
        let uri = test_file("endpoint_program.arrow");
        let _cleanup = FileCleanup::new(&uri);

        // `TO PROGRAM` hands the file to a command's stdin and `FROM PROGRAM` reads it from a
        // command's stdout, so between the two the bytes are nobody's file at all
        psql(&format!(
            "CREATE TABLE program_source AS SELECT i AS a FROM generate_series(1, 3) i;
             COPY program_source TO PROGRAM 'cat > {uri}' WITH (format 'arrow');
             CREATE TABLE program_target (a int);
             COPY program_target FROM PROGRAM 'cat {uri}' WITH (format 'arrow');"
        ));

        let sum = psql("SELECT sum(a) FROM program_target");

        assert_eq!(sum.trim(), "6");
    }

    #[pg_test]
    fn test_a_pattern_reads_every_file_that_matches() {
        let directory = test_file("endpoint_pattern");
        let _cleanup = FileCleanup::new(&directory);

        std::fs::create_dir_all(&directory).expect("failed to make the test directory");

        Spi::run(&format!(
            "CREATE TABLE part AS SELECT 1 AS a;
             COPY part TO '{directory}/one.arrow' WITH (format 'arrow');
             COPY part TO '{directory}/two.arrow' WITH (format 'arrow');
             CREATE TABLE matched (a int);
             COPY matched FROM '{directory}/*.arrow' WITH (format 'arrow');"
        ))
        .unwrap();

        assert_eq!(
            Spi::get_one::<i64>("SELECT count(*) FROM matched").unwrap(),
            Some(2)
        );
    }

    #[pg_test]
    fn test_the_format_is_inferred_from_the_uri() {
        // no `format` option anywhere: the registry infers it from the extension, and in a
        // session where nothing has been named yet that also means loading this library
        for extension in ["arrow", "arrows", "ipc", "feather"] {
            let uri = test_file(&format!("inferred.{extension}"));
            let _cleanup = FileCleanup::new(&uri);

            Spi::run(&format!(
                "DROP TABLE IF EXISTS inferred_source, inferred_target;
                 CREATE TABLE inferred_source AS SELECT 1 AS a;
                 COPY inferred_source TO '{uri}';
                 CREATE TABLE inferred_target (a int);
                 COPY inferred_target FROM '{uri}';"
            ))
            .unwrap_or_else(|e| panic!("a .{extension} uri was not recognised: {e}"));

            assert_eq!(
                Spi::get_one::<i64>("SELECT count(*) FROM inferred_target").unwrap(),
                Some(1),
                "a .{extension} uri did not round trip"
            );
        }
    }

    #[pg_test]
    fn test_a_table_is_defined_from_a_file() {
        let uri = test_file("endpoint_definition.arrow");
        let _cleanup = FileCleanup::new(&uri);

        Spi::run(&format!(
            "CREATE TABLE defined_source AS
               SELECT 1 AS a, 'x'::text AS b, array[1.5::float8] AS c;
             COPY defined_source TO '{uri}' WITH (format 'arrow');
             CREATE TABLE defined () WITH (load_from = '{uri}');"
        ))
        .unwrap();

        // the column types came out of the file's schema, which is this provider's answer to
        // the base's `schema` entry point
        let columns = Spi::get_one::<String>(
            "SELECT string_agg(attname || ' ' || format_type(atttypid, atttypmod), ', '
                               ORDER BY attnum)
               FROM pg_attribute
              WHERE attrelid = 'defined'::regclass AND attnum > 0 AND NOT attisdropped",
        )
        .unwrap();

        assert_eq!(
            columns.as_deref(),
            Some("a integer, b text, c double precision[]")
        );

        // `load_from` also reads the rows, which `definition_from` does not
        assert_eq!(
            Spi::get_one::<i64>("SELECT count(*) FROM defined").unwrap(),
            Some(1)
        );
    }

    #[pg_test]
    #[should_panic(expected = "there is no file at")]
    fn test_a_file_that_is_not_there_is_reported_as_missing() {
        let uri = test_file("endpoint_absent.arrow");

        Spi::run(&format!(
            "CREATE TABLE absent (a int);
             COPY absent FROM '{uri}' WITH (format 'arrow');"
        ))
        .unwrap();
    }
}
