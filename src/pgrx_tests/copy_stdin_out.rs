#[pgrx::pg_schema]
mod tests {
    use std::io::Read;
    use std::io::Write;
    use std::process::Command;
    use std::process::Stdio;

    use pgrx::pg_test;
    use pgrx::Spi;

    use crate::pgrx_tests::common::{test_pg_port, LOCAL_TEST_FILE_PATH};

    #[pg_test]
    fn test_copy_stdin_out() {
        let test_port = test_pg_port();

        // create test_expected
        let output = Command::new("psql")
            .arg("-p")
            .arg(test_port.clone())
            .arg("-h")
            .arg("localhost")
            .arg("-d")
            .arg("pgrx_tests")
            .arg("-c")
            .arg("CREATE TABLE test_expected (a int, b int generated always as (a + 2) stored);")
            .output()
            .expect("failed to execute process");
        assert!(
            output.status.success(),
            "Failed to create test_expected table"
        );

        // create test_result
        let output = Command::new("psql")
            .arg("-p")
            .arg(test_port.clone())
            .arg("-h")
            .arg("localhost")
            .arg("-d")
            .arg("pgrx_tests")
            .arg("-c")
            .arg("CREATE TABLE test_result (a int, b int);")
            .output()
            .expect("failed to execute process");
        assert!(
            output.status.success(),
            "Failed to create test_result table"
        );

        // insert data into test_expected
        let output = Command::new("psql")
            .arg("-p")
            .arg(test_port.clone())
            .arg("-h")
            .arg("localhost")
            .arg("-d")
            .arg("pgrx_tests")
            .arg("-c")
            .arg(
                "INSERT INTO test_expected SELECT i FROM generate_series(1, 3) i;
                  INSERT INTO test_expected VALUES (NULL);",
            )
            .output()
            .expect("failed to execute process");
        assert!(
            output.status.success(),
            "Failed to insert into test_expected table"
        );

        // create a dummy role
        let role = "dummy";
        let output = Command::new("psql")
            .arg("-p")
            .arg(test_port.clone())
            .arg("-h")
            .arg("localhost")
            .arg("-d")
            .arg("pgrx_tests")
            .arg("-c")
            .arg(format!(
                "CREATE ROLE {role} LOGIN;
                 GRANT SELECT, INSERT, UPDATE, DELETE ON test_expected TO {role};
                 GRANT SELECT, INSERT, UPDATE, DELETE ON test_result TO {role};"
            ))
            .output()
            .expect("failed to execute process");
        assert!(output.status.success(), "Failed to create role {role}");

        // copy to stdout with dummy role
        let mut copy_to = Command::new("psql")
            .arg("-p")
            .arg(test_port.clone())
            .arg("-h")
            .arg("localhost")
            .arg("-U")
            .arg(role)
            .arg("-d")
            .arg("pgrx_tests")
            .arg("-c")
            .arg("COPY test_expected TO STDOUT WITH (format parquet);")
            .stdout(Stdio::piped())
            .spawn()
            .expect("failed to execute process");

        let mut buffer = Vec::new();
        {
            let copy_to_stdout = copy_to.stdout.as_mut().expect("Failed to open stdout");
            copy_to_stdout
                .read_to_end(&mut buffer)
                .expect("Failed to read from stdout");

            let status = copy_to.wait().expect("Failed to wait for 'copy_to'");
            assert!(status.success(), "psql COPY TO process did not succeed");
        }

        // copy from stdin with dummy role
        let mut copy_from = Command::new("psql")
            .arg("-p")
            .arg(test_port.clone())
            .arg("-h")
            .arg("localhost")
            .arg("-U")
            .arg(role)
            .arg("-d")
            .arg("pgrx_tests")
            .arg("-c")
            .arg("COPY test_result FROM STDIN WITH (format parquet);")
            .stdin(Stdio::piped())
            .spawn()
            .expect("failed to execute process");

        {
            // Write the data we just read to the new child's stdin
            let copy_from_stdin = copy_from.stdin.as_mut().expect("Failed to open stdin");
            copy_from_stdin
                .write_all(&buffer)
                .expect("Failed to write to stdin");
            copy_from_stdin.flush().expect("Failed to flush stdin");

            let status = copy_from.wait().expect("Failed to wait for 'copy_from'");
            assert!(status.success(), "psql COPY FROM process did not succeed");
        }

        // write to a file (this is needed because Spi::run cannot see external transactions)
        let mut copy_to_file = Command::new("psql")
            .arg("-p")
            .arg(test_port.clone())
            .arg("-h")
            .arg("localhost")
            .arg("-d")
            .arg("pgrx_tests")
            .arg("-c")
            .arg(format!(
                "COPY test_result TO '{LOCAL_TEST_FILE_PATH}' with (format parquet);"
            ))
            .spawn()
            .expect("failed to execute process");

        let status = copy_to_file
            .wait()
            .expect("Failed to wait for 'copy_to_file'");
        assert!(
            status.success(),
            "psql COPY TO FILE process did not succeed"
        );

        // assert table data
        Spi::run("create temp table test_tmp (a int, b int);").unwrap();
        Spi::run(format!("copy test_tmp from '{LOCAL_TEST_FILE_PATH}';").as_str()).unwrap();

        let select_command = "SELECT * FROM test_tmp ORDER BY 1,2;";
        let result = Spi::connect(|client| {
            let mut results = Vec::new();
            let tup_table = client.select(select_command, None, &[]).unwrap();

            for row in tup_table {
                let a = row["a"].value().unwrap();
                let b = row["b"].value().unwrap();
                results.push((a, b));
            }

            results
        });

        assert_eq!(
            result,
            [
                (Some(1), Some(3)),
                (Some(2), Some(4)),
                (Some(3), Some(5)),
                (None, None),
            ]
        );

        // drop tables and role
        let output = Command::new("psql")
            .arg("-p")
            .arg(test_port.clone())
            .arg("-h")
            .arg("localhost")
            .arg("-d")
            .arg("pgrx_tests")
            .arg("-c")
            .arg(format!(
                "DROP TABLE test_expected, test_result; DROP ROLE {role};"
            ))
            .output()
            .expect("failed to execute process");
        assert!(output.status.success(), "Failed to clean up");
    }

    // Regression test for https://github.com/CrunchyData/pg_parquet/issues/175: the temporary
    // file that backs COPY .. TO STDOUT / FROM STDIN must be the one postgres manages, so that
    // it is gone once the backend closes it.
    #[pg_test]
    fn test_copy_stdin_out_does_not_leak_tmp_files() {
        let test_port = test_pg_port();

        let data_dir = Spi::get_one::<String>(
            "SELECT setting FROM pg_settings WHERE name = 'data_directory';",
        )
        .unwrap()
        .expect("data_directory is not set");

        let pgsql_tmp = std::path::Path::new(&data_dir)
            .join("base")
            .join("pgsql_tmp");

        let mut copy_to = Command::new("psql")
            .arg("-p")
            .arg(test_port.clone())
            .arg("-h")
            .arg("localhost")
            .arg("-d")
            .arg("pgrx_tests")
            .arg("-c")
            .arg("COPY (SELECT i AS a, i::text AS b FROM generate_series(1, 1000) i) TO STDOUT WITH (format parquet);")
            .stdout(Stdio::piped())
            .spawn()
            .expect("failed to execute process");

        let mut buffer = Vec::new();
        {
            let copy_to_stdout = copy_to.stdout.as_mut().expect("Failed to open stdout");
            copy_to_stdout
                .read_to_end(&mut buffer)
                .expect("Failed to read from stdout");

            let status = copy_to.wait().expect("Failed to wait for 'copy_to'");
            assert!(status.success(), "psql COPY TO STDOUT did not succeed");
        }

        // the temp file is still written and streamed as a valid parquet file
        assert!(buffer.starts_with(b"PAR1") && buffer.ends_with(b"PAR1"));

        assert_tmp_dir_has_no_unmanaged_entries(&pgsql_tmp);

        // the same temp file is used by COPY .. FROM STDIN
        let mut copy_from = Command::new("psql")
            .arg("-p")
            .arg(test_port.clone())
            .arg("-h")
            .arg("localhost")
            .arg("-d")
            .arg("pgrx_tests")
            .arg("-c")
            .arg(
                "CREATE TABLE tmp_file_test (a int, b text);
                  COPY tmp_file_test FROM STDIN WITH (format parquet);",
            )
            .stdin(Stdio::piped())
            .spawn()
            .expect("failed to execute process");

        {
            let copy_from_stdin = copy_from.stdin.as_mut().expect("Failed to open stdin");
            copy_from_stdin
                .write_all(&buffer)
                .expect("Failed to write to stdin");
            copy_from_stdin.flush().expect("Failed to flush stdin");

            let status = copy_from.wait().expect("Failed to wait for 'copy_from'");
            assert!(status.success(), "psql COPY FROM STDIN did not succeed");
        }

        assert_tmp_dir_has_no_unmanaged_entries(&pgsql_tmp);

        let output = Command::new("psql")
            .arg("-p")
            .arg(test_port.clone())
            .arg("-h")
            .arg("localhost")
            .arg("-d")
            .arg("pgrx_tests")
            .arg("-c")
            .arg("DROP TABLE tmp_file_test;")
            .output()
            .expect("failed to execute process");
        assert!(output.status.success(), "Failed to clean up");
    }

    // asserts that the temp directory contains only files that postgres itself created and
    // therefore removes. They are all prefixed with "pgsql_tmp", so anything else under the
    // temp directory is a file that we wrote behind postgres' back and nobody cleans up.
    //
    // We cannot assert that the directory is empty instead, because the other tests run
    // concurrently and may have temp files of their own in flight.
    fn assert_tmp_dir_has_no_unmanaged_entries(tmp_dir: &std::path::Path) {
        let entries = match std::fs::read_dir(tmp_dir) {
            Ok(entries) => entries,
            // postgres creates the temp directory on demand
            Err(_) => return,
        };

        let unmanaged: Vec<String> = entries
            .flatten()
            .map(|entry| entry.file_name().to_string_lossy().to_string())
            .filter(|name| !name.starts_with("pgsql_tmp"))
            .collect();

        assert!(
            unmanaged.is_empty(),
            "unmanaged temporary entries under {}: {:?}",
            tmp_dir.display(),
            unmanaged
        );
    }
}
