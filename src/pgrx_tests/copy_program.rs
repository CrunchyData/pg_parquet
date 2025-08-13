#[pgrx::pg_schema]
mod tests {
    use std::io::pipe;
    use std::io::Read;
    use std::io::Write;
    use std::process::Command;
    use std::vec;

    use pgrx::pg_test;
    use pgrx::Spi;

    use crate::pgrx_tests::common::LOCAL_TEST_FILE_PATH;

    #[pg_test]
    fn test_copy_program() {
        let pg_version = std::env::var("PG_MAJOR").unwrap().parse::<i32>().unwrap();

        let test_base_port = std::env::var("PGRX_TEST_PG_BASE_PORT")
            .unwrap()
            .parse::<i32>()
            .unwrap();

        let test_port = (test_base_port + pg_version).to_string();

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
                "DROP ROLE IF EXISTS {role};
                 CREATE ROLE {role} LOGIN;
                 GRANT SELECT, INSERT, UPDATE, DELETE ON test_expected TO {role};
                 GRANT SELECT, INSERT, UPDATE, DELETE ON test_result TO {role};"
            ))
            .output()
            .expect("failed to execute process");
        assert!(output.status.success(), "Failed to create role {role}");

        // try copy to without pg_execute_server_program permission
        let output = Command::new("psql")
            .arg("-p")
            .arg(test_port.clone())
            .arg("-h")
            .arg("localhost")
            .arg("-U")
            .arg(role)
            .arg("-d")
            .arg("pgrx_tests")
            .arg("-c")
            .arg(format!(
                "COPY test_expected TO PROGRAM 'cat > {LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
            ))
            .output().expect("failed to execute process");
        assert!(
            !output.status.success(),
            "Copy to PROGRAM should fail without permission"
        );
        assert!(String::from_utf8(output.stderr)
            .unwrap()
            .contains("pg_execute_server_program"));

        // try copy from without pg_execute_server_program permission
        let output = Command::new("psql")
            .arg("-p")
            .arg(test_port.clone())
            .arg("-h")
            .arg("localhost")
            .arg("-U")
            .arg(role)
            .arg("-d")
            .arg("pgrx_tests")
            .arg("-c")
            .arg(format!(
                "COPY test_result FROM PROGRAM 'cat {LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
            ))
            .output()
            .expect("failed to execute process");
        assert!(
            !output.status.success(),
            "Copy from PROGRAM should fail without permission"
        );
        assert!(String::from_utf8(output.stderr)
            .unwrap()
            .contains("pg_execute_server_program"));

        // grant pg_execute_server_program permission
        let output = Command::new("psql")
            .arg("-p")
            .arg(test_port.clone())
            .arg("-h")
            .arg("localhost")
            .arg("-d")
            .arg("pgrx_tests")
            .arg("-c")
            .arg(format!("GRANT pg_execute_server_program TO {role};"))
            .output()
            .expect("failed to execute process");
        assert!(
            output.status.success(),
            "Failed to grant pg_execute_server_program permission"
        );

        // copy to program with dummy role
        let (mut pipe_in, pipe_out) = pipe().expect("Failed to create pipe");
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
            .arg(format!("COPY test_expected TO PROGRAM 'cat > {LOCAL_TEST_FILE_PATH}' WITH (format parquet);"))
            .stdout(pipe_out)
            .spawn()
            .expect("failed to execute process");

        let mut buffer = Vec::new();
        {
            pipe_in
                .read_to_end(&mut buffer)
                .expect("Failed to read from input pipe");

            let status = copy_to.wait().expect("Failed to wait for 'copy_to'");
            assert!(status.success(), "psql COPY TO process did not succeed");
        }

        // copy from program with dummy role
        let (pipe_in, mut pipe_out) = pipe().expect("Failed to create pipe");
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
            .arg(format!(
                "COPY test_result FROM PROGRAM 'cat {LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
            ))
            .stdin(pipe_in)
            .spawn()
            .expect("failed to execute process");

        {
            // Write the data we just read to the pipe's output
            pipe_out
                .write_all(&buffer)
                .expect("Failed to write to pipe");
            pipe_out.flush().expect("Failed to flush pipe");

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
}
