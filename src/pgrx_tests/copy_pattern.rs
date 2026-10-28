#[pgrx::pg_schema]
mod tests {
    use pgrx::{pg_test, Spi};

    use crate::pgrx_tests::common::FileCleanup;

    #[pg_test]
    #[should_panic(expected = "EOF: file size of 2 is less than footer")]
    fn test_non_parquet_path_in_pattern() {
        let filename1 = "/tmp/pg_parquet_test/dummy1.parquet";
        let filename2 = "/tmp/pg_parquet_test/dummy2.csv";
        let _file1 = FileCleanup::new(filename1);
        let _file2 = FileCleanup::new(filename2);

        let copy_to_parquet =
            format!("copy (select 1 as a) to '{filename1}' with (format parquet);");
        Spi::run(&copy_to_parquet).unwrap();

        let copy_to_csv = format!("copy (select 1 as a) to '{filename2}' with (format csv);");
        Spi::run(&copy_to_csv).unwrap();

        let create_table = "create table test_table(a int);";
        Spi::run(create_table).unwrap();

        let file_pattern = "/tmp/pg_parquet_test/*";
        let copy_from_command = format!(
            "COPY test_table FROM '{}' WITH (format parquet);",
            file_pattern
        );
        Spi::run(copy_from_command.as_str()).unwrap();
    }

    #[pg_test]
    fn test_path_with_special_parquet_file_name() {
        let filename = "/tmp/pg_parquet_test/du**mm*y1.parquet";
        let _file = FileCleanup::new(filename);

        let copy_to_parquet =
            format!("copy (select 1 as a) to '{filename}' with (format parquet);");
        Spi::run(&copy_to_parquet).unwrap();

        let create_table = "create table test_table(a int);";
        Spi::run(create_table).unwrap();

        let copy_from_parquet = format!("COPY test_table FROM '{filename}' WITH (format parquet);");
        Spi::run(copy_from_parquet.as_str()).unwrap();

        let count_query = "select count(*) from test_table;";
        let result = Spi::get_one::<i64>(count_query).unwrap().unwrap();
        assert_eq!(result, 1);
    }

    #[pg_test]
    #[should_panic(expected = "failed to get object store metadata")]
    fn test_path_with_special_chars() {
        let filename = "/tmp/pg_parquet_test/du**\\mm*y1.parquet";
        let _file = FileCleanup::new(filename);

        let create_table = "create table test_table(a int);";
        Spi::run(create_table).unwrap();

        let copy_from_parquet = format!("COPY test_table FROM '{filename}' WITH (format parquet);");
        Spi::run(copy_from_parquet.as_str()).unwrap();
    }

    // a folder is what a COPY TO with file_size_bytes writes, and reading it back by its own
    // name finds no object of that name, which an object store reports as a missing object
    // and nothing else. The error says that such a uri is read with a pattern over the files
    // the folder holds.
    #[pg_test]
    fn test_folder_source_names_the_pattern_it_is_read_with() {
        let folder = "/tmp/pg_parquet_test_folder_source";
        let _folder = FileCleanup::new(folder);

        Spi::run("create table test_table(a text);").unwrap();
        Spi::run(&format!(
            "insert into test_table select 'hellooooo' || i from generate_series(1, 100) i;
             copy test_table to '{folder}' with (format parquet, file_size_bytes '1MB');"
        ))
        .unwrap();

        let folder_error = copy_from_error(folder);
        assert!(
            folder_error.contains(&format!("\"{folder}\" holds files rather than being one"))
                && folder_error.contains(&format!("\"{folder}/*.parquet\"")),
            "reading the folder reported: {folder_error}"
        );

        // a uri that names no object and holds none under it is missing, and nothing more
        let missing_error = copy_from_error("/tmp/pg_parquet_test_no_such_file.parquet");
        assert!(
            !missing_error.contains("holds files"),
            "reading a missing file reported: {missing_error}"
        );
    }

    // copy_from_error is what a COPY FROM the given uri reported, caught in plpgsql so that
    // the test can look at the message rather than failing on it
    fn copy_from_error(uri: &str) -> String {
        Spi::run(&format!(
            "CREATE OR REPLACE FUNCTION failing_copy_from() RETURNS text
             LANGUAGE plpgsql AS $$
             BEGIN
                 EXECUTE 'COPY test_table FROM ''{uri}'' WITH (format parquet)';

                 RETURN 'no error';
             EXCEPTION WHEN others THEN
                 RETURN SQLERRM;
             END;
             $$;"
        ))
        .unwrap();

        Spi::get_one::<String>("SELECT failing_copy_from()")
            .unwrap()
            .unwrap()
    }

    #[pg_test]
    #[should_panic(expected = "no files found that match the pattern /tmp/pg_parquet_test/**")]
    fn test_with_nonexistent_pattern_path() {
        let filename = "/tmp/pg_parquet_test/**";
        let _file = FileCleanup::new(filename);

        let create_table = "create table test_table(a int);";
        Spi::run(create_table).unwrap();

        let copy_from_parquet = format!("COPY test_table FROM '{filename}' WITH (format parquet);");
        Spi::run(copy_from_parquet.as_str()).unwrap();
    }
}
