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

    #[pg_test]
    #[should_panic(expected = "no files found that match the pattern")]
    fn test_with_nonexistent_pattern_path() {
        let filename = "/tmp/pg_parquet_test/**";
        let _file = FileCleanup::new(filename);

        let create_table = "create table test_table(a int);";
        Spi::run(create_table).unwrap();

        let copy_from_parquet = format!("COPY test_table FROM '{filename}' WITH (format parquet);");
        Spi::run(copy_from_parquet.as_str()).unwrap();
    }
}
