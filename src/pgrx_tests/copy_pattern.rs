#[pgrx::pg_schema]
mod tests {
    use pgrx::{pg_test, Spi};

    #[pg_test]
    #[should_panic(expected = "EOF: file size of 2 is less than footer")]
    fn test_pattern_invalid_parquet_file() {
        let copy_to_parquet =
            "copy (select 1 as a) to '/tmp/pg_parquet_test/dummy1.parquet' with (format parquet);";
        Spi::run(copy_to_parquet).unwrap();

        let copy_to_csv =
            "copy (select 1 as a) to '/tmp/pg_parquet_test/dummy2.csv' with (format csv);";
        Spi::run(copy_to_csv).unwrap();

        let create_table = "create table test_table(a int);";
        Spi::run(create_table).unwrap();

        let file_pattern = "/tmp/pg_parquet_test/*";
        let copy_from_command = format!("COPY test_table FROM '{}';", file_pattern);
        Spi::run(copy_from_command.as_str()).unwrap();
    }
}
