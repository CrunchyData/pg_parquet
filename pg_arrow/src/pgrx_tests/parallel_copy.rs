//! A `COPY` several processes take part in, over the two IPC containers.
//!
//! The mechanism is tier 2's — the splits are strided across workers, the leader is the only
//! process that inserts — so what is under test here is the half that is this format's: a file
//! whose record batches can be read one range at a time, and a stream whose cannot. Both have to
//! end with every row in the table exactly once, which is the only thing a user of
//! `arrow.max_parallel_workers` is promised.

#[pgrx::pg_schema]
mod tests {
    use pgrx::{pg_test, Spi};

    use crate::pgrx_tests::common::{test_file, FileCleanup};

    /// Rows enough for several record batches: tier 2's dest receiver writes 1024 rows a batch,
    /// so this is five of them and the file is divisible five ways.
    const ROWS: i32 = 5000;

    /// Writes `ROWS` rows to `uri` and reads them back with the workers turned on.
    fn round_trip(uri: &str, options: &str) {
        Spi::run(&format!(
            "CREATE TABLE written AS
                 SELECT i AS id, 'row ' || i AS label FROM generate_series(1, {ROWS}) i;
             COPY written TO '{uri}' WITH ({options});

             SET arrow.max_parallel_workers TO 4;

             CREATE TABLE came_back (id int, label text);
             COPY came_back FROM '{uri}' WITH ({options});"
        ))
        .unwrap();
    }

    /// Whether the rows are in the table in the order the file holds them, which they are when
    /// the ids ascend with the physical order.
    fn out_of_order() -> i64 {
        Spi::get_one::<i64>(
            "SELECT count(*) FROM (
                 SELECT id, lag(id) OVER (ORDER BY ctid) AS before FROM came_back
             ) ordered WHERE before IS NOT NULL AND id <> before + 1",
        )
        .unwrap()
        .unwrap()
    }

    #[pg_test]
    fn test_a_parallel_copy_from_a_file_inserts_every_row_once_in_file_order() {
        let uri = test_file("parallel_from.arrow");
        let _cleanup = FileCleanup::new(&uri);

        round_trip(&uri, "format 'arrow'");

        assert_eq!(
            Spi::get_one::<i64>("SELECT count(*) FROM came_back").unwrap(),
            Some(ROWS as i64)
        );

        // the ids were written in order, and only the leader inserts, so the physical order of
        // the table is the order of the file — a worker reading a range it was not given, or a
        // range read twice, shows up here before it shows up in the count
        assert_eq!(out_of_order(), 0);

        assert_eq!(
            Spi::get_one::<i64>("SELECT sum(id) FROM came_back").unwrap(),
            Some((ROWS as i64 * (ROWS as i64 + 1)) / 2)
        );
    }

    #[pg_test]
    fn test_a_parallel_copy_from_a_stream_reads_it_in_one_process() {
        let uri = test_file("parallel_from.arrows");
        let _cleanup = FileCleanup::new(&uri);

        // a stream has no footer, so it has no split count and tier 2 reads it where it always
        // did. The rows are the assertion: a fallback that read the file in every worker would
        // multiply them.
        round_trip(&uri, "format 'arrow', variant 'stream'");

        assert_eq!(
            Spi::get_one::<i64>("SELECT count(*) FROM came_back").unwrap(),
            Some(ROWS as i64)
        );

        assert_eq!(out_of_order(), 0);
    }

    #[pg_test]
    fn test_a_parallel_copy_to_fills_a_folder_that_reads_back_whole() {
        let uri = test_file("parallel_to.arrow");
        let _cleanup = FileCleanup::new(&uri);

        // tier 2 only divides a table of at least 1024 pages, so this one is written wide
        // enough to reach that rather than silently taking the single process path
        const WIDE_ROWS: i32 = 120_000;

        // `file_size_bytes` makes the destination a folder, which is the shape a parallel
        // `COPY TO` writes: each worker writes whole files of its own into it
        Spi::run(&format!(
            "CREATE TABLE written AS
                 SELECT i AS id, repeat('x', 100) AS label FROM generate_series(1, {WIDE_ROWS}) i;

             SET arrow.max_parallel_workers TO 4;

             COPY written TO '{uri}' WITH (format 'arrow', file_size_bytes '1MB');

             CREATE TABLE came_back (id int, label text);
             COPY came_back FROM '{uri}/*' WITH (format 'arrow');"
        ))
        .unwrap();

        // several files, which is what makes the folder the destination a worker can be given
        // one of
        let files = Spi::get_one_with_args::<i64>(
            "SELECT count(*) FROM arrow.list($1)",
            &[uri.clone().into()],
        )
        .unwrap()
        .unwrap();

        assert!(files > 1, "the folder holds {files} files");

        // the rows of a folder come back in whatever order the files are read in, so the sum is
        // the claim rather than the physical order
        assert_eq!(
            Spi::get_one::<i64>("SELECT count(*) FROM came_back").unwrap(),
            Some(WIDE_ROWS as i64)
        );

        assert_eq!(
            Spi::get_one::<i64>("SELECT sum(id) FROM came_back").unwrap(),
            Some((WIDE_ROWS as i64 * (WIDE_ROWS as i64 + 1)) / 2)
        );
    }
}
