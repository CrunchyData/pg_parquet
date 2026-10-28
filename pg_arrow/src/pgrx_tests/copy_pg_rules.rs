//! The parts of `COPY` that belong to Postgres rather than to a format.
//!
//! A column list, a `WHERE` clause, a generated column, a privilege: none of it is Arrow IPC's
//! business, and all of it is in the path of a statement that names this format. The base is
//! what implements them and has its own tests for each; what these assert is that naming
//! `format 'arrow'` does not take a statement down a path where one of them stops applying.

#[pgrx::pg_schema]
mod tests {
    use pgrx::{pg_test, Spi};

    use crate::pgrx_tests::common::{test_file, FileCleanup};

    #[pg_test]
    fn test_a_column_list_on_both_sides() {
        let uri = test_file("rules_columns.arrow");
        let _cleanup = FileCleanup::new(&uri);

        // the file's columns are the ones the statement named, in the order it named them, so
        // reading them back into the other order has to go by name and not by position
        Spi::run(&format!(
            "CREATE TABLE listed (a int, b text);
             INSERT INTO listed VALUES (1, 'one');
             COPY listed (b, a) TO '{uri}' WITH (format 'arrow');
             CREATE TABLE listed_back (a int, b text);
             COPY listed_back (b, a) FROM '{uri}' WITH (format 'arrow');"
        ))
        .unwrap();

        assert_eq!(
            Spi::get_one::<String>("SELECT a || ':' || b FROM listed_back").unwrap(),
            Some("1:one".to_string())
        );
    }

    #[pg_test]
    fn test_a_where_clause_filters_what_is_read() {
        let uri = test_file("rules_where.arrow");
        let _cleanup = FileCleanup::new(&uri);

        Spi::run(&format!(
            "CREATE TABLE filtered AS SELECT i AS a FROM generate_series(1, 5) i;
             COPY filtered TO '{uri}' WITH (format 'arrow');
             CREATE TABLE kept (a int);
             COPY kept FROM '{uri}' WITH (format 'arrow') WHERE a > 3;"
        ))
        .unwrap();

        assert_eq!(
            Spi::get_one::<String>("SELECT string_agg(a::text, ',' ORDER BY a) FROM kept").unwrap(),
            Some("4,5".to_string())
        );
    }

    #[pg_test]
    fn test_a_generated_column_is_written() {
        let uri = test_file("rules_generated.arrow");
        let _cleanup = FileCleanup::new(&uri);

        Spi::run(&format!(
            "CREATE TABLE generated (a int, b int GENERATED ALWAYS AS (a + 2) STORED);
             INSERT INTO generated (a) VALUES (1), (2);
             COPY generated TO '{uri}' WITH (format 'arrow');"
        ))
        .unwrap();

        // the generated column is in the file: a `COPY tab TO uri` is run as
        // `COPY (SELECT * FROM tab) TO`, and a `SELECT *` includes it. Core's own `COPY tab TO`
        // leaves it out, so this differs from a copy to a plain path — the same way it does for
        // `format 'parquet'`, which is why it is pinned here rather than worked around.
        let fields = Spi::get_one::<String>(&format!(
            "SELECT string_agg(field_name, ',' ORDER BY field_position)
               FROM arrow.schema('{uri}')"
        ))
        .unwrap();

        assert_eq!(fields.as_deref(), Some("a,b"), "the file's fields");
    }

    #[pg_test]
    #[should_panic(expected = "COPY FROM a file into generated columns is not supported")]
    fn test_reading_into_a_generated_column_is_refused() {
        // the value in the file is not the one the expression would produce, so there is no
        // honest way to load it: a generated column is refused rather than overwritten
        let uri = test_file("rules_generated_from.arrow");
        let _cleanup = FileCleanup::new(&uri);

        Spi::run(&format!(
            "CREATE TABLE loaded (a int, b int GENERATED ALWAYS AS (a + 2) STORED);
             INSERT INTO loaded (a) VALUES (1);
             COPY loaded TO '{uri}' WITH (format 'arrow');
             TRUNCATE loaded;
             COPY loaded FROM '{uri}' WITH (format 'arrow');"
        ))
        .unwrap();
    }

    #[pg_test]
    fn test_a_dropped_column_is_not_in_the_file() {
        let uri = test_file("rules_dropped.arrow");
        let _cleanup = FileCleanup::new(&uri);

        // a dropped attribute still has a slot in the tuple descriptor, so a path that walks it
        // by position rather than by what is live would write a column of garbage
        Spi::run(&format!(
            "CREATE TABLE dropped (a int, gone text, b int);
             INSERT INTO dropped VALUES (1, 'x', 2);
             ALTER TABLE dropped DROP COLUMN gone;
             COPY dropped TO '{uri}' WITH (format 'arrow');
             CREATE TABLE dropped_back (a int, b int);
             COPY dropped_back FROM '{uri}' WITH (format 'arrow');"
        ))
        .unwrap();

        assert_eq!(
            Spi::get_one::<String>("SELECT a || ':' || b FROM dropped_back").unwrap(),
            Some("1:2".to_string())
        );
    }

    #[pg_test]
    #[should_panic(expected = "violates not-null constraint")]
    fn test_a_not_null_constraint_is_checked_on_the_way_in() {
        let uri = test_file("rules_not_null.arrow");
        let _cleanup = FileCleanup::new(&uri);

        Spi::run(&format!(
            "COPY (SELECT NULL::int AS a) TO '{uri}' WITH (format 'arrow');
             CREATE TABLE required (a int NOT NULL);
             COPY required FROM '{uri}' WITH (format 'arrow');"
        ))
        .unwrap();
    }

    #[pg_test]
    fn test_a_view_and_a_partitioned_table() {
        let view_uri = test_file("rules_view.arrow");
        let partitioned_uri = test_file("rules_partitioned.arrow");
        let _view_cleanup = FileCleanup::new(&view_uri);
        let _partitioned_cleanup = FileCleanup::new(&partitioned_uri);

        // both are read through a query, because core refuses `COPY view TO` and (before PG19)
        // `COPY partitioned_table TO` as well, and this path refuses them the same way. Reading
        // *into* a partitioned table is by name, and routes.
        Spi::run(&format!(
            "CREATE TABLE base AS SELECT i AS a FROM generate_series(1, 3) i;
             CREATE VIEW three AS SELECT * FROM base;
             COPY (SELECT * FROM three) TO '{view_uri}' WITH (format 'arrow');

             CREATE TABLE split (a int) PARTITION BY RANGE (a);
             CREATE TABLE split_low PARTITION OF split FOR VALUES FROM (1) TO (3);
             CREATE TABLE split_high PARTITION OF split FOR VALUES FROM (3) TO (10);
             COPY split FROM '{view_uri}' WITH (format 'arrow');
             COPY (SELECT * FROM split) TO '{partitioned_uri}' WITH (format 'arrow');"
        ))
        .unwrap();

        // the rows went to the right partitions, and reading a partitioned table back gives all
        // of them exactly once
        assert_eq!(
            Spi::get_one::<String>(
                "SELECT (SELECT count(*) FROM split_low) || '/' || (SELECT count(*) FROM split_high)"
            )
            .unwrap(),
            Some("2/1".to_string())
        );

        // an IPC file has no footer row count, so the rows are counted by reading them back
        Spi::run(&format!(
            "CREATE TABLE split_back (a int);
             COPY split_back FROM '{partitioned_uri}' WITH (format 'arrow');"
        ))
        .unwrap();

        assert_eq!(
            Spi::get_one::<String>("SELECT string_agg(a::text, ',' ORDER BY a) FROM split_back")
                .unwrap(),
            Some("1,2,3".to_string())
        );
    }

    #[pg_test]
    fn test_an_empty_table_is_still_a_file_with_a_schema() {
        let uri = test_file("rules_empty.arrow");
        let _cleanup = FileCleanup::new(&uri);

        // a file with no rows still has to be readable, and its schema is what a
        // `definition_from` on it would use
        Spi::run(&format!(
            "CREATE TABLE empty (a int, b text);
             COPY empty TO '{uri}' WITH (format 'arrow');
             CREATE TABLE empty_back (a int, b text);
             COPY empty_back FROM '{uri}' WITH (format 'arrow');"
        ))
        .unwrap();

        // the schema is in the file even though no row is
        assert_eq!(
            Spi::get_one::<String>(&format!(
                "SELECT string_agg(field_name, ',' ORDER BY field_position)
                   FROM arrow.schema('{uri}')"
            ))
            .unwrap(),
            Some("a,b".to_string())
        );

        assert_eq!(
            Spi::get_one::<i64>("SELECT count(*) FROM empty_back").unwrap(),
            Some(0)
        );
    }

    #[pg_test]
    fn test_a_one_dimensional_and_an_empty_array() {
        let uri = test_file("rules_arrays.arrow");
        let _cleanup = FileCleanup::new(&uri);

        Spi::run(&format!(
            "CREATE TABLE arrays (a int[]);
             INSERT INTO arrays VALUES ('{{1,2,3}}'), ('{{}}'), (null);
             COPY arrays TO '{uri}' WITH (format 'arrow');
             TRUNCATE arrays;
             COPY arrays FROM '{uri}' WITH (format 'arrow');"
        ))
        .unwrap();

        assert_eq!(
            Spi::get_one::<String>(
                "SELECT string_agg(coalesce(a::text, 'null'), ',' ORDER BY a NULLS LAST)
                   FROM arrays"
            )
            .unwrap(),
            Some("{},{1,2,3},null".to_string())
        );
    }

    #[pg_test]
    #[should_panic(expected = "has a 2-dimensional array value")]
    fn test_a_multi_dimensional_array_is_refused() {
        // Arrow has no equivalent of a Postgres array with more than one dimension, and the
        // dimensions are a property of the value rather than of the column, so this cannot be
        // caught before the rows are read
        let uri = test_file("rules_multi_dimensional.arrow");
        let _cleanup = FileCleanup::new(&uri);

        Spi::run(&format!(
            "CREATE TABLE square (a int[]);
             INSERT INTO square VALUES ('{{{{1,2}},{{3,4}}}}');
             COPY square TO '{uri}' WITH (format 'arrow');"
        ))
        .unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "permission denied to write")]
    fn test_writing_needs_the_object_store_write_role() {
        let uri = test_file("rules_no_write.arrow");

        Spi::run(&format!(
            "CREATE TABLE unprivileged (a int);
             CREATE ROLE no_write;
             GRANT ALL ON ALL TABLES IN SCHEMA public TO no_write;
             SET ROLE no_write;
             COPY unprivileged TO '{uri}' WITH (format 'arrow');"
        ))
        .unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "permission denied to read")]
    fn test_reading_needs_the_object_store_read_role() {
        let uri = test_file("rules_no_read.arrow");
        let _cleanup = FileCleanup::new(&uri);

        Spi::run(&format!(
            "CREATE TABLE unprivileged (a int);
             COPY unprivileged TO '{uri}' WITH (format 'arrow');
             CREATE ROLE no_read;
             GRANT ALL ON ALL TABLES IN SCHEMA public TO no_read;
             SET ROLE no_read;
             COPY unprivileged FROM '{uri}' WITH (format 'arrow');"
        ))
        .unwrap();
    }

    #[pg_test]
    fn test_a_copy_inside_a_function_and_inside_another_copy() {
        let inner_uri = test_file("rules_nested_inner.arrow");
        let outer_uri = test_file("rules_nested_outer.arrow");
        let _inner_cleanup = FileCleanup::new(&inner_uri);
        let _outer_cleanup = FileCleanup::new(&outer_uri);

        // the hook is re-entered while it is already running one statement, which is the case
        // where per-statement state kept in a static rather than on the stack goes wrong
        Spi::run(&format!(
            "CREATE TABLE nested AS SELECT i AS a FROM generate_series(1, 3) i;

             CREATE FUNCTION inner_copy() RETURNS int LANGUAGE plpgsql AS $$
             BEGIN
                 COPY nested TO '{inner_uri}' WITH (format 'arrow');
                 RETURN 1;
             END;
             $$;

             COPY (SELECT inner_copy() AS a) TO '{outer_uri}' WITH (format 'arrow');"
        ))
        .unwrap();

        // both statements wrote a readable file, which is what the re-entry would have broken
        for uri in [&inner_uri, &outer_uri] {
            let size =
                Spi::get_one::<i64>(&format!("SELECT size_bytes FROM arrow.file_info('{uri}')"))
                    .unwrap();

            assert!(size.unwrap_or(0) > 0, "no file was written at {uri}");
        }
    }
}
