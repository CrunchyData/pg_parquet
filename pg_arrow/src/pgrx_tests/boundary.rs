//! The boundary itself: a schema across two `.so`s, a NULL that stays a NULL, and the registry
//! handing a statement to this provider at all.
//!
//! Nothing here re-tests Arrow IPC; the format is tested without a Postgres anywhere in
//! `arrow-ipc-format`. What these reach for is the descriptor and the dispatch around it.

#[pgrx::pg_schema]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{Int32Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow_ipc_format::ArrowIpc;
    use format_registry::options::FormatOptions;
    use format_registry::provider::FormatProvider;
    use pgrx::prelude::*;

    fn schema() -> SchemaRef {
        let mut metadata = HashMap::new();
        metadata.insert("written_by".to_string(), "pg_arrow's tests".to_string());

        Arc::new(
            Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("label", DataType::Utf8, true),
            ])
            .with_metadata(metadata),
        )
    }

    /// Writes a file the way a non-Postgres producer would — through the provider directly,
    /// bypassing every tier — so that what the SQL below reads was not written by the code path
    /// it is testing.
    fn write_file(name: &str, batches: usize, options: &[(&str, &str)]) -> String {
        let mut format_options = FormatOptions::new();

        for (option, value) in options {
            format_options.push(*option, *value);
        }

        let mut sink = ArrowIpc
            .open_writer(schema(), &format_options)
            .expect("the options are valid");

        let ids = Int32Array::from_iter_values(0..4);
        let labels = StringArray::from_iter_values(["a", "b", "c", "d"]);

        let batch = RecordBatch::try_new(schema(), vec![Arc::new(ids), Arc::new(labels)])
            .expect("the arrays match the schema");

        for _ in 0..batches {
            sink.write_batch(&batch).expect("failed to write a batch");
        }

        let data = sink.finish().expect("failed to finish the file");

        // the backend's pid keeps concurrent test runs out of each other's files
        let path = std::env::temp_dir().join(format!("pg_arrow_{}_{name}", std::process::id()));

        std::fs::write(&path, data).expect("failed to write the test file");

        path.to_string_lossy().into_owned()
    }

    #[pg_test]
    fn test_the_provider_registers_itself_with_the_base() {
        // the library is loaded, because these tests are in it, so _PG_init has run
        let extensions = Spi::get_one::<String>(
            "SELECT array_to_string(file_extensions, ',')
             FROM arrow.formats() WHERE format = 'arrow'",
        )
        .expect("failed to look up the format");

        assert_eq!(extensions.as_deref(), Some("arrow,arrows,ipc,feather"));
    }

    #[pg_test]
    fn test_the_base_knows_this_format_reads_parts_and_writes_streaming() {
        // the descriptor's two optional slots, as the base sees them. Both default to false, so
        // true,true is the proof that the base reads them off this provider: the ranged reader
        // is what a split read and a parallel COPY FROM go through, and the streaming writer is
        // what a COPY TO goes through, neither of which a finished file records.
        let capabilities = Spi::get_one::<String>(
            "SELECT reads_ranges || ',' || writes_streaming
             FROM arrow.formats() WHERE format = 'arrow'",
        )
        .expect("failed to read the listing");

        assert_eq!(capabilities.as_deref(), Some("true,true"));
    }

    #[pg_test]
    fn test_a_schema_survives_both_boundaries() {
        let path = write_file("schema.arrow", 1, &[]);

        let fields = Spi::connect(|client| {
            let mut fields = Vec::new();

            let table = client
                .select(
                    "SELECT field_name, arrow_type, nullable
                     FROM arrow.schema($1) ORDER BY field_position",
                    None,
                    &[path.clone().into()],
                )
                .expect("failed to read the schema");

            for row in table {
                fields.push((
                    row.get::<String>(1).unwrap().unwrap(),
                    row.get::<String>(2).unwrap().unwrap(),
                    row.get::<bool>(3).unwrap().unwrap(),
                ));
            }

            fields
        });

        // the format was inferred from the `.arrow` extension; nothing in the statement named it
        assert_eq!(
            fields,
            vec![
                ("id".to_string(), "Int32".to_string(), false),
                ("label".to_string(), "Utf8".to_string(), true),
            ]
        );
    }

    #[pg_test]
    fn test_an_explicit_format_reaches_the_same_provider() {
        // no extension at all, so only the explicit format can resolve this
        let path = write_file("explicit", 1, &[]);

        let count = Spi::get_one_with_args::<i64>(
            "SELECT count(*) FROM arrow.schema($1, 'arrow')",
            &[path.into()],
        )
        .expect("failed to read the schema");

        assert_eq!(count, Some(2));
    }

    #[pg_test]
    fn test_file_info_reports_the_shape_and_nulls_what_it_cannot_know() {
        let path = write_file("info.arrow", 3, &[]);

        let info = Spi::get_two_with_args::<i64, String>(
            "SELECT num_splits, format_version FROM arrow.file_info($1)",
            &[path.clone().into()],
        )
        .expect("failed to describe the file");

        assert_eq!(info.0, Some(3));
        assert_eq!(info.1.as_deref(), Some("Arrow IPC file"));

        // an unsupported answer has to arrive as NULL rather than as 0 or as an error; this is
        // the one thing about the ABI's three return codes that is visible from SQL
        let unknown = Spi::get_one_with_args::<bool>(
            "SELECT num_rows IS NULL AND compression IS NULL FROM arrow.file_info($1)",
            &[path.into()],
        )
        .expect("failed to describe the file");

        assert_eq!(unknown, Some(true));
    }

    #[pg_test]
    fn test_the_stream_variant_is_recognised_by_its_bytes() {
        // `.arrows` infers this provider, and the provider reads the container the bytes
        // actually are rather than the one the extension suggests
        let path = write_file("stream.arrows", 2, &[("variant", "stream")]);

        let version = Spi::get_one_with_args::<String>(
            "SELECT format_version FROM arrow.file_info($1)",
            &[path.clone().into()],
        )
        .expect("failed to describe the stream");

        assert_eq!(version.as_deref(), Some("Arrow IPC stream"));

        let splits = Spi::get_one_with_args::<bool>(
            "SELECT num_splits IS NULL FROM arrow.file_info($1)",
            &[path.into()],
        )
        .expect("failed to describe the stream");

        // no footer to count them in
        assert_eq!(splits, Some(true));
    }

    #[pg_test]
    fn test_kv_metadata_crosses_as_key_and_value() {
        let path = write_file("metadata.arrow", 1, &[]);

        let value = Spi::get_one_with_args::<String>(
            "SELECT value FROM arrow.kv_metadata($1) WHERE key = 'written_by'",
            &[path.into()],
        )
        .expect("failed to read the metadata");

        assert_eq!(value.as_deref(), Some("pg_arrow's tests"));
    }

    #[pg_test]
    fn test_a_corrupt_file_is_an_error_and_not_a_crash() {
        let path =
            std::env::temp_dir().join(format!("pg_arrow_{}_corrupt.arrow", std::process::id()));

        std::fs::write(&path, b"ARROW1\0\0this is not a footer").expect("failed to write");

        // caught in plpgsql rather than in Rust: an ereport out of SPI longjmps, so the thing
        // being asserted — that a corrupt file is an ordinary catchable error and not a crash —
        // is only really demonstrated by catching it the way a user would
        Spi::run(
            "CREATE FUNCTION pg_temp.read_schema(uri text) RETURNS text LANGUAGE plpgsql AS $$
             BEGIN
                 PERFORM count(*) FROM arrow.schema(uri);
                 RETURN 'no error';
             EXCEPTION WHEN OTHERS THEN
                 RETURN SQLERRM;
             END $$",
        )
        .expect("failed to define the helper");

        let error = Spi::get_one_with_args::<String>(
            "SELECT pg_temp.read_schema($1)",
            &[path.to_string_lossy().into_owned().into()],
        )
        .expect("failed to read the file")
        .expect("the helper always returns something");

        // the provider's own message, having travelled back as a code and a palloc'd string;
        // the tail of it is arrow's wording and is not asserted on
        assert!(
            error.starts_with("not a readable Arrow IPC file"),
            "{error}"
        );
    }

    #[pg_test(
        error = "the arrow format has no option \"compresion\"; it takes variant, compression, compression_level"
    )]
    fn test_an_option_typo_is_reported_by_this_provider() {
        // the hook is installed because the test cluster preloads the base with
        // arrow.enable_copy_hooks on; see `pg_test::postgresql_conf_options`. Validation happens
        // there before any I/O, and the message a user sees comes from here.
        Spi::run(
            "COPY (SELECT 1) TO '/tmp/pg_arrow_never_written.arrow'
             WITH (format 'arrow', compresion 'zstd')",
        )
        .expect("the typo should be reported");
    }

    #[pg_test]
    fn test_a_statement_round_trips_through_the_base() {
        // the one thing that proves end to end dispatch in both directions: Postgres tuples out
        // through the base's tuple-to-Arrow path, into this provider's writer, back in through
        // its reader and into tuples again. Every tier is in the path and none of it is this
        // crate's code except the descriptor.
        let path = std::env::temp_dir()
            .join(format!("pg_arrow_{}_round_trip.arrow", std::process::id()))
            .to_string_lossy()
            .into_owned();

        Spi::run(&format!(
            "CREATE TABLE source (id int, label text);
             INSERT INTO source VALUES (1, 'one'), (2, NULL), (3, 'three');
             COPY source TO '{path}' WITH (format 'arrow', compression 'zstd');
             CREATE TABLE target (id int, label text);
             COPY target FROM '{path}' WITH (format 'arrow')"
        ))
        .expect("the round trip should be carried out");

        // ordered and aggregated in one value so that a row lost, a row doubled, or a NULL
        // turned into a string all read as one failed comparison
        let rows = Spi::get_one::<String>(
            "SELECT string_agg(id || ':' || coalesce(label, '<null>'), ',' ORDER BY id)
             FROM target",
        )
        .expect("failed to read the target table");

        assert_eq!(rows.as_deref(), Some("1:one,2:<null>,3:three"));

        // what the base wrote is the file variant, not the footerless stream variant. Nothing
        // asserts the compression here: an IPC footer does not record it, which the format crate
        // tests in `compressed_files_still_report_no_compression`.
        let version = Spi::get_one_with_args::<String>(
            "SELECT format_version FROM arrow.file_info($1)",
            &[path.clone().into()],
        )
        .expect("failed to read the file info");

        assert_eq!(version.as_deref(), Some("Arrow IPC file"));

        std::fs::remove_file(&path).expect("failed to clean up");
    }

    #[pg_test]
    fn test_a_batch_body_larger_than_one_fetch_is_read_in_pieces() {
        // a range comes back in a palloc'd buffer, which stops at 1GB, while arrow asks for a
        // whole batch body in one read. Three 30MB values are one body past that single-fetch
        // window, so this fails if the reader does not accept a short read
        let path = "/tmp/pg_arrow_big_batch.arrow";

        Spi::run(&format!(
            "CREATE TABLE big AS
                 SELECT i AS id, repeat('x', 30 * 1024 * 1024) AS payload
                 FROM generate_series(1, 3) i;
             COPY big TO '{path}' WITH (format 'arrow');
             CREATE TABLE big_back (id int, payload text);
             COPY big_back FROM '{path}' WITH (format 'arrow')"
        ))
        .expect("a file with a batch body past one fetch should round trip");

        let total = Spi::get_one::<i64>("SELECT sum(length(payload))::bigint FROM big_back")
            .expect("failed to read the target table");

        assert_eq!(total, Some(3 * 30 * 1024 * 1024));

        std::fs::remove_file(path).expect("failed to clean up");
    }

    #[pg_test]
    fn test_a_corrupt_file_is_refused_rather_than_ending_the_backend() {
        // arrow's reader panics on a buffer offset it cannot trust rather than returning an
        // error, and a panic cannot leave the stream callbacks the descriptor hands over. Before
        // the reader was wrapped in `panic_guarded`, a handful of flipped bytes aborted the
        // backend and took the cluster into recovery, so what this asserts is that the session
        // is still there afterwards.
        let path = "/tmp/pg_arrow_corrupt.arrow";

        Spi::run(&format!(
            "CREATE TABLE good AS
                 SELECT i AS id, 'row' || i AS label, i % 3 = 0 AS flag
                 FROM generate_series(1, 2000) i;
             COPY good TO '{path}' WITH (format 'arrow');
             CREATE TABLE back (id int, label text, flag bool);
             CREATE TABLE refusals (why text)"
        ))
        .expect("the fixture should be written");

        let good = std::fs::read(path).expect("the file should be readable");

        // a fixed sequence rather than a random one: a regression has to fail the same way on
        // the run after it
        let mut seed = 0x2545_f491_4f6c_dd1d_u64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };

        for _ in 0..128 {
            let mut mutant = good.clone();

            for _ in 0..4 {
                let at = (next() % mutant.len() as u64) as usize;
                mutant[at] = (next() % 256) as u8;
            }

            std::fs::write(path, &mutant).expect("the mutant should be written");

            // plpgsql's EXCEPTION is the subtransaction that lets the test carry on past a
            // refusal, which is the expected outcome for most of these
            Spi::run(&format!(
                "DO $$
                 BEGIN
                     COPY back FROM '{path}' WITH (format 'arrow');
                 EXCEPTION WHEN OTHERS THEN
                     INSERT INTO refusals VALUES (SQLERRM);
                 END $$"
            ))
            .expect("a refusal is caught rather than raised");
        }

        let refused = Spi::get_one::<i64>("SELECT count(*) FROM refusals")
            .expect("failed to count the refusals");

        // the session answering at all is the assertion; the count only says the mutants were
        // corrupt enough for the test to have been about something
        assert!(
            refused.unwrap_or(0) > 0,
            "no mutant was refused, so nothing was tested"
        );

        std::fs::remove_file(path).expect("failed to clean up");
    }
}
