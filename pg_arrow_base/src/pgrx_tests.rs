//! Tests that need a running backend.
//!
//! What is deliberately *not* here: anything that reads or writes a file in an object store.
//! Those need credentials. A local path is the exception — the control file's `requires` line
//! means `CREATE EXTENSION ... CASCADE` has installed tier 1 in the test cluster, so the vtable
//! can be driven against `/tmp` — and the streaming writer is exercised that way below, since
//! a handle that outlives its statement is the kind of mistake no end-to-end test would
//! localize. The cloud read and write paths are covered end to end from the provider
//! repositories, which install both extensions and have credentials. The
//! format-agnostic logic that can be tested without a backend at all — option parsing, the
//! registry, extension inference — is unit tested in `format-registry`, where it lives.

// -- a provider that exists only here -------------------------------------------------
//
// A fake format is what makes the `COPY` paths testable at all: a real one lives in another
// extension and another repository, and depending on one here would be the layering inverted.
// The format writes a line per row of displayed values, which is a deliberately boring
// container — what is under test is everything between the statement and the sink (the hook,
// the portal, the dest receiver, the batching, the schema, the conversion and the upload), and
// a format whose output is hard to read would only make a failure harder to localize.
//
// Outside the test module, and registered from `_PG_init` under the `pg_test` feature, because
// one of the statement forms cannot be driven from inside the backend running the test: a
// `COPY ... TO STDOUT` needs a client, so that test connects with `psql` — and a provider
// registered only by the test harness would not exist in the backend `psql` gets.

pub(crate) mod lines {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::error::ArrowError;
    use arrow::util::display::{ArrayFormatter, FormatOptions as DisplayOptions};
    use format_registry::options::{FormatOptions, FILE_NAME_OPTION};
    use format_registry::provider::{
        ByteSink, ByteSource, FileInfo, FormatProvider, StreamingSink,
    };
    use format_registry::Result;

    pub(crate) const FORMAT: &str = "testlines";

    /// The rows of one of the fake's splits, which is also its batch size.
    pub(crate) const SPLIT_ROWS: usize = 1000;

    /// Where to leave a marker per process and split read, for the tests that have to prove
    /// which process read what.
    pub(crate) const TRACE_SPLITS_OPTION: &str = "trace_splits";

    pub(crate) struct LinesProvider;

    impl FormatProvider for LinesProvider {
        fn name(&self) -> String {
            FORMAT.to_string()
        }

        fn file_extensions(&self) -> Vec<String> {
            vec!["testlines".to_string()]
        }

        fn validate_option(&self, name: &str, _value: &str) -> Result<()> {
            // two options: one so that a statement can prove its options reach the
            // provider, one so that it can prove the file's name does
            if name == "prefix" || name == "echo_file_name" || name == TRACE_SPLITS_OPTION {
                Ok(())
            } else {
                Err(format!("{FORMAT} has no option named \"{name}\""))
            }
        }

        fn schema(&self, data: &[u8]) -> Result<SchemaRef> {
            let file = LinesFile::parse(data, "")?;

            Ok(file.schema)
        }

        fn kv_metadata(&self, _data: &[u8]) -> Result<Vec<(String, Option<String>)>> {
            // the fake's container has no place to put any
            Ok(vec![])
        }

        fn file_info(&self, data: &[u8]) -> Result<FileInfo> {
            let file = LinesFile::parse(data, "")?;

            Ok(FileInfo {
                num_rows: Some(file.rows.len() as i64),
                ..Default::default()
            })
        }

        fn open_reader(
            &self,
            data: bytes::Bytes,
            options: &FormatOptions,
        ) -> Result<Box<dyn arrow::record_batch::RecordBatchReader + Send>> {
            let prefix = options.get("prefix").unwrap_or_default().to_string();

            let file = LinesFile::parse(&data, &prefix)?;

            Ok(Box::new(file.into_reader()))
        }

        // the fake reads ranges so that the parallel `COPY FROM` path is testable without a
        // real format: a split is a batch of the file's rows, which is as independently
        // readable as a parquet row group is
        fn can_read_ranges(&self) -> bool {
            true
        }

        fn ranged_file_info(&self, source: Arc<dyn ByteSource>) -> Result<FileInfo> {
            let file = LinesFile::read(&source, "")?;

            Ok(FileInfo {
                num_rows: Some(file.rows.len() as i64),
                num_splits: Some(file.rows.len().div_ceil(SPLIT_ROWS) as i64),
                ..Default::default()
            })
        }

        fn open_ranged_reader(
            &self,
            source: Arc<dyn ByteSource>,
            options: &FormatOptions,
            splits: Option<std::ops::Range<i64>>,
        ) -> Result<Box<dyn arrow::record_batch::RecordBatchReader + Send>> {
            let prefix = options.get("prefix").unwrap_or_default().to_string();

            let file = LinesFile::read(&source, &prefix)?;

            // which process read which split, for the tests that have to prove the splits were
            // handed out rather than all read here
            if let Some(directory) = options.get(TRACE_SPLITS_OPTION) {
                let all = 0..file.rows.len().div_ceil(SPLIT_ROWS) as i64;
                let splits = splits.clone().unwrap_or(all);

                for split in splits.start..splits.end {
                    let marker = format!("{directory}/{}-{split}", std::process::id());

                    std::fs::write(&marker, "").map_err(|e| e.to_string())?;
                }
            }

            Ok(Box::new(file.into_reader_for(splits)))
        }

        fn open_writer(
            &self,
            schema: SchemaRef,
            options: &FormatOptions,
        ) -> Result<Box<dyn format_registry::provider::RecordBatchSink>> {
            let _ = (schema, options);
            Err("this fake only streams".to_string())
        }

        fn can_stream_write(&self) -> bool {
            true
        }

        fn open_streaming_writer(
            &self,
            schema: SchemaRef,
            options: &FormatOptions,
            mut sink: Box<dyn ByteSink>,
        ) -> Result<Box<dyn StreamingSink>> {
            let prefix = options.get("prefix").unwrap_or_default().to_string();

            // the base sets the name on every writer, so the fake insists on it: that makes
            // every `COPY ... TO` in this file a check that it is still there
            let file_name = options
                .get(FILE_NAME_OPTION)
                .ok_or_else(|| format!("the base passed no {FILE_NAME_OPTION}"))?;

            if file_name.contains('/') {
                return Err(format!(
                    "{FILE_NAME_OPTION} is a uri, not a name: {file_name}"
                ));
            }

            if options.get_bool("echo_file_name")?.unwrap_or(false) {
                sink.write(format!("-- file: {file_name}\n").as_bytes())?;
            }

            // a header, so that a file written by a provider that produced no batch at all
            // is still distinguishable from no file
            let header = format!(
                "{prefix}{}\n",
                schema
                    .fields()
                    .iter()
                    .map(|field| field.name().as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            );

            sink.write(header.as_bytes())?;

            Ok(Box::new(LinesWriter { sink, prefix }))
        }
    }

    struct LinesWriter {
        sink: Box<dyn ByteSink>,
        prefix: String,
    }

    impl StreamingSink for LinesWriter {
        fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
            let display = DisplayOptions::default().with_null("");

            let formatters = batch
                .columns()
                .iter()
                .map(|column| ArrayFormatter::try_new(column.as_ref(), &display))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| e.to_string())?;

            let mut out = String::new();

            for row in 0..batch.num_rows() {
                out.push_str(&self.prefix);

                for (idx, formatter) in formatters.iter().enumerate() {
                    if idx > 0 {
                        out.push(',');
                    }

                    out.push_str(&formatter.value(row).to_string());
                }

                out.push('\n');
            }

            self.sink.write(out.as_bytes())
        }

        fn finish(self: Box<Self>) -> Result<()> {
            // a footer is a provider's last write and nothing more: whether the object
            // becomes visible is the base's call, after this returns
            let mut sink = self.sink;
            sink.write(b"-- end\n")
        }
    }

    /// A file the fake wrote, parsed back: the header's names, and a row per line with an
    /// empty field read as a null.
    ///
    /// Every column comes back as a string, which is not laziness — it is what makes the
    /// read tests cover the casting the base does on the way into a table, since a `text`
    /// file column landing in an `int` one is the ordinary case for this format.
    struct LinesFile {
        schema: SchemaRef,
        rows: Vec<Vec<Option<String>>>,
    }

    impl LinesFile {
        fn parse(data: &[u8], prefix: &str) -> Result<LinesFile> {
            let text = std::str::from_utf8(data).map_err(|e| e.to_string())?;

            let mut lines = text
                .lines()
                .map(|line| line.strip_prefix(prefix).unwrap_or(line))
                .filter(|line| *line != "-- end");

            let header = lines
                .next()
                .ok_or_else(|| "the file is empty".to_string())?;

            let fields = header
                .split(',')
                .map(|name| Field::new(name, DataType::Utf8, true))
                .collect::<Vec<_>>();

            let rows = lines
                .map(|line| {
                    line.split(',')
                        .map(|value| (!value.is_empty()).then(|| value.to_string()))
                        .collect()
                })
                .collect();

            Ok(LinesFile {
                schema: Arc::new(Schema::new(fields)),
                rows,
            })
        }

        /// The whole file, through a [`ByteSource`]: what the ranged path is handed instead of
        /// a buffer. The fake's files are small enough to read in one range, which is the one
        /// liberty it takes — a real format reads its footer first.
        fn read(source: &Arc<dyn ByteSource>, prefix: &str) -> Result<LinesFile> {
            let size = source.size()?;

            let data = source
                .get_ranges(&[(0, size)])?
                .into_iter()
                .next()
                .ok_or_else(|| "the source returned no range".to_string())?;

            LinesFile::parse(&data, prefix)
        }

        fn into_reader(self) -> LinesReader {
            self.into_reader_for(None)
        }

        /// Batches of at most [`SPLIT_ROWS`] rows, so that a file big enough to need more than
        /// one is read by more than one — and, restricted to a range of them, so that one
        /// worker of a parallel `COPY FROM` reads only its own.
        fn into_reader_for(self, splits: Option<std::ops::Range<i64>>) -> LinesReader {
            let chunks = self.rows.chunks(SPLIT_ROWS).collect::<Vec<_>>();

            let splits = splits.unwrap_or(0..chunks.len() as i64);

            let batches = chunks
                .into_iter()
                .enumerate()
                .filter(|(split, _)| splits.contains(&(*split as i64)))
                .map(|(_, chunk)| {
                    let columns = (0..self.schema.fields().len())
                        .map(|column| {
                            Arc::new(StringArray::from_iter(
                                chunk.iter().map(|row| row[column].clone()),
                            )) as ArrayRef
                        })
                        .collect();

                    RecordBatch::try_new(self.schema.clone(), columns)
                        .expect("the fake builds a batch of its own schema")
                })
                .collect::<Vec<_>>();

            LinesReader {
                schema: self.schema,
                batches: batches.into_iter(),
            }
        }
    }

    struct LinesReader {
        schema: SchemaRef,
        batches: std::vec::IntoIter<RecordBatch>,
    }

    impl Iterator for LinesReader {
        type Item = std::result::Result<RecordBatch, ArrowError>;

        fn next(&mut self) -> Option<Self::Item> {
            self.batches.next().map(Ok)
        }
    }

    impl arrow::record_batch::RecordBatchReader for LinesReader {
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }
    }

    /// Registers the fake for this backend. Idempotent, because every test in a run shares
    /// one backend and the registry is per-backend.
    pub(crate) fn register() {
        if format_registry::registry::lookup(FORMAT).is_none() {
            format_registry::registry::register(Arc::new(LinesProvider))
                .expect("the fake provider registers");
        }
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use format_registry::options::FILE_NAME_OPTION;
    use pgrx::prelude::*;

    use super::lines::TRACE_SPLITS_OPTION;

    use super::lines;

    #[pg_test]
    fn test_the_arrow_schema_has_the_format_agnostic_functions() {
        for function in ["formats", "schema", "kv_metadata", "file_info", "list"] {
            let exists = Spi::get_one_with_args::<bool>(
                "SELECT EXISTS (
                     SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
                     WHERE n.nspname = 'arrow' AND p.proname = $1
                 )",
                &[function.into()],
            )
            .expect("failed to look for the function");

            assert_eq!(exists, Some(true), "arrow.{function} is missing");
        }
    }

    #[pg_test]
    fn test_the_base_is_not_a_format_of_its_own() {
        // the base ships no provider, deliberately: it has nothing to be the provider of.
        // Everything arrow.formats() lists comes from an installed provider extension, so the
        // one name that must never appear is the base's own.
        let count = Spi::get_one::<i64>(
            "SELECT count(*) FROM arrow.formats()
             WHERE format IN ('arrow_base', 'pg_arrow_base')",
        )
        .expect("failed to count the registered formats");

        assert_eq!(count, Some(0));
    }

    #[pg_test]
    fn test_the_gucs_are_defined_and_settable() {
        // what is under test here is that the GUC exists under the reserved prefix and is
        // settable; the test configuration sets it explicitly rather than relying on the
        // default, because the hook has to be installed at load time either way
        let enabled = Spi::get_one::<String>("SELECT current_setting('arrow.enable_copy_hooks')")
            .expect("arrow.enable_copy_hooks is not defined");

        assert_eq!(enabled.as_deref(), Some("on"));

        Spi::run("SET arrow.enable_copy_hooks = off").expect("failed to turn the hook off");

        let disabled = Spi::get_one::<String>("SELECT current_setting('arrow.enable_copy_hooks')")
            .expect("arrow.enable_copy_hooks is not defined");

        assert_eq!(disabled.as_deref(), Some("off"));

        Spi::run("SET arrow.default_format = 'parquet'").expect("failed to set the format");

        let format = Spi::get_one::<String>("SELECT current_setting('arrow.default_format')")
            .expect("arrow.default_format is not defined");

        assert_eq!(format.as_deref(), Some("parquet"));
    }

    #[pg_test(error = "there is no provider for format \"nope\"")]
    fn test_an_unknown_format_is_reported_as_such() {
        // the uri is never reached: resolving the format comes first, so a typo costs no
        // network round trip
        Spi::run("SELECT * FROM arrow.schema('s3://bucket/key', 'nope')")
            .expect("an unknown format should be reported");
    }

    #[pg_test(error = "there is no provider for format \"../../evil\"")]
    fn test_a_format_name_that_looks_like_a_path_is_not_loaded() {
        // a format name is an identifier; it must not be able to name a library
        Spi::run("SELECT * FROM arrow.schema('/tmp/x', '../../evil')")
            .expect("a path-shaped format name should be refused");
    }

    #[pg_test(error = "could not tell what format /tmp/mystery is in")]
    fn test_an_uninferable_uri_says_so_instead_of_guessing() {
        Spi::run("SELECT * FROM arrow.schema('/tmp/mystery')")
            .expect("an unknown extension should be reported");
    }

    // a uri in a scheme core COPY cannot reach at all: the statement is claimed so that the
    // message is about the format, rather than core's "relative path not allowed for COPY to
    // file" about a uri it was never going to open
    #[pg_test(error = "could not tell what format s3://bucket/data is in")]
    fn test_a_store_uri_with_nothing_to_infer_from_is_still_claimed() {
        Spi::run("CREATE TABLE unnamed_format (id int); COPY unnamed_format TO 's3://bucket/data'")
            .expect("the uri should be claimed and the format reported missing");
    }

    #[pg_test]
    fn test_the_registry_vtable_is_published_and_well_formed() {
        use format_registry::abi::{
            PgArrowRegistryApiV1, PG_ARROW_REGISTRY_MAGIC, PG_ARROW_REGISTRY_RENDEZVOUS_KEY,
            PG_ARROW_REGISTRY_VERSION,
        };

        let key = std::ffi::CString::new(PG_ARROW_REGISTRY_RENDEZVOUS_KEY).unwrap();

        let api = unsafe {
            let slot = pgrx::pg_sys::find_rendezvous_variable(key.as_ptr());

            assert!(!(*slot).is_null(), "the registry vtable was not published");

            &*(*slot as *const PgArrowRegistryApiV1)
        };

        assert_eq!(api.magic, PG_ARROW_REGISTRY_MAGIC);
        assert_eq!(api.version, PG_ARROW_REGISTRY_VERSION);

        // reserved has to stay null until a v1.1 fills a slot, because a provider is entitled
        // to read a non-null slot as "this feature is available"
        for slot in api.reserved {
            assert!(slot.is_null(), "a reserved slot is not null");
        }
    }

    #[pg_test]
    fn test_a_bad_registration_is_reported_rather_than_crashing() {
        // registration goes through the same vtable a real provider uses, so this covers the
        // C boundary and not just the Rust registry underneath it
        use format_registry::abi::{
            PgArrowRegistryApiV1, PGARROW_ERROR, PG_ARROW_REGISTRY_RENDEZVOUS_KEY,
        };

        let key = std::ffi::CString::new(PG_ARROW_REGISTRY_RENDEZVOUS_KEY).unwrap();

        let api = unsafe {
            let slot = pgrx::pg_sys::find_rendezvous_variable(key.as_ptr());
            &*(*slot as *const PgArrowRegistryApiV1)
        };

        // a null descriptor is the cheapest way to prove the boundary reports rather than
        // crashes; a real descriptor needs a provider, which this repository has none of
        let mut error = std::ptr::null_mut();

        let code = unsafe { (api.register_provider)(std::ptr::null(), &mut error) };

        assert_eq!(code, PGARROW_ERROR);
        assert!(!error.is_null(), "a refusal must come with a message");

        let message = unsafe { std::ffi::CStr::from_ptr(error) }
            .to_string_lossy()
            .into_owned();

        assert!(message.contains("must not be null"), "{message}");
    }

    #[pg_test]
    fn test_a_streamed_write_round_trips() {
        let uri = "/tmp/pg_arrow_base_streamed.bin";
        std::fs::remove_file(uri).ok();

        let mut writer = crate::object_store::Writer::open(uri);

        // three writes, because the point of the writer is that the caller never holds the
        // whole file
        for chunk in [
            b"arrow".as_slice(),
            b" base".as_slice(),
            b" wrote this".as_slice(),
        ] {
            writer.write(chunk);
        }

        writer.finish();

        let read = crate::object_store::read_all(uri).expect("the finished file is readable");
        assert_eq!(read.as_ref(), b"arrow base wrote this");

        std::fs::remove_file(uri).ok();
    }

    #[pg_test]
    fn test_dropping_a_writer_without_finishing_writes_nothing() {
        let uri = "/tmp/pg_arrow_base_dropped.bin";
        std::fs::remove_file(uri).ok();

        {
            let mut writer = crate::object_store::Writer::open(uri);
            writer.write(b"never finished");
        }

        // what a `COPY TO` that ereports halfway through has to leave behind. On a local path
        // the empty file itself stays: tier 1 creates and truncates it when it builds the
        // store, because object_store will not write a path that is not there.
        assert_eq!(
            std::fs::metadata(uri).map(|m| m.len()).unwrap_or(0),
            0,
            "an unfinished write must not leave a readable file"
        );

        std::fs::remove_file(uri).ok();
    }

    #[pg_test]
    fn test_a_provider_sink_streams_and_the_base_finishes_it() {
        let uri = "/tmp/pg_arrow_base_sink.bin";
        std::fs::remove_file(uri).ok();

        let upload = crate::io::Upload::open(&crate::io::Place::object(uri));
        let mut sink = upload.sink();

        // this is the shape a provider sees: a `Box<dyn ByteSink>`, no uri, no handle, and no
        // way to complete the object
        for chunk in [b"a provider".as_slice(), b" wrote this".as_slice()] {
            sink.write(chunk).expect("the sink accepts bytes");
        }

        // the provider's writer is done with its sink before the base completes the upload
        drop(sink);
        upload.finish().expect("the base finishes the upload");

        let read = crate::object_store::read_all(uri).expect("the finished file is readable");
        assert_eq!(read.as_ref(), b"a provider wrote this");

        std::fs::remove_file(uri).ok();
    }

    #[pg_test]
    fn test_a_sink_dropped_by_a_failing_provider_writes_nothing() {
        let uri = "/tmp/pg_arrow_base_sink_dropped.bin";
        std::fs::remove_file(uri).ok();

        {
            let upload = crate::io::Upload::open(&crate::io::Place::object(uri));
            let mut sink = upload.sink();
            sink.write(b"half a file").expect("the sink accepts bytes");
            // a provider that returns an error instead of finishing: the base drops the upload
            // rather than calling `finish`, and the object has to go with it
        }

        assert_eq!(
            std::fs::metadata(uri).map(|m| m.len()).unwrap_or(0),
            0,
            "a sink that was never finished must not leave a readable file"
        );

        std::fs::remove_file(uri).ok();
    }

    #[pg_test]
    fn test_a_source_reports_the_ranges_a_provider_asked_for() {
        use format_registry::provider::ByteSource;

        let uri = "/tmp/pg_arrow_base_source.bin";
        std::fs::write(uri, b"0123456789abcdef").expect("failed to write the fixture");

        let source =
            crate::io::FileSource::open(&crate::io::Place::object(uri)).expect("the file is there");
        assert_eq!(source.size().expect("the size is known"), 16);

        // two ranges in one call, which is the whole reason the callback is plural: tier 1
        // fetches them concurrently
        let parts = source
            .get_ranges(&[(0, 4), (12, 4)])
            .expect("the ranges are readable");

        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].as_ref(), b"0123");
        assert_eq!(parts[1].as_ref(), b"cdef");

        std::fs::remove_file(uri).ok();
    }

    #[pg_test]
    fn test_a_missing_file_is_a_message_rather_than_an_error() {
        // a provider's callback cannot be allowed to longjmp, so the source reports a missing
        // file as a value even though every other read path in the base ereports
        let missing = crate::io::FileSource::open(&crate::io::Place::object(
            "/tmp/pg_arrow_base_not_here.bin",
        ));

        let message = missing.expect_err("there is no such file");
        assert!(
            message.contains("there is no file at"),
            "unexpected message: {message}"
        );
    }

    #[pg_test]
    fn test_a_staging_place_is_read_and_written_as_a_local_file() {
        use format_registry::provider::ByteSource;

        // the base's own file for a STDIO or PROGRAM statement. A uri is tier 1's to allow or
        // refuse; this one is not, and routing it there would refuse a `TO PROGRAM` by a user
        // who holds pg_execute_server_program and no file-access role at all.
        let place = crate::io::Place::new(
            &crate::endpoint::Endpoint::Program("cat".to_string()),
            "/tmp/pg_arrow_base_staging.bin",
        );

        let upload = crate::io::Upload::open(&place);
        let mut sink = upload.sink();
        sink.write(b"0123456789abcdef")
            .expect("the sink accepts bytes");
        drop(sink);
        upload.finish().expect("the base finishes the staging file");

        let source = crate::io::FileSource::open(&place).expect("the staging file is there");
        assert_eq!(source.size().expect("the size is known"), 16);

        let parts = source
            .get_ranges(&[(0, 4), (12, 4)])
            .expect("the ranges are readable");

        assert_eq!(parts[0].as_ref(), b"0123");
        assert_eq!(parts[1].as_ref(), b"cdef");

        assert_eq!(
            crate::io::read_all(&place)
                .expect("the whole file is readable")
                .as_ref(),
            b"0123456789abcdef"
        );

        std::fs::remove_file(place.path()).ok();
    }

    fn written(uri: &str) -> String {
        let read = crate::object_store::read_all(uri).expect("the copied file is readable");

        String::from_utf8(read.to_vec()).expect("the fake format writes text")
    }

    #[pg_test]
    fn test_the_default_format_claims_a_uri_no_extension_implies() {
        lines::register();

        let uri = "/tmp/pg_arrow_base_default_format.dat";
        std::fs::remove_file(uri).ok();

        Spi::run(
            "CREATE TABLE defaulted (id int);
                  INSERT INTO defaulted VALUES (1);
                  SET arrow.default_format = 'testlines'",
        )
        .expect("failed to set up the table");

        // without the setting this statement is core's, and core would write text under this
        // name rather than report that it could not tell what format to use
        Spi::run(&format!("COPY defaulted TO '{uri}'")).expect("the COPY is carried out");

        assert_eq!(written(uri), "id\n1\n-- end\n");

        Spi::run("RESET arrow.default_format").expect("failed to reset the format");

        std::fs::remove_file(uri).ok();
    }

    #[pg_test]
    fn test_a_format_library_is_looked_for_without_being_loaded() {
        // what recognition asks before claiming a uri whose extension no loaded provider has:
        // this library is installed, under the name the convention predicts
        assert!(crate::providers::library_exists("arrow_base"));

        assert!(!crate::providers::library_exists("definitelynotaformat"));
        assert!(!crate::providers::library_exists("../../evil"));
        assert!(!crate::providers::library_exists(""));
    }

    /// Nests composite types `lv1` inside `lv2` inside ... up to `levels`, and returns the SQL
    /// for a table with one column of the deepest one.
    fn nested_types(table: &str, levels: usize) -> String {
        format!(
            "CREATE TYPE lv1 AS (x int);
             DO $$
             BEGIN
               FOR i IN 2..{levels} LOOP
                 EXECUTE format('CREATE TYPE lv%s AS (x lv%s)', i, i - 1);
               END LOOP;
             END $$;
             CREATE TABLE {table} (v lv{levels});
             INSERT INTO {table} VALUES (NULL);"
        )
    }

    #[pg_test]
    fn test_a_schema_at_the_nesting_limit_is_written() {
        lines::register();

        let uri = "/tmp/pg_arrow_base_nesting_limit.testlines";
        std::fs::remove_file(uri).ok();

        Spi::run(&nested_types(
            "deep",
            pg_arrow_convert::schema::MAX_NESTING_LEVELS,
        ))
        .expect("failed to set up the nested types");

        Spi::run(&format!("COPY deep TO '{uri}' WITH (format 'testlines')"))
            .expect("a schema at the limit is written");

        assert_eq!(written(uri), "v\n\n-- end\n");

        std::fs::remove_file(uri).ok();
    }

    // a deeper schema would be written and then be unreadable by anything, including this
    // process, so it is refused before a file exists
    #[pg_test(
        error = "column \"v\" nests 61 levels deep, and a file's schema may nest at most 60; a deeper one cannot be read back"
    )]
    fn test_a_schema_past_the_nesting_limit_is_refused() {
        lines::register();

        Spi::run(&nested_types(
            "too_deep",
            pg_arrow_convert::schema::MAX_NESTING_LEVELS + 1,
        ))
        .expect("failed to set up the nested types");

        Spi::run(
            "COPY too_deep TO '/tmp/pg_arrow_base_nesting_past_limit.testlines' \
             WITH (format 'testlines')",
        )
        .expect("unreachable: the COPY is refused");
    }

    #[pg_test]
    fn test_the_listing_reports_what_the_registry_dispatches_on() {
        lines::register();

        // the fake both streams and range-reads, and both flags default to false, so this says
        // they are read off the provider rather than defaulted. Which path a statement then
        // takes is what the `COPY` tests below cover.
        let capabilities = Spi::get_one::<String>(
            "SELECT reads_ranges || ',' || writes_streaming
             FROM arrow.formats() WHERE format = 'testlines'",
        )
        .expect("failed to read the listing");

        assert_eq!(capabilities.as_deref(), Some("true,true"));
    }

    #[pg_test]
    fn test_a_listing_expands_the_same_pattern_a_copy_does() {
        lines::register();

        let dir = "/tmp/pg_arrow_base_list_pattern";
        std::fs::remove_dir_all(dir).ok();
        std::fs::create_dir_all(format!("{dir}/sub")).expect("failed to make the directory");

        Spi::run("CREATE TABLE list_pattern (id int); INSERT INTO list_pattern VALUES (1)")
            .expect("failed to set up the table");

        for name in ["a.testlines", "b.testlines", "c.other", "sub/d.testlines"] {
            Spi::run(&format!(
                "COPY list_pattern TO '{dir}/{name}' WITH (format 'testlines')"
            ))
            .expect("failed to write the file");
        }

        // tier 1 takes a uri as a literal prefix, so a pattern used to list nothing at all --
        // for the very uri a COPY FROM reads every match of. The pattern also stops at a `/`
        // here exactly as it does there, so the file in the subdirectory is not a match
        let matched = Spi::get_one::<String>(&format!(
            "SELECT string_agg(location, ',' ORDER BY location)
             FROM arrow.list('{dir}/*.testlines')"
        ))
        .expect("failed to list the pattern");

        assert_eq!(
            matched.as_deref(),
            Some(format!("{dir}/a.testlines,{dir}/b.testlines").as_str())
        );

        // the format each would be read as comes with it, and a prefix still lists everything
        // under it including the one no format claims
        let formats = Spi::get_one::<String>(&format!(
            "SELECT string_agg(coalesce(format, 'none'), ',' ORDER BY location)
             FROM arrow.list('{dir}/')"
        ))
        .expect("failed to list the prefix");

        assert_eq!(
            formats.as_deref(),
            Some("testlines,testlines,none,testlines")
        );

        std::fs::remove_dir_all(dir).ok();
    }

    #[pg_test(error = "/tmp/pg_arrow_base_bad_pattern/[*.testlines is not a valid pattern")]
    fn test_a_listing_says_so_when_a_pattern_will_not_parse() {
        // an unparseable pattern used to come back as an empty listing, which reads as "there
        // is nothing there" for a uri that names nothing in the first place
        Spi::run("SELECT * FROM arrow.list('/tmp/pg_arrow_base_bad_pattern/[*.testlines')")
            .expect("unreachable: the pattern is refused");
    }

    #[pg_test]
    fn test_copy_a_table_to_a_uri() {
        lines::register();

        let uri = "/tmp/pg_arrow_base_copy_table.testlines";
        std::fs::remove_file(uri).ok();

        Spi::run(
            "CREATE TABLE copy_me (id int, name text, ok boolean);
             INSERT INTO copy_me VALUES (1, 'one', true), (2, NULL, false);",
        )
        .expect("failed to set up the table");

        Spi::run(&format!(
            "COPY copy_me TO '{uri}' WITH (format 'testlines')"
        ))
        .expect("the COPY is carried out");

        assert_eq!(written(uri), "id,name,ok\n1,one,true\n2,,false\n-- end\n");

        std::fs::remove_file(uri).ok();
    }

    #[pg_test]
    fn test_copy_a_query_to_a_uri_flushes_more_than_one_batch() {
        lines::register();

        let uri = "/tmp/pg_arrow_base_copy_query.testlines";
        std::fs::remove_file(uri).ok();

        // more rows than a batch holds, so that the receiver's flushing is what is under test
        // rather than a single conversion
        Spi::run(&format!(
            "COPY (SELECT i FROM generate_series(1, 2500) i) TO '{uri}' \
             WITH (format 'testlines')"
        ))
        .expect("the COPY is carried out");

        let contents = written(uri);
        let lines: Vec<_> = contents.lines().collect();

        // a header, 2500 rows and a footer
        assert_eq!(lines.len(), 2502);
        assert_eq!(lines[0], "i");
        assert_eq!(lines[1], "1");
        assert_eq!(lines[2500], "2500");
        assert_eq!(lines[2501], "-- end");

        std::fs::remove_file(uri).ok();
    }

    #[pg_test]
    fn test_copy_a_column_list_to_a_uri() {
        lines::register();

        let uri = "/tmp/pg_arrow_base_copy_columns.testlines";
        std::fs::remove_file(uri).ok();

        Spi::run(
            "CREATE TABLE copy_columns (a int, b text, c int);
             INSERT INTO copy_columns VALUES (1, 'x', 3);",
        )
        .expect("failed to set up the table");

        Spi::run(&format!(
            "COPY copy_columns (c, a) TO '{uri}' WITH (format 'testlines')"
        ))
        .expect("the COPY is carried out");

        // the listed columns, in the order the statement listed them
        assert_eq!(written(uri), "c,a\n3,1\n-- end\n");

        std::fs::remove_file(uri).ok();
    }

    #[pg_test]
    fn test_a_format_option_reaches_the_provider() {
        lines::register();

        let uri = "/tmp/pg_arrow_base_copy_option.testlines";
        std::fs::remove_file(uri).ok();

        Spi::run(&format!(
            "COPY (SELECT 1 AS n) TO '{uri}' WITH (format 'testlines', prefix '> ')"
        ))
        .expect("the COPY is carried out");

        assert_eq!(written(uri), "> n\n> 1\n-- end\n");

        std::fs::remove_file(uri).ok();
    }

    #[pg_test]
    fn test_the_file_name_reaches_the_provider() {
        lines::register();

        // the name a format may legitimately want — `orders.parquet.zst` is zstd — without the
        // directory, the bucket or anything else a provider has no business with
        let uri = "/tmp/pg_arrow_base_copy_file_name.testlines";
        std::fs::remove_file(uri).ok();

        Spi::run(&format!(
            "COPY (SELECT 1 AS n) TO '{uri}' WITH (format 'testlines', echo_file_name 'on')"
        ))
        .expect("the COPY is carried out");

        assert_eq!(
            written(uri),
            "-- file: pg_arrow_base_copy_file_name.testlines\nn\n1\n-- end\n"
        );

        std::fs::remove_file(uri).ok();
    }

    #[pg_test]
    fn test_the_format_is_inferred_from_the_extension() {
        lines::register();

        let uri = "/tmp/pg_arrow_base_copy_inferred.testlines";
        std::fs::remove_file(uri).ok();

        Spi::run(&format!("COPY (SELECT 1 AS n) TO '{uri}'")).expect("the COPY is carried out");

        assert_eq!(written(uri), "n\n1\n-- end\n");

        std::fs::remove_file(uri).ok();
    }

    /// Runs a statement that is expected to fail, and returns its message.
    ///
    /// A postgres `ERROR` is a longjmp rather than an `Err`, so the only way to assert on one
    /// *and* on what it left behind is to catch it. The transaction is aborted afterwards and
    /// nothing may touch the database again — which is why every caller here only looks at the
    /// filesystem.
    fn failure_of(statement: &str) -> String {
        PgTryBuilder::new(|| {
            Spi::run(statement).expect("spi failed for a reason other than an error");
            panic!("the statement was expected to fail: {statement}");
        })
        .catch_others(|cause| format!("{cause:?}"))
        .execute()
    }

    #[pg_test]
    fn test_an_option_the_provider_rejects_is_reported_before_anything_is_written() {
        lines::register();

        let uri = "/tmp/pg_arrow_base_copy_bad_option.testlines";
        std::fs::remove_file(uri).ok();

        let failure = failure_of(&format!(
            "COPY (SELECT 1 AS n) TO '{uri}' WITH (format 'testlines', nonsense 'x')"
        ));

        assert!(
            failure.contains("no option named"),
            "unexpected failure: {failure}"
        );

        assert!(
            !std::path::Path::new(uri).exists(),
            "a rejected statement must not have opened the file"
        );
    }

    #[pg_test]
    fn test_a_statement_cannot_set_an_option_the_base_reserves() {
        lines::register();

        // nothing guards the `__` prefix except that a provider is offered every option a
        // statement names and no provider recognizes a reserved one
        let failure = failure_of(&format!(
            "COPY (SELECT 1 AS n) TO '/tmp/pg_arrow_base_copy_reserved.testlines' \
             WITH (format 'testlines', {FILE_NAME_OPTION} 'elsewhere.testlines')"
        ));

        assert!(
            failure.contains("no option named"),
            "unexpected failure: {failure}"
        );
    }

    #[pg_test(error = "cannot copy from view \"copy_view\"")]
    fn test_copying_from_a_view_is_refused_the_way_core_refuses_it() {
        lines::register();

        Spi::run("CREATE VIEW copy_view AS SELECT 1 AS n").expect("failed to set up the view");

        Spi::run(
            "COPY copy_view TO '/tmp/pg_arrow_base_copy_view.testlines' \
             WITH (format 'testlines')",
        )
        .expect("unreachable: a view cannot be copied from");
    }

    #[pg_test(error = "cannot copy from sequence \"copy_sequence\"")]
    fn test_copying_from_a_sequence_is_refused_the_way_core_refuses_it() {
        lines::register();

        Spi::run("CREATE SEQUENCE copy_sequence").expect("failed to set up the sequence");

        // the query form is allowed, since what it copies is a result and not the relation
        Spi::run(
            "COPY (SELECT last_value FROM copy_sequence) \
             TO '/tmp/pg_arrow_base_copy_sequence.testlines' WITH (format 'testlines')",
        )
        .expect("a query over a sequence is copyable");

        Spi::run(
            "COPY copy_sequence TO '/tmp/pg_arrow_base_copy_sequence.testlines' \
             WITH (format 'testlines')",
        )
        .expect("unreachable: a sequence cannot be copied from");
    }

    // PG 18 is the first version to read a populated matview rather than refuse the relation
    // kind outright, and the hook follows whatever core does with it
    #[cfg(not(pre_pg18))]
    #[pg_test]
    fn test_copying_from_a_materialized_view_follows_core() {
        lines::register();

        let uri = "/tmp/pg_arrow_base_copy_matview.testlines";
        std::fs::remove_file(uri).ok();

        Spi::run(
            "CREATE TABLE matview_source(n int); \
             INSERT INTO matview_source SELECT i FROM generate_series(1, 10) i; \
             CREATE MATERIALIZED VIEW copy_matview AS SELECT * FROM matview_source",
        )
        .expect("failed to set up the matview");

        Spi::run(&format!(
            "COPY copy_matview TO '{uri}' WITH (format 'testlines')"
        ))
        .expect("a populated matview is copyable on PG 18 and later");

        Spi::run("CREATE TABLE matview_result(n int)").expect("failed to set up the target");

        Spi::run(&format!(
            "COPY matview_result FROM '{uri}' WITH (format 'testlines')"
        ))
        .expect("failed to read the matview's rows back");

        assert_eq!(
            Spi::get_one::<i64>("SELECT count(*) FROM matview_result")
                .expect("failed to count the rows"),
            Some(10)
        );

        std::fs::remove_file(uri).ok();
    }

    #[cfg(pre_pg18)]
    #[pg_test(error = "cannot copy from materialized view \"copy_matview\"")]
    fn test_copying_from_a_materialized_view_follows_core() {
        lines::register();

        Spi::run(
            "CREATE TABLE matview_source(n int); \
             CREATE MATERIALIZED VIEW copy_matview AS SELECT * FROM matview_source",
        )
        .expect("failed to set up the matview");

        Spi::run(
            "COPY copy_matview TO '/tmp/pg_arrow_base_copy_matview.testlines' \
             WITH (format 'testlines')",
        )
        .expect("unreachable: a matview cannot be copied from before PG 18");
    }

    // a matview created WITH NO DATA holds no rows to read, which PG 18 is the first to say
    // rather than to return an empty copy for
    #[cfg(not(pre_pg18))]
    #[pg_test(error = "cannot copy from unpopulated materialized view \"copy_empty_matview\"")]
    fn test_copying_from_an_unpopulated_materialized_view_is_refused() {
        lines::register();

        Spi::run(
            "CREATE TABLE matview_source(n int); \
             CREATE MATERIALIZED VIEW copy_empty_matview AS SELECT * FROM matview_source \
             WITH NO DATA",
        )
        .expect("failed to set up the matview");

        Spi::run(
            "COPY copy_empty_matview TO '/tmp/pg_arrow_base_copy_matview.testlines' \
             WITH (format 'testlines')",
        )
        .expect("unreachable: an unpopulated matview holds no rows");
    }

    #[pg_test(error = "cannot copy from foreign table \"copy_foreign_table\"")]
    fn test_copying_from_a_foreign_table_is_refused_the_way_core_refuses_it() {
        lines::register();

        std::fs::write("/tmp/pg_arrow_base_foreign_table.csv", "1\n2\n3\n")
            .expect("failed to write the csv the foreign table reads");

        Spi::run(
            "CREATE EXTENSION IF NOT EXISTS file_fdw; \
             CREATE SERVER copy_file_server FOREIGN DATA WRAPPER file_fdw; \
             CREATE FOREIGN TABLE copy_foreign_table (n int) SERVER copy_file_server \
             OPTIONS (filename '/tmp/pg_arrow_base_foreign_table.csv', format 'csv')",
        )
        .expect("failed to set up the foreign table");

        // as with a sequence, the query form copies a result rather than the relation
        Spi::run(
            "COPY (SELECT * FROM copy_foreign_table) \
             TO '/tmp/pg_arrow_base_copy_foreign.testlines' WITH (format 'testlines')",
        )
        .expect("a query over a foreign table is copyable");

        Spi::run(
            "COPY copy_foreign_table TO '/tmp/pg_arrow_base_copy_foreign.testlines' \
             WITH (format 'testlines')",
        )
        .expect("unreachable: a foreign table cannot be copied from");
    }

    #[pg_test]
    fn test_a_failing_query_leaves_no_file_behind() {
        lines::register();

        let uri = "/tmp/pg_arrow_base_copy_failed.testlines";
        std::fs::remove_file(uri).ok();

        // the division fails after the header and a batch have already gone into the upload,
        // which is the case that distinguishes discarding an upload from completing a
        // truncated one
        let failure = failure_of(&format!(
            "COPY (SELECT 1 / (i - 2000) FROM generate_series(1, 2000) i) TO '{uri}' \
             WITH (format 'testlines')"
        ));

        assert!(
            failure.contains("division by zero"),
            "unexpected failure: {failure}"
        );

        assert_eq!(
            std::fs::metadata(uri).map(|m| m.len()).unwrap_or(0),
            0,
            "a failed COPY TO must not leave a readable file"
        );

        std::fs::remove_file(uri).ok();
    }

    // -- the COPY FROM path ----------------------------------------------------------------
    //
    // The fake reads every column as a string, so each of these also covers the cast from the
    // file's type to the table's, which is the part of a `COPY FROM` a provider never sees.

    #[pg_test]
    fn test_a_table_round_trips_through_a_uri() {
        lines::register();

        let uri = "/tmp/pg_arrow_base_from_roundtrip.testlines";
        std::fs::remove_file(uri).ok();

        Spi::run(
            "CREATE TABLE round_trip (id int, name text, ok boolean);
             INSERT INTO round_trip VALUES (1, 'one', true), (2, NULL, false);",
        )
        .expect("failed to set up the table");

        Spi::run(&format!(
            "COPY round_trip TO '{uri}' WITH (format 'testlines')"
        ))
        .expect("the COPY TO is carried out");

        Spi::run(&format!(
            "CREATE TABLE came_back (LIKE round_trip);
             COPY came_back FROM '{uri}' WITH (format 'testlines');"
        ))
        .expect("the COPY FROM is carried out");

        let same = Spi::get_one::<i64>(
            "SELECT count(*) FROM (
                 TABLE round_trip EXCEPT ALL TABLE came_back
                 UNION ALL
                 TABLE came_back EXCEPT ALL TABLE round_trip
             ) difference",
        )
        .expect("failed to compare the tables");

        assert_eq!(same, Some(0), "the rows did not survive the round trip");

        std::fs::remove_file(uri).ok();
    }

    #[pg_test]
    fn test_copy_from_reports_the_rows_it_inserted() {
        lines::register();

        let uri = "/tmp/pg_arrow_base_from_count.testlines";
        std::fs::remove_file(uri).ok();

        // more rows than one of the fake's batches holds, so the count is the sum over batches
        // and files rather than whatever the last one happened to be
        Spi::run(&format!(
            "COPY (SELECT i FROM generate_series(1, 2500) i) TO '{uri}' \
             WITH (format 'testlines')"
        ))
        .expect("the COPY TO is carried out");

        Spi::run("CREATE TABLE counted (i int)").expect("failed to set up the table");

        Spi::run(&format!(
            "COPY counted FROM '{uri}' WITH (format 'testlines')"
        ))
        .expect("the COPY FROM is carried out");

        let count = Spi::get_one::<i64>("SELECT count(*) FROM counted").expect("failed to count");
        let sum = Spi::get_one::<i64>("SELECT sum(i) FROM counted").expect("failed to sum");

        assert_eq!(count, Some(2500));
        assert_eq!(sum, Some(2500 * 2501 / 2));

        std::fs::remove_file(uri).ok();
    }

    #[pg_test]
    fn test_copy_from_a_column_list_leaves_the_other_columns_to_the_table() {
        lines::register();

        let uri = "/tmp/pg_arrow_base_from_columns.testlines";
        std::fs::remove_file(uri).ok();

        Spi::run(&format!(
            "COPY (SELECT 7 AS a) TO '{uri}' WITH (format 'testlines')"
        ))
        .expect("the COPY TO is carried out");

        // the defaults, the generated column and the constraint are all core's work on the
        // tuples our callback hands it, which is the reason the stream is the binary copy format
        Spi::run(&format!(
            "CREATE TABLE partial (
                 a int CHECK (a > 0),
                 b text DEFAULT 'defaulted',
                 c int GENERATED ALWAYS AS (a * 2) STORED
             );
             COPY partial (a) FROM '{uri}' WITH (format 'testlines');"
        ))
        .expect("the COPY FROM is carried out");

        let row = Spi::get_one::<String>("SELECT format('%s|%s|%s', a, b, c) FROM partial")
            .expect("failed to read the row back");

        assert_eq!(row.as_deref(), Some("7|defaulted|14"));

        std::fs::remove_file(uri).ok();
    }

    #[pg_test]
    fn test_match_by_name_reorders_the_columns_and_by_position_does_not() {
        lines::register();

        let uri = "/tmp/pg_arrow_base_from_match_by.testlines";
        std::fs::remove_file(uri).ok();

        // written b,a and read into a table that is a,b: by name the values follow their
        // names, by position they follow their place
        Spi::run(&format!(
            "COPY (SELECT 'bee' AS b, 'aye' AS a) TO '{uri}' WITH (format 'testlines')"
        ))
        .expect("the COPY TO is carried out");

        Spi::run(&format!(
            "CREATE TABLE matched (a text, b text);
             COPY matched FROM '{uri}' WITH (format 'testlines', match_by 'name');
             COPY matched FROM '{uri}' WITH (format 'testlines', match_by 'position');"
        ))
        .expect("both COPY FROMs are carried out");

        let rows = Spi::get_one::<String>(
            "SELECT string_agg(format('%s/%s', a, b), ' ' ORDER BY a) FROM matched",
        )
        .expect("failed to read the rows back");

        assert_eq!(rows.as_deref(), Some("aye/bee bee/aye"));

        std::fs::remove_file(uri).ok();
    }

    #[pg_test]
    fn test_copy_from_a_uri_with_a_wildcard_reads_every_matching_file() {
        lines::register();

        let dir = "/tmp/pg_arrow_base_from_glob";
        std::fs::remove_dir_all(dir).ok();
        std::fs::create_dir_all(dir).expect("failed to make the directory");

        for part in 1..=3 {
            Spi::run(&format!(
                "COPY (SELECT {part} AS n) TO '{dir}/part-{part}.testlines' \
                 WITH (format 'testlines')"
            ))
            .expect("the COPY TO is carried out");
        }

        // a file the pattern must not pick up, in the same directory
        std::fs::write(format!("{dir}/notes.txt"), b"not a testlines file")
            .expect("failed to write the decoy");

        Spi::run(&format!(
            "CREATE TABLE globbed (n int);
             COPY globbed FROM '{dir}/*.testlines' WITH (format 'testlines');"
        ))
        .expect("the COPY FROM is carried out");

        let sum = Spi::get_one::<i64>("SELECT sum(n) FROM globbed").expect("failed to sum");

        assert_eq!(sum, Some(6), "all three parts should have been read");

        std::fs::remove_dir_all(dir).ok();
    }

    #[pg_test]
    fn test_a_copy_to_that_fails_partway_leaves_the_old_file_alone() {
        lines::register();

        let uri = "/tmp/pg_arrow_base_kept.testlines";
        std::fs::remove_file(uri).ok();

        Spi::run(&format!(
            "COPY (SELECT 1 AS n) TO '{uri}' WITH (format 'testlines')"
        ))
        .expect("the first COPY TO is carried out");

        let before = std::fs::read(uri).expect("failed to read the file written");

        // the fourth row divides by zero, so the statement fails with a file half produced
        let failure = failure_of(&format!(
            "COPY (SELECT 1 / (i - 4) AS n FROM generate_series(1, 5) i) TO '{uri}' \
             WITH (format 'testlines')"
        ));

        assert!(
            failure.contains("division by zero"),
            "unexpected failure: {failure}"
        );

        assert_eq!(
            std::fs::read(uri).expect("the old file should still be there"),
            before,
            "a failed COPY TO must not have replaced the file"
        );

        std::fs::remove_file(uri).ok();
    }

    #[pg_test]
    fn test_a_wildcard_in_the_last_component_does_not_walk_subdirectories() {
        lines::register();

        let dir = "/tmp/pg_arrow_base_glob_shallow";
        let unreadable = format!("{dir}/locked");

        std::fs::remove_dir_all(dir).ok();
        std::fs::create_dir_all(&unreadable).expect("failed to make the directories");

        Spi::run(&format!(
            "COPY (SELECT 7 AS n) TO '{dir}/part.testlines' WITH (format 'testlines')"
        ))
        .expect("the COPY TO is carried out");

        // the owner's own bits deny the owner too: a recursive list of the prefix fails here,
        // and nothing inside it could have matched a pattern that cannot cross a separator
        std::fs::set_permissions(
            &unreadable,
            std::os::unix::fs::PermissionsExt::from_mode(0o000),
        )
        .expect("failed to lock the directory");

        let read = Spi::run(&format!(
            "CREATE TABLE shallow_globbed (n int);
             COPY shallow_globbed FROM '{dir}/*.testlines' WITH (format 'testlines');"
        ));

        std::fs::set_permissions(
            &unreadable,
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .expect("failed to unlock the directory");

        read.expect("an unreadable sibling directory should not fail the pattern");

        let sum = Spi::get_one::<i64>("SELECT sum(n) FROM shallow_globbed").expect("failed to sum");

        assert_eq!(sum, Some(7));

        std::fs::remove_dir_all(dir).ok();
    }

    #[pg_test]
    fn test_a_where_clause_is_applied_to_the_rows_read() {
        lines::register();

        let uri = "/tmp/pg_arrow_base_from_where.testlines";
        std::fs::remove_file(uri).ok();

        Spi::run(&format!(
            "COPY (SELECT i FROM generate_series(1, 10) i) TO '{uri}' \
             WITH (format 'testlines')"
        ))
        .expect("the COPY TO is carried out");

        Spi::run(&format!(
            "CREATE TABLE filtered (i int);
             COPY filtered FROM '{uri}' WITH (format 'testlines') WHERE i > 7;"
        ))
        .expect("the COPY FROM is carried out");

        let kept =
            Spi::get_one::<String>("SELECT string_agg(i::text, ',' ORDER BY i) FROM filtered")
                .expect("failed to read the rows back");

        assert_eq!(kept.as_deref(), Some("8,9,10"));

        std::fs::remove_file(uri).ok();
    }

    #[pg_test(error = "unrecognized match_by method: sideways")]
    fn test_a_bad_match_by_value_is_reported() {
        lines::register();

        Spi::run(
            "CREATE TABLE bad_match_by (n int);
             COPY bad_match_by FROM '/tmp/pg_arrow_base_never_read.testlines' \
                 WITH (format 'testlines', match_by 'sideways');",
        )
        .expect("unreachable: the value is not a match_by");
    }

    #[pg_test(error = "match_by is only meaningful for COPY FROM")]
    fn test_match_by_on_a_copy_to_is_refused() {
        lines::register();

        Spi::run(
            "COPY (SELECT 1 AS n) TO '/tmp/pg_arrow_base_never_written.testlines' \
             WITH (format 'testlines', match_by 'name')",
        )
        .expect("unreachable: there is nothing to match a written file against");
    }

    #[pg_test]
    fn test_a_missing_file_is_reported_and_nothing_is_inserted() {
        lines::register();

        Spi::run("CREATE TABLE never_filled (n int)").expect("failed to set up the table");

        let failure = failure_of(
            "COPY never_filled FROM '/tmp/pg_arrow_base_absent.testlines' \
             WITH (format 'testlines')",
        );

        assert!(
            failure.contains("there is no file at"),
            "unexpected failure: {failure}"
        );
    }

    // -- CREATE TABLE ... FROM '<uri>' -----------------------------------------------------
    //
    // The fake's fields are all `Utf8`, so every inferred column is `text`. What these cover is
    // the rewriting: that the columns arrive in the file's order, that Postgres is left to
    // create the table, and that our reloptions are gone by the time it does.

    /// Writes a two-column file and returns its uri.
    fn a_file_with_two_columns(name: &str) -> String {
        let uri = format!("/tmp/pg_arrow_base_{name}.testlines");

        std::fs::remove_file(&uri).ok();

        Spi::run(&format!(
            "COPY (SELECT i AS id, 'row ' || i AS label FROM generate_series(1, 3) i) \
             TO '{uri}' WITH (format 'testlines')"
        ))
        .expect("the COPY TO is carried out");

        uri
    }

    fn columns_of(table: &str) -> Vec<String> {
        Spi::connect(|client| {
            let rows = client
                .select(
                    &format!(
                        "SELECT attname || ' ' || format_type(atttypid, atttypmod) \
                         FROM pg_attribute WHERE attrelid = '{table}'::regclass \
                         AND attnum > 0 AND NOT attisdropped ORDER BY attnum"
                    ),
                    None,
                    &[],
                )
                .expect("failed to read the columns");

            rows.filter_map(|row| row.get::<String>(1).expect("a column description"))
                .collect()
        })
    }

    #[pg_test]
    fn test_load_from_creates_the_columns_and_loads_the_rows() {
        lines::register();

        let uri = a_file_with_two_columns("load_from");

        Spi::run(&format!(
            "CREATE TABLE loaded () WITH (load_from = '{uri}')"
        ))
        .expect("the CREATE TABLE is carried out");

        assert_eq!(columns_of("loaded"), ["id text", "label text"]);

        let rows = Spi::get_one::<i64>("SELECT count(*) FROM loaded").expect("failed to count");
        let third = Spi::get_one::<String>("SELECT label FROM loaded WHERE id = '3'")
            .expect("failed to read a row");

        assert_eq!(rows, Some(3));
        assert_eq!(third.as_deref(), Some("row 3"));

        std::fs::remove_file(&uri).ok();
    }

    #[pg_test]
    fn test_definition_from_creates_the_columns_and_no_rows() {
        lines::register();

        let uri = a_file_with_two_columns("definition_from");

        Spi::run(&format!(
            "CREATE TABLE defined () WITH (definition_from = '{uri}')"
        ))
        .expect("the CREATE TABLE is carried out");

        assert_eq!(columns_of("defined"), ["id text", "label text"]);

        let rows = Spi::get_one::<i64>("SELECT count(*) FROM defined").expect("failed to count");

        assert_eq!(rows, Some(0), "definition_from read the file's rows");

        std::fs::remove_file(&uri).ok();
    }

    #[pg_test]
    fn test_an_explicit_format_is_taken_out_of_the_reloptions() {
        lines::register();

        // the uri's extension would have resolved the format anyway; what is under test is that
        // naming it does not leave Postgres with a reloption it refuses
        let uri = a_file_with_two_columns("create_format_option");

        Spi::run(&format!(
            "CREATE TABLE named_format () WITH (load_from = '{uri}', format = 'testlines', \
             fillfactor = 70)"
        ))
        .expect("the CREATE TABLE is carried out");

        let rows =
            Spi::get_one::<i64>("SELECT count(*) FROM named_format").expect("failed to count");

        assert_eq!(rows, Some(3));

        // the reloption that was not ours is still the table's
        let fillfactor = Spi::get_one::<String>(
            "SELECT unnest(reloptions) FROM pg_class WHERE oid = 'named_format'::regclass",
        )
        .expect("failed to read the reloptions");

        assert_eq!(fillfactor.as_deref(), Some("fillfactor=70"));

        std::fs::remove_file(&uri).ok();
    }

    #[pg_test]
    fn test_if_not_exists_on_an_existing_table_reads_nothing() {
        lines::register();

        let uri = a_file_with_two_columns("create_if_not_exists");

        Spi::run("CREATE TABLE already_there (n int)").expect("failed to set up the table");

        Spi::run(&format!(
            "CREATE TABLE IF NOT EXISTS already_there () WITH (load_from = '{uri}')"
        ))
        .expect("the CREATE TABLE is a no-op");

        assert_eq!(columns_of("already_there"), ["n integer"]);

        let rows =
            Spi::get_one::<i64>("SELECT count(*) FROM already_there").expect("failed to count");

        assert_eq!(rows, Some(0), "the file was loaded into an existing table");

        std::fs::remove_file(&uri).ok();
    }

    #[pg_test]
    fn test_the_two_uri_options_contradict_each_other() {
        lines::register();

        let uri = a_file_with_two_columns("create_both_options");

        let failure = failure_of(&format!(
            "CREATE TABLE contradicted () WITH (load_from = '{uri}', definition_from = '{uri}')"
        ));

        assert!(
            // the failure is a debug-formatted report, so the quotes in the message are escaped
            failure.contains("cannot specify both"),
            "unexpected failure: {failure}"
        );

        std::fs::remove_file(&uri).ok();
    }

    #[pg_test]
    fn test_a_repeated_uri_option_is_reported_as_postgres_reports_one() {
        lines::register();

        let uri = a_file_with_two_columns("create_repeated_option");

        let failure = failure_of(&format!(
            "CREATE TABLE repeated () WITH (load_from = '{uri}', load_from = '{uri}')"
        ));

        assert!(
            failure.contains("load_from\\\" specified more than once"),
            "unexpected failure: {failure}"
        );

        std::fs::remove_file(&uri).ok();
    }

    #[pg_test]
    fn test_the_statement_is_refused_when_the_columns_are_not_the_files_to_give() {
        lines::register();

        let uri = a_file_with_two_columns("create_refusals");

        Spi::run("CREATE TABLE a_parent (n int)").expect("failed to set up the table");

        let refusals = [
            (
                "(id text) WITH (load_from =",
                "when column definitions are provided",
            ),
            (
                "() INHERITS (a_parent) WITH (load_from =",
                "when the table inherits columns",
            ),
        ];

        for (shape, expected) in refusals {
            let failure = failure_of(&format!("CREATE TABLE refused {shape} '{uri}')"));

            assert!(
                failure.contains(expected),
                "unexpected failure for {shape}: {failure}"
            );
        }

        std::fs::remove_file(&uri).ok();
    }

    #[pg_test]
    fn test_a_create_table_that_is_not_ours_still_means_what_it_meant() {
        lines::register();

        Spi::run("CREATE TABLE plain (n int) WITH (fillfactor = 60)")
            .expect("core carries out the CREATE TABLE");

        assert_eq!(columns_of("plain"), ["n integer"]);

        // an unrecognized reloption is still Postgres' to refuse
        let failure = failure_of("CREATE TABLE odd (n int) WITH (loud_from = '/tmp/x')");

        assert!(
            failure.contains("unrecognized parameter \\\"loud_from\\\""),
            "unexpected failure: {failure}"
        );
    }

    #[pg_test]
    fn test_a_copy_that_is_not_ours_still_means_what_it_meant() {
        lines::register();

        let uri = "/tmp/pg_arrow_base_copy_csv.csv";
        std::fs::remove_file(uri).ok();

        // the hook is installed in this cluster, so what is under test is that it declines:
        // a csv COPY has to reach core and be written by core
        Spi::run(&format!(
            "COPY (SELECT 1 AS n, 'two' AS t) TO '{uri}' WITH (format 'csv', header true)"
        ))
        .expect("core carries out the csv COPY");

        assert_eq!(written(uri), "n,t\n1,two\n");

        std::fs::remove_file(uri).ok();
    }

    // -- the ends that are not a uri -------------------------------------------------------
    //
    // `TO PROGRAM`, `FROM PROGRAM`, `TO STDOUT` and `FROM STDIN` are all one mechanism: the file
    // is written to or read from a temporary file, and the bytes are copied at the far end. What
    // these cover is that the statement is claimed at all, that the copy goes the right way, and
    // that a failure at the far end fails the statement rather than being lost.

    #[pg_test]
    fn test_copy_to_program_pipes_the_whole_file_through_the_command() {
        lines::register();

        let path = "/tmp/pg_arrow_base_to_program.testlines";
        std::fs::remove_file(path).ok();

        Spi::run(
            "CREATE TABLE to_program (id int, name text);
             INSERT INTO to_program VALUES (1, 'one'), (2, NULL);",
        )
        .expect("failed to set up the table");

        // `cat` is the identity, so what lands in the file is exactly what the provider wrote --
        // header and footer included, which is what says the program got the *finished* file and
        // not the batches as they were produced
        Spi::run(&format!(
            "COPY to_program TO PROGRAM 'cat > {path}' WITH (format 'testlines')"
        ))
        .expect("the COPY TO PROGRAM is carried out");

        assert_eq!(written(path), "id,name\n1,one\n2,\n-- end\n");

        std::fs::remove_file(path).ok();
    }

    #[pg_test]
    fn test_copy_from_program_reads_what_the_command_wrote() {
        lines::register();

        let path = "/tmp/pg_arrow_base_from_program.testlines";
        std::fs::remove_file(path).ok();

        Spi::run(&format!(
            "CREATE TABLE from_program (id int, name text);
             INSERT INTO from_program VALUES (1, 'one'), (2, NULL);
             COPY from_program TO '{path}' WITH (format 'testlines');
             CREATE TABLE through_a_pipe (LIKE from_program);
             COPY through_a_pipe FROM PROGRAM 'cat {path}' WITH (format 'testlines');"
        ))
        .expect("the COPY FROM PROGRAM is carried out");

        let rows = Spi::get_one::<String>(
            "SELECT string_agg(id || ':' || coalesce(name, '<null>'), ',' ORDER BY id)
             FROM through_a_pipe",
        )
        .expect("failed to read the table");

        assert_eq!(rows.as_deref(), Some("1:one,2:<null>"));

        std::fs::remove_file(path).ok();
    }

    #[pg_test]
    fn test_a_program_that_fails_fails_the_statement() {
        lines::register();

        Spi::run("CREATE TABLE unread (id int)").expect("failed to set up the table");

        // the program's exit status arrives only when its pipe is closed, which is after the
        // last byte has been read -- so a status dropped on the floor would leave this statement
        // succeeding against an empty, truncated or half-written file
        let failure = failure_of("COPY unread FROM PROGRAM 'exit 7' WITH (format 'testlines')");

        assert!(
            failure.contains("exit 7") && failure.contains("failed"),
            "unexpected failure: {failure}"
        );
    }

    #[pg_test]
    fn test_a_program_the_shell_cannot_run_is_reported_as_a_failure() {
        lines::register();

        Spi::run("CREATE TABLE missing_program (id int)").expect("failed to set up the table");

        // OpenPipeStream succeeds here -- the shell starts, and it is the shell that exits 127 --
        // so a typo in a command arrives as the same nonzero status as any other failure, which
        // is also how core reports one
        let failure = failure_of(
            "COPY missing_program FROM PROGRAM 'pg_arrow_base_no_such_command' \
             WITH (format 'testlines')",
        );

        assert!(failure.contains("failed"), "unexpected failure: {failure}");
    }

    #[pg_test]
    fn test_copy_to_program_needs_the_privilege_for_it() {
        lines::register();

        Spi::run(
            "CREATE TABLE guarded (id int);
             CREATE ROLE no_programs;
             GRANT SELECT ON guarded TO no_programs;
             SET ROLE no_programs;",
        )
        .expect("failed to set up the role");

        // this hook runs before the `standard_ProcessUtility` that would otherwise make this
        // check, so a statement served here is a statement core never sees: the check has to
        // happen in the base or `COPY TO PROGRAM` becomes available to everyone
        let failure =
            failure_of("COPY guarded TO PROGRAM 'cat > /dev/null' WITH (format 'testlines')");

        assert!(
            failure.contains("pg_execute_server_program"),
            "unexpected failure: {failure}"
        );
    }

    #[pg_test]
    fn test_a_statement_copies_to_stdout_and_back_from_stdin() {
        // the only test here that needs a client: `TO STDOUT` writes the file into the protocol
        // stream, and nothing inside this backend is one. So the file makes a full round trip
        // through psql -- out of one statement's stdout, into another's stdin -- which is also
        // the way the forms are actually used, by `\copy`.
        let port = Spi::get_one::<String>("SELECT current_setting('port')")
            .expect("failed to read the port")
            .expect("the port is set");

        psql(
            &port,
            "CREATE TABLE out_and_back (id int, name text);
                     INSERT INTO out_and_back VALUES (1, 'one'), (2, NULL);
                     CREATE TABLE back_again (LIKE out_and_back);",
        );

        let mut copy_out = std::process::Command::new("psql")
            .args(["-p", &port, "-h", "localhost", "-d", "pgrx_tests", "-c"])
            .arg("COPY out_and_back TO STDOUT WITH (format 'testlines')")
            .stdout(std::process::Stdio::piped())
            .spawn()
            .expect("failed to run psql");

        let mut file = Vec::new();

        {
            use std::io::Read;

            copy_out
                .stdout
                .as_mut()
                .expect("psql has a stdout")
                .read_to_end(&mut file)
                .expect("failed to read the file from psql");
        }

        assert!(
            copy_out.wait().expect("failed to wait for psql").success(),
            "the COPY TO STDOUT did not succeed"
        );

        // the file as the provider wrote it, having travelled through the protocol as CopyData
        assert_eq!(
            String::from_utf8(file.clone()).expect("the fake format writes text"),
            "id,name\n1,one\n2,\n-- end\n"
        );

        let mut copy_in = std::process::Command::new("psql")
            .args(["-p", &port, "-h", "localhost", "-d", "pgrx_tests", "-c"])
            .arg("COPY back_again FROM STDIN WITH (format 'testlines')")
            .stdin(std::process::Stdio::piped())
            .spawn()
            .expect("failed to run psql");

        {
            use std::io::Write;

            let stdin = copy_in.stdin.as_mut().expect("psql has a stdin");

            stdin.write_all(&file).expect("failed to write the file");
            stdin.flush().expect("failed to flush");
        }

        assert!(
            copy_in.wait().expect("failed to wait for psql").success(),
            "the COPY FROM STDIN did not succeed"
        );

        // read back through psql too: the rows were inserted in another backend's transaction,
        // which this one cannot see
        let rows = psql(
            &port,
            "SELECT string_agg(id || ':' || coalesce(name, '<null>'), ',' ORDER BY id)
             FROM back_again",
        );

        assert_eq!(rows.trim(), "1:one,2:<null>");

        psql(&port, "DROP TABLE out_and_back, back_again");
    }

    // -- file_size_bytes, where one statement writes several files

    /// The files a split statement left under its uri, in name order.
    fn files_under(uri: &str) -> Vec<std::path::PathBuf> {
        let mut files = std::fs::read_dir(uri)
            .expect("the uri of a split statement is a folder")
            .map(|entry| entry.expect("the folder is readable").path())
            .collect::<Vec<_>>();

        files.sort();

        files
    }

    #[pg_test]
    fn test_file_size_bytes_writes_several_files_that_read_back_as_one() {
        lines::register();

        let uri = "/tmp/pg_arrow_base_split.testlines";
        std::fs::remove_dir_all(uri).ok();

        // about 110 bytes a row in the fake's text form, so 50000 rows is a handful of files
        // at a 1MB threshold
        Spi::run(
            "CREATE TABLE split_source (id int, payload text);
             INSERT INTO split_source
             SELECT i, repeat('x', 100) FROM generate_series(1, 50000) i;
             CREATE TABLE split_back (LIKE split_source);",
        )
        .expect("failed to set up the tables");

        Spi::run(&format!(
            "COPY split_source TO '{uri}' WITH (format 'testlines', file_size_bytes '1MB')"
        ))
        .expect("the COPY is carried out");

        let files = files_under(uri);

        assert!(
            files.len() > 1,
            "5MB of rows at a 1MB threshold is more than one file: {files:?}"
        );

        for (id, file) in files.iter().enumerate() {
            assert_eq!(
                file.file_name()
                    .expect("a file has a name")
                    .to_string_lossy(),
                format!("data_{id}.testlines"),
                "the files are named in the order they were written"
            );

            let contents = std::fs::read_to_string(file).expect("the file is readable");

            // a whole file each, header and footer included, rather than slices of one file
            assert!(
                contents.starts_with("id,payload\n") && contents.ends_with("-- end\n"),
                "a split file is not a complete one: {file:?}"
            );
        }

        // every file but the last is over the threshold, and none is far over it: a file is
        // closed by the first batch that crosses the line, and a batch is 1024 rows
        for file in &files[..files.len() - 1] {
            let size = std::fs::metadata(file).expect("the file is there").len();

            assert!(
                (1024 * 1024..2 * 1024 * 1024).contains(&size),
                "{file:?} is {size} bytes, which is not what a 1MB threshold asks for"
            );
        }

        // and the whole set reads back as what was written, which is the point of splitting
        // into whole files: a pattern names them all and the rows come back in order
        Spi::run(&format!(
            "COPY split_back FROM '{uri}/*' WITH (format 'testlines')"
        ))
        .expect("the COPY FROM is carried out");

        let differences = Spi::get_one::<i64>(
            "SELECT count(*) FROM split_source s
             FULL JOIN split_back b USING (id)
             WHERE s.payload IS DISTINCT FROM b.payload",
        )
        .expect("failed to compare the tables");

        assert_eq!(differences, Some(0));

        std::fs::remove_dir_all(uri).ok();
    }

    #[pg_test]
    fn test_each_split_file_is_named_to_the_provider_as_itself() {
        lines::register();

        // what a format reads out of a file's name — `orders.parquet.zst` is zstd — has to
        // keep working when the statement writes several files, so each one is told its own
        // name and the suffix the statement wrote is kept
        let uri = "/tmp/pg_arrow_base_split_names.testlines.gz";
        std::fs::remove_dir_all(uri).ok();

        Spi::run(
            "CREATE TABLE split_named (payload text);
             INSERT INTO split_named SELECT repeat('y', 100) FROM generate_series(1, 30000);",
        )
        .expect("failed to set up the table");

        Spi::run(&format!(
            "COPY split_named TO '{uri}'
             WITH (format 'testlines', file_size_bytes '1MB', echo_file_name 'on')"
        ))
        .expect("the COPY is carried out");

        for (id, file) in files_under(uri).iter().enumerate() {
            let contents = std::fs::read_to_string(file).expect("the file is readable");

            assert!(
                contents.starts_with(&format!("-- file: data_{id}.testlines.gz\n")),
                "{file:?} was not told its own name: {}",
                contents.lines().next().unwrap_or_default()
            );
        }

        std::fs::remove_dir_all(uri).ok();
    }

    #[pg_test]
    fn test_a_file_size_smaller_than_a_file_is_refused() {
        lines::register();

        let failure = failure_of(
            "COPY (SELECT 1 AS n) TO '/tmp/pg_arrow_base_split_tiny.testlines' \
             WITH (format 'testlines', file_size_bytes '512kB')",
        );

        assert!(
            failure.contains("at least 1MB"),
            "unexpected failure: {failure}"
        );
    }

    #[pg_test]
    fn test_a_file_size_in_an_unknown_unit_is_refused() {
        lines::register();

        let failure = failure_of(
            "COPY (SELECT 1 AS n) TO '/tmp/pg_arrow_base_split_unit.testlines' \
             WITH (format 'testlines', file_size_bytes '4TB')",
        );

        assert!(
            failure.contains("unknown unit"),
            "unexpected failure: {failure}"
        );
    }

    #[pg_test]
    fn test_file_size_bytes_is_refused_for_the_ends_that_cannot_hold_a_second_file() {
        lines::register();

        let to_program = failure_of(
            "COPY (SELECT 1 AS n) TO PROGRAM 'cat > /dev/null' \
             WITH (format 'testlines', file_size_bytes '1MB')",
        );

        assert!(
            to_program.contains("needs a uri"),
            "unexpected failure: {to_program}"
        );

        // `TO STDOUT` has to go through a client, since SPI refuses the form outright and so
        // never reaches the hook that would refuse the option
        let port = Spi::get_one::<String>("SELECT current_setting('port')")
            .expect("failed to read the port")
            .expect("the port is set");

        let to_stdout = failing_psql(
            &port,
            "COPY (SELECT 1 AS n) TO STDOUT WITH (format 'testlines', file_size_bytes '1MB')",
        );

        assert!(
            to_stdout.contains("needs a uri"),
            "unexpected failure: {to_stdout}"
        );
    }

    #[pg_test]
    fn test_file_size_bytes_is_refused_for_a_copy_from() {
        lines::register();

        let uri = "/tmp/pg_arrow_base_split_from.testlines";
        std::fs::remove_file(uri).ok();

        std::fs::write(uri, "n\n1\n-- end\n").expect("failed to write the file");

        Spi::run("CREATE TABLE split_reader (n int)").expect("failed to set up the table");

        let failure = failure_of(&format!(
            "COPY split_reader FROM '{uri}' WITH (format 'testlines', file_size_bytes '1MB')"
        ));

        assert!(
            failure.contains("only meaningful for COPY TO"),
            "unexpected failure: {failure}"
        );

        std::fs::remove_file(uri).ok();
    }

    // -- arrow.max_parallel_workers, where several processes fill one folder

    /// Lays down a table large enough for the parallel path to take it, and asserts that it is:
    /// a statement under the page threshold falls back to the single process path, and a test
    /// that silently exercised the fallback would prove nothing.
    fn create_parallel_source(table: &str, rows: i64) {
        Spi::run(&format!(
            "CREATE TABLE {table} (id int, payload text);
             INSERT INTO {table}
             SELECT i, repeat('x', 100) FROM generate_series(1, {rows}) i;"
        ))
        .expect("failed to set up the table");

        let blocks = Spi::get_one::<i64>(&format!(
            "SELECT (pg_relation_size('{table}') / current_setting('block_size')::int)::bigint"
        ))
        .expect("failed to measure the table")
        .expect("a table has a size");

        assert!(
            blocks >= 1024,
            "{table} is {blocks} pages, which is below what the parallel path asks for"
        );
    }

    #[pg_test]
    fn test_a_parallel_copy_to_a_folder_writes_every_row_once() {
        lines::register();

        let uri = "/tmp/pg_arrow_base_parallel.testlines";
        std::fs::remove_dir_all(uri).ok();

        create_parallel_source("parallel_source", 100_000);

        Spi::run(
            "CREATE TABLE parallel_back (LIKE parallel_source);
             SET max_parallel_workers TO 4;
             SET arrow.max_parallel_workers TO 4;",
        )
        .expect("failed to set up the statement");

        Spi::run(&format!(
            "COPY parallel_source TO '{uri}' WITH (format 'testlines', file_size_bytes '1MB')"
        ))
        .expect("the COPY is carried out");

        let files = files_under(uri);

        // five participants claim four ranges each, and the rows of a range are well under the
        // threshold here, so most files are short. A single process statement leaves exactly
        // one short file -- its tail -- so several of them is proof the rows were written as
        // ranges rather than as one stream. Whether a worker was actually launched is the
        // server's to decide: the leader claims ranges too, and the file ids come out of
        // shared memory either way.
        let short = files
            .iter()
            .filter(|file| std::fs::metadata(file).expect("the file is there").len() < 1024 * 1024)
            .count();

        assert!(
            short >= 2,
            "{short} of {} files are short, which is what a single process statement leaves",
            files.len()
        );

        // no two participants named the same file, which is what the shared id claim is for:
        // the ids run from 0 without a gap, and a clobbered file would have cost rows
        let mut ids = files
            .iter()
            .map(|file| {
                let name = file
                    .file_name()
                    .expect("a file has a name")
                    .to_string_lossy()
                    .to_string();

                name.trim_start_matches("data_")
                    .trim_end_matches(".testlines")
                    .parse::<usize>()
                    .unwrap_or_else(|_| panic!("a file of the folder is named \"{name}\""))
            })
            .collect::<Vec<_>>();

        ids.sort();

        assert_eq!(
            ids,
            (0..files.len()).collect::<Vec<_>>(),
            "the file ids are claimed once each"
        );

        // and every row is in the folder exactly once, which is the whole point: the ranges
        // cover every block of the table and no block twice
        Spi::run(&format!(
            "COPY parallel_back FROM '{uri}/*' WITH (format 'testlines')"
        ))
        .expect("the COPY FROM is carried out");

        let differences = Spi::get_one::<i64>(
            "SELECT count(*) FROM parallel_source s
             FULL JOIN parallel_back b USING (id)
             WHERE s.payload IS DISTINCT FROM b.payload",
        )
        .expect("failed to compare the tables");

        assert_eq!(differences, Some(0));

        std::fs::remove_dir_all(uri).ok();
    }

    #[pg_test]
    fn test_a_statement_the_parallel_path_cannot_split_still_writes_one_file() {
        lines::register();

        let uri = "/tmp/pg_arrow_base_parallel_single.testlines";
        std::fs::remove_file(uri).ok();
        std::fs::remove_dir_all(uri).ok();

        create_parallel_source("parallel_whole", 100_000);

        Spi::run("SET max_parallel_workers TO 4; SET arrow.max_parallel_workers TO 4;")
            .expect("failed to set up the statement");

        // no file_size_bytes, so the destination is one file written from its header to its
        // footer: there is nothing to hand a second process, and the statement is carried out
        // here as it always was
        Spi::run(&format!(
            "COPY parallel_whole TO '{uri}' WITH (format 'testlines')"
        ))
        .expect("the COPY is carried out");

        assert!(
            std::fs::metadata(uri)
                .expect("the destination is there")
                .is_file(),
            "a statement with no file_size_bytes writes a file and not a folder"
        );

        // and so is one with a folder destination but no relation to split into ranges
        let query_uri = "/tmp/pg_arrow_base_parallel_query.testlines";
        std::fs::remove_dir_all(query_uri).ok();

        Spi::run(&format!(
            "COPY (SELECT id, payload FROM parallel_whole) TO '{query_uri}'
             WITH (format 'testlines', file_size_bytes '1MB')"
        ))
        .expect("the COPY of a query is carried out");

        assert!(
            !files_under(query_uri).is_empty(),
            "the statement wrote nothing"
        );

        std::fs::remove_file(uri).ok();
        std::fs::remove_dir_all(query_uri).ok();
    }

    #[pg_test]
    fn test_a_parallel_copy_to_reports_the_rows_every_participant_copied() {
        // through psql, because the row count of a COPY is in its command tag and nothing
        // inside this backend reads one. It is also the one thing of a parallel statement the
        // leader cannot count for itself: the files are finished by the participants, so the
        // count is summed in shared memory.
        let port = Spi::get_one::<String>("SELECT current_setting('port')")
            .expect("failed to read the port")
            .expect("the port is set");

        let uri = "/tmp/pg_arrow_base_parallel_tag.testlines";
        std::fs::remove_dir_all(uri).ok();

        psql(
            &port,
            "CREATE TABLE parallel_tag (id int, payload text);
             INSERT INTO parallel_tag
             SELECT i, repeat('x', 100) FROM generate_series(1, 100000) i;",
        );

        let tag = psql(
            &port,
            &format!(
                "SET max_parallel_workers TO 4;
                 SET arrow.max_parallel_workers TO 4;
                 COPY parallel_tag TO '{uri}'
                 WITH (format 'testlines', file_size_bytes '1MB')"
            ),
        );

        psql(&port, "DROP TABLE parallel_tag");

        // the last tag of the three, the two SETs having printed their own
        assert_eq!(
            tag.trim().lines().last(),
            Some("COPY 100000"),
            "the statement did not report every row it copied: {tag}"
        );

        std::fs::remove_dir_all(uri).ok();
    }

    // -- arrow.max_parallel_workers, where several processes decode one file

    /// A source file of `rows` rows, which is `rows / lines::SPLIT_ROWS` splits for the
    /// workers to divide, and an empty folder to leave the markers in.
    fn create_parallel_from_source(uri: &str, trace: &str, rows: i64) {
        std::fs::remove_file(uri).ok();
        std::fs::remove_dir_all(trace).ok();
        std::fs::create_dir_all(trace).expect("failed to make the trace folder");

        Spi::run(&format!(
            "COPY (SELECT i FROM generate_series(1, {rows}) i) TO '{uri}'
             WITH (format 'testlines')"
        ))
        .expect("failed to write the source file");

        Spi::run("SET max_parallel_workers TO 4; SET arrow.max_parallel_workers TO 4;")
            .expect("failed to set up the statement");
    }

    /// Which process read which split, out of the markers the fake left: a pair per read.
    fn traced_splits(trace: &str) -> Vec<(u32, i64)> {
        let mut read = std::fs::read_dir(trace)
            .expect("the trace folder is there")
            .map(|entry| {
                let name = entry
                    .expect("a folder entry is readable")
                    .file_name()
                    .to_string_lossy()
                    .to_string();

                let (pid, split) = name
                    .split_once('-')
                    .unwrap_or_else(|| panic!("a marker is named \"{name}\""));

                (
                    pid.parse().expect("a marker names a pid"),
                    split.parse().expect("a marker names a split"),
                )
            })
            .collect::<Vec<_>>();

        read.sort();

        read
    }

    #[pg_test]
    fn test_a_parallel_copy_from_inserts_every_row_once_in_file_order() {
        lines::register();

        let uri = "/tmp/pg_arrow_base_parallel_from.testlines";
        let trace = "/tmp/pg_arrow_base_parallel_from_trace";

        create_parallel_from_source(uri, trace, 5000);

        Spi::run("CREATE TABLE parallel_from (i int)").expect("failed to set up the table");

        Spi::run(&format!(
            "COPY parallel_from FROM '{uri}'
             WITH (format 'testlines', {TRACE_SPLITS_OPTION} '{trace}')"
        ))
        .expect("the COPY FROM is carried out");

        // the rows the file holds, in the order it holds them: the splits are decoded at once
        // but only the leader inserts, and it inserts them in file order
        let out_of_order = Spi::get_one::<i64>(
            "SELECT count(*) FROM (
                 SELECT i, row_number() OVER (ORDER BY ctid) AS n FROM parallel_from
             ) s WHERE i <> n",
        )
        .expect("failed to read the table back");

        assert_eq!(
            Spi::get_one::<i64>("SELECT count(*) FROM parallel_from"),
            Ok(Some(5000))
        );
        assert_eq!(
            out_of_order,
            Some(0),
            "the rows did not arrive in file order"
        );

        // every split read exactly once, and none of them here: the leader counts the splits
        // out of the footer and inserts what comes back, and a split read twice would have
        // doubled its rows
        let read = traced_splits(trace);

        let mut splits = read.iter().map(|(_, split)| *split).collect::<Vec<_>>();
        splits.sort();

        assert_eq!(
            splits,
            vec![0, 1, 2, 3, 4],
            "the splits were not read once each: {read:?}"
        );

        assert!(
            !read.iter().any(|(pid, _)| *pid == std::process::id()),
            "the leader decoded a split itself: {read:?}"
        );

        std::fs::remove_file(uri).ok();
        std::fs::remove_dir_all(trace).ok();
    }

    #[pg_test]
    fn test_a_parallel_copy_from_reads_every_file_of_a_wildcard() {
        lines::register();

        let dir = "/tmp/pg_arrow_base_parallel_from_glob";
        let trace = "/tmp/pg_arrow_base_parallel_from_glob_trace";

        std::fs::remove_dir_all(dir).ok();
        std::fs::create_dir_all(dir).expect("failed to make the source folder");

        create_parallel_from_source(&format!("{dir}/first.testlines"), trace, 3000);
        create_parallel_from_source(&format!("{dir}/second.testlines"), trace, 2000);

        Spi::run("CREATE TABLE parallel_from_glob (i int)").expect("failed to set up the table");

        Spi::run(&format!(
            "COPY parallel_from_glob FROM '{dir}/*.testlines'
             WITH (format 'testlines', {TRACE_SPLITS_OPTION} '{trace}')"
        ))
        .expect("the COPY FROM is carried out");

        assert_eq!(
            Spi::get_one::<i64>("SELECT count(*) FROM parallel_from_glob"),
            Ok(Some(5000))
        );

        // the splits of all the files make up one list the workers stride over, so the five of
        // them are read once each whichever file they came from
        let read = traced_splits(trace);

        assert_eq!(
            read.len(),
            5,
            "the splits were not read once each: {read:?}"
        );

        std::fs::remove_dir_all(dir).ok();
        std::fs::remove_dir_all(trace).ok();
    }

    #[pg_test]
    fn test_a_statement_the_parallel_path_cannot_split_is_read_here() {
        lines::register();

        let uri = "/tmp/pg_arrow_base_parallel_from_single.testlines";
        let trace = "/tmp/pg_arrow_base_parallel_from_single_trace";

        create_parallel_from_source(uri, trace, 5000);

        // a column with a default: filling one is an insert the leader may not make in parallel
        // mode, so the statement is read in this process as it always was
        Spi::run("CREATE TABLE parallel_from_default (i int, extra int DEFAULT 7)")
            .expect("failed to set up the table");

        Spi::run(&format!(
            "COPY parallel_from_default (i) FROM '{uri}'
             WITH (format 'testlines', {TRACE_SPLITS_OPTION} '{trace}')"
        ))
        .expect("the COPY FROM is carried out");

        assert_eq!(
            Spi::get_one::<i64>("SELECT count(*) FROM parallel_from_default WHERE extra = 7"),
            Ok(Some(5000))
        );

        let read = traced_splits(trace);

        assert!(
            read.iter().all(|(pid, _)| *pid == std::process::id()),
            "the statement was handed to a worker after all: {read:?}"
        );

        std::fs::remove_file(uri).ok();
        std::fs::remove_dir_all(trace).ok();
    }

    /// One `psql -c` that is expected to fail, and the message it failed with.
    fn failing_psql(port: &str, command: &str) -> String {
        let output = std::process::Command::new("psql")
            .args(["-p", port, "-h", "localhost", "-d", "pgrx_tests", "-c"])
            .arg(command)
            .output()
            .expect("failed to run psql");

        assert!(
            !output.status.success(),
            "the statement was expected to fail: {command}"
        );

        String::from_utf8(output.stderr).expect("psql's output is utf-8")
    }

    /// One `psql -c`, and its output, against the test cluster. Asserts the command succeeded,
    /// since every use of it here is setup or verification rather than the thing under test.
    fn psql(port: &str, command: &str) -> String {
        let output = std::process::Command::new("psql")
            .args([
                "-p",
                port,
                "-h",
                "localhost",
                "-d",
                "pgrx_tests",
                "-At",
                "-c",
            ])
            .arg(command)
            .output()
            .expect("failed to run psql");

        assert!(
            output.status.success(),
            "psql failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        String::from_utf8(output.stdout).expect("psql's output is utf-8")
    }
}
