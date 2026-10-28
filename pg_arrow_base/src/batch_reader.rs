//! A file to tuples, for whichever format the statement named.
//!
//! This is the mirror of [`crate::batch_writer`] and the middle of `COPY FROM`: the provider
//! turns a file into `RecordBatch`es, `pg-arrow-convert` turns those into datums, and this
//! encodes the datums as the **binary `COPY` stream** Postgres already knows how to read.
//!
//! That last step is what makes the read path short. Core `COPY FROM` can take its input from a
//! callback, so rather than building tuples and inserting them — which would mean reimplementing
//! defaults, generated columns, triggers, partition routing, `WHERE` and `ON ERROR` — the base
//! hands core bytes in the format it reads from `COPY ... WITH (format binary)` and lets
//! `CopyFrom` do all of it. The buffer here is a byte stream, not a tuple store.
//!
//! The reader comes in the two shapes provider ABI v2 allows, and the choice is the provider's:
//!
//! - **ranged**, if it reports `can_read_ranges`: it is handed a [`FileSource`] and fetches the
//!   footer and then the splits it needs, so a 40 GB file is never in the backend's memory.
//! - **whole**, otherwise: the base reads the object and hands over the bytes.
//!
//! A uri with a `*` in it is a set of files read in listing order, which is how a `COPY FROM`
//! reads a directory of parts written by a parallel `COPY TO`.

use std::ops::Range;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatchReader;
use arrow_cast::{cast_with_options, CastOptions};
use arrow_schema::Schema;
use format_registry::options::FormatOptions;
use format_registry::provider::FormatProvider;
use glob::{MatchOptions, Pattern};
use pg_arrow_convert::arrow_to_pg::context::{
    collect_arrow_to_pg_attribute_contexts, ArrowToPgAttributeContext,
};
use pg_arrow_convert::arrow_to_pg::to_pg_datum;
use pg_arrow_convert::coerce::{
    ensure_file_schema_match_tupledesc_schema,
    error_if_copy_from_match_by_position_with_generated_columns, overlay_file_geospatial_crs,
};
use pg_arrow_convert::integer_cast::round_for_integer_cast;
use pg_arrow_convert::match_by::{column_index_by_name, MatchBy};
use pg_arrow_convert::pgrx_utils::{collect_attributes_for, CollectAttributesFor};
use pg_arrow_convert::schema::arrow_schema_from_attributes;
use pg_arrow_convert::type_compat::geometry::reset_postgis_context;
use pg_arrow_convert::type_compat::map::reset_map_context;
use pgrx::pg_sys::{
    fmgr_info, getTypeBinaryOutputInfo, varlena, Datum, FmgrInfo, FormData_pg_attribute,
    InvalidOid, SendFunctionCall,
};
use pgrx::{
    check_for_interrupts, prelude::*, vardata_any, varsize_any_exhdr, void_mut_ptr,
    AllocatedByPostgres, PgMemoryContexts, PgTupleDesc,
};

use crate::copy_progress;
use crate::io::{self, FileSource, Place};
use crate::object_store;

/// One file, opened and matched up against the tuple descriptor.
struct FileReader {
    reader: Box<dyn RecordBatchReader + Send>,
    attribute_contexts: Vec<ArrowToPgAttributeContext>,
    match_by: MatchBy,
    /// How many rows the file says it has, when the format could say: out of the bytes the
    /// base held, or out of the footer a ranged provider read through the source.
    num_rows: Option<i64>,
}

impl FileReader {
    /// `Err` rather than a report, so that a uri which turns out to be a pattern can be listed
    /// instead. A schema that does not match the table is *not* one of these: that is a
    /// statement-level failure and is reported where it is found.
    fn open(
        provider: &Arc<dyn FormatProvider>,
        place: &Place,
        options: &FormatOptions,
        tupledesc_schema: SchemaRef,
        attributes: &[FormData_pg_attribute],
        match_by: MatchBy,
    ) -> Result<FileReader, String> {
        Self::open_splits(
            provider,
            place,
            options,
            tupledesc_schema,
            attributes,
            match_by,
            None,
        )
    }

    /// `splits` restricts the reader to a range of the file's splits — the rows of those
    /// parquet row groups, of those IPC record batches — which is what one worker of a parallel
    /// `COPY FROM` reads. `None` is the whole file, and the only shape a provider that cannot
    /// read ranges is asked for.
    fn open_splits(
        provider: &Arc<dyn FormatProvider>,
        place: &Place,
        options: &FormatOptions,
        tupledesc_schema: SchemaRef,
        attributes: &[FormData_pg_attribute],
        match_by: MatchBy,
        splits: Option<Range<i64>>,
    ) -> Result<FileReader, String> {
        let (reader, num_rows) = if provider.can_read_ranges() {
            let source = Arc::new(
                FileSource::open(place)
                    .map_err(|failure| with_folder_hint(failure, place, provider))?,
            );

            // the footer, before the reader is opened: the row count is what the progress view
            // extrapolates its byte total from, and on this path nothing else ever sees it
            let num_rows = provider
                .ranged_file_info(source.clone())
                .ok()
                .and_then(|info| info.num_rows);

            (
                provider.open_ranged_reader(source, options, splits)?,
                num_rows,
            )
        } else {
            let data = io::read_all(place).ok_or_else(|| {
                with_folder_hint(
                    format!("there is no file at {}", place.path()),
                    place,
                    provider,
                )
            })?;

            // only worth asking on this path: it is the one where the whole file is already
            // here, so the answer costs a footer parse rather than a download
            let num_rows = provider
                .file_info(&data)
                .ok()
                .and_then(|info| info.num_rows);

            (provider.open_reader(data, options)?, num_rows)
        };

        let file_schema = reader.schema();

        // the tupledesc schema only knows the crs of the target columns, so the geometry and
        // geography columns take the crs the file has for them. Before the coercion below,
        // which builds its cast targets from the tupledesc schema and would otherwise drop the
        // crs of a geospatial field nested in a struct, list or map.
        let tupledesc_schema = Arc::new(Schema::new_with_metadata(
            overlay_file_geospatial_crs(&file_schema, &tupledesc_schema, match_by),
            tupledesc_schema.metadata().clone(),
        ));

        let cast_to_types = ensure_file_schema_match_tupledesc_schema(
            file_schema,
            tupledesc_schema.clone(),
            attributes,
            match_by,
        );

        let attribute_contexts = collect_arrow_to_pg_attribute_contexts(
            attributes,
            tupledesc_schema.fields(),
            Some(cast_to_types),
        );

        Ok(FileReader {
            reader,
            attribute_contexts,
            match_by,
            num_rows,
        })
    }

    fn attribute_count(&self) -> usize {
        self.attribute_contexts.len()
    }

    /// One row of a batch as datums, cast where the file's type is not the table's.
    fn tuple_datums(&self, record_batch: RecordBatch) -> Vec<Option<Datum>> {
        let mut datums = Vec::with_capacity(self.attribute_contexts.len());

        for (attribute_idx, attribute_context) in self.attribute_contexts.iter().enumerate() {
            let name = attribute_context.name();

            let column_array = match self.match_by {
                MatchBy::Position => record_batch
                    .columns()
                    .get(attribute_idx)
                    .unwrap_or_else(|| error!("column {name} not found")),
                // the attribute names of the relation are truncated to the identifier length
                // limit, so a longer column name of the file is matched by its truncation
                MatchBy::Name => column_index_by_name(record_batch.schema_ref().fields(), name)
                    .map(|column_idx| record_batch.column(column_idx))
                    .unwrap_or_else(|| error!("column {name} not found")),
            };

            let datum = if attribute_context.needs_cast() {
                // a cast that fails at runtime has to fail the statement rather than produce a
                // null, which is what the default options would do
                let cast_options = CastOptions {
                    safe: false,
                    ..Default::default()
                };

                // arrow truncates towards zero when casting to an integer, Postgres rounds
                let column_array =
                    round_for_integer_cast(column_array, attribute_context.data_type(), name);

                let casted =
                    cast_with_options(&column_array, attribute_context.data_type(), &cast_options)
                        .unwrap_or_else(|e| error!("failed to cast column {name}: {e}"));

                to_pg_datum(casted.to_data(), attribute_context)
            } else {
                to_pg_datum(column_array.to_data(), attribute_context)
            };

            datums.push(datum);
        }

        datums
    }
}

/// The binary `COPY` stream core pulls on, and the readers filling it.
///
/// Lives for the whole statement, is pulled on from a callback core invokes, and is the only
/// state the read path has. One per `COPY FROM`; nested statements get a stack of them, which
/// is [`crate::copy_from`]'s problem rather than this type's.
pub(crate) struct CopyFromStream {
    buffer: Vec<u8>,
    /// How much of `buffer` core has already taken.
    offset: usize,
    started: bool,
    finished: bool,
    readers: Vec<FileReader>,
    current_reader_idx: usize,
    binary_out_funcs: Vec<PgBox<FmgrInfo, AllocatedByPostgres>>,
    per_row_memory_ctx: PgMemoryContexts,
    total_rows: Option<i64>,
    rows_copied: i64,
    row_bytes_copied: i64,
    stream_bytes_copied: i64,
    /// Whether the rows are wrapped in the binary `COPY` header and trailer, and whether the
    /// progress view is reported to. Both belong to the stream core reads, and a parallel
    /// `COPY FROM` worker's rows are a piece of that stream rather than the whole of it: the
    /// leader frames what it assembles out of every worker's pieces, and reports it.
    framed: bool,
}

impl CopyFromStream {
    /// Opens every file the uri names and matches each against the tuple descriptor, so a
    /// mismatch in the last file of a pattern is reported before the first row is inserted.
    pub(crate) fn open(
        provider: Arc<dyn FormatProvider>,
        place: &Place,
        options: &FormatOptions,
        match_by: MatchBy,
        tupledesc: &PgTupleDesc,
    ) -> CopyFromStream {
        // both caches are keyed on oids that a dropped and recreated extension reuses, and a
        // COPY is the only thing that reads them, so the start of one is where they are cleared
        reset_postgis_context();
        reset_map_context();

        error_if_copy_from_match_by_position_with_generated_columns(tupledesc, match_by);

        let attributes = collect_attributes_for(CollectAttributesFor::CopyFrom, tupledesc);

        let tupledesc_schema: SchemaRef = Arc::new(arrow_schema_from_attributes(&attributes));

        let readers = Self::open_readers(
            &provider,
            place,
            options,
            tupledesc_schema,
            &attributes,
            match_by,
        );

        // exact when every file told us its row count, and absent as soon as one did not: a
        // partial total would make the progress view read as though the statement were further
        // along than it is
        let total_rows = readers
            .iter()
            .map(|reader| reader.num_rows)
            .sum::<Option<i64>>();

        CopyFromStream {
            buffer: Vec::new(),
            offset: 0,
            started: false,
            finished: false,
            readers,
            current_reader_idx: 0,
            binary_out_funcs: Self::collect_binary_out_funcs(&attributes),
            per_row_memory_ctx: PgMemoryContexts::new("pg_arrow_base COPY FROM per row context"),
            total_rows,
            rows_copied: 0,
            row_bytes_copied: 0,
            stream_bytes_copied: 0,
            framed: true,
        }
    }

    /// One split of one file, unframed: the stream a parallel `COPY FROM` worker hands its
    /// leader.
    ///
    /// The file is opened without the literal-file-then-pattern dance of [`Self::open`], since
    /// the leader has already resolved the uri to the files it names, and a failure here is a
    /// failure of the statement rather than a hint to try the uri as a pattern.
    pub(crate) fn open_split(
        provider: Arc<dyn FormatProvider>,
        place: &Place,
        options: &FormatOptions,
        match_by: MatchBy,
        tupledesc: &PgTupleDesc,
        split: i64,
    ) -> CopyFromStream {
        reset_postgis_context();
        reset_map_context();

        let attributes = collect_attributes_for(CollectAttributesFor::CopyFrom, tupledesc);

        let tupledesc_schema: SchemaRef = Arc::new(arrow_schema_from_attributes(&attributes));

        let reader = FileReader::open_splits(
            &provider,
            place,
            options,
            tupledesc_schema,
            &attributes,
            match_by,
            Some(split..split + 1),
        )
        .unwrap_or_else(|e| error!("{e}"));

        CopyFromStream {
            buffer: Vec::new(),
            offset: 0,
            started: false,
            finished: false,
            readers: vec![reader],
            current_reader_idx: 0,
            binary_out_funcs: Self::collect_binary_out_funcs(&attributes),
            per_row_memory_ctx: PgMemoryContexts::new("pg_arrow_base COPY FROM per row context"),
            // the leader extrapolates the total from the splits that arrived, so a worker
            // has nothing to say about it
            total_rows: None,
            rows_copied: 0,
            row_bytes_copied: 0,
            stream_bytes_copied: 0,
            framed: false,
        }
    }

    fn open_readers(
        provider: &Arc<dyn FormatProvider>,
        place: &Place,
        options: &FormatOptions,
        tupledesc_schema: SchemaRef,
        attributes: &[FormData_pg_attribute],
        match_by: MatchBy,
    ) -> Vec<FileReader> {
        // the literal file first, always: a `*` is legal in the name of a file on every store
        // this reads, so a path that names one existing file is that file and not a pattern
        let opened = FileReader::open(
            provider,
            place,
            options,
            tupledesc_schema.clone(),
            attributes,
            match_by,
        );

        let failure = match opened {
            Ok(reader) => return vec![reader],
            Err(failure) => failure,
        };

        // a pattern is a property of a uri the statement wrote; a staging file is one the base
        // named itself, and a `*` that happened to be in its path would not be a glob
        let pattern = match place {
            Place::Object(uri) if uri.contains('*') => uri.as_str(),
            _ => error!("{failure}"),
        };

        // and a path that is not a valid pattern either is simply a file that is not there,
        // which is what the failed open already said
        let Some(matched) = matching_uris(pattern) else {
            error!("{failure}")
        };

        if matched.is_empty() {
            ereport!(
                PgLogLevel::ERROR,
                PgSqlErrorCode::ERRCODE_UNDEFINED_FILE,
                format!("no files found that match the pattern {pattern}"),
                "arrow.list() with the prefix shows what is actually there."
            );
        }

        matched
            .iter()
            .map(|file_uri| {
                FileReader::open(
                    provider,
                    &Place::object(file_uri),
                    options,
                    tupledesc_schema.clone(),
                    attributes,
                    match_by,
                )
                .unwrap_or_else(|e| error!("{e}"))
            })
            .collect()
    }

    /// The `send` function of every attribute, which is what produces the bytes core's binary
    /// reader expects to find for it.
    fn collect_binary_out_funcs(
        attributes: &[FormData_pg_attribute],
    ) -> Vec<PgBox<FmgrInfo, AllocatedByPostgres>> {
        unsafe {
            attributes
                .iter()
                .map(|attribute| {
                    let mut send_func_oid = InvalidOid;
                    let mut is_varlena = false;

                    getTypeBinaryOutputInfo(
                        attribute.type_oid().value(),
                        &mut send_func_oid,
                        &mut is_varlena,
                    );

                    let func_info = PgBox::<FmgrInfo>::alloc0().into_pg_boxed();
                    fmgr_info(send_func_oid, func_info.as_ptr());

                    func_info
                })
                .collect()
        }
    }

    fn current_reader_mut(&mut self) -> Option<&mut FileReader> {
        self.readers.get_mut(self.current_reader_idx)
    }

    /// Encodes the next batch into the buffer. `false` means the stream is over and core should
    /// see end of input.
    pub(crate) fn read_next(&mut self) -> bool {
        if self.finished {
            return false;
        }

        if !self.started {
            self.copy_start();
        }

        loop {
            let batch = self
                .current_reader_mut()
                .and_then(|reader| reader.reader.next());

            match batch {
                Some(batch) => {
                    let batch =
                        batch.unwrap_or_else(|e| error!("failed to read a record batch: {e}"));

                    self.copy_batch(batch);
                    break;
                }
                // the file is done: on to the next one, or to the trailer
                None if self.current_reader_idx + 1 < self.readers.len() => {
                    self.current_reader_idx += 1;
                }
                None => {
                    self.copy_finish();
                    break;
                }
            }
        }

        self.update_bytes_total();

        true
    }

    fn copy_batch(&mut self, batch: RecordBatch) {
        let num_rows = batch.num_rows();

        for row in 0..num_rows {
            check_for_interrupts!();

            let reader = self
                .readers
                .get(self.current_reader_idx)
                .expect("the current reader produced this batch");

            let natts = reader.attribute_count() as i16;
            let tuple_datums = reader.tuple_datums(batch.slice(row, 1));

            self.copy_row(natts, tuple_datums);
        }
    }

    /// One tuple of the binary stream: the attribute count, then each attribute's length and
    /// bytes, or -1 for a null.
    fn copy_row(&mut self, natts: i16, tuple_datums: Vec<Option<Datum>>) {
        let buffer_len_before_row = self.buffer.len();

        unsafe {
            // the send functions palloc, once per attribute per row, which is the one place in
            // the read path where a long COPY would otherwise grow the whole statement's context
            let mut old_ctx = self.per_row_memory_ctx.set_as_current();

            self.buffer.extend_from_slice(&natts.to_be_bytes());

            for (datum, out_func) in tuple_datums.into_iter().zip(self.binary_out_funcs.iter()) {
                match datum {
                    Some(datum) => {
                        let datum_bytes: *mut varlena = SendFunctionCall(out_func.as_ptr(), datum);

                        let data_size = varsize_any_exhdr(datum_bytes);
                        self.buffer
                            .extend_from_slice(&(data_size as i32).to_be_bytes());

                        let data = vardata_any(datum_bytes) as _;
                        self.buffer
                            .extend_from_slice(std::slice::from_raw_parts(data, data_size));
                    }
                    None => self.buffer.extend_from_slice(&(-1_i32).to_be_bytes()),
                }
            }

            old_ctx.set_as_current();
            self.per_row_memory_ctx.reset();
        };

        let row_bytes = (self.buffer.len() - buffer_len_before_row) as i64;

        self.row_bytes_copied += row_bytes;
        self.stream_bytes_copied += row_bytes;
        self.rows_copied += 1;
    }

    /// The binary `COPY` header: signature, flags, and an empty header extension.
    fn copy_start(&mut self) {
        self.started = true;

        if !self.framed {
            return;
        }

        let buffer_len_before_header = self.buffer.len();

        self.buffer
            .extend_from_slice(b"\x50\x47\x43\x4f\x50\x59\x0a\xff\x0d\x0a\x00");
        self.buffer.extend_from_slice(&0_i32.to_be_bytes());
        self.buffer.extend_from_slice(&0_i32.to_be_bytes());

        self.stream_bytes_copied += (self.buffer.len() - buffer_len_before_header) as i64;
    }

    fn copy_finish(&mut self) {
        self.finished = true;

        if !self.framed {
            return;
        }

        let buffer_len_before_trailer = self.buffer.len();

        self.buffer.extend_from_slice(&(-1_i16).to_be_bytes());

        self.stream_bytes_copied += (self.buffer.len() - buffer_len_before_trailer) as i64;
    }

    /// Reports the size of the binary stream core is consuming, which is the only total that
    /// means anything for a callback source: core counts the bytes it pulls, not the bytes of
    /// the file, and the two are unrelated for a compressed one.
    ///
    /// The exact size is known only once the last row is encoded, so it is extrapolated from the
    /// rows encoded so far and corrected after every batch. A file whose rows are fixed width is
    /// exact after the first one. A file that did not report a row count — a format whose
    /// container records none — gets no total, and the view shows what core shows for a source
    /// it cannot size.
    fn update_bytes_total(&self) {
        if !self.framed {
            return;
        }

        if self.finished {
            copy_progress::update_copy_from_bytes_total(self.stream_bytes_copied);
            return;
        }

        let Some(total_rows) = self.total_rows else {
            return;
        };

        if self.rows_copied == 0 {
            return;
        }

        let rows_left = (total_rows - self.rows_copied).max(0) as i128;
        let bytes_left = self.row_bytes_copied as i128 * rows_left / self.rows_copied as i128;
        let bytes_total = (self.stream_bytes_copied as i128 + bytes_left).min(i64::MAX as i128);

        copy_progress::update_copy_from_bytes_total(bytes_total as i64);
    }

    pub(crate) fn not_yet_copied_bytes(&self) -> usize {
        self.buffer.len() - self.offset
    }

    /// What has been encoded and not taken yet, for a caller that moves the bytes itself
    /// rather than letting core pull them: a parallel `COPY FROM` worker puts them on a queue.
    pub(crate) fn encoded_bytes(&self) -> &[u8] {
        &self.buffer[self.offset..]
    }

    pub(crate) fn rows_copied(&self) -> i64 {
        self.rows_copied
    }

    pub(crate) fn row_bytes_copied(&self) -> i64 {
        self.row_bytes_copied
    }

    pub(crate) fn reset_buffer(&mut self) {
        self.buffer.clear();
        self.offset = 0;
    }

    /// Hands core the next `len` bytes. Core asks for as much as its own buffer takes, which is
    /// why the offset is kept here rather than the buffer being drained.
    pub(crate) fn copy_to_outbuf(&mut self, len: usize, outbuf: void_mut_ptr) {
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.buffer.as_ptr().add(self.offset),
                outbuf as *mut u8,
                len,
            );
        };

        self.offset += len;
    }
}

/// How many splits the file at `place` holds and how many rows, out of its footer alone.
///
/// `None` for anything a parallel `COPY FROM` cannot divide: a provider that reads no ranges, a
/// format whose container records no splits, a uri that is not a file of its own. Every one of
/// those is a reason to read the file in one process, and none of them is a failure to report —
/// the single process path reads the same uri next and reports whatever it finds there.
pub(crate) fn file_splits(
    provider: &Arc<dyn FormatProvider>,
    place: &Place,
) -> Option<(i64, Option<i64>)> {
    if !provider.can_read_ranges() {
        return None;
    }

    let source = Arc::new(FileSource::open(place).ok()?);

    let info = provider.ranged_file_info(source).ok()?;

    Some((info.num_splits?, info.num_rows))
}

/// The Arrow schema of the file a uri names, for `CREATE TABLE ... FROM '<uri>'`.
///
/// A pattern names the schema of the *first* matching file in lexicographic order rather than
/// the listing's, because an object store lists lexicographically and a local directory lists
/// whatever order its entries are in — and a `CREATE TABLE` whose columns depend on that would
/// infer different tables from the same files. Every other matching file is checked against the
/// table by the `COPY FROM` that follows anyway.
pub(crate) fn file_schema(
    provider: &Arc<dyn FormatProvider>,
    uri: &str,
    options: &FormatOptions,
) -> SchemaRef {
    // the literal uri first, as in a `COPY FROM`: one existing file beats a pattern that would
    // have matched it, and a path that is no file and no pattern reports why the file failed
    let failure = match schema_of(provider, uri, options) {
        Ok(schema) => return schema,
        Err(failure) => failure,
    };

    if !uri.contains('*') {
        error!("{failure}");
    }

    let Some(matched) = matching_uris(uri) else {
        error!("{failure}")
    };

    let first_uri = matched
        .iter()
        .min()
        .unwrap_or_else(|| error!("no files found that match the pattern {uri}"));

    schema_of(provider, first_uri, options).unwrap_or_else(|e| error!("{e}"))
}

/// The schema of one file, or why it could not be read.
fn schema_of(
    provider: &Arc<dyn FormatProvider>,
    file_uri: &str,
    options: &FormatOptions,
) -> Result<SchemaRef, String> {
    let place = Place::object(file_uri);

    // the ranged path is the one worth having here: a format that reads its own footer answers
    // this from a few kilobytes rather than from the whole file
    if provider.can_read_ranges() {
        let source = Arc::new(FileSource::open(&place)?);

        return Ok(provider.open_ranged_reader(source, options, None)?.schema());
    }

    let data = io::read_all(&place).ok_or_else(|| format!("there is no file at {file_uri}"))?;

    provider
        .schema(&data)
        .map_err(|e| format!("failed to read the schema of {file_uri}: {e}"))
}

/// Adds to a "no file" failure what a uri that holds files rather than being one is read with.
///
/// A folder, which is what a `COPY TO` with `file_size_bytes` writes, holds no object of its own
/// name, so reading it back by that name is a missing object to an object store and nothing
/// else. A store that cannot list, e.g. an http one, says nothing about what is under the uri,
/// and neither does a staging file, which is this backend's own.
fn with_folder_hint(failure: String, place: &Place, provider: &Arc<dyn FormatProvider>) -> String {
    let Place::Object(uri) = place else {
        return failure;
    };

    let listed = object_store::try_list_shallow(uri).unwrap_or_default();

    if listed.is_empty() {
        return failure;
    }

    let uri = uri.trim_end_matches('/');

    // the pattern names the files the folder holds, which carry the format's own extension
    let extension = provider
        .file_extensions()
        .first()
        .map(|extension| format!(".{extension}"))
        .unwrap_or_default();

    format!(
        "{failure}. \"{uri}\" holds files rather than being one, so it is read with a pattern \
         that names them, e.g. \"{uri}/*{extension}\"."
    )
}

/// The uris a pattern names, in listing order.
///
/// The prefix is the part of the uri before the first `*`, cut back to the last `/`, because
/// that is the most tier 1 can list: it treats a uri as a literal prefix and knows nothing
/// about patterns. The matching is then done here, with `*` stopping at a `/` so that
/// `dir/*.parquet` does not reach into `dir/sub/`.
pub(crate) fn matching_uris(uri: &str) -> Option<Vec<String>> {
    Some(
        matching_objects(uri)?
            .into_iter()
            .map(|(file_uri, _)| file_uri)
            .collect(),
    )
}

/// The same, keeping the size tier 1 already reported for each file, for a caller that would
/// otherwise have to stat every match again.
pub(crate) fn matching_objects(uri: &str) -> Option<Vec<(String, i64)>> {
    let pattern = Pattern::new(uri).ok()?;

    let star = uri.find('*').expect("the uri has a pattern in it");

    let slash = uri[..star].rfind('/')?;
    let prefix_uri = &uri[..=slash];

    let match_options = MatchOptions {
        case_sensitive: true,
        require_literal_separator: true,
        require_literal_leading_dot: false,
    };

    // the listing is confined to the directory the pattern names whenever the pattern can only
    // match inside it, which is the usual shape. A recursive list of that prefix would walk
    // every subtree under it — on a local filesystem one unreadable directory nowhere near the
    // match then fails the statement, and on an object store every key under the prefix is
    // paged through to be filtered out again. A remainder that spans a `/`, or a `**` that
    // matches one itself, does reach deeper, so those are listed recursively.
    let remainder = &uri[slash + 1..];
    let listed = if remainder.contains('/') || remainder.contains("**") {
        object_store::list(prefix_uri)
    } else {
        object_store::list_shallow(prefix_uri)
    };

    listed
        .into_iter()
        .map(|(location, size)| (file_uri_under(prefix_uri, &location), size))
        .filter(|(file_uri, _)| {
            pattern.matches_path_with(std::path::Path::new(file_uri), match_options)
        })
        .collect::<Vec<_>>()
        .into()
}

/// The uri of a listed object, from the prefix uri it was listed under.
///
/// Tier 1 reports a location as the object store sees it — a path with no scheme, no bucket and
/// no leading slash — and what is needed is something that can be opened again. Every location
/// starts with the path part of the prefix uri, and the prefix uri ends with it, so overlapping
/// the two reconstructs the whole uri without this file having to know how any scheme spells a
/// bucket.
fn file_uri_under(prefix_uri: &str, location: &str) -> String {
    let overlap = (0..=location.len())
        .rev()
        .find(|&len| prefix_uri.ends_with(&location[..len]))
        .unwrap_or(0);

    format!("{prefix_uri}{}", &location[overlap..])
}

// no backend needed: rebuilding a uri is string work, so it is a plain unit test
#[cfg(test)]
mod tests {
    use super::file_uri_under;

    #[test]
    fn test_a_listed_object_is_turned_back_into_a_uri() {
        assert_eq!(
            file_uri_under("s3://bucket/dir/", "dir/part-0.parquet"),
            "s3://bucket/dir/part-0.parquet"
        );

        // a bucket-root prefix: the location and the uri path share nothing
        assert_eq!(
            file_uri_under("s3://bucket/", "part-0.parquet"),
            "s3://bucket/part-0.parquet"
        );

        // a local path, whose leading slash the object store dropped
        assert_eq!(
            file_uri_under("/tmp/parts/", "tmp/parts/a.arrow"),
            "/tmp/parts/a.arrow"
        );

        // a repeated path component, where the longest overlap is the right one
        assert_eq!(
            file_uri_under("s3://bucket/a/a/", "a/a/f.parquet"),
            "s3://bucket/a/a/f.parquet"
        );
    }
}
