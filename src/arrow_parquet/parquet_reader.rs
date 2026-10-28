use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow_cast::{cast_with_options, CastOptions};
use arrow_schema::{DataType, Schema, SchemaRef, TimeUnit};
use futures::StreamExt;
use glob::Pattern;
use parquet::arrow::async_reader::ParquetRecordBatchStream;
use pgrx::{
    check_for_interrupts,
    pg_sys::{
        fmgr_info, getTypeBinaryOutputInfo, varlena, Datum, FmgrInfo, FormData_pg_attribute,
        InvalidOid, SendFunctionCall,
    },
    vardata_any, varsize_any_exhdr, void_mut_ptr, AllocatedByPostgres, PgBox, PgMemoryContexts,
    PgTupleDesc,
};

use crate::{
    arrow_parquet::{
        arrow_to_pg::{context::collect_arrow_to_pg_attribute_contexts, to_pg_datum},
        field_ids::FieldIds,
        integer_cast::round_for_integer_cast,
        schema::{
            coerce_schema::{
                ensure_file_schema_match_tupledesc_schema,
                error_if_copy_from_match_by_position_with_generated_columns,
                overlay_file_geospatial_crs,
            },
            parse_schema::{
                parquet_schema_string_from_attributes, parse_arrow_schema_from_attributes,
            },
        },
    },
    object_store::object_store_error_message,
    parquet_copy_hook::copy_progress::update_copy_from_bytes_total,
    parquet_udfs::list::list_uri,
    pgrx_utils::{collect_attributes_for, CollectAttributesFor},
    type_compat::{
        geometry::reset_postgis_context,
        map::reset_map_context,
        pg_arrow_type_conversions::{NEG_INFINITY_DATE, NEG_INFINITY_TIMESTAMP},
    },
    PG_BACKEND_TOKIO_RUNTIME,
};

use super::{
    arrow_to_pg::context::ArrowToPgAttributeContext,
    match_by::{column_index_by_name, MatchBy},
    uri_utils::{
        parquet_reader_from_uri, uri_as_string, ParquetObjectStoreReader, ParquetRowGroupOpener,
        ParsedUriInfo,
    },
};

// Per column encoder for one record batch. The fast variants write the PG binary COPY
// representation straight from the arrow buffers: no per row slice, no PG datum, no
// SendFunctionCall. Other types fall back to one datum per row plus SendFunctionCall.
enum ColEnc {
    Int16(Int16Array),
    Int32(Int32Array),
    Int64(Int64Array),
    Float32(Float32Array),
    Float64(Float64Array),
    Bool(BooleanArray),
    Date32(Date32Array),
    TsMicros(TimestampMicrosecondArray),
    Utf8(StringArray),
    Binary(BinaryArray),
    Numeric(Decimal128Array, u32),
    Datums(Vec<Option<Datum>>),
}

const NULL_FIELD: [u8; 4] = [0xff, 0xff, 0xff, 0xff];

// Only claims a column when the arrow type and the PG attribute type agree, so the
// encoder below can skip the generic arrow -> datum -> send round trip.
fn fast_encoder(col: &ArrayRef, context: &ArrowToPgAttributeContext) -> Option<ColEnc> {
    use pgrx::pg_sys::{
        BOOLOID, BPCHAROID, BYTEAOID, DATEOID, FLOAT4OID, FLOAT8OID, INT2OID, INT4OID, INT8OID,
        NUMERICOID, TEXTOID, TIMESTAMPOID, VARCHAROID,
    };

    let typoid = context.typoid();
    let data_type = col.data_type();

    macro_rules! downcast {
        ($ty:ty) => {
            col.as_any().downcast_ref::<$ty>()?.clone()
        };
    }

    let enc = if matches!(data_type, DataType::Int16) && typoid == INT2OID {
        ColEnc::Int16(downcast!(Int16Array))
    } else if matches!(data_type, DataType::Int32) && typoid == INT4OID {
        ColEnc::Int32(downcast!(Int32Array))
    } else if matches!(data_type, DataType::Int64) && typoid == INT8OID {
        ColEnc::Int64(downcast!(Int64Array))
    } else if matches!(data_type, DataType::Float32) && typoid == FLOAT4OID {
        ColEnc::Float32(downcast!(Float32Array))
    } else if matches!(data_type, DataType::Float64) && typoid == FLOAT8OID {
        ColEnc::Float64(downcast!(Float64Array))
    } else if matches!(data_type, DataType::Boolean) && typoid == BOOLOID {
        ColEnc::Bool(downcast!(BooleanArray))
    } else if matches!(data_type, DataType::Date32) && typoid == DATEOID {
        ColEnc::Date32(downcast!(Date32Array))
    } else if matches!(data_type, DataType::Timestamp(TimeUnit::Microsecond, None))
        && typoid == TIMESTAMPOID
    {
        ColEnc::TsMicros(downcast!(TimestampMicrosecondArray))
    } else if matches!(data_type, DataType::Utf8)
        && (typoid == TEXTOID || typoid == VARCHAROID || typoid == BPCHAROID)
    {
        let array = downcast!(StringArray);

        // the slow path rejects a null byte with a hint that names the column, which is
        // worth one scan of the whole data buffer per batch to keep
        if array.values().as_slice().contains(&0) {
            return None;
        }

        ColEnc::Utf8(array)
    } else if matches!(data_type, DataType::Binary) && typoid == BYTEAOID {
        ColEnc::Binary(downcast!(BinaryArray))
    } else if let (DataType::Decimal128(precision, scale), NUMERICOID) = (data_type, typoid) {
        // the encoder below shifts the unscaled value by up to 3 decimal digits, which only
        // stays inside u128 below precision 36 unless the scale is already group aligned
        let scale = *scale;

        if scale < 0 || (*precision > 35 && scale % 4 != 0) {
            return None;
        }

        ColEnc::Numeric(downcast!(Decimal128Array), scale as u32)
    } else {
        return None;
    };

    Some(enc)
}

#[inline]
fn write_fixed(buffer: &mut Vec<u8>, bytes: &[u8]) {
    /* 4 bytes: attribute's data size, then the data itself */
    buffer.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
    buffer.extend_from_slice(bytes);
}

// Writes the PG binary representation of numeric: ndigits, weight, sign and dscale
// followed by base-10000 digits, most significant first.
fn write_numeric(buffer: &mut Vec<u8>, unscaled: i128, scale: u32) {
    const NUMERIC_POS: i16 = 0x0000;
    const NUMERIC_NEG: i16 = 0x4000;

    let sign = if unscaled < 0 {
        NUMERIC_NEG
    } else {
        NUMERIC_POS
    };

    // pad the fractional part out to a whole number of base-10000 digits so that the
    // group boundaries line up with the decimal point
    let pad = (4 - (scale % 4)) % 4;
    let abs = unscaled.unsigned_abs() * 10_u128.pow(pad);
    let fractional_groups = ((scale + pad) / 4) as i16;

    let mut digits = [0_i16; 11];
    let mut ndigits = 0;
    let mut rest = abs;

    while rest > 0 {
        digits[ndigits] = (rest % 10_000) as i16;
        rest /= 10_000;
        ndigits += 1;
    }

    let weight = if ndigits == 0 {
        0
    } else {
        ndigits as i16 - fractional_groups - 1
    };

    buffer.extend_from_slice(&((8 + 2 * ndigits) as i32).to_be_bytes());
    buffer.extend_from_slice(&(ndigits as i16).to_be_bytes());
    buffer.extend_from_slice(&weight.to_be_bytes());
    buffer.extend_from_slice(&sign.to_be_bytes());
    buffer.extend_from_slice(&(scale as i16).to_be_bytes());

    for digit in digits[..ndigits].iter().rev() {
        buffer.extend_from_slice(&digit.to_be_bytes());
    }
}

#[inline]
fn write_field(
    buffer: &mut Vec<u8>,
    enc: &ColEnc,
    row: usize,
    out_func: &PgBox<FmgrInfo, AllocatedByPostgres>,
) {
    macro_rules! fixed {
        ($array:expr, $value:expr) => {{
            if $array.is_null(row) {
                buffer.extend_from_slice(&NULL_FIELD);
            } else {
                write_fixed(buffer, &$value);
            }
        }};
    }

    match enc {
        ColEnc::Int16(a) => fixed!(a, a.value(row).to_be_bytes()),
        ColEnc::Int32(a) => fixed!(a, a.value(row).to_be_bytes()),
        ColEnc::Int64(a) => fixed!(a, a.value(row).to_be_bytes()),
        ColEnc::Float32(a) => fixed!(a, a.value(row).to_bits().to_be_bytes()),
        ColEnc::Float64(a) => fixed!(a, a.value(row).to_bits().to_be_bytes()),
        ColEnc::Bool(a) => fixed!(a, [a.value(row) as u8]),
        ColEnc::Date32(a) => {
            if a.is_null(row) {
                buffer.extend_from_slice(&NULL_FIELD);
            } else {
                // PG keeps infinity at the limits of the underlying i32 and its epoch is
                // 10957 days after the unix epoch that parquet stores. -infinity also
                // arrives one above the minimum, which is what DuckDB and we write for it.
                let unix_days = a.value(row);
                let pg_days = if unix_days == i32::MAX {
                    i32::MAX
                } else if unix_days == i32::MIN || unix_days == NEG_INFINITY_DATE {
                    i32::MIN
                } else {
                    unix_days
                        .checked_sub(10957)
                        .unwrap_or_else(|| panic!("date {unix_days} is out of range for postgres"))
                };
                write_fixed(buffer, &pg_days.to_be_bytes());
            }
        }
        ColEnc::TsMicros(a) => {
            if a.is_null(row) {
                buffer.extend_from_slice(&NULL_FIELD);
            } else {
                let unix_micros = a.value(row);
                let pg_micros = if unix_micros == i64::MAX {
                    i64::MAX
                } else if unix_micros == i64::MIN || unix_micros == NEG_INFINITY_TIMESTAMP {
                    i64::MIN
                } else {
                    unix_micros
                        .checked_sub(946_684_800_000_000)
                        .unwrap_or_else(|| {
                            panic!("timestamp {unix_micros} is out of range for postgres")
                        })
                };
                write_fixed(buffer, &pg_micros.to_be_bytes());
            }
        }
        ColEnc::Utf8(a) => fixed!(a, a.value(row).as_bytes()),
        ColEnc::Binary(a) => fixed!(a, a.value(row)),
        ColEnc::Numeric(a, scale) => {
            if a.is_null(row) {
                buffer.extend_from_slice(&NULL_FIELD);
            } else {
                write_numeric(buffer, a.value(row), *scale);
            }
        }
        ColEnc::Datums(datums) => match datums.get(row).copied().flatten() {
            Some(datum) => unsafe {
                let datum_bytes: *mut varlena = SendFunctionCall(out_func.as_ptr(), datum);
                let data_size = varsize_any_exhdr(datum_bytes);
                buffer.extend_from_slice(&(data_size as i32).to_be_bytes());
                let data = vardata_any(datum_bytes) as _;
                buffer.extend_from_slice(std::slice::from_raw_parts(data, data_size));
            },
            None => buffer.extend_from_slice(&NULL_FIELD),
        },
    }
}

pub(crate) struct SingleParquetReader {
    reader: ParquetRecordBatchStream<ParquetObjectStoreReader>,
    attribute_contexts: Vec<ArrowToPgAttributeContext>,
    match_by: MatchBy,
    total_rows: i64,
}

impl Deref for SingleParquetReader {
    type Target = ParquetRecordBatchStream<ParquetObjectStoreReader>;

    fn deref(&self) -> &Self::Target {
        &self.reader
    }
}

impl DerefMut for SingleParquetReader {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.reader
    }
}

// ParquetReaderSource is what a reader context reads: either a uri, which it opens itself,
// or a single row group of a file that a parallel COPY FROM worker already has open.
enum ParquetReaderSource<'a> {
    Uri(&'a ParsedUriInfo),
    RowGroup(&'a ParquetRowGroupOpener, usize),
}

impl SingleParquetReader {
    fn try_new(
        uri_info: &ParsedUriInfo,
        match_by: MatchBy,
        tupledesc_schema: SchemaRef,
        attributes: &[FormData_pg_attribute],
    ) -> Result<Self, String> {
        let (reader, total_rows) = parquet_reader_from_uri(uri_info)?;

        Ok(Self::from_reader(
            reader,
            total_rows,
            match_by,
            tupledesc_schema,
            attributes,
        ))
    }

    // try_new_for_row_group creates a reader over a single row group of the already opened
    // file, which is how the parallel COPY FROM workers split one file between themselves.
    fn try_new_for_row_group(
        opener: &ParquetRowGroupOpener,
        match_by: MatchBy,
        tupledesc_schema: SchemaRef,
        attributes: &[FormData_pg_attribute],
        row_group: usize,
    ) -> Result<Self, String> {
        let (reader, total_rows) = opener.reader_for_row_group(row_group)?;

        Ok(Self::from_reader(
            reader,
            total_rows,
            match_by,
            tupledesc_schema,
            attributes,
        ))
    }

    fn from_reader(
        reader: ParquetRecordBatchStream<ParquetObjectStoreReader>,
        total_rows: i64,
        match_by: MatchBy,
        tupledesc_schema: SchemaRef,
        attributes: &[FormData_pg_attribute],
    ) -> Self {
        // the tupledesc schema only knows the crs of the target columns, so the geometry and
        // geography columns take the crs that the file has for them. This happens before the
        // coercion below, which builds its cast targets from the tupledesc schema and would
        // otherwise drop the crs of a geospatial field nested in a struct, list or map.
        let tupledesc_schema = Arc::new(Schema::new_with_metadata(
            overlay_file_geospatial_crs(reader.schema(), &tupledesc_schema, match_by),
            tupledesc_schema.metadata().clone(),
        ));

        // Ensure that the file schema matches the tupledesc schema.
        // Gets cast_to_types for each attribute if a cast is needed for the attribute's columnar array
        // to match the expected columnar array for its tupledesc type.
        let cast_to_types = ensure_file_schema_match_tupledesc_schema(
            reader.schema().clone(),
            tupledesc_schema.clone(),
            attributes,
            match_by,
        );

        let attribute_contexts = collect_arrow_to_pg_attribute_contexts(
            attributes,
            tupledesc_schema.fields(),
            Some(cast_to_types),
        );

        SingleParquetReader {
            reader,
            attribute_contexts,
            match_by,
            total_rows,
        }
    }

    fn create_readers_from_pattern_uri(
        uri_info: &ParsedUriInfo,
        match_by: MatchBy,
        tupledesc_schema: SchemaRef,
        attributes: &[FormData_pg_attribute],
    ) -> Vec<Self> {
        debug_assert!(uri_info.path.as_ref().contains('*'));

        list_uri(uri_info)
            .into_iter()
            .map(|(file_uri, _)| {
                let file_uri_info = ParsedUriInfo::try_from(file_uri.as_str())
                    .unwrap_or_else(|e| panic!("failed to parse file uri {}: {}", file_uri, e));

                SingleParquetReader::try_new(
                    &file_uri_info,
                    match_by,
                    tupledesc_schema.clone(),
                    attributes,
                )
                .unwrap_or_else(|e| {
                    panic!(
                        "failed to create parquet reader for uri {}: {}",
                        file_uri, e
                    )
                })
            })
            .collect()
    }

    fn total_rows(&self) -> i64 {
        self.total_rows
    }

    fn attribute_count(&self) -> usize {
        self.attribute_contexts.len()
    }

    // Resolves each attribute's column once per batch, casts once per batch instead of
    // once per row, and picks a direct encoder wherever the arrow and PG types line up.
    fn build_column_encoders(&self, record_batch: &RecordBatch) -> Vec<ColEnc> {
        let num_rows = record_batch.num_rows();
        let mut encoders = Vec::with_capacity(self.attribute_contexts.len());

        for (attribute_idx, attribute_context) in self.attribute_contexts.iter().enumerate() {
            let name = attribute_context.name();

            let column_array = match self.match_by {
                MatchBy::Position => record_batch
                    .columns()
                    .get(attribute_idx)
                    .unwrap_or_else(|| panic!("column {name} not found"))
                    .clone(),

                MatchBy::Name => column_index_by_name(record_batch.schema_ref().fields(), name)
                    .map(|column_idx| record_batch.column(column_idx))
                    .unwrap_or_else(|| panic!("column {name} not found"))
                    .clone(),
            };

            let column_array = if attribute_context.needs_cast() {
                // should fail instead of returning None if the cast fails at runtime
                let cast_options = CastOptions {
                    safe: false,
                    ..Default::default()
                };

                // arrow truncates toward zero when it casts to an integer, PostgreSQL rounds
                let column_array =
                    round_for_integer_cast(&column_array, attribute_context.data_type(), name);

                cast_with_options(&column_array, attribute_context.data_type(), &cast_options)
                    .unwrap_or_else(|e| panic!("failed to cast column {name}: {e}"))
            } else {
                column_array
            };

            let encoder = match fast_encoder(&column_array, attribute_context) {
                Some(encoder) => encoder,
                None => {
                    let mut datums = Vec::with_capacity(num_rows);

                    for row in 0..num_rows {
                        datums.push(to_pg_datum(
                            column_array.slice(row, 1).to_data(),
                            attribute_context,
                        ));
                    }

                    ColEnc::Datums(datums)
                }
            };

            encoders.push(encoder);
        }

        encoders
    }
}

pub(crate) struct ParquetReaderContext {
    buffer: Vec<u8>,
    offset: usize,
    started: bool,
    finished: bool,
    // whether the binary copy header and trailer belong to this context. A parallel COPY FROM
    // worker encodes one row group out of the middle of the stream, so the leader is the one
    // that frames the stream it hands to PG.
    framing: bool,
    parquet_readers: Vec<SingleParquetReader>,
    current_parquet_reader_idx: usize,
    binary_out_funcs: Vec<PgBox<FmgrInfo>>,
    per_row_memory_ctx: PgMemoryContexts,
    total_rows: i64,
    rows_copied: i64,
    row_bytes_copied: i64,
    stream_bytes_copied: i64,
}

impl ParquetReaderContext {
    pub(crate) fn new(
        uri_info: &ParsedUriInfo,
        match_by: MatchBy,
        tupledesc: &PgTupleDesc,
    ) -> Self {
        let framing = true;

        Self::new_internal(
            ParquetReaderSource::Uri(uri_info),
            match_by,
            tupledesc,
            framing,
        )
    }

    // new_for_row_group creates a context that reads a single row group of a single file and
    // encodes neither the binary copy header nor its trailer, so that the rows several
    // processes encode out of one file can be concatenated into one copy stream.
    pub(crate) fn new_for_row_group(
        opener: &ParquetRowGroupOpener,
        match_by: MatchBy,
        tupledesc: &PgTupleDesc,
        row_group: usize,
    ) -> Self {
        let framing = false;

        Self::new_internal(
            ParquetReaderSource::RowGroup(opener, row_group),
            match_by,
            tupledesc,
            framing,
        )
    }

    fn new_internal(
        source: ParquetReaderSource,
        match_by: MatchBy,
        tupledesc: &PgTupleDesc,
        framing: bool,
    ) -> Self {
        // Postgis and Map contexts are used throughout reading the parquet file.
        // We need to reset them to avoid reading the stale data. (e.g. extension could be dropped)
        reset_postgis_context();
        reset_map_context();

        error_if_copy_from_match_by_position_with_generated_columns(tupledesc, match_by);

        let attributes = collect_attributes_for(CollectAttributesFor::CopyFrom, tupledesc);

        pgrx::debug2!(
            "schema for tuples: {}",
            parquet_schema_string_from_attributes(&attributes, FieldIds::None)
        );

        let tupledesc_schema = parse_arrow_schema_from_attributes(&attributes, FieldIds::None);

        let tupledesc_schema = Arc::new(tupledesc_schema);

        let uri_info = match source {
            ParquetReaderSource::RowGroup(opener, row_group) => {
                let reader = SingleParquetReader::try_new_for_row_group(
                    opener,
                    match_by,
                    tupledesc_schema,
                    &attributes,
                    row_group,
                )
                .unwrap_or_else(|e| panic!("{e}"));

                return Self::from_readers(vec![reader], &attributes, framing);
            }
            ParquetReaderSource::Uri(uri_info) => uri_info,
        };

        let parquet_readers =
            SingleParquetReader::try_new(uri_info, match_by, tupledesc_schema.clone(), &attributes)
                .map(|reader| vec![reader])
                .unwrap_or_else(|e| {
                    // if uri contains a valid pattern, try to create readers from the pattern uri
                    // otherwise, panic with the original error
                    if !uri_info.path.as_ref().contains('*') || Pattern::try_from(uri_info).is_err()
                    {
                        panic!("{e}");
                    }

                    SingleParquetReader::create_readers_from_pattern_uri(
                        uri_info,
                        match_by,
                        tupledesc_schema.clone(),
                        &attributes,
                    )
                });

        if parquet_readers.is_empty() {
            // object_store's Path strips the leading "/" from a local path, so report the
            // pattern as the user wrote it rather than a relative path that does not exist
            panic!(
                "no files found that match the pattern {}",
                uri_as_string(&uri_info.uri)
            );
        }

        Self::from_readers(parquet_readers, &attributes, framing)
    }

    fn from_readers(
        parquet_readers: Vec<SingleParquetReader>,
        attributes: &[FormData_pg_attribute],
        framing: bool,
    ) -> Self {
        let binary_out_funcs = Self::collect_binary_out_funcs(attributes);

        let per_row_memory_ctx = PgMemoryContexts::new("COPY FROM parquet per row memory context");

        let total_rows = parquet_readers
            .iter()
            .map(|reader| reader.total_rows())
            .sum();

        ParquetReaderContext {
            buffer: Vec::new(),
            offset: 0,
            parquet_readers,
            current_parquet_reader_idx: 0,
            binary_out_funcs,
            started: false,
            finished: false,
            framing,
            per_row_memory_ctx,
            total_rows,
            rows_copied: 0,
            row_bytes_copied: 0,
            stream_bytes_copied: 0,
        }
    }

    fn current_reader(&self) -> Option<&SingleParquetReader> {
        self.parquet_readers.get(self.current_parquet_reader_idx)
    }

    fn current_reader_mut(&mut self) -> Option<&mut SingleParquetReader> {
        self.parquet_readers
            .get_mut(self.current_parquet_reader_idx)
    }

    fn collect_binary_out_funcs(
        attributes: &[FormData_pg_attribute],
    ) -> Vec<PgBox<FmgrInfo, AllocatedByPostgres>> {
        unsafe {
            let mut binary_out_funcs = vec![];

            for att in attributes.iter() {
                let typoid = att.type_oid();

                let mut send_func_oid = InvalidOid;
                let mut is_varlena = false;
                getTypeBinaryOutputInfo(typoid.value(), &mut send_func_oid, &mut is_varlena);

                let arg_fninfo = PgBox::<FmgrInfo>::alloc0().into_pg_boxed();
                fmgr_info(send_func_oid, arg_fninfo.as_ptr());

                binary_out_funcs.push(arg_fninfo);
            }

            binary_out_funcs
        }
    }

    pub(crate) fn read_parquet(&mut self) -> bool {
        if self.finished {
            return false;
        }

        if !self.started {
            // starts PG copy
            self.copy_start();
        }

        // read a record batch from the parquet file. Record batch will contain
        // DEFAULT_BATCH_SIZE rows as we configured in the parquet reader.
        let record_batch = PG_BACKEND_TOKIO_RUNTIME
            .block_on(self.current_reader_mut().expect("no reader found").next());

        if let Some(batch_result) = record_batch {
            let record_batch = batch_result.unwrap_or_else(|e| {
                panic!(
                    "failed to read record batch: {}",
                    object_store_error_message(e)
                )
            });

            let num_rows = record_batch.num_rows();

            // raw pointer so the encoders can be built from the reader while the buffer
            // on self is written to
            let reader: *const SingleParquetReader =
                self.current_reader().expect("no reader found");

            let natts = unsafe { (*reader).attribute_count() } as i16;

            // fallback datums live in this context, so it is reset once per batch
            let mut old_ctx = unsafe { self.per_row_memory_ctx.set_as_current() };

            let encoders = unsafe { (*reader).build_column_encoders(&record_batch) };

            for row in 0..num_rows {
                check_for_interrupts!();
                self.copy_row_fast(natts, &encoders, row);
            }

            drop(encoders);

            unsafe {
                old_ctx.set_as_current();
                self.per_row_memory_ctx.reset();
            }
        } else if self.current_parquet_reader_idx + 1 < self.parquet_readers.len() {
            // move to the next parquet reader
            self.current_parquet_reader_idx += 1;
            self.read_parquet();
        } else {
            // finish PG copy
            self.copy_finish();
        }

        self.update_bytes_total();

        true
    }

    fn copy_row_fast(&mut self, natts: i16, encoders: &[ColEnc], row: usize) {
        let buffer_len_before_row = self.buffer.len();

        /* 2 bytes: per-tuple header */
        self.buffer.extend_from_slice(&natts.to_be_bytes());

        for (encoder, out_func) in encoders.iter().zip(self.binary_out_funcs.iter()) {
            write_field(&mut self.buffer, encoder, row, out_func);
        }

        let row_bytes = (self.buffer.len() - buffer_len_before_row) as i64;
        self.row_bytes_copied += row_bytes;
        self.stream_bytes_copied += row_bytes;
        self.rows_copied += 1;
    }

    fn copy_start(&mut self) {
        self.started = true;

        if !self.framing {
            return;
        }

        let buffer_len_before_header = self.buffer.len();

        /* Binary signature */
        let signature_bytes = b"\x50\x47\x43\x4f\x50\x59\x0a\xff\x0d\x0a\x00";
        self.buffer.extend_from_slice(signature_bytes);

        /* Flags field */
        let flags = 0_i32;
        let flags_bytes = flags.to_be_bytes();
        self.buffer.extend_from_slice(&flags_bytes);

        /* No header extension */
        let header_ext_len = 0_i32;
        let header_ext_len_bytes = header_ext_len.to_be_bytes();
        self.buffer.extend_from_slice(&header_ext_len_bytes);

        self.stream_bytes_copied += (self.buffer.len() - buffer_len_before_header) as i64;
    }

    fn copy_finish(&mut self) {
        self.finished = true;

        if !self.framing {
            return;
        }

        let buffer_len_before_trailer = self.buffer.len();

        /* trailer */
        let trailer_len = -1_i16;
        let trailer_len_bytes = trailer_len.to_be_bytes();
        self.buffer.extend_from_slice(&trailer_len_bytes);

        self.stream_bytes_copied += (self.buffer.len() - buffer_len_before_trailer) as i64;
    }

    // update_bytes_total reports the size of the binary copy stream that PG consumes to
    // pg_stat_progress_copy. The exact size is only known once the last row is encoded, so it is
    // extrapolated from the rows encoded so far and corrected after each record batch. It is
    // already exact after the first batch for the files with fixed size rows.
    fn update_bytes_total(&self) {
        // a parallel COPY FROM worker does not own the stream PG consumes, so the leader is the
        // one that reports its size
        if !self.framing {
            return;
        }

        if self.finished {
            update_copy_from_bytes_total(self.stream_bytes_copied);
            return;
        }

        if self.rows_copied == 0 {
            return;
        }

        let rows_left = (self.total_rows - self.rows_copied).max(0) as i128;

        let bytes_left = self.row_bytes_copied as i128 * rows_left / self.rows_copied as i128;

        let bytes_total = (self.stream_bytes_copied as i128 + bytes_left).min(i64::MAX as i128);

        update_copy_from_bytes_total(bytes_total as i64);
    }

    // encoded_bytes returns the bytes the last read encoded and did not hand out yet.
    pub(crate) fn encoded_bytes(&self) -> &[u8] {
        &self.buffer[self.offset..]
    }

    // rows_copied returns how many rows this context encoded so far.
    pub(crate) fn rows_copied(&self) -> i64 {
        self.rows_copied
    }

    // row_bytes_copied returns how many bytes of row data this context encoded so far, which
    // excludes the binary copy header and trailer.
    pub(crate) fn row_bytes_copied(&self) -> i64 {
        self.row_bytes_copied
    }

    // not_yet_copied_bytes returns the number of the bytes that are not yet consumed
    // in the ParquetReader's internal buffer.
    pub(crate) fn not_yet_copied_bytes(&self) -> usize {
        self.buffer.len() - self.offset
    }

    // reset_buffer resets the ParquetReader's internal buffer.
    pub(crate) fn reset_buffer(&mut self) {
        self.buffer.clear();
        self.offset = 0;
    }

    // copy_to_outbuf copies the next len bytes from the ParquetReader's internal buffer to the
    // outbuf. It also updates the offset of the ParquetReader's internal buffer to keep track of
    // the consumed bytes.
    pub(crate) fn copy_to_outbuf(&mut self, len: usize, outbuf: void_mut_ptr) {
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.buffer.as_ptr().add(self.offset),
                outbuf as *mut u8,
                len,
            );

            self.offset += len;
        };
    }
}
