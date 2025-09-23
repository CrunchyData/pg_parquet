use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};

use arrow::array::RecordBatch;
use arrow_cast::{cast_with_options, CastOptions};
use arrow_schema::SchemaRef;
use futures::StreamExt;
use glob::Pattern;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStream};
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
        schema_parser::{
            error_if_copy_from_match_by_position_with_generated_columns,
            parquet_schema_string_from_attributes,
        },
    },
    parquet_udfs::list::list_uri,
    pgrx_utils::{collect_attributes_for, CollectAttributesFor},
    type_compat::{geometry::reset_postgis_context, map::reset_map_context},
    PG_BACKEND_TOKIO_RUNTIME,
};

use super::{
    arrow_to_pg::context::ArrowToPgAttributeContext,
    match_by::MatchBy,
    schema_parser::{
        ensure_file_schema_match_tupledesc_schema, parse_arrow_schema_from_attributes,
    },
    uri_utils::{parquet_reader_from_uri, ParsedUriInfo},
};

pub(crate) struct SingleParquetReader {
    reader: ParquetRecordBatchStream<ParquetObjectReader>,
    attribute_contexts: Vec<ArrowToPgAttributeContext>,
    match_by: MatchBy,
}

impl Deref for SingleParquetReader {
    type Target = ParquetRecordBatchStream<ParquetObjectReader>;

    fn deref(&self) -> &Self::Target {
        &self.reader
    }
}

impl DerefMut for SingleParquetReader {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.reader
    }
}

impl SingleParquetReader {
    fn try_new(
        uri_info: &ParsedUriInfo,
        match_by: MatchBy,
        tupledesc_schema: SchemaRef,
        attributes: &[FormData_pg_attribute],
    ) -> Result<Self, String> {
        let reader = parquet_reader_from_uri(uri_info)?;

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
            &tupledesc_schema.fields,
            Some(cast_to_types),
        );

        Ok(SingleParquetReader {
            reader,
            attribute_contexts,
            match_by,
        })
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

    fn attribute_count(&self) -> usize {
        self.attribute_contexts.len()
    }

    fn record_batch_to_tuple_datums(&self, record_batch: RecordBatch) -> Vec<Option<Datum>> {
        let mut datums = vec![];

        for (attribute_idx, attribute_context) in self.attribute_contexts.iter().enumerate() {
            let name = attribute_context.name();

            let column_array = match self.match_by {
                MatchBy::Position => record_batch
                    .columns()
                    .get(attribute_idx)
                    .unwrap_or_else(|| panic!("column {name} not found")),

                MatchBy::Name => record_batch
                    .column_by_name(name)
                    .unwrap_or_else(|| panic!("column {name} not found")),
            };

            let datum = if attribute_context.needs_cast() {
                // should fail instead of returning None if the cast fails at runtime
                let cast_options = CastOptions {
                    safe: false,
                    ..Default::default()
                };

                let casted_column_array =
                    cast_with_options(&column_array, attribute_context.data_type(), &cast_options)
                        .unwrap_or_else(|e| panic!("failed to cast column {name}: {e}"));

                to_pg_datum(casted_column_array.to_data(), attribute_context)
            } else {
                to_pg_datum(column_array.to_data(), attribute_context)
            };

            datums.push(datum);
        }

        datums
    }
}

pub(crate) struct ParquetReaderContext {
    buffer: Vec<u8>,
    offset: usize,
    started: bool,
    finished: bool,
    parquet_readers: Vec<SingleParquetReader>,
    current_parquet_reader_idx: usize,
    binary_out_funcs: Vec<PgBox<FmgrInfo>>,
    per_row_memory_ctx: PgMemoryContexts,
}

impl ParquetReaderContext {
    pub(crate) fn new(
        uri_info: &ParsedUriInfo,
        match_by: MatchBy,
        tupledesc: &PgTupleDesc,
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
            panic!("no files found that match the pattern {}", uri_info.path);
        }

        let binary_out_funcs = Self::collect_binary_out_funcs(&attributes);

        let per_row_memory_ctx = PgMemoryContexts::new("COPY FROM parquet per row memory context");

        ParquetReaderContext {
            buffer: Vec::new(),
            offset: 0,
            parquet_readers,
            current_parquet_reader_idx: 0,
            binary_out_funcs,
            started: false,
            finished: false,
            per_row_memory_ctx,
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
            let record_batch =
                batch_result.unwrap_or_else(|e| panic!("failed to read record batch: {e}"));

            let num_rows = record_batch.num_rows();

            for i in 0..num_rows {
                check_for_interrupts!();

                // slice the record batch to get the next row
                let record_batch = record_batch.slice(i, 1);

                let natts = self
                    .current_reader()
                    .expect("no reader found")
                    .attribute_count() as i16;

                let tuple_datums = self
                    .current_reader()
                    .expect("no reader found")
                    .record_batch_to_tuple_datums(record_batch);

                self.copy_row(natts, tuple_datums);
            }
        } else if self.current_parquet_reader_idx + 1 < self.parquet_readers.len() {
            // move to the next parquet reader
            self.current_parquet_reader_idx += 1;
            self.read_parquet();
        } else {
            // finish PG copy
            self.copy_finish();
        }

        true
    }

    fn copy_row(&mut self, natts: i16, tuple_datums: Vec<Option<Datum>>) {
        unsafe {
            let mut old_ctx = self.per_row_memory_ctx.set_as_current();

            /* 2 bytes: per-tuple header */
            let attnum_len_bytes = natts.to_be_bytes();
            self.buffer.extend_from_slice(&attnum_len_bytes);

            // write the tuple datums to the ParquetReader's internal buffer in PG copy format
            for (datum, out_func) in tuple_datums.into_iter().zip(self.binary_out_funcs.iter()) {
                if let Some(datum) = datum {
                    let datum_bytes: *mut varlena = SendFunctionCall(out_func.as_ptr(), datum);

                    /* 4 bytes: attribute's data size */
                    let data_size = varsize_any_exhdr(datum_bytes);
                    let data_size_bytes = (data_size as i32).to_be_bytes();
                    self.buffer.extend_from_slice(&data_size_bytes);

                    /* variable bytes: attribute's data */
                    let data = vardata_any(datum_bytes) as _;
                    let data_bytes = std::slice::from_raw_parts(data, data_size);
                    self.buffer.extend_from_slice(data_bytes);
                } else {
                    /* 4 bytes: null */
                    let null_value = -1_i32;
                    let null_value_bytes = null_value.to_be_bytes();
                    self.buffer.extend_from_slice(&null_value_bytes);
                }
            }

            old_ctx.set_as_current();
            self.per_row_memory_ctx.reset();
        };
    }

    fn copy_start(&mut self) {
        self.started = true;

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
    }

    fn copy_finish(&mut self) {
        self.finished = true;

        /* trailer */
        let trailer_len = -1_i16;
        let trailer_len_bytes = trailer_len.to_be_bytes();
        self.buffer.extend_from_slice(&trailer_len_bytes);
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
