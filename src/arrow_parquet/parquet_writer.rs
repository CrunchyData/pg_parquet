use std::{ffi::CString, path::Path, rc::Rc, sync::Arc};

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use parquet::{
    arrow::{async_writer::ParquetObjectWriter, AsyncArrowWriter},
    file::properties::{EnabledStatistics, WriterProperties},
    format::KeyValue,
};
use pgrx::{
    heap_tuple::PgHeapTuple,
    pg_sys::{
        appendBinaryStringInfo, fmgr_info, getTypeBinaryInputInfo, get_typlenbyval, makeStringInfo,
        toast_raw_datum_size, Datum, FmgrInfo, FormData_pg_attribute, InvalidOid, Oid,
        ReceiveFunctionCall, VARHDRSZ,
    },
    AllocatedByRust, FromDatum, PgBox, PgMemoryContexts, PgTupleDesc,
};

use crate::{
    arrow_parquet::{
        compression::PgParquetCompressionWithLevel,
        field_ids::validate_field_ids,
        pg_to_arrow::context::collect_pg_to_arrow_attribute_contexts,
        schema_parser::{
            parquet_schema_string_from_attributes, parse_arrow_schema_from_attributes,
        },
        uri_utils::{parquet_writer_from_uri, uri_as_string},
    },
    parquet_copy_hook::copy_to::CopyToParquetOptions,
    pgrx_utils::{collect_attributes_for, CollectAttributesFor},
    type_compat::{
        geometry::{geoparquet_metadata_json_from_tupledesc, reset_postgis_context},
        map::reset_map_context,
    },
    PG_BACKEND_TOKIO_RUNTIME,
};

use super::{
    pg_to_arrow::{context::PgToArrowAttributeContext, to_arrow_array},
    uri_utils::ParsedUriInfo,
};

pub(crate) const DEFAULT_ROW_GROUP_SIZE: i64 = 122880;
pub(crate) const DEFAULT_ROW_GROUP_SIZE_BYTES: i64 = DEFAULT_ROW_GROUP_SIZE * 1024;

pub(crate) struct ParquetWriterContext<'a> {
    current_parquet_writer_idx: usize,
    current_parquet_writer: Option<AsyncArrowWriter<ParquetObjectWriter>>,
    uri_info: Rc<ParsedUriInfo>,
    writer_props: WriterProperties,
    schema: SchemaRef,
    attribute_contexts: Vec<PgToArrowAttributeContext>,
    options: CopyToParquetOptions,
    tupledesc: PgTupleDesc<'a>,
    binary_in_funcs: Vec<BinaryInputFunctionInfo>,
    collected_tuples: Vec<Option<PgHeapTuple<'a, AllocatedByRust>>>,
    collected_tuple_column_sizes: Vec<usize>,
    pg_copy_started: bool,
    pg_copy_finished: bool,
    copy_per_batch_memory_ctx: PgMemoryContexts,
    pub(crate) copy_per_tuple_memory_ctx: PgMemoryContexts,
}

struct BinaryInputFunctionInfo {
    recv_func: PgBox<FmgrInfo>,
    io_param: Oid,
    typmod: i32,
}

impl<'a> ParquetWriterContext<'a> {
    pub(crate) fn new(
        uri_info: Rc<ParsedUriInfo>,
        options: CopyToParquetOptions,
        tupledesc: PgTupleDesc<'a>,
    ) -> Self {
        // Postgis and Map contexts are used throughout writing the parquet file.
        // We need to reset them to avoid reading the stale data. (e.g. extension could be dropped)
        reset_postgis_context();
        reset_map_context();

        let attributes = collect_attributes_for(CollectAttributesFor::CopyTo, &tupledesc);

        let binary_in_funcs = Self::collect_binary_in_funcs(&attributes);

        pgrx::debug2!(
            "schema for tuples: {}",
            parquet_schema_string_from_attributes(&attributes, options.field_ids.clone())
        );

        let schema = parse_arrow_schema_from_attributes(&attributes, options.field_ids.clone());

        validate_field_ids(options.field_ids.clone(), &schema).unwrap_or_else(|e| panic!("{e}"));

        let schema = Arc::new(schema);

        let writer_props = Self::writer_props(&tupledesc, options.clone());

        let attribute_contexts =
            collect_pg_to_arrow_attribute_contexts(&attributes, &schema.fields);

        let collected_tuple_column_sizes = vec![0; tupledesc.len()];

        let copy_per_batch_memory_ctx = PgMemoryContexts::new("pg_parquet copy_to batch tuples");

        let copy_per_tuple_memory_ctx = PgMemoryContexts::new("pg_parquet copy_to per tuple");

        ParquetWriterContext {
            current_parquet_writer: None,
            current_parquet_writer_idx: 0,
            uri_info,
            writer_props,
            schema,
            attribute_contexts,
            options,
            binary_in_funcs,
            copy_per_batch_memory_ctx,
            copy_per_tuple_memory_ctx,
            tupledesc,
            collected_tuples: vec![],
            collected_tuple_column_sizes,
            pg_copy_started: false,
            pg_copy_finished: false,
        }
    }

    fn collect_binary_in_funcs(
        attributes: &[FormData_pg_attribute],
    ) -> Vec<BinaryInputFunctionInfo> {
        unsafe {
            let mut binary_in_funcs = vec![];

            for att in attributes.iter() {
                let typoid = att.type_oid();
                let typmod = att.type_mod();

                let mut recv_func_oid = InvalidOid;
                let mut io_param = InvalidOid;
                getTypeBinaryInputInfo(typoid.value(), &mut recv_func_oid, &mut io_param);

                let arg_fninfo = PgBox::<FmgrInfo>::alloc0().into_pg_boxed();
                fmgr_info(recv_func_oid, arg_fninfo.as_ptr());

                binary_in_funcs.push(BinaryInputFunctionInfo {
                    recv_func: arg_fninfo,
                    io_param,
                    typmod,
                });
            }

            binary_in_funcs
        }
    }

    fn writer_props(tupledesc: &PgTupleDesc, options: CopyToParquetOptions) -> WriterProperties {
        let compression = PgParquetCompressionWithLevel {
            compression: options.compression,
            compression_level: options.compression_level,
        };

        let mut writer_props_builder = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_compression(compression.into())
            .set_max_row_group_size(options.row_group_size as usize)
            .set_writer_version(options.parquet_version.into())
            .set_created_by("pg_parquet".to_string());

        let geometry_columns_metadata_value = geoparquet_metadata_json_from_tupledesc(tupledesc);

        if geometry_columns_metadata_value.is_some() {
            let key_value_metadata = KeyValue::new("geo".into(), geometry_columns_metadata_value);

            writer_props_builder =
                writer_props_builder.set_key_value_metadata(Some(vec![key_value_metadata]));
        }

        writer_props_builder.build()
    }

    pub(crate) fn collect_tuple_from_buffer(&mut self, inbuf: &[u8]) {
        if self.pg_copy_finished {
            return;
        }

        let mut current_offset = 0_usize;

        if !self.pg_copy_started {
            // consume binary header
            self.pg_copy_started = true;
            current_offset = 19;
        }

        if inbuf[current_offset..].len() == 2 {
            // consume binary trailer
            self.pg_copy_finished = true;

            self.finalize_current_parquet_writer();

            return;
        }

        if self.current_parquet_writer.is_none() {
            self.set_current_parquet_writer();
        }

        let (tuple_datums, tuple_column_sizes) = if inbuf[current_offset..].len() == 4 {
            // null tuple
            (None, vec![0; self.tupledesc.len()])
        } else {
            let tuple_datums = self.buffer_to_tuple_datums(inbuf, &mut current_offset);

            let tuple_column_sizes = Self::tuple_column_sizes(&tuple_datums, &self.tupledesc);

            (Some(tuple_datums), tuple_column_sizes)
        };

        if self.collected_tuples_exceeds_max_col_size(&tuple_column_sizes) {
            self.flush_new_row_group();
        }

        self.collect_tuple(tuple_datums, &tuple_column_sizes);

        const WRITE_BATCH_SIZE: usize = 1024;

        // instead of writing each RecordBatch to parquet file
        // we accumulate them and write in larger batches to reduce IO
        if self.collected_tuples.len() >= WRITE_BATCH_SIZE {
            self.write_tuples_to_current_row_group();
        }

        if self.collected_tuples_exceeds_row_group_size() {
            self.flush_new_row_group();
        }

        if self.collected_tuples_exceeds_max_file_size() {
            self.finalize_current_parquet_writer();
        }
    }

    fn buffer_to_tuple_datums(
        &self,
        inbuf: &[u8],
        current_offset: &mut usize,
    ) -> Vec<Option<Datum>> {
        let mut datums = vec![];

        // skip 2 bytes per tuple header
        *current_offset += 2;

        for func_info in self.binary_in_funcs.iter() {
            let BinaryInputFunctionInfo {
                recv_func,
                io_param,
                typmod,
            } = func_info;

            // read 4 bytes: attribute's data size
            let attribute_size = i32::from_be_bytes(
                inbuf[*current_offset..*current_offset + 4]
                    .try_into()
                    .expect("cannot convert 4 bytes to i32"),
            );

            *current_offset += 4;

            let datum = if attribute_size == -1 {
                // null
                None
            } else {
                // read variable bytes: attribute's data
                let attribute_bytes =
                    &inbuf[*current_offset..*current_offset + attribute_size as usize];

                *current_offset += attribute_size as usize;

                let attribute_bytes_str = unsafe { makeStringInfo() };
                unsafe {
                    appendBinaryStringInfo(
                        attribute_bytes_str,
                        attribute_bytes.as_ptr() as _,
                        attribute_size,
                    );
                };

                Some(unsafe {
                    ReceiveFunctionCall(recv_func.as_ptr(), attribute_bytes_str, *io_param, *typmod)
                })
            };

            datums.push(datum);
        }

        datums
    }

    fn collect_tuple(
        &mut self,
        tuple_datums: Option<Vec<Option<Datum>>>,
        tuple_column_sizes: &[usize],
    ) {
        let tuple = tuple_datums.map(|tuple_datums| unsafe {
            let mut old_ctx = self.copy_per_batch_memory_ctx.set_as_current();

            // keep tuple for later use
            let tuple = PgHeapTuple::from_datums(self.tupledesc.clone(), tuple_datums)
                .expect("cannot create tuple from datums during copy to");

            old_ctx.set_as_current();

            tuple
        });

        self.collected_tuples.push(tuple);

        self.collected_tuple_column_sizes
            .iter_mut()
            .zip(tuple_column_sizes.iter())
            .for_each(|(a, b)| *a += *b);
    }

    fn write_tuples_to_current_row_group(&mut self) {
        let record_batch = Self::pg_tuples_to_record_batch(
            &self.collected_tuples,
            &self.attribute_contexts,
            self.schema.clone(),
        );

        let parquet_writer = self
            .current_parquet_writer
            .as_mut()
            .expect("parquet writer not set");

        PG_BACKEND_TOKIO_RUNTIME
            .block_on(parquet_writer.write(&record_batch))
            .unwrap_or_else(|e| panic!("failed to write record batch: {}", e));

        self.reset_collected_tuples();
    }

    fn flush_new_row_group(&mut self) {
        if !self.collected_tuples.is_empty() {
            self.write_tuples_to_current_row_group();
        }

        let parquet_writer = self
            .current_parquet_writer
            .as_mut()
            .expect("parquet writer not set");

        PG_BACKEND_TOKIO_RUNTIME
            .block_on(parquet_writer.flush())
            .unwrap_or_else(|e| panic!("failed to flush record batch: {}", e));
    }

    // flushes the last row group if any and writes metadata to the file.
    fn finalize_current_parquet_writer(&mut self) {
        if !self.collected_tuples.is_empty() {
            self.flush_new_row_group();
        }

        let parquet_writer = self
            .current_parquet_writer
            .as_mut()
            .expect("parquet writer not set");

        PG_BACKEND_TOKIO_RUNTIME
            .block_on(parquet_writer.finish())
            .unwrap_or_else(|e| panic!("failed to finish parquet writer: {e}"));

        self.current_parquet_writer = None;
    }

    fn reset_collected_tuples(&mut self) {
        self.collected_tuples.clear();
        self.collected_tuple_column_sizes = vec![0; self.tupledesc.len()];

        unsafe { self.copy_per_batch_memory_ctx.reset() };
    }

    fn set_current_parquet_writer(&mut self) {
        let uri_info = self.create_uri_for_current_parquet_writer();

        self.current_parquet_writer = Some(parquet_writer_from_uri(
            &uri_info,
            self.schema.clone(),
            self.writer_props.clone(),
        ));

        self.current_parquet_writer_idx += 1;
    }

    fn create_uri_for_current_parquet_writer(&self) -> ParsedUriInfo {
        let uri = uri_as_string(&self.uri_info.uri);

        // file_size_bytes not specified, use the original uri
        let current_parquet_writer_uri = if !self.options.has_file_size_bytes() {
            uri
        } else {
            let parent_folder = Path::new(&uri);

            let file_id = self.current_parquet_writer_idx;

            let child_uri = parent_folder.join(format!("data_{file_id}.parquet"));
            child_uri.to_str().expect("invalid child uri").to_string()
        };

        ParsedUriInfo::try_from(current_parquet_writer_uri.as_str()).unwrap_or_else(|e| {
            panic!("failed to create URI for child parquet writer: {e}");
        })
    }

    fn collected_tuples_exceeds_row_group_size(&self) -> bool {
        let parquet_writer = self
            .current_parquet_writer
            .as_ref()
            .expect("parquet writer not set");

        parquet_writer.in_progress_rows() >= self.options.row_group_size as _
            || parquet_writer.in_progress_size() >= self.options.row_group_size_bytes as _
    }

    fn collected_tuples_exceeds_max_col_size(&self, tuple_column_sizes: &[usize]) -> bool {
        const MAX_ARROW_ARRAY_SIZE: usize = i32::MAX as _;

        self.collected_tuple_column_sizes
            .iter()
            .zip(tuple_column_sizes)
            .map(|(a, b)| *a + *b)
            .any(|size| size > MAX_ARROW_ARRAY_SIZE)
    }

    fn collected_tuples_exceeds_max_file_size(&self) -> bool {
        if !self.options.has_file_size_bytes() {
            return false;
        }

        let parquet_writer = self
            .current_parquet_writer
            .as_ref()
            .expect("parquet writer not set");

        parquet_writer.bytes_written() > self.options.file_size_bytes() as _
    }

    fn tuple_column_sizes(tuple_datums: &[Option<Datum>], tupledesc: &PgTupleDesc) -> Vec<usize> {
        let mut column_sizes = vec![];

        for (idx, column_datum) in tuple_datums.iter().enumerate() {
            if column_datum.is_none() {
                column_sizes.push(0);
                continue;
            }

            let column_datum = column_datum.as_ref().expect("datum should not be None");

            let attribute = tupledesc.get(idx).expect("cannot get attribute");

            let typoid = attribute.type_oid();

            let mut typlen = -1_i16;
            let mut typbyval = false;
            unsafe { get_typlenbyval(typoid.value(), &mut typlen, &mut typbyval) };

            let column_size = if typlen == -1 {
                (unsafe { toast_raw_datum_size(*column_datum) }) - VARHDRSZ
            } else if typlen == -2 {
                // cstring
                let cstring = unsafe {
                    CString::from_datum(*column_datum, false)
                        .expect("cannot get cstring from datum")
                };
                cstring.as_bytes().len() + 1
            } else {
                // fixed size type
                typlen as usize
            };

            column_sizes.push(column_size);
        }

        column_sizes
    }

    fn pg_tuples_to_record_batch(
        tuples: &Vec<Option<PgHeapTuple<AllocatedByRust>>>,
        attribute_contexts: &[PgToArrowAttributeContext],
        schema: SchemaRef,
    ) -> RecordBatch {
        let mut attribute_arrays = vec![];

        for attribute_context in attribute_contexts {
            let attribute_array = to_arrow_array(tuples, attribute_context);

            attribute_arrays.push(attribute_array);
        }

        RecordBatch::try_new(schema, attribute_arrays).expect("Expected record batch")
    }
}
