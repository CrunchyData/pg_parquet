use std::{cell::RefCell, rc::Rc, sync::Arc};

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use parquet::{
    arrow::{async_writer::ParquetObjectWriter, AsyncArrowWriter},
    file::properties::{EnabledStatistics, WriterProperties},
    format::KeyValue,
};
use pgrx::{heap_tuple::PgHeapTuple, AllocatedByRust, PgTupleDesc};

use crate::{
    arrow_parquet::{
        compression::PgParquetCompressionWithLevel,
        field_ids::validate_field_ids,
        pg_to_arrow::context::collect_pg_to_arrow_attribute_contexts,
        schema_parser::{
            parquet_schema_string_from_attributes, parse_arrow_schema_from_attributes,
        },
        uri_utils::parquet_writer_from_uri,
    },
    parquet_copy_hook::copy_to_split_dest_receiver::CopyToParquetOptions,
    pgrx_utils::{collect_attributes_for, CollectAttributesFor},
    type_compat::{
        geometry::{geoparquet_metadata_to_json, reset_postgis_context, GeoparquetMetadata},
        map::reset_map_context,
    },
    PG_BACKEND_TOKIO_RUNTIME,
};

use super::{
    field_ids::FieldIds,
    pg_to_arrow::{context::PgToArrowAttributeContext, to_arrow_array},
    uri_utils::ParsedUriInfo,
};

pub(crate) const DEFAULT_ROW_GROUP_SIZE: i64 = 122880;
pub(crate) const DEFAULT_ROW_GROUP_SIZE_BYTES: i64 = DEFAULT_ROW_GROUP_SIZE * 1024;

pub(crate) struct ParquetWriterContext {
    parquet_writer: AsyncArrowWriter<ParquetObjectWriter>,
    schema: SchemaRef,
    attribute_contexts: Vec<PgToArrowAttributeContext>,
    geoparquet_metadata: Rc<RefCell<GeoparquetMetadata>>,
    options: CopyToParquetOptions,
}

impl ParquetWriterContext {
    pub(crate) fn new(
        uri_info: ParsedUriInfo,
        options: CopyToParquetOptions,
        field_ids: FieldIds,
        tupledesc: &PgTupleDesc,
    ) -> ParquetWriterContext {
        // Postgis and Map contexts are used throughout writing the parquet file.
        // We need to reset them to avoid reading the stale data. (e.g. extension could be dropped)
        reset_postgis_context();
        reset_map_context();

        let attributes = collect_attributes_for(CollectAttributesFor::CopyTo, tupledesc);

        pgrx::debug2!(
            "schema for tuples: {}",
            parquet_schema_string_from_attributes(&attributes, field_ids.clone())
        );

        let schema = parse_arrow_schema_from_attributes(&attributes, field_ids.clone());

        validate_field_ids(field_ids, &schema).unwrap_or_else(|e| panic!("{e}"));

        let schema = Arc::new(schema);

        let writer_props = Self::writer_props(options);

        let parquet_writer = parquet_writer_from_uri(&uri_info, schema.clone(), writer_props);

        let geoparquet_metadata = Rc::new(RefCell::new(GeoparquetMetadata::new()));

        let attribute_contexts = collect_pg_to_arrow_attribute_contexts(
            &attributes,
            &schema.fields,
            geoparquet_metadata.clone(),
        );

        ParquetWriterContext {
            parquet_writer,
            schema,
            attribute_contexts,
            geoparquet_metadata,
            options,
        }
    }

    fn writer_props(options: CopyToParquetOptions) -> WriterProperties {
        let compression = PgParquetCompressionWithLevel {
            compression: options.compression,
            compression_level: options.compression_level,
        };

        WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_compression(compression.into())
            .set_max_row_group_size(options.row_group_size as usize)
            .set_created_by("pg_parquet".to_string())
            .build()
    }

    // write_tuples writes the tuples to the parquet file. It flushes the in progress rows to a new row group
    // if the row group size is reached.
    pub(crate) fn write_tuples(&mut self, tuples: Vec<Option<PgHeapTuple<AllocatedByRust>>>) {
        let record_batch = Self::pg_tuples_to_record_batch(
            tuples,
            &mut self.attribute_contexts,
            self.schema.clone(),
        );

        let parquet_writer = &mut self.parquet_writer;

        PG_BACKEND_TOKIO_RUNTIME
            .block_on(parquet_writer.write(&record_batch))
            .unwrap_or_else(|e| panic!("failed to write record batch: {e}"));

        if parquet_writer.in_progress_rows() >= self.options.row_group_size as _
            || parquet_writer.in_progress_size() >= self.options.row_group_size_bytes as _
        {
            PG_BACKEND_TOKIO_RUNTIME
                .block_on(parquet_writer.flush())
                .unwrap_or_else(|e| panic!("failed to flush record batch: {e}"));
        }
    }

    fn write_geoparquet_metadata_if_exists(&mut self) {
        let geoparquet_metadata = self.geoparquet_metadata.borrow_mut();

        let has_geoparquet_columns = !geoparquet_metadata.columns.is_empty();

        if !has_geoparquet_columns {
            // No geoparquet columns to write, so we skip writing metadata.
            return;
        }

        let geometry_columns_metadata_value = geoparquet_metadata_to_json(&geoparquet_metadata);

        let key_value_metadata = KeyValue::new("geo".into(), geometry_columns_metadata_value);

        self.parquet_writer
            .append_key_value_metadata(key_value_metadata);
    }

    // finalize flushes the in progress rows to a new row group and finally writes metadata to the file.
    pub(crate) fn finalize(&mut self) {
        PG_BACKEND_TOKIO_RUNTIME
            .block_on(async {
                self.write_geoparquet_metadata_if_exists();

                self.parquet_writer.finish().await
            })
            .unwrap_or_else(|e| panic!("failed to finish parquet writer: {e}"));
    }

    pub(crate) fn bytes_written(&self) -> usize {
        self.parquet_writer.bytes_written()
    }

    fn pg_tuples_to_record_batch(
        tuples: Vec<Option<PgHeapTuple<AllocatedByRust>>>,
        attribute_contexts: &mut [PgToArrowAttributeContext],
        schema: SchemaRef,
    ) -> RecordBatch {
        let mut attribute_arrays = vec![];

        for attribute_context in attribute_contexts {
            let attribute_array = to_arrow_array(&tuples, attribute_context);

            attribute_arrays.push(attribute_array);
        }

        RecordBatch::try_new(schema, attribute_arrays).expect("Expected record batch")
    }
}
