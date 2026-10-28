use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use parquet::{
    arrow::{async_writer::AsyncFileWriter, AsyncArrowWriter},
    file::properties::{EnabledStatistics, WriterProperties},
};
use pgrx::{pg_sys::Datum, PgTupleDesc};

use crate::{
    arrow_parquet::{
        column_selection::leaf_column_paths,
        compression::PgParquetCompressionWithLevel,
        field_ids::validate_field_ids,
        parquet_shared_upload::stash_range_metadata,
        pg_to_arrow::context::collect_pg_to_arrow_attribute_contexts,
        schema::parse_schema::{
            parquet_schema_string_from_attributes, parse_arrow_schema_from_attributes,
        },
        uri_utils::parquet_writer_from_uri,
    },
    block_on,
    object_store::object_store_error_message,
    parquet_copy_hook::copy_to_split_dest_receiver::CopyToParquetOptions,
    pgrx_utils::{collect_attributes_for, CollectAttributesFor},
    type_compat::{geometry::reset_postgis_context, map::reset_map_context},
};

use super::{
    column_selection::ColumnSelection,
    field_ids::FieldIds,
    pg_to_arrow::{context::PgToArrowAttributeContext, to_arrow_array},
    uri_utils::ParsedUriInfo,
};

pub(crate) const DEFAULT_ROW_GROUP_SIZE: i64 = 122880;
pub(crate) const DEFAULT_ROW_GROUP_SIZE_BYTES: i64 = DEFAULT_ROW_GROUP_SIZE * 1024;

pub(crate) struct ParquetWriterContext {
    parquet_writer: AsyncArrowWriter<Box<dyn AsyncFileWriter>>,
    schema: SchemaRef,
    attribute_contexts: Vec<PgToArrowAttributeContext>,
    options: CopyToParquetOptions,
}

impl ParquetWriterContext {
    pub(crate) fn new(
        uri_info: ParsedUriInfo,
        options: CopyToParquetOptions,
        field_ids: FieldIds,
        bloom_filter: ColumnSelection,
        dictionary: ColumnSelection,
        tupledesc: &PgTupleDesc,
    ) -> ParquetWriterContext {
        // Postgis and Map contexts are used throughout writing the parquet file.
        // We need to reset them to avoid reading the stale data. (e.g. extension could be dropped)
        reset_postgis_context();
        reset_map_context();

        let attributes = collect_attributes_for(CollectAttributesFor::CopyTo, tupledesc);

        // the field ids are validated against the schema parsed without them, since parsing with
        // them is what cannot explain a mapping that does not fit the schema
        let schema_without_field_ids =
            parse_arrow_schema_from_attributes(&attributes, FieldIds::None);

        validate_field_ids(&field_ids, &schema_without_field_ids).unwrap_or_else(|e| panic!("{e}"));

        pgrx::debug2!(
            "schema for tuples: {}",
            parquet_schema_string_from_attributes(&attributes, field_ids.clone())
        );

        let schema = parse_arrow_schema_from_attributes(&attributes, field_ids);

        bloom_filter
            .validate_against_schema(&schema, "bloom_filter")
            .unwrap_or_else(|e| panic!("{e}"));

        dictionary
            .validate_against_schema(&schema, "dictionary")
            .unwrap_or_else(|e| panic!("{e}"));

        let writer_props = Self::writer_props(&schema, options, &bloom_filter, &dictionary);

        let schema = Arc::new(schema);

        let parquet_writer = parquet_writer_from_uri(&uri_info, schema.clone(), writer_props);

        let attribute_contexts =
            collect_pg_to_arrow_attribute_contexts(&attributes, &schema.fields);

        ParquetWriterContext {
            parquet_writer,
            schema,
            attribute_contexts,
            options,
        }
    }

    fn writer_props(
        schema: &Schema,
        options: CopyToParquetOptions,
        bloom_filter: &ColumnSelection,
        dictionary: &ColumnSelection,
    ) -> WriterProperties {
        let compression = PgParquetCompressionWithLevel {
            compression: options.compression,
            compression_level: options.compression_level,
        };

        let mut writer_props_builder = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_compression(compression.into())
            .set_max_row_group_row_count(Some(options.row_group_size as usize))
            .set_writer_version(options.parquet_version.into())
            .set_created_by("pg_parquet".to_string());

        // bloom filters and dictionaries are set per leaf column rather than for all columns
        // at once, so that a selection can enable them for some of the columns
        for leaf_column_path in leaf_column_paths(schema) {
            if let Some(enabled) = bloom_filter.enabled_for(&leaf_column_path) {
                writer_props_builder = writer_props_builder
                    .set_column_bloom_filter_enabled(leaf_column_path.clone(), enabled);

                // setting the false positive probability also enables the bloom filter, so it
                // is only set for the columns that have one
                if enabled {
                    writer_props_builder = writer_props_builder.set_column_bloom_filter_fpp(
                        leaf_column_path.clone(),
                        options.bloom_filter_fpp,
                    );
                }
            }

            if let Some(enabled) = dictionary.enabled_for(&leaf_column_path) {
                writer_props_builder =
                    writer_props_builder.set_column_dictionary_enabled(leaf_column_path, enabled);
            }
        }

        writer_props_builder.build()
    }

    // write_columns writes the collected columns to the parquet file. It flushes the in progress
    // rows to a new row group if the row group size is reached.
    pub(crate) fn write_columns(&mut self, columns: &[Vec<Option<Datum>>]) {
        let record_batch = Self::pg_columns_to_record_batch(
            columns,
            &self.attribute_contexts,
            self.schema.clone(),
        );

        let parquet_writer = &mut self.parquet_writer;

        block_on(parquet_writer.write(&record_batch)).unwrap_or_else(|e| {
            panic!(
                "failed to write record batch: {}",
                object_store_error_message(e)
            )
        });

        if parquet_writer.in_progress_rows() >= self.options.row_group_size as _
            || parquet_writer.in_progress_size() >= self.options.row_group_size_bytes as _
        {
            block_on(parquet_writer.flush()).unwrap_or_else(|e| {
                panic!(
                    "failed to flush record batch: {}",
                    object_store_error_message(e)
                )
            });
        }
    }

    // finalize flushes the in progress rows to a new row group and finally writes metadata to the file.
    pub(crate) fn finalize(&mut self) {
        let metadata = block_on(self.parquet_writer.finish()).unwrap_or_else(|e| {
            panic!(
                "failed to finish parquet writer: {}",
                object_store_error_message(e)
            )
        });

        // a range of a parallel COPY TO wrote its rows into the destination's own upload
        // rather than into a file of its own, so the leader is left to write a footer that
        // covers every range, which is what the metadata of this one says where to find
        stash_range_metadata(&metadata);
    }

    pub(crate) fn bytes_written(&self) -> usize {
        self.parquet_writer.bytes_written()
    }

    fn pg_columns_to_record_batch(
        columns: &[Vec<Option<Datum>>],
        attribute_contexts: &[PgToArrowAttributeContext],
        schema: SchemaRef,
    ) -> RecordBatch {
        let mut attribute_arrays = vec![];

        for (attribute_context, column_datums) in attribute_contexts.iter().zip(columns) {
            let attribute_array = to_arrow_array(column_datums, attribute_context);

            attribute_arrays.push(attribute_array);
        }

        RecordBatch::try_new(schema, attribute_arrays).expect("Expected record batch")
    }
}
