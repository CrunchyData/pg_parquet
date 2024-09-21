use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use parquet::{
    arrow::{async_writer::ParquetObjectWriter, AsyncArrowWriter},
    file::properties::{EnabledStatistics, WriterProperties},
};
use pgrx::{heap_tuple::PgHeapTuple, pg_sys::RECORDOID, AllocatedByRust, PgTupleDesc};
use tokio::runtime::Runtime;

use crate::{
    arrow_parquet::{
        codec::ParquetCodecOption, schema_visitor::parse_arrow_schema_from_tupledesc,
        uri_utils::parquet_writer_from_uri,
    },
    type_compat::{geometry::reset_postgis_context, map::reset_crunchy_map_context},
};

use super::pg_to_arrow::{collect_attribute_contexts, to_arrow_array, PgToArrowAttributeContext};

pub(crate) struct ParquetWriterContext<'a> {
    runtime: Runtime,
    parquet_writer: AsyncArrowWriter<ParquetObjectWriter>,
    tupledesc: PgTupleDesc<'a>,
    schema: SchemaRef,
}

impl<'a> ParquetWriterContext<'a> {
    pub(crate) fn new(
        uri: &str,
        codec: ParquetCodecOption,
        tupledesc: PgTupleDesc<'a>,
    ) -> ParquetWriterContext<'a> {
        debug_assert!(tupledesc.oid() == RECORDOID);

        reset_postgis_context();
        reset_crunchy_map_context();

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let writer_props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_compression(codec.into())
            .set_created_by("pg_parquet".to_string())
            .build();

        let schema = parse_arrow_schema_from_tupledesc(tupledesc.clone());
        let schema = Arc::new(schema);

        let parquet_writer =
            runtime.block_on(parquet_writer_from_uri(uri, schema.clone(), writer_props));

        ParquetWriterContext {
            runtime,
            parquet_writer,
            tupledesc,
            schema,
        }
    }

    pub(crate) fn write_new_row_group(
        &mut self,
        tuples: Vec<Option<PgHeapTuple<AllocatedByRust>>>,
    ) {
        let attribute_contexts = collect_attribute_contexts(&self.tupledesc, &self.schema.fields);

        let record_batch =
            Self::pg_tuples_to_record_batch(tuples, &attribute_contexts, self.schema.clone());

        let parquet_writer = &mut self.parquet_writer;

        self.runtime
            .block_on(parquet_writer.write(&record_batch))
            .unwrap();
        self.runtime.block_on(parquet_writer.flush()).unwrap();
    }

    fn pg_tuples_to_record_batch(
        tuples: Vec<Option<PgHeapTuple<AllocatedByRust>>>,
        attribute_contexts: &Vec<PgToArrowAttributeContext>,
        schema: SchemaRef,
    ) -> RecordBatch {
        let mut attribute_arrays = vec![];

        for attribute_context in attribute_contexts {
            let attribute_array = to_arrow_array(&tuples, attribute_context);

            attribute_arrays.push(attribute_array);
        }

        RecordBatch::try_new(schema, attribute_arrays).expect("Expected record batch")
    }

    pub(crate) fn close(self) {
        self.runtime.block_on(self.parquet_writer.close()).unwrap();
    }
}
