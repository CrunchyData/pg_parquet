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

        let mut record_batches = vec![];

        for tuple in tuples {
            pgrx::pg_sys::check_for_interrupts!();

            let record_batch = if let Some(tuple) = tuple {
                Self::pg_tuple_to_record_batch(tuple, &attribute_contexts, self.schema.clone())
            } else {
                RecordBatch::new_empty(self.schema.clone())
            };

            record_batches.push(record_batch);
        }

        let record_batch = arrow::compute::concat_batches(&self.schema, &record_batches).unwrap();

        let parquet_writer = &mut self.parquet_writer;

        self.runtime
            .block_on(parquet_writer.write(&record_batch))
            .unwrap();
        self.runtime.block_on(parquet_writer.flush()).unwrap();
    }

    fn pg_tuple_to_record_batch(
        tuple: PgHeapTuple<AllocatedByRust>,
        attribute_contexts: &Vec<PgToArrowAttributeContext>,
        schema: SchemaRef,
    ) -> RecordBatch {
        let mut attribute_arrow_arrays = vec![];

        for attribute_context in attribute_contexts {
            let array = to_arrow_array(&tuple, attribute_context);

            attribute_arrow_arrays.push(array);
        }

        RecordBatch::try_new(schema, attribute_arrow_arrays).expect("Expected record batch")
    }

    pub(crate) fn close(self) {
        // should not panic as we can call from try catch block
        self.runtime.block_on(self.parquet_writer.close()).ok();
    }
}
