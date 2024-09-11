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
    pgrx_utils::collect_valid_attributes,
    type_compat::{geometry::reset_postgis_context, map::reset_crunchy_map_context},
};

use super::pg_to_arrow::to_arrow_array;

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
        let mut record_batches = vec![];

        for tuple in tuples {
            pgrx::pg_sys::check_for_interrupts!();

            if let Some(tuple) = tuple {
                let record_batch =
                    pg_tuple_to_record_batch(tuple, &self.tupledesc, self.schema.clone());

                record_batches.push(record_batch);
            } else {
                let null_record_batch = RecordBatch::new_empty(self.schema.clone());
                record_batches.push(null_record_batch);
            }
        }

        let record_batch = arrow::compute::concat_batches(&self.schema, &record_batches).unwrap();

        let parquet_writer = &mut self.parquet_writer;

        self.runtime
            .block_on(parquet_writer.write(&record_batch))
            .unwrap();
        self.runtime.block_on(parquet_writer.flush()).unwrap();
    }

    pub(crate) fn close(self) {
        self.runtime.block_on(self.parquet_writer.close()).unwrap();
    }
}

fn pg_tuple_to_record_batch(
    tuple: PgHeapTuple<AllocatedByRust>,
    tupledesc: &PgTupleDesc,
    schema: SchemaRef,
) -> RecordBatch {
    let include_generated_columns = true;
    let attributes = collect_valid_attributes(tupledesc, include_generated_columns);

    let mut attribute_arrow_arrays = vec![];

    for attribute in attributes {
        let attribute_name = attribute.name();
        let attribute_typoid = attribute.type_oid().value();
        let attribute_typmod = attribute.type_mod();
        let attribute_field = schema
            .field_with_name(attribute_name)
            .expect("Expected attribute field");

        let (_field, array) = to_arrow_array(
            &tuple,
            attribute_name,
            attribute_typoid,
            attribute_typmod,
            Arc::new(attribute_field.clone()),
        );

        attribute_arrow_arrays.push(array);
    }

    RecordBatch::try_new(schema, attribute_arrow_arrays).expect("Expected record batch")
}
