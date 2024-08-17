use std::sync::Arc;

use arrow::{
    array::{ArrayRef, RecordBatch, StructArray},
    datatypes::FieldRef,
};
use arrow_schema::SchemaRef;
use parquet::{
    arrow::{async_writer::ParquetObjectWriter, AsyncArrowWriter},
    file::properties::{EnabledStatistics, WriterProperties},
};
use pgrx::{heap_tuple::PgHeapTuple, pg_sys::RECORDOID, AllocatedByRust, PgTupleDesc};
use tokio::runtime::Runtime;

use crate::{
    arrow_parquet::{
        codec::ParquetCodecOption, pg_to_arrow::collect_attribute_array_from_tuples,
        schema_visitor::parse_arrow_schema_from_tupledesc, uri_utils::parquet_writer_from_uri,
    },
    pgrx_utils::collect_valid_attributes,
};

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
        pgrx::pg_sys::check_for_interrupts!();

        // collect arrow arrays for each attribute in the tuples
        let tuple_attribute_arrow_arrays = collect_arrow_attribute_arrays_from_tupledesc(
            tuples,
            self.tupledesc.clone(),
            self.schema.clone(),
        );

        let struct_array = StructArray::from(tuple_attribute_arrow_arrays);
        let record_batch = RecordBatch::from(struct_array);

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

fn collect_arrow_attribute_arrays_from_tupledesc(
    tuples: Vec<Option<PgHeapTuple<AllocatedByRust>>>,
    tupledesc: PgTupleDesc,
    schema: SchemaRef,
) -> Vec<(FieldRef, ArrayRef)> {
    let include_generated_columns = true;
    let attributes = collect_valid_attributes(&tupledesc, include_generated_columns);

    let mut tuple_attribute_arrow_arrays = vec![];

    let mut tuples = tuples;

    for attribute in attributes {
        let attribute_name = attribute.name();
        let attribute_typoid = attribute.type_oid().value();
        let attribute_typmod = attribute.type_mod();
        let attribute_field = schema
            .field_with_name(attribute_name)
            .expect("Expected attribute field");

        let (field, array, tups) = collect_attribute_array_from_tuples(
            tuples,
            tupledesc.clone(),
            attribute_name,
            attribute_typoid,
            attribute_typmod,
            Arc::new(attribute_field.clone()),
        );

        tuples = tups;
        let tuple_attribute_arrow_array = (field, array);

        tuple_attribute_arrow_arrays.push(tuple_attribute_arrow_array);
    }

    tuple_attribute_arrow_arrays
}
