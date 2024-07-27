use std::{fs::File, sync::Arc};

use arrow::{
    array::ArrayRef,
    datatypes::{FieldRef, SchemaRef},
};
use parquet::{
    arrow::arrow_writer::{compute_leaves, get_column_writers},
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    schema::types::SchemaDescriptor,
};
use pgrx::{heap_tuple::PgHeapTuple, pg_sys::RECORDOID, AllocatedByRust, PgTupleDesc};

use crate::{
    arrow_parquet::{
        pg_to_arrow::record::collect_attribute_array_from_tuples,
        schema_visitor::{parse_arrow_schema_from_tupledesc, parse_parquet_schema_from_tupledesc},
    },
    pgrx_utils::collect_valid_attributes,
};

pub(crate) struct ParquetWriterContext<'a> {
    writer_props: Arc<WriterProperties>,
    parquet_writer: SerializedFileWriter<File>,
    tupledesc: PgTupleDesc<'a>,
    parquet_schema: SchemaDescriptor,
    arrow_schema: SchemaRef,
}

impl<'a> ParquetWriterContext<'a> {
    pub(crate) fn new(filename: &str, tupledesc: PgTupleDesc<'a>) -> ParquetWriterContext<'a> {
        assert!(tupledesc.oid() == RECORDOID);

        let writer_props = Arc::new(WriterProperties::default());

        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(filename)
            .unwrap();

        let arrow_schema = parse_arrow_schema_from_tupledesc(tupledesc.clone()).into();
        let parquet_schema = parse_parquet_schema_from_tupledesc(tupledesc.clone());

        let root_schema = parquet_schema.root_schema_ptr();
        let parquet_writer =
            SerializedFileWriter::new(file, root_schema, writer_props.clone()).unwrap();

        ParquetWriterContext {
            writer_props,
            parquet_writer,
            parquet_schema,
            arrow_schema,
            tupledesc,
        }
    }

    pub(crate) fn write_new_row_group(
        &mut self,
        tuples: Vec<Option<PgHeapTuple<AllocatedByRust>>>,
    ) {
        pgrx::pg_sys::check_for_interrupts!();

        let parquet_schema = &self.parquet_schema;
        let writer_props = self.writer_props.clone();
        let arrow_schema = self.arrow_schema.clone();
        let parquet_writer = &mut self.parquet_writer;

        let mut row_group = parquet_writer.next_row_group().unwrap();

        // collect arrow arrays for each attribute in the tuples
        let tuple_attribute_arrow_arrays =
            collect_arrow_attribute_arrays_from_tupledesc(tuples, self.tupledesc.clone());

        // get column writers
        let col_writers =
            get_column_writers(&parquet_schema, &writer_props, &arrow_schema).unwrap();

        // compute and append leave columns to row group, each column writer writes a column chunk
        let mut worker_iter = col_writers.into_iter();
        for (field, arr) in tuple_attribute_arrow_arrays.iter() {
            for leave_columns in compute_leaves(field, arr).unwrap() {
                let mut col_writer = worker_iter.next().unwrap();
                col_writer.write(&leave_columns).unwrap();

                let chunk = col_writer.close().unwrap();
                chunk.append_to_row_group(&mut row_group).unwrap();
            }
        }

        row_group.close().unwrap();
    }
}

impl Drop for ParquetWriterContext<'_> {
    fn drop(&mut self) {
        self.parquet_writer.finish().unwrap();
    }
}

fn collect_arrow_attribute_arrays_from_tupledesc(
    tuples: Vec<Option<PgHeapTuple<AllocatedByRust>>>,
    tupledesc: PgTupleDesc,
) -> Vec<(FieldRef, ArrayRef)> {
    let include_generated_columns = true;
    let attributes = collect_valid_attributes(&tupledesc, include_generated_columns);

    let mut tuple_attribute_arrow_arrays = vec![];

    let mut tuples = tuples;

    for attribute in attributes {
        let attribute_name = attribute.name();
        let attribute_typoid = attribute.type_oid().value();
        let attribute_typmod = attribute.type_mod();

        let (field, array, tups) = collect_attribute_array_from_tuples(
            tuples,
            tupledesc.clone(),
            attribute_name,
            attribute_typoid,
            attribute_typmod,
        );

        tuples = tups;
        let tuple_attribute_arrow_array = (field, array);

        tuple_attribute_arrow_arrays.push(tuple_attribute_arrow_array);
    }

    tuple_attribute_arrow_arrays
}
