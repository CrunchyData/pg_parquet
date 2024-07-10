use std::sync::Arc;

use arrow::{
    array::ArrayRef,
    buffer::{OffsetBuffer, ScalarBuffer},
    datatypes::SchemaRef,
};
use parquet::{
    arrow::arrow_writer::{compute_leaves, get_column_writers},
    file::{
        properties::WriterProperties,
        writer::{SerializedFileWriter, SerializedRowGroupWriter},
    },
    schema::types::SchemaDescriptor,
};
use pgrx::{heap_tuple::PgHeapTuple, AllocatedByRust, FromDatum, IntoDatum, PgTupleDesc};

use crate::{
    arrow_parquet::{
        array_visitor::visit_list_array,
        schema_visitor::{parse_schema, to_parquet_schema},
    },
    pgrx_utils::tupledesc_for_tuples,
};

pub(crate) fn write_to_parquet(filename: &str, tuples: Vec<PgHeapTuple<'_, AllocatedByRust>>) {
    let (tuples, tupledesc) = tupledesc_for_tuples(tuples);

    let array_oid = tuples
        .composite_type_oid()
        .expect("array of records are expected");

    // parse and verify schema for given tuples
    let arrow_schema = parse_schema(array_oid, tupledesc.clone(), "root");
    let parquet_schema = to_parquet_schema(&arrow_schema);

    // write tuples to parquet file
    let writer_props = Arc::new(WriterProperties::default());
    let mut parquet_writer =
        prepare_parquet_writer(filename, &parquet_schema, writer_props.clone());
    let mut row_group = parquet_writer.next_row_group().unwrap();

    write_to_row_group(
        tuples,
        tupledesc,
        parquet_schema.into(),
        arrow_schema.into(),
        writer_props,
        &mut row_group,
    );

    row_group.close().unwrap();
    parquet_writer.close().unwrap();
}

fn write_to_row_group(
    tuples: Vec<PgHeapTuple<'_, AllocatedByRust>>,
    tupledesc: PgTupleDesc,
    parquet_schema: Arc<SchemaDescriptor>,
    arrow_schema: SchemaRef,
    writer_props: Arc<WriterProperties>,
    row_group: &mut SerializedRowGroupWriter<std::fs::File>,
) {
    // compute arrow root array
    let root_array = collect_arrow_root_array(tuples, tupledesc);
    let root = vec![root_array];

    // get column writers
    let col_writers = get_column_writers(&parquet_schema, &writer_props, &arrow_schema).unwrap();

    // compute and append leave columns to row group, each column writer writes a column chunk
    let mut worker_iter = col_writers.into_iter();
    for (arr, field) in root.iter().zip(&arrow_schema.fields) {
        for leave_columns in compute_leaves(field, arr).unwrap() {
            let mut col_writer = worker_iter.next().unwrap();
            col_writer.write(&leave_columns).unwrap();

            let chunk = col_writer.close().unwrap();
            chunk.append_to_row_group(row_group).unwrap();
        }
    }
}

fn prepare_parquet_writer(
    filename: &str,
    parquet_schema: &SchemaDescriptor,
    writer_props: Arc<WriterProperties>,
) -> SerializedFileWriter<std::fs::File> {
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(filename)
        .unwrap();

    let root_schema = parquet_schema.root_schema_ptr();
    SerializedFileWriter::new(file, root_schema, writer_props).unwrap()
}

fn collect_arrow_root_array(
    tuples: Vec<PgHeapTuple<'_, AllocatedByRust>>,
    tupledesc: PgTupleDesc,
) -> ArrayRef {
    let total_tuples = tuples.len();

    let array_oid = tuples.composite_type_oid().unwrap();

    let tuple_array = unsafe {
        pgrx::AnyArray::from_polymorphic_datum(tuples.into_datum().unwrap(), false, array_oid)
    }
    .unwrap();

    let (_, data) = visit_list_array(
        "root",
        tuple_array,
        Some(tupledesc),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, total_tuples as i32])),
    );

    data
}
