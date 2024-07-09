use parquet::{
    arrow::arrow_writer::{compute_leaves, get_column_writers, ArrowLeafColumn},
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    schema::printer,
};
use pgrx::{pg_getarg, prelude::*};

use schema_parser::parse_schema;
use schema_parser::to_parquet_schema;
use serializer::tupledesc_for_tuples;

mod conversion;
mod copy_hook;
mod parquet_dest_receiver;
mod schema_parser;
mod serializer;

pgrx::pg_module_magic!();

#[pg_schema]
mod pgparquet {
    use arrow::buffer::{OffsetBuffer, ScalarBuffer};
    use serializer::visit_list_array;

    use super::*;

    #[pg_extern(sql = "
        create function pgparquet.serialize(elems record[], filename text)
            returns void
            strict
            language c
            AS 'MODULE_PATHNAME', 'serialize_wrapper';
    ")]
    pub(crate) fn serialize(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
        let records = unsafe {
            pg_getarg::<Vec<PgHeapTuple<'_, AllocatedByRust>>>(fcinfo, 0)
                .expect("record array is required as first argument")
        };
        let filename = unsafe {
            pg_getarg::<&str>(fcinfo, 1).expect("filename is required as second argument")
        };

        if records.is_empty() {
            return pg_sys::Datum::from(0);
        }

        let total_records = records.len();

        let (records, tupledesc) = tupledesc_for_tuples(records);

        let array_oid = records
            .composite_type_oid()
            .expect("array of records are expected");

        let arrow_schema = parse_schema(array_oid, tupledesc.clone(), "root");
        let parquet_schema = to_parquet_schema(&arrow_schema);

        let writer_props = WriterProperties::default().into();
        let col_writers =
            get_column_writers(&parquet_schema, &writer_props, &arrow_schema.clone().into())
                .unwrap();

        let mut workers: Vec<_> = col_writers
            .into_iter()
            .map(|mut col_writer| {
                let (send, recv) = std::sync::mpsc::channel::<ArrowLeafColumn>();
                let handle = std::thread::spawn(move || {
                    for col in recv {
                        col_writer.write(&col)?;
                    }
                    col_writer.close()
                });
                (handle, send)
            })
            .collect();

        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(filename)
            .unwrap();
        let root_schema = parquet_schema.root_schema_ptr();
        let mut writer =
            SerializedFileWriter::new(&mut file, root_schema, writer_props.clone()).unwrap();

        let mut row_group = writer.next_row_group().unwrap();

        let tuple_array = unsafe {
            pgrx::AnyArray::from_polymorphic_datum(records.into_datum().unwrap(), false, array_oid)
        }
        .unwrap();

        let (_, data) = visit_list_array(
            "root",
            tuple_array,
            Some(tupledesc),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, total_records as i32])),
        );
        let to_write = vec![data];

        let mut worker_iter = workers.iter_mut();
        for (arr, field) in to_write.iter().zip(&arrow_schema.fields) {
            for leaves in compute_leaves(field, arr).unwrap() {
                worker_iter.next().unwrap().1.send(leaves).unwrap();
            }
        }

        for (handle, send) in workers {
            drop(send); // Drop send side to signal termination
            let chunk = handle.join().unwrap().unwrap();
            chunk.append_to_row_group(&mut row_group).unwrap();
        }
        row_group.close().unwrap();

        writer.close().unwrap();

        pg_sys::Datum::from(0)
    }

    #[pg_extern(sql = "
        create function pgparquet.schema(elems record[])
            returns text
            strict
            language c
            AS 'MODULE_PATHNAME', 'schema_wrapper';
    ")]
    fn schema(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
        let records = unsafe {
            pg_getarg::<Vec<PgHeapTuple<'_, AllocatedByRust>>>(fcinfo, 0)
                .expect("record array is required as first argument")
        };

        if records.is_empty() {
            return "".into_datum().unwrap();
        }

        let array_oid = records
            .composite_type_oid()
            .expect("array of records are expected");

        let (_, tupledesc) = tupledesc_for_tuples(records);

        let arrow_schema = parse_schema(array_oid, tupledesc, "root");
        let parquet_schema = to_parquet_schema(&arrow_schema);

        let mut buf = Vec::new();
        printer::print_schema(&mut buf, &parquet_schema.root_schema_ptr());
        String::from_utf8(buf).unwrap().into_datum().unwrap()
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
