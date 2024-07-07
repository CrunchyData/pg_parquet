use parquet::{file::writer::SerializedFileWriter, schema::printer};
use pgrx::prelude::*;
use pgrx::PgTupleDesc;

use schema_parser::parse_record_schema;
use serializer::serialize_record;

mod conversion;
mod schema_parser;
mod serializer;

pgrx::pg_module_magic!();

#[pg_schema]
mod pgparquet {
    use super::*;

    fn ensure_composite_type(oid: pg_sys::Oid) {
        let is_composite_type = unsafe { pg_sys::type_is_rowtype(oid) };
        if !is_composite_type {
            // PgHeapTuple is not supported yet as udf argument
            panic!("composite type is expected, got {}", oid);
        }
    }

    #[pg_extern]
    fn serialize(elem: pgrx::AnyElement, file_path: &str) {
        ensure_composite_type(elem.oid());

        let attribute_tupledesc = unsafe { pg_sys::lookup_rowtype_tupdesc(elem.oid(), 0) };
        let attribute_tupledesc = unsafe { PgTupleDesc::from_pg(attribute_tupledesc) };
        let schema = parse_record_schema(attribute_tupledesc, "root");

        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(file_path)
            .unwrap();

        let mut writer = SerializedFileWriter::new(file, schema, Default::default()).unwrap();
        let mut row_group_writer = writer.next_row_group().unwrap();

        let record = unsafe {
            PgHeapTuple::from_polymorphic_datum(elem.datum(), false, elem.oid()).unwrap()
        };
        serialize_record(record, &mut row_group_writer);

        row_group_writer.close().unwrap();
        writer.close().unwrap();
    }

    #[pg_extern]
    fn schema(elem: pgrx::AnyElement) -> String {
        ensure_composite_type(elem.oid());

        let attribute_tupledesc = unsafe { pg_sys::lookup_rowtype_tupdesc(elem.oid(), 0) };
        let attribute_tupledesc = unsafe { PgTupleDesc::from_pg(attribute_tupledesc) };
        let schema = parse_record_schema(attribute_tupledesc, "root");

        let mut buf = Vec::new();
        printer::print_schema(&mut buf, &schema);
        String::from_utf8(buf).unwrap()
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
