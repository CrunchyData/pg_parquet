use parquet::{file::writer::SerializedFileWriter, schema::printer};

use pgrx::{pg_getarg, pg_return_null, prelude::*};

use schema_parser::parse_schema;
use serializer::serialize_array;

mod conversion;
mod copy_hook;
mod parquet_dest_receiver;
mod schema_parser;
mod serializer;

pgrx::pg_module_magic!();

#[pg_schema]
mod pgparquet {
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
            return unsafe { pg_return_null(fcinfo) };
        }

        let array_oid = records
            .composite_type_oid()
            .expect("array of records are expected");

        let schema = parse_schema(array_oid, "root");

        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(filename)
            .unwrap();

        let mut writer = SerializedFileWriter::new(file, schema, Default::default()).unwrap();
        let mut row_group_writer = writer.next_row_group().unwrap();

        let anyarray = unsafe {
            pgrx::AnyArray::from_polymorphic_datum(records.into_datum().unwrap(), false, array_oid)
                .unwrap()
        };
        serialize_array(anyarray, &mut vec![], &mut row_group_writer);

        row_group_writer.close().unwrap();
        writer.close().unwrap();

        unsafe { pg_return_null(fcinfo) }
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
            return unsafe { pg_return_null(fcinfo) };
        }

        let array_oid = records
            .composite_type_oid()
            .expect("array of records are expected");

        let schema = parse_schema(array_oid, "root");

        let mut buf = Vec::new();
        printer::print_schema(&mut buf, &schema);
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
