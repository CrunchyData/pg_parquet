use pgrx::{pg_getarg, pg_return_void, prelude::*};

use crate::arrow_parquet::arrow_to_parquet_writer::write_to_parquet;
use crate::arrow_parquet::schema_visitor::schema_string_for_tuples;

mod arrow_parquet;
mod conversion;
mod parquet_copy_hook;
mod pgrx_utils;

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
            return unsafe { pg_return_void() };
        }

        write_to_parquet(filename, records);

        return unsafe { pg_return_void() };
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

        let schema_string = schema_string_for_tuples(records);
        schema_string.into_datum().unwrap()
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
