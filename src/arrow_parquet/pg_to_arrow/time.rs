use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, ListArray, Time64MicrosecondArray},
    datatypes::FieldRef,
};
use pgrx::{pg_sys::Oid, Time};

use crate::{
    arrow_parquet::{
        arrow_utils::arrow_array_offsets,
        pg_to_arrow::PgTypeToArrowArray,
        schema_visitor::{visit_list_schema, visit_primitive_schema},
    },
    type_compat::pg_arrow_type_conversions::time_to_i64,
};

// Time
impl PgTypeToArrowArray<Time> for Vec<Option<Time>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let time_field = visit_primitive_schema(typoid, typmod, name);

        let times = self
            .into_iter()
            .map(|time| time.and_then(time_to_i64))
            .collect::<Vec<_>>();

        let time_array = Time64MicrosecondArray::from(times);

        (time_field, Arc::new(time_array))
    }
}

// Time[]
impl PgTypeToArrowArray<Vec<Option<Time>>> for Vec<Option<Vec<Option<Time>>>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let time_field = visit_primitive_schema(typoid, typmod, name);

        let times = self
            .into_iter()
            .flatten()
            .flatten()
            .map(|time| time.and_then(time_to_i64))
            .collect::<Vec<_>>();

        let time_array = Time64MicrosecondArray::from(times);

        let list_field = visit_list_schema(typoid, typmod, name);
        let list_array = ListArray::new(time_field, offsets, Arc::new(time_array), Some(nulls));
        (list_field, make_array(list_array.into()))
    }
}
