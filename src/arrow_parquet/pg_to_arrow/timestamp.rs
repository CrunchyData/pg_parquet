use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, ListArray, TimestampMicrosecondArray},
    datatypes::FieldRef,
};
use pgrx::{pg_sys::Oid, Timestamp};

use crate::{
    arrow_parquet::{
        arrow_utils::arrow_array_offsets,
        pg_to_arrow::PgTypeToArrowArray,
        schema_visitor::{visit_list_schema, visit_primitive_schema},
    },
    type_compat::pg_arrow_type_conversions::timestamp_to_i64,
};

// Timestamp
impl PgTypeToArrowArray<Timestamp> for Vec<Option<Timestamp>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let timestamp_field = visit_primitive_schema(typoid, typmod, name);

        let timestamps = self
            .into_iter()
            .map(|timstamp| timstamp.and_then(timestamp_to_i64))
            .collect::<Vec<_>>();

        let timestamp_array = TimestampMicrosecondArray::from(timestamps);

        (timestamp_field, Arc::new(timestamp_array))
    }
}

// Timestamp[]
impl PgTypeToArrowArray<Vec<Option<Timestamp>>> for Vec<Option<Vec<Option<Timestamp>>>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let timestamp_field = visit_primitive_schema(typoid, typmod, name);

        let timestamps = self
            .into_iter()
            .flatten()
            .flatten()
            .map(|timestamp| timestamp.and_then(timestamp_to_i64))
            .collect::<Vec<_>>();

        let timestamp_array = TimestampMicrosecondArray::from(timestamps);

        let list_field = visit_list_schema(typoid, typmod, name);
        let list_array = ListArray::new(
            timestamp_field,
            offsets,
            Arc::new(timestamp_array),
            Some(nulls),
        );
        (list_field, make_array(list_array.into()))
    }
}
