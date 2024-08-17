use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, ListArray, TimestampMicrosecondArray},
    datatypes::FieldRef,
};
use pgrx::{pg_sys::Oid, TimestampWithTimeZone};

use crate::{
    arrow_parquet::{
        arrow_utils::arrow_array_offsets,
        pg_to_arrow::PgTypeToArrowArray,
        schema_visitor::{visit_list_schema, visit_primitive_schema},
    },
    type_compat::pg_arrow_type_conversions::timestamptz_to_i64,
};

// TimestampTz
impl PgTypeToArrowArray<TimestampWithTimeZone> for Vec<Option<TimestampWithTimeZone>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let timestamptz_field = visit_primitive_schema(typoid, typmod, name);

        let timestamptzs = self
            .into_iter()
            .map(|timestamptz| timestamptz.and_then(timestamptz_to_i64))
            .collect::<Vec<_>>();

        let timestamptz_array = TimestampMicrosecondArray::from(timestamptzs).with_timezone_utc();

        (timestamptz_field, Arc::new(timestamptz_array))
    }
}

// TimestampTz[]
impl PgTypeToArrowArray<Vec<Option<TimestampWithTimeZone>>>
    for Vec<Option<Vec<Option<TimestampWithTimeZone>>>>
{
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let timestamptz_field = visit_primitive_schema(typoid, typmod, name);

        let timestamptzs = self
            .into_iter()
            .flatten()
            .flatten()
            .map(|timestamptz| timestamptz.and_then(timestamptz_to_i64))
            .collect::<Vec<_>>();

        let timestamptz_array = TimestampMicrosecondArray::from(timestamptzs).with_timezone_utc();

        let list_field = visit_list_schema(typoid, typmod, name);
        let list_array = ListArray::new(
            timestamptz_field,
            offsets,
            Arc::new(timestamptz_array),
            Some(nulls),
        );
        (list_field, make_array(list_array.into()))
    }
}
