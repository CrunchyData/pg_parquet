use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, ListArray, Time64MicrosecondArray},
    datatypes::FieldRef,
};
use pgrx::{pg_sys::Oid, TimeWithTimeZone};

use crate::{
    arrow_parquet::{
        arrow_utils::arrow_array_offsets,
        pg_to_arrow::PgTypeToArrowArray,
        schema_visitor::{visit_list_schema, visit_primitive_schema},
    },
    type_compat::pg_arrow_type_conversions::timetz_to_i64,
};

// TimeTz
impl PgTypeToArrowArray<TimeWithTimeZone> for Vec<Option<TimeWithTimeZone>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let timetz_field = visit_primitive_schema(typoid, typmod, name);

        let timetzs = self
            .into_iter()
            .map(|timetz| timetz.and_then(timetz_to_i64))
            .collect::<Vec<_>>();

        let timetz_array = Time64MicrosecondArray::from(timetzs);

        (timetz_field, Arc::new(timetz_array))
    }
}

// TimeTz[]
impl PgTypeToArrowArray<Vec<Option<TimeWithTimeZone>>>
    for Vec<Option<Vec<Option<TimeWithTimeZone>>>>
{
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let timetz_field = visit_primitive_schema(typoid, typmod, name);

        let timetzs = self
            .into_iter()
            .flatten()
            .flatten()
            .map(|timetz| timetz.and_then(timetz_to_i64))
            .collect::<Vec<_>>();

        let timetz_array = Time64MicrosecondArray::from(timetzs);

        let list_field = visit_list_schema(typoid, typmod, name);
        let list_array = ListArray::new(timetz_field, offsets, Arc::new(timetz_array), Some(nulls));
        (list_field, make_array(list_array.into()))
    }
}
