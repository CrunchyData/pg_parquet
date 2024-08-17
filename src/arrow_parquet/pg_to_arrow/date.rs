use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, Date32Array, ListArray},
    datatypes::FieldRef,
};
use pgrx::{pg_sys::Oid, Date};

use crate::{
    arrow_parquet::{
        arrow_utils::arrow_array_offsets,
        pg_to_arrow::PgTypeToArrowArray,
        schema_visitor::{visit_list_schema, visit_primitive_schema},
    },
    type_compat::pg_arrow_type_conversions::date_to_i32,
};

// Date
impl PgTypeToArrowArray<Date> for Vec<Option<Date>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let date_field = visit_primitive_schema(typoid, typmod, name);

        let dates = self
            .into_iter()
            .map(|date| date.and_then(date_to_i32))
            .collect::<Vec<_>>();

        let date_array = Date32Array::from(dates);

        (date_field, Arc::new(date_array))
    }
}

// Date[]
impl PgTypeToArrowArray<Vec<Option<Date>>> for Vec<Option<Vec<Option<Date>>>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let date_field = visit_primitive_schema(typoid, typmod, name);

        let dates = self
            .into_iter()
            .flatten()
            .flatten()
            .map(|date| date.and_then(date_to_i32))
            .collect::<Vec<_>>();

        let date_array = Date32Array::from(dates);

        let list_field = visit_list_schema(typoid, typmod, name);
        let list_array = ListArray::new(date_field, offsets, Arc::new(date_array), Some(nulls));
        (list_field, make_array(list_array.into()))
    }
}
