use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, Float64Array, ListArray},
    datatypes::FieldRef,
};
use pgrx::pg_sys::Oid;

use crate::arrow_parquet::{
    arrow_utils::arrow_array_offsets,
    pg_to_arrow::PgTypeToArrowArray,
    schema_visitor::{visit_list_schema, visit_primitive_schema},
};

// Float64
impl PgTypeToArrowArray<f64> for Vec<Option<f64>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let double_field = visit_primitive_schema(typoid, typmod, name);

        let double_array = Float64Array::from(self);

        (double_field, Arc::new(double_array))
    }
}

// Float64[]
impl PgTypeToArrowArray<Vec<Option<f64>>> for Vec<Option<Vec<Option<f64>>>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let double_field = visit_primitive_schema(typoid, typmod, name);

        let doubles = self.into_iter().flatten().flatten().collect::<Vec<_>>();

        let double_array = Float64Array::from(doubles);

        let list_field = visit_list_schema(typoid, typmod, name);
        let list_array = ListArray::new(double_field, offsets, Arc::new(double_array), Some(nulls));
        (list_field, make_array(list_array.into()))
    }
}
