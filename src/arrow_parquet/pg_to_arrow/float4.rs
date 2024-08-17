use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, Float32Array, ListArray},
    datatypes::FieldRef,
};
use pgrx::pg_sys::Oid;

use crate::arrow_parquet::{
    arrow_utils::arrow_array_offsets,
    pg_to_arrow::PgTypeToArrowArray,
    schema_visitor::{visit_list_schema, visit_primitive_schema},
};

// Float32
impl PgTypeToArrowArray<f32> for Vec<Option<f32>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let float_field = visit_primitive_schema(typoid, typmod, name);

        let float_array = Float32Array::from(self);

        (float_field, Arc::new(float_array))
    }
}

// Float32[]
impl PgTypeToArrowArray<Vec<Option<f32>>> for Vec<Option<Vec<Option<f32>>>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let float_field = visit_primitive_schema(typoid, typmod, name);

        let floats = self.into_iter().flatten().flatten().collect::<Vec<_>>();
        let float_array = Float32Array::from(floats);

        let list_field = visit_list_schema(typoid, typmod, name);
        let list_array = ListArray::new(float_field, offsets, Arc::new(float_array), Some(nulls));
        (list_field, make_array(list_array.into()))
    }
}
