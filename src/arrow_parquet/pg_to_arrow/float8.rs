use std::sync::Arc;

use arrow::{
    array::{ArrayRef, Float64Array},
    datatypes::{DataType, Field, FieldRef},
};
use pgrx::pg_sys::Oid;

use crate::arrow_parquet::{
    arrow_utils::{arrow_array_offsets, create_arrow_list_array},
    pg_to_arrow::PgTypeToArrowArray,
};

// Float64
impl PgTypeToArrowArray<f64> for Vec<Option<f64>> {
    fn to_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let field = Field::new(name, DataType::Float64, true);
        let array = Float64Array::from(self);
        (Arc::new(field), Arc::new(array))
    }
}

// Float64[]
impl PgTypeToArrowArray<Vec<Option<f64>>> for Vec<Option<Vec<Option<f64>>>> {
    fn to_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let field = Field::new(name, DataType::Float64, true);

        let array = self.into_iter().flatten().flatten().collect::<Vec<_>>();
        let array = Float64Array::from(array);
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        create_arrow_list_array(name, field, primitive_array, offsets, nulls)
    }
}
