use std::sync::Arc;

use arrow::{
    array::{ArrayRef, Int8Array},
    datatypes::{DataType, Field, FieldRef},
};
use pgrx::pg_sys::Oid;

use crate::arrow_parquet::{
    pg_to_arrow::PgTypeToArrowArray,
    utils::{array_offsets, create_arrow_list_array, create_arrow_null_list_array},
};

// Bool
impl PgTypeToArrowArray<bool> for Vec<Option<bool>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let array = self
            .into_iter()
            .map(|v| v.and_then(|v| Some(v as i8)))
            .collect::<Vec<_>>();
        let field = Field::new(name, DataType::Int8, true);
        let array = Int8Array::from(array);
        (Arc::new(field), Arc::new(array))
    }
}

// Bool[]
impl PgTypeToArrowArray<Vec<Option<bool>>> for Vec<Option<Vec<Option<bool>>>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, all_nulls) = array_offsets(&self);

        let field = Field::new(name, DataType::Int8, true);

        if all_nulls {
            return create_arrow_null_list_array(name, &field, self.len());
        }

        let array = self
            .into_iter()
            .flatten()
            .flatten()
            .map(|v| v.and_then(|v| Some(v as i8)))
            .collect::<Vec<_>>();
        let array = Int8Array::from(array);
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        create_arrow_list_array(name, field, primitive_array, offsets)
    }
}