use std::sync::Arc;

use arrow::{
    array::{ArrayRef, BooleanArray},
    datatypes::{DataType, Field, FieldRef},
};
use pgrx::pg_sys::Oid;

use crate::arrow_parquet::{
    arrow_utils::{arrow_array_offsets, create_arrow_list_array},
    pg_to_arrow::PgTypeToArrowArray,
};

// Bool
impl PgTypeToArrowArray<bool> for Vec<Option<bool>> {
    fn to_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let field = Field::new(name, DataType::Boolean, true);
        let array = BooleanArray::from(self);
        (Arc::new(field), Arc::new(array))
    }
}

// Bool[]
impl PgTypeToArrowArray<Vec<Option<bool>>> for Vec<Option<Vec<Option<bool>>>> {
    fn to_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let field = Field::new(name, DataType::Boolean, true);

        let array = self.into_iter().flatten().flatten().collect::<Vec<_>>();
        let array = BooleanArray::from(array);
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        create_arrow_list_array(name, field, primitive_array, offsets, nulls)
    }
}
