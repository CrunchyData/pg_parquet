use std::sync::Arc;

use arrow::{
    array::{ArrayRef, Int16Array},
    datatypes::{DataType, Field, FieldRef},
};
use pgrx::pg_sys::Oid;

use crate::arrow_parquet::{
    pg_to_arrow::PgTypeToArrowArray,
    utils::{arrow_array_offsets, create_arrow_list_array, create_arrow_null_list_array},
};

// Int16
impl PgTypeToArrowArray<i16> for Vec<Option<i16>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let field = Field::new(name, DataType::Int16, true);
        let array = Int16Array::from(self);
        (Arc::new(field), Arc::new(array))
    }
}

// Int16[]
impl PgTypeToArrowArray<Vec<Option<i16>>> for Vec<Option<Vec<Option<i16>>>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, all_nulls) = arrow_array_offsets(&self);

        let field = Field::new(name, DataType::Int16, true);

        if all_nulls {
            return create_arrow_null_list_array(name, &field, self.len());
        }

        let array = self.into_iter().flatten().flatten().collect::<Vec<_>>();
        let array = Int16Array::from(array);
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        create_arrow_list_array(name, field, primitive_array, offsets)
    }
}
