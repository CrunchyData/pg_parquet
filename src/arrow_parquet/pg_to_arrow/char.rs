use std::sync::Arc;

use arrow::{
    array::{ArrayRef, StringArray},
    datatypes::{DataType, Field, FieldRef},
};
use pgrx::pg_sys::Oid;

use crate::arrow_parquet::{
    arrow_utils::{arrow_array_offsets, create_arrow_list_array},
    pg_to_arrow::PgTypeToArrowArray,
};

// Char
impl PgTypeToArrowArray<i8> for Vec<Option<i8>> {
    fn to_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let array = self
            .into_iter()
            .map(|c| c.map(|c| (c as u8 as char).to_string()))
            .collect::<Vec<_>>();

        let field = Field::new(name, DataType::Utf8, true);
        let array = StringArray::from(array);
        (Arc::new(field), Arc::new(array))
    }
}

// "Char"[]
impl PgTypeToArrowArray<Vec<Option<i8>>> for Vec<Option<Vec<Option<i8>>>> {
    fn to_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let field = Field::new(name, DataType::Utf8, true);

        let array = self
            .into_iter()
            .flatten()
            .flatten()
            .map(|c| c.map(|c| (c as u8 as char).to_string()))
            .collect::<Vec<_>>();

        let array = StringArray::from(array);
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        create_arrow_list_array(name, field, primitive_array, offsets, nulls)
    }
}
