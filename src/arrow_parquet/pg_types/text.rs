use std::sync::Arc;

use arrow::{
    array::{ArrayRef, StringArray},
    datatypes::{DataType, Field, FieldRef},
};
use pgrx::pg_sys::Oid;

use crate::arrow_parquet::{
    pg_to_arrow::PgTypeToArrowArray,
    utils::{array_offsets, create_arrow_list_array, create_arrow_null_list_array},
};

// Text, Varchar
impl PgTypeToArrowArray<String> for Vec<Option<String>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let field = Field::new(name, DataType::Utf8, true);
        let array = StringArray::from(self);
        (Arc::new(field), Arc::new(array))
    }
}

// Text[], Varchar[]
impl PgTypeToArrowArray<Vec<Option<String>>> for Vec<Option<Vec<Option<String>>>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, all_nulls) = array_offsets(&self);

        let field = Field::new(name, DataType::Utf8, true);

        if all_nulls {
            return create_arrow_null_list_array(name, &field, self.len());
        }

        let array = self.into_iter().flatten().flatten().collect::<Vec<_>>();
        let array = StringArray::from(array);
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        create_arrow_list_array(name, field, primitive_array, offsets)
    }
}
