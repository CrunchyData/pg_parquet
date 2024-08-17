use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, ListArray, StringArray},
    datatypes::FieldRef,
};
use pgrx::pg_sys::Oid;

use crate::arrow_parquet::{
    arrow_utils::arrow_array_offsets,
    pg_to_arrow::PgTypeToArrowArray,
    schema_visitor::{visit_list_schema, visit_primitive_schema},
};

// Char
impl PgTypeToArrowArray<i8> for Vec<Option<i8>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let char_field = visit_primitive_schema(typoid, typmod, name);

        let chars = self
            .into_iter()
            .map(|c| c.map(|c| (c as u8 as char).to_string()))
            .collect::<Vec<_>>();

        let char_array = StringArray::from(chars);

        (char_field, Arc::new(char_array))
    }
}

// "Char"[]
impl PgTypeToArrowArray<Vec<Option<i8>>> for Vec<Option<Vec<Option<i8>>>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let char_field = visit_primitive_schema(typoid, typmod, name);

        let chars = self
            .into_iter()
            .flatten()
            .flatten()
            .map(|c| c.map(|c| (c as u8 as char).to_string()))
            .collect::<Vec<_>>();

        let char_array = StringArray::from(chars);

        let list_field = visit_list_schema(typoid, typmod, name);
        let list_array = ListArray::new(char_field, offsets, Arc::new(char_array), Some(nulls));
        (list_field, make_array(list_array.into()))
    }
}
