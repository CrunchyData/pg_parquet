use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, BinaryArray, ListArray},
    datatypes::FieldRef,
};
use pgrx::pg_sys::Oid;

use crate::arrow_parquet::{
    arrow_utils::arrow_array_offsets,
    pg_to_arrow::PgTypeToArrowArray,
    schema_visitor::{visit_list_schema, visit_primitive_schema},
};

// Bytea
impl PgTypeToArrowArray<&[u8]> for Vec<Option<&[u8]>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let byte_field = visit_primitive_schema(typoid, typmod, name);

        let byte_array = BinaryArray::from(self);

        (byte_field, Arc::new(byte_array))
    }
}

// Bytea[]
impl PgTypeToArrowArray<Vec<Option<&[u8]>>> for Vec<Option<Vec<Option<&[u8]>>>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let binary_field = visit_primitive_schema(typoid, typmod, name);

        let byteas = self.into_iter().flatten().flatten().collect::<Vec<_>>();
        let bytea_array = BinaryArray::from(byteas);

        let list_field = visit_list_schema(typoid, typmod, name);
        let list_array = ListArray::new(binary_field, offsets, Arc::new(bytea_array), Some(nulls));
        (list_field, make_array(list_array.into()))
    }
}
