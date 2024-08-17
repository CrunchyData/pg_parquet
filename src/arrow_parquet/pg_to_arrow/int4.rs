use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, Int32Array, ListArray},
    datatypes::FieldRef,
};
use pgrx::pg_sys::Oid;

use crate::arrow_parquet::{
    arrow_utils::arrow_array_offsets,
    pg_to_arrow::PgTypeToArrowArray,
    schema_visitor::{visit_list_schema, visit_primitive_schema},
};

// Int32
impl PgTypeToArrowArray<i32> for Vec<Option<i32>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let int32_field = visit_primitive_schema(typoid, typmod, name);

        let int32_array = Int32Array::from(self);

        (int32_field, Arc::new(int32_array))
    }
}

// Int32[]
impl PgTypeToArrowArray<Vec<Option<i32>>> for Vec<Option<Vec<Option<i32>>>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let int32_field = visit_primitive_schema(typoid, typmod, name);

        let int32s = self.into_iter().flatten().flatten().collect::<Vec<_>>();

        let int32_array = Int32Array::from(int32s);

        let list_field = visit_list_schema(typoid, typmod, name);
        let list_array = ListArray::new(int32_field, offsets, Arc::new(int32_array), Some(nulls));
        (list_field, make_array(list_array.into()))
    }
}
