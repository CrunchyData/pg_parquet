use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, BooleanArray, ListArray},
    datatypes::FieldRef,
};
use pgrx::pg_sys::Oid;

use crate::arrow_parquet::{
    arrow_utils::arrow_array_offsets,
    pg_to_arrow::PgTypeToArrowArray,
    schema_visitor::{visit_list_schema, visit_primitive_schema},
};

// Bool
impl PgTypeToArrowArray<bool> for Vec<Option<bool>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let bool_field = visit_primitive_schema(typoid, typmod, name);

        let bool_array = BooleanArray::from(self);

        (bool_field, Arc::new(bool_array))
    }
}

// Bool[]
impl PgTypeToArrowArray<Vec<Option<bool>>> for Vec<Option<Vec<Option<bool>>>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let bool_field = visit_primitive_schema(typoid, typmod, name);

        let bools = self.into_iter().flatten().flatten().collect::<Vec<_>>();
        let bool_array = BooleanArray::from(bools);

        let list_field = visit_list_schema(typoid, typmod, name);
        let list_array = ListArray::new(bool_field, offsets, Arc::new(bool_array), Some(nulls));
        (list_field, make_array(list_array.into()))
    }
}
