use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, Int64Array, ListArray},
    datatypes::FieldRef,
};
use pgrx::pg_sys::Oid;

use crate::arrow_parquet::{
    arrow_utils::arrow_array_offsets,
    pg_to_arrow::PgTypeToArrowArray,
    schema_visitor::{visit_list_schema, visit_primitive_schema},
};

// Int64
impl PgTypeToArrowArray<i64> for Vec<Option<i64>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let int64_field = visit_primitive_schema(typoid, typmod, name);

        let int64_array = Int64Array::from(self);

        (int64_field, Arc::new(int64_array))
    }
}

// Int64[]
impl PgTypeToArrowArray<Vec<Option<i64>>> for Vec<Option<Vec<Option<i64>>>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let int64_field = visit_primitive_schema(typoid, typmod, name);

        let int64s = self.into_iter().flatten().flatten().collect::<Vec<_>>();

        let int64_array = Int64Array::from(int64s);

        let list_field = visit_list_schema(typoid, typmod, name);
        let list_array = ListArray::new(int64_field, offsets, Arc::new(int64_array), Some(nulls));
        (list_field, make_array(list_array.into()))
    }
}
