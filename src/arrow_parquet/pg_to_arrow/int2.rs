use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, Int16Array, ListArray},
    datatypes::FieldRef,
};
use pgrx::pg_sys::Oid;

use crate::arrow_parquet::{
    arrow_utils::arrow_array_offsets,
    pg_to_arrow::PgTypeToArrowArray,
    schema_visitor::{visit_list_schema, visit_primitive_schema},
};

// Int16
impl PgTypeToArrowArray<i16> for Vec<Option<i16>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let int16_field = visit_primitive_schema(typoid, typmod, name);

        let int16_array = Int16Array::from(self);

        (int16_field, Arc::new(int16_array))
    }
}

// Int16[]
impl PgTypeToArrowArray<Vec<Option<i16>>> for Vec<Option<Vec<Option<i16>>>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let int16_field = visit_primitive_schema(typoid, typmod, name);

        let int16s = self.into_iter().flatten().flatten().collect::<Vec<_>>();

        let int16_array = Int16Array::from(int16s);

        let list_field = visit_list_schema(typoid, typmod, name);
        let list_array = ListArray::new(int16_field, offsets, Arc::new(int16_array), Some(nulls));
        (list_field, make_array(list_array.into()))
    }
}
