use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, ListArray, UInt32Array},
    datatypes::FieldRef,
};
use pgrx::pg_sys::Oid;

use crate::arrow_parquet::{
    arrow_utils::arrow_array_offsets,
    pg_to_arrow::PgTypeToArrowArray,
    schema_visitor::{visit_list_schema, visit_primitive_schema},
};

// Oid
impl PgTypeToArrowArray<Oid> for Vec<Option<Oid>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let oid_field = visit_primitive_schema(typoid, typmod, name);

        let oids = self
            .into_iter()
            .map(|x| x.map(|x| x.as_u32()))
            .collect::<Vec<_>>();

        let oid_array = UInt32Array::from(oids);

        (oid_field, Arc::new(oid_array))
    }
}

// Oid[]
impl PgTypeToArrowArray<Vec<Option<Oid>>> for Vec<Option<Vec<Option<Oid>>>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let oid_field = visit_primitive_schema(typoid, typmod, name);

        let oids = self.into_iter().flatten().flatten().collect::<Vec<_>>();
        let oids = oids
            .into_iter()
            .map(|x| x.map(|x| x.as_u32()))
            .collect::<Vec<_>>();

        let oid_array = UInt32Array::from(oids);

        let list_field = visit_list_schema(typoid, typmod, name);
        let list_array = ListArray::new(oid_field, offsets, Arc::new(oid_array), Some(nulls));
        (list_field, make_array(list_array.into()))
    }
}
