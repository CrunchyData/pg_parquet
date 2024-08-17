use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, BinaryArray, ListArray},
    datatypes::FieldRef,
};
use pgrx::pg_sys::Oid;

use crate::{
    arrow_parquet::{
        arrow_utils::arrow_array_offsets,
        pg_to_arrow::PgTypeToArrowArray,
        schema_visitor::{visit_list_schema, visit_primitive_schema},
    },
    type_compat::geometry::Geometry,
};

// Geometry
impl PgTypeToArrowArray<Geometry> for Vec<Option<Geometry>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let binary_field = visit_primitive_schema(typoid, typmod, name);

        let wkbs = self
            .iter()
            .map(|geometry| geometry.as_deref())
            .collect::<Vec<_>>();

        let wkb_array = BinaryArray::from(wkbs);

        (binary_field, Arc::new(wkb_array))
    }
}

// Geometry[]
impl PgTypeToArrowArray<Vec<Option<Geometry>>> for Vec<Option<Vec<Option<Geometry>>>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let binary_field = visit_primitive_schema(typoid, typmod, name);

        let wkbs = self.into_iter().flatten().flatten().collect::<Vec<_>>();

        let wkbs = wkbs
            .iter()
            .map(|geometry| geometry.as_deref())
            .collect::<Vec<_>>();

        let wkb_array = BinaryArray::from(wkbs);

        let list_field = visit_list_schema(typoid, typmod, name);
        let list_array = ListArray::new(binary_field, offsets, Arc::new(wkb_array), Some(nulls));
        (list_field, make_array(list_array.into()))
    }
}
