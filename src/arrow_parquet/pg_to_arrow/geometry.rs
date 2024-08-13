use std::sync::Arc;

use arrow::{
    array::{ArrayRef, BinaryArray},
    datatypes::{DataType, Field, FieldRef},
};
use pgrx::pg_sys::Oid;

use crate::{
    arrow_parquet::{
        arrow_utils::{arrow_array_offsets, create_arrow_list_array},
        pg_to_arrow::PgTypeToArrowArray,
    },
    type_compat::geometry::Geometry,
};

// Geometry
impl PgTypeToArrowArray<Geometry> for Vec<Option<Geometry>> {
    fn to_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let field = Field::new(name, DataType::Binary, true);

        let array = self
            .iter()
            .map(|geometry| geometry.as_deref())
            .collect::<Vec<_>>();
        let array = BinaryArray::from(array);
        (Arc::new(field), Arc::new(array))
    }
}

// Geometry[]
impl PgTypeToArrowArray<Vec<Option<Geometry>>> for Vec<Option<Vec<Option<Geometry>>>> {
    fn to_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let field = Field::new(name, DataType::Binary, true);

        let array = self.into_iter().flatten().flatten().collect::<Vec<_>>();
        let array = array
            .iter()
            .map(|geometry| geometry.as_deref())
            .collect::<Vec<_>>();
        let array = BinaryArray::from(array);
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        create_arrow_list_array(name, field, primitive_array, offsets, nulls)
    }
}
