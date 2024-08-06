use std::sync::Arc;

use arrow::{
    array::{ArrayRef, UInt32Array},
    datatypes::{DataType, Field, FieldRef},
};
use pgrx::pg_sys::Oid;

use crate::arrow_parquet::{
    pg_to_arrow::PgTypeToArrowArray,
    utils::{arrow_array_offsets, create_arrow_list_array},
};

// Oid
impl PgTypeToArrowArray<Oid> for Vec<Option<Oid>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let field = Field::new(name, DataType::UInt32, true);
        let array = self
            .into_iter()
            .map(|x| x.map(|x| x.as_u32()))
            .collect::<Vec<_>>();
        let array = UInt32Array::from(array);
        (Arc::new(field), Arc::new(array))
    }
}

// Oid[]
impl PgTypeToArrowArray<Vec<Option<Oid>>> for Vec<Option<Vec<Option<Oid>>>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let field = Field::new(name, DataType::UInt32, true);

        let array = self.into_iter().flatten().flatten().collect::<Vec<_>>();
        let array = array
            .into_iter()
            .map(|x| x.and_then(|x| Some(x.as_u32())))
            .collect::<Vec<_>>();
        let array = UInt32Array::from(array);
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        create_arrow_list_array(name, field, primitive_array, offsets, nulls)
    }
}
