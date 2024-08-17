use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, BinaryArray, ListArray},
    datatypes::FieldRef,
};
use arrow_schema::DataType;

use crate::{
    arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray},
    type_compat::geometry::Geometry,
};

use super::PgTypeToArrowContext;

// Geometry
impl PgTypeToArrowArray<Geometry> for Vec<Option<Geometry>> {
    fn to_arrow_array(self, context: PgTypeToArrowContext) -> (FieldRef, ArrayRef) {
        let wkbs = self
            .iter()
            .map(|geometry| geometry.as_deref())
            .collect::<Vec<_>>();

        let wkb_array = BinaryArray::from(wkbs);

        (context.field, Arc::new(wkb_array))
    }
}

// Geometry[]
impl PgTypeToArrowArray<Vec<Option<Geometry>>> for Vec<Option<Vec<Option<Geometry>>>> {
    fn to_arrow_array(self, context: PgTypeToArrowContext) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let wkbs = self.into_iter().flatten().flatten().collect::<Vec<_>>();

        let wkbs = wkbs
            .iter()
            .map(|geometry| geometry.as_deref())
            .collect::<Vec<_>>();

        let wkb_array = BinaryArray::from(wkbs);

        let list_field = context.field;

        match list_field.data_type() {
            DataType::List(wkb_field) => {
                let list_array =
                    ListArray::new(wkb_field.clone(), offsets, Arc::new(wkb_array), Some(nulls));

                (list_field, make_array(list_array.into()))
            }
            _ => panic!("Expected List field"),
        }
    }
}
