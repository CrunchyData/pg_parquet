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

use super::PgToArrowPerAttributeContext;

// Geometry
impl PgTypeToArrowArray<Geometry> for Option<Geometry> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let wkb = self.as_deref();

        let wkb_array = BinaryArray::from(vec![wkb]);

        (context.field, Arc::new(wkb_array))
    }
}

// Geometry[]
impl PgTypeToArrowArray<pgrx::Array<'_, Geometry>> for Option<pgrx::Array<'_, Geometry>> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let pg_array = if let Some(pg_array) = self {
            pg_array.iter().collect::<Vec<_>>()
        } else {
            vec![]
        };

        let wkbs = pg_array
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
