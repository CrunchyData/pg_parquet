use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryArray, ListArray};

use crate::{
    arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray},
    type_compat::geometry::Geometry,
};

use super::PgToArrowAttributeContext;

// Geometry
impl PgTypeToArrowArray<Geometry> for Vec<Option<Geometry>> {
    fn to_arrow_array(self, _context: &PgToArrowAttributeContext) -> ArrayRef {
        let wkbs = self
            .iter()
            .map(|geometry| geometry.as_deref())
            .collect::<Vec<_>>();
        let wkb_array = BinaryArray::from(wkbs);
        Arc::new(wkb_array)
    }
}

// Geometry[]
impl PgTypeToArrowArray<pgrx::Array<'_, Geometry>> for Vec<Option<pgrx::Array<'_, Geometry>>> {
    fn to_arrow_array(self, context: &PgToArrowAttributeContext) -> ArrayRef {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let pg_array = self
            .into_iter()
            .flatten()
            .flat_map(|pg_array| pg_array.iter().collect::<Vec<_>>())
            .collect::<Vec<_>>();

        let wkbs = pg_array
            .iter()
            .map(|geometry| geometry.as_deref())
            .collect::<Vec<_>>();

        let wkb_array = BinaryArray::from(wkbs);

        let list_array = ListArray::new(
            context.field.clone(),
            offsets,
            Arc::new(wkb_array),
            Some(nulls),
        );

        Arc::new(list_array)
    }
}
