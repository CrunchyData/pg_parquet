use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryArray, ListArray};
use pgrx::IntoDatum;

use crate::{
    arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray},
    type_compat::geometry::Geography,
};

use super::PgToArrowAttributeContext;

// Geography
impl PgTypeToArrowArray<Geography> for Vec<Option<Geography>> {
    fn to_arrow_array(self, context: &mut PgToArrowAttributeContext) -> ArrayRef {
        let wkbs = self
            .iter()
            .map(|geography| {
                let geoparquet_metadata = context.geoparquet_metadata();

                geoparquet_metadata.borrow_mut().update_with_datum(
                    geography.clone().into_datum(),
                    context.typoid(),
                    context.field().name().clone(),
                );

                geography.as_deref()
            })
            .collect::<Vec<_>>();
        let wkb_array = BinaryArray::from(wkbs);
        Arc::new(wkb_array)
    }
}

// Geography[]
impl PgTypeToArrowArray<Geography> for Vec<Option<Vec<Option<Geography>>>> {
    fn to_arrow_array(self, element_context: &mut PgToArrowAttributeContext) -> ArrayRef {
        let (offsets, nulls) = arrow_array_offsets(&self);

        // gets rid of the first level of Option, then flattens the inner Vec<Option<bool>>.
        let pg_array = self.into_iter().flatten().flatten().collect::<Vec<_>>();

        let wkbs = pg_array
            .iter()
            .map(|geography| {
                let geoparquet_metadata = element_context.geoparquet_metadata();

                geoparquet_metadata.borrow_mut().update_with_datum(
                    geography.clone().into_datum(),
                    element_context.typoid(),
                    element_context.field().name().clone(),
                );

                geography.as_deref()
            })
            .collect::<Vec<_>>();

        let wkb_array = BinaryArray::from(wkbs);

        let list_array = ListArray::new(
            element_context.field(),
            offsets,
            Arc::new(wkb_array),
            Some(nulls),
        );

        Arc::new(list_array)
    }
}
