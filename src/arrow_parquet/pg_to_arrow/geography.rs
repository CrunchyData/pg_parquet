use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryArray, ListArray};

use crate::{
    arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray},
    type_compat::geometry::{Geography, GeometryColumn},
};

use super::PgToArrowAttributeContext;

// Geography
impl PgTypeToArrowArray<Geography> for Vec<Option<Geography>> {
    fn to_arrow_array(self, context: &PgToArrowAttributeContext) -> ArrayRef {
        // update GeoParquet metadata
        let geometry_column: GeometryColumn = self
            .clone()
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .into();

        context
            .geoparquet_metadata()
            .update(context.field().name(), geometry_column);

        // prepare WKB array
        let wkbs = self
            .iter()
            .map(|geography| geography.as_deref())
            .collect::<Vec<_>>();
        let wkb_array = BinaryArray::from(wkbs);
        Arc::new(wkb_array)
    }
}

// Geography[]
impl PgTypeToArrowArray<Geography> for Vec<Option<Vec<Option<Geography>>>> {
    fn to_arrow_array(self, element_context: &PgToArrowAttributeContext) -> ArrayRef {
        let (offsets, nulls) = arrow_array_offsets(&self);

        // gets rid of the first level of Option, then flattens the inner Vec<Option<bool>>.
        let pg_array = self.into_iter().flatten().flatten().collect::<Vec<_>>();

        // update GeoParquet metadata
        let geometry_column: GeometryColumn = pg_array
            .clone()
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .into();

        element_context
            .geoparquet_metadata()
            .update(element_context.field().name(), geometry_column);

        // prepare WKB array
        let wkbs = pg_array
            .iter()
            .map(|geography| geography.as_deref())
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
