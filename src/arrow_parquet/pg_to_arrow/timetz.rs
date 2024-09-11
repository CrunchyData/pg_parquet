use std::sync::Arc;

use arrow::array::{ArrayRef, ListArray, Time64MicrosecondArray};
use pgrx::datum::TimeWithTimeZone;

use crate::{
    arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray},
    type_compat::pg_arrow_type_conversions::timetz_to_i64,
};

use super::PgToArrowAttributeContext;

// TimeTz
impl PgTypeToArrowArray<TimeWithTimeZone> for Vec<Option<TimeWithTimeZone>> {
    fn to_arrow_array(self, _context: &PgToArrowAttributeContext) -> ArrayRef {
        let timetzs = self
            .into_iter()
            .map(|timetz| timetz.map(timetz_to_i64))
            .collect::<Vec<_>>();
        let timetz_array = Time64MicrosecondArray::from(timetzs);
        Arc::new(timetz_array)
    }
}

// TimeTz[]
impl PgTypeToArrowArray<pgrx::Array<'_, TimeWithTimeZone>>
    for Vec<Option<pgrx::Array<'_, TimeWithTimeZone>>>
{
    fn to_arrow_array(self, context: &PgToArrowAttributeContext) -> ArrayRef {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let pg_array = self
            .into_iter()
            .flatten()
            .flat_map(|pg_array| pg_array.iter().collect::<Vec<_>>())
            .map(|timetz| timetz.map(timetz_to_i64))
            .collect::<Vec<_>>();

        let timetz_array = Time64MicrosecondArray::from(pg_array);

        let list_array = ListArray::new(
            context.field.clone(),
            offsets,
            Arc::new(timetz_array),
            Some(nulls),
        );

        Arc::new(list_array)
    }
}
