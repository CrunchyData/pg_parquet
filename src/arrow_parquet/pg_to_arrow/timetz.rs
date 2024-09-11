use std::sync::Arc;

use arrow::array::{ArrayRef, ListArray, Time64MicrosecondArray};
use pgrx::datum::TimeWithTimeZone;

use crate::{
    arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray},
    type_compat::pg_arrow_type_conversions::timetz_to_i64,
};

use super::PgToArrowAttributeContext;

// TimeTz
impl PgTypeToArrowArray<TimeWithTimeZone> for Option<TimeWithTimeZone> {
    fn to_arrow_array(self, _context: &PgToArrowAttributeContext) -> ArrayRef {
        let timetz = self.map(timetz_to_i64);
        let timetz_array = Time64MicrosecondArray::from(vec![timetz]);
        Arc::new(timetz_array)
    }
}

// TimeTz[]
impl PgTypeToArrowArray<pgrx::Array<'_, TimeWithTimeZone>>
    for Option<pgrx::Array<'_, TimeWithTimeZone>>
{
    fn to_arrow_array(self, context: &PgToArrowAttributeContext) -> ArrayRef {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let pg_array = if let Some(pg_array) = self {
            pg_array
                .iter()
                .map(|timetz| timetz.map(timetz_to_i64))
                .collect::<Vec<_>>()
        } else {
            vec![]
        };

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
