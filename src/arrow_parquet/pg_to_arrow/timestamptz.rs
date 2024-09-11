use std::sync::Arc;

use arrow::array::{ArrayRef, ListArray, TimestampMicrosecondArray};
use pgrx::datum::TimestampWithTimeZone;

use crate::{
    arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray},
    type_compat::pg_arrow_type_conversions::timestamptz_to_i64,
};

use super::PgToArrowAttributeContext;

// TimestampTz
impl PgTypeToArrowArray<TimestampWithTimeZone> for Option<TimestampWithTimeZone> {
    fn to_arrow_array(self, _context: &PgToArrowAttributeContext) -> ArrayRef {
        let timestamptz = self.map(timestamptz_to_i64);
        let timestamptz_array =
            TimestampMicrosecondArray::from(vec![timestamptz]).with_timezone_utc();
        Arc::new(timestamptz_array)
    }
}

// TimestampTz[]
impl PgTypeToArrowArray<pgrx::Array<'_, TimestampWithTimeZone>>
    for Option<pgrx::Array<'_, TimestampWithTimeZone>>
{
    fn to_arrow_array(self, context: &PgToArrowAttributeContext) -> ArrayRef {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let pg_array = if let Some(pg_array) = self {
            pg_array
                .iter()
                .map(|timestamptz| timestamptz.map(timestamptz_to_i64))
                .collect::<Vec<_>>()
        } else {
            vec![]
        };

        let timestamptz_array = TimestampMicrosecondArray::from(pg_array).with_timezone_utc();

        let list_array = ListArray::new(
            context.field.clone(),
            offsets,
            Arc::new(timestamptz_array),
            Some(nulls),
        );

        Arc::new(list_array)
    }
}
