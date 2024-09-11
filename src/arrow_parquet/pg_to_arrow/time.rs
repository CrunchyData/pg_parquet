use std::sync::Arc;

use arrow::array::{ArrayRef, ListArray, Time64MicrosecondArray};
use pgrx::datum::Time;

use crate::{
    arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray},
    type_compat::pg_arrow_type_conversions::time_to_i64,
};

use super::PgToArrowAttributeContext;

// Time
impl PgTypeToArrowArray<Time> for Vec<Option<Time>> {
    fn to_arrow_array(self, _context: &PgToArrowAttributeContext) -> ArrayRef {
        let times = self
            .into_iter()
            .map(|time| time.map(time_to_i64))
            .collect::<Vec<_>>();
        let time_array = Time64MicrosecondArray::from(times);
        Arc::new(time_array)
    }
}

// Time[]
impl PgTypeToArrowArray<pgrx::Array<'_, Time>> for Vec<Option<pgrx::Array<'_, Time>>> {
    fn to_arrow_array(self, context: &PgToArrowAttributeContext) -> ArrayRef {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let pg_array = self
            .into_iter()
            .flatten()
            .flat_map(|pg_array| pg_array.iter().collect::<Vec<_>>())
            .map(|time| time.map(time_to_i64))
            .collect::<Vec<_>>();

        let time_array = Time64MicrosecondArray::from(pg_array);

        let list_array = ListArray::new(
            context.field.clone(),
            offsets,
            Arc::new(time_array),
            Some(nulls),
        );

        Arc::new(list_array)
    }
}
