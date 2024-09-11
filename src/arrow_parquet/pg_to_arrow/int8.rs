use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, ListArray};

use crate::arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray};

use super::PgToArrowAttributeContext;

// Int64
impl PgTypeToArrowArray<i64> for Vec<Option<i64>> {
    fn to_arrow_array(self, _context: &PgToArrowAttributeContext) -> ArrayRef {
        let int64_array = Int64Array::from(self);
        Arc::new(int64_array)
    }
}

// Int64[]
impl PgTypeToArrowArray<pgrx::Array<'_, i64>> for Vec<Option<pgrx::Array<'_, i64>>> {
    fn to_arrow_array(self, context: &PgToArrowAttributeContext) -> ArrayRef {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let pg_array = self
            .into_iter()
            .flatten()
            .flat_map(|pg_array| pg_array.iter().collect::<Vec<_>>())
            .collect::<Vec<_>>();

        let int64_array = Int64Array::from(pg_array);

        let list_array = ListArray::new(
            context.field.clone(),
            offsets,
            Arc::new(int64_array),
            Some(nulls),
        );

        Arc::new(list_array)
    }
}
