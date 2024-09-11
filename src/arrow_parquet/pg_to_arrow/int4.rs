use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, ListArray};

use crate::arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray};

use super::PgToArrowAttributeContext;

// Int32
impl PgTypeToArrowArray<i32> for Option<i32> {
    fn to_arrow_array(self, _context: &PgToArrowAttributeContext) -> ArrayRef {
        let int32_array = Int32Array::from(vec![self]);
        Arc::new(int32_array)
    }
}

// Int32[]
impl PgTypeToArrowArray<pgrx::Array<'_, i32>> for Option<pgrx::Array<'_, i32>> {
    fn to_arrow_array(self, context: &PgToArrowAttributeContext) -> ArrayRef {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let pg_array = if let Some(pg_array) = self {
            pg_array.iter().collect::<Vec<_>>()
        } else {
            vec![]
        };

        let int32_array = Int32Array::from(pg_array);

        let list_array = ListArray::new(
            context.field.clone(),
            offsets,
            Arc::new(int32_array),
            Some(nulls),
        );

        Arc::new(list_array)
    }
}
