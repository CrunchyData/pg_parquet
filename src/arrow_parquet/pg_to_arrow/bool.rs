use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, ListArray};

use crate::arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray};

use super::PgToArrowAttributeContext;

// Bool
impl PgTypeToArrowArray<bool> for Option<bool> {
    fn to_arrow_array(self, _context: &PgToArrowAttributeContext) -> ArrayRef {
        let bool_array = BooleanArray::from(vec![self]);
        Arc::new(bool_array)
    }
}

// Bool[]
impl<'a> PgTypeToArrowArray<pgrx::Array<'a, bool>> for Option<pgrx::Array<'a, bool>> {
    fn to_arrow_array(self, context: &PgToArrowAttributeContext) -> ArrayRef {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let pg_array = if let Some(pg_array) = self {
            pg_array.iter().collect::<Vec<_>>()
        } else {
            vec![]
        };

        let bool_array = BooleanArray::from(pg_array);

        let list_array = ListArray::new(
            context.field.clone(),
            offsets,
            Arc::new(bool_array),
            Some(nulls),
        );

        Arc::new(list_array)
    }
}
