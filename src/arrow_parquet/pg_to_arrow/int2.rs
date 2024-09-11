use std::sync::Arc;

use arrow::array::{ArrayRef, Int16Array, ListArray};

use crate::arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray};

use super::PgToArrowAttributeContext;

// Int16
impl PgTypeToArrowArray<i16> for Vec<Option<i16>> {
    fn to_arrow_array(self, _context: &PgToArrowAttributeContext) -> ArrayRef {
        let int16_array = Int16Array::from(self);
        Arc::new(int16_array)
    }
}

// Int16[]
impl PgTypeToArrowArray<pgrx::Array<'_, i16>> for Vec<Option<pgrx::Array<'_, i16>>> {
    fn to_arrow_array(self, context: &PgToArrowAttributeContext) -> ArrayRef {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let pg_array = self
            .into_iter()
            .flatten()
            .flat_map(|pg_array| pg_array.iter().collect::<Vec<_>>())
            .collect::<Vec<_>>();

        let int16_array = Int16Array::from(pg_array);

        let list_array = ListArray::new(
            context.field.clone(),
            offsets,
            Arc::new(int16_array),
            Some(nulls),
        );

        Arc::new(list_array)
    }
}
