use std::sync::Arc;

use arrow::array::{ArrayRef, ListArray, StringArray};

use crate::arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray};

use super::PgToArrowAttributeContext;

// Text
impl PgTypeToArrowArray<String> for Vec<Option<String>> {
    fn to_arrow_array(self, _context: &PgToArrowAttributeContext) -> ArrayRef {
        let text_array = StringArray::from(self);
        Arc::new(text_array)
    }
}

// Text[]
impl PgTypeToArrowArray<pgrx::Array<'_, String>> for Vec<Option<pgrx::Array<'_, String>>> {
    fn to_arrow_array(self, context: &PgToArrowAttributeContext) -> ArrayRef {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let pg_array = self
            .into_iter()
            .flatten()
            .flat_map(|pg_array| pg_array.iter().collect::<Vec<_>>())
            .collect::<Vec<_>>();

        let text_array = StringArray::from(pg_array);

        let list_array = ListArray::new(
            context.field.clone(),
            offsets,
            Arc::new(text_array),
            Some(nulls),
        );

        Arc::new(list_array)
    }
}
