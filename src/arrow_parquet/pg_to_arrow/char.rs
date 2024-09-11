use std::sync::Arc;

use arrow::array::{ArrayRef, ListArray, StringArray};

use crate::arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray};

use super::PgToArrowAttributeContext;

// Char
impl PgTypeToArrowArray<i8> for Vec<Option<i8>> {
    fn to_arrow_array(self, _context: &PgToArrowAttributeContext) -> ArrayRef {
        let chars = self
            .into_iter()
            .map(|c| c.map(|c| (c as u8 as char).to_string()))
            .collect::<Vec<_>>();
        let char_array = StringArray::from(chars);
        Arc::new(char_array)
    }
}

// "Char"[]
impl PgTypeToArrowArray<pgrx::Array<'_, i8>> for Vec<Option<pgrx::Array<'_, i8>>> {
    fn to_arrow_array(self, context: &PgToArrowAttributeContext) -> ArrayRef {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let pg_array = self
            .into_iter()
            .flatten()
            .flat_map(|pg_array| pg_array.iter().collect::<Vec<_>>())
            .map(|c| c.map(|c| (c as u8 as char).to_string()))
            .collect::<Vec<_>>();

        let char_array = StringArray::from(pg_array);

        let list_array = ListArray::new(
            context.field.clone(),
            offsets,
            Arc::new(char_array),
            Some(nulls),
        );

        Arc::new(list_array)
    }
}
