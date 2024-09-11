use std::sync::Arc;

use arrow::array::{ArrayRef, ListArray, StringArray};

use crate::arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray};

use super::PgToArrowAttributeContext;

// Char
impl PgTypeToArrowArray<i8> for Option<i8> {
    fn to_arrow_array(self, _context: &PgToArrowAttributeContext) -> ArrayRef {
        let char = self.map(|c| (c as u8 as char).to_string());
        let char_array = StringArray::from(vec![char]);
        Arc::new(char_array)
    }
}

// "Char"[]
impl PgTypeToArrowArray<pgrx::Array<'_, i8>> for Option<pgrx::Array<'_, i8>> {
    fn to_arrow_array(self, context: &PgToArrowAttributeContext) -> ArrayRef {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let pg_array = if let Some(pg_array) = self {
            pg_array
                .iter()
                .map(|c| c.map(|c| (c as u8 as char).to_string()))
                .collect::<Vec<_>>()
        } else {
            vec![]
        };

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
