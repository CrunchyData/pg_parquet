use std::sync::Arc;

use arrow::array::{ArrayRef, ListArray, StringArray};

use crate::{
    arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray},
    type_compat::fallback_to_text::FallbackToText,
};

use super::PgToArrowAttributeContext;

// Text representation of any type
impl PgTypeToArrowArray<FallbackToText> for Option<FallbackToText> {
    fn to_arrow_array(self, _context: &PgToArrowAttributeContext) -> ArrayRef {
        let text = self.map(String::from);
        let text_array = StringArray::from(vec![text]);
        Arc::new(text_array)
    }
}

// Text[] representation of any type
impl PgTypeToArrowArray<pgrx::Array<'_, FallbackToText>>
    for Option<pgrx::Array<'_, FallbackToText>>
{
    fn to_arrow_array(self, context: &PgToArrowAttributeContext) -> ArrayRef {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let pg_array = if let Some(pg_array) = self {
            pg_array
                .iter()
                .map(|f| f.map(String::from))
                .collect::<Vec<_>>()
        } else {
            vec![]
        };

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
