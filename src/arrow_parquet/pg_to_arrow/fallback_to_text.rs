use std::sync::Arc;

use arrow::array::{ArrayRef, ListArray, StringArray};

use crate::{
    arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray},
    type_compat::fallback_to_text::FallbackToText,
};

use super::PgToArrowAttributeContext;

// Text representation of any type
impl PgTypeToArrowArray<FallbackToText> for Vec<Option<FallbackToText>> {
    fn to_arrow_array(self, _context: &PgToArrowAttributeContext) -> ArrayRef {
        let texts = self
            .into_iter()
            .map(|f| f.map(String::from))
            .collect::<Vec<_>>();
        let text_array = StringArray::from(texts);
        Arc::new(text_array)
    }
}

// Text[] representation of any type
impl PgTypeToArrowArray<pgrx::Array<'_, FallbackToText>>
    for Vec<Option<pgrx::Array<'_, FallbackToText>>>
{
    fn to_arrow_array(self, context: &PgToArrowAttributeContext) -> ArrayRef {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let pg_array = self
            .into_iter()
            .flatten()
            .flat_map(|pg_array| pg_array.iter().collect::<Vec<_>>())
            .map(|f| f.map(String::from))
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
