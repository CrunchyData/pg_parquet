use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, ListArray, StringArray},
    datatypes::FieldRef,
};
use arrow_schema::DataType;

use crate::{
    arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray},
    type_compat::fallback_to_text::FallbackToText,
};

use super::PgToArrowPerAttributeContext;

// Text representation of any type
impl PgTypeToArrowArray<FallbackToText> for Option<FallbackToText> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let text = self.map(String::from);

        let text_array = StringArray::from(vec![text]);

        (context.field, Arc::new(text_array))
    }
}

// Text[] representation of any type
impl PgTypeToArrowArray<pgrx::Array<'_, FallbackToText>>
    for Option<pgrx::Array<'_, FallbackToText>>
{
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
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

        let list_field = context.field;

        match list_field.data_type() {
            DataType::List(text_field) => {
                let list_array = ListArray::new(
                    text_field.clone(),
                    offsets,
                    Arc::new(text_array),
                    Some(nulls),
                );

                (list_field, make_array(list_array.into()))
            }
            _ => panic!("Expected List field"),
        }
    }
}
