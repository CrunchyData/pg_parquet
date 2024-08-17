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

use super::PgTypeToArrowContext;

// Text representation of any type
impl PgTypeToArrowArray<FallbackToText> for Vec<Option<FallbackToText>> {
    fn to_arrow_array(self, context: PgTypeToArrowContext) -> (FieldRef, ArrayRef) {
        let texts = self
            .into_iter()
            .map(|val| val.map(String::from))
            .collect::<Vec<_>>();

        let text_array = StringArray::from(texts);

        (context.field, Arc::new(text_array))
    }
}

// Text[] representation of any type
impl PgTypeToArrowArray<Vec<Option<FallbackToText>>> for Vec<Option<Vec<Option<FallbackToText>>>> {
    fn to_arrow_array(self, context: PgTypeToArrowContext) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let texts = self.into_iter().flatten().flatten().collect::<Vec<_>>();

        let texts = texts
            .into_iter()
            .map(|val| val.map(String::from))
            .collect::<Vec<_>>();

        let text_array = StringArray::from(texts);

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
