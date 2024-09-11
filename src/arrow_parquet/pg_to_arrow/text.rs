use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, ListArray, StringArray},
    datatypes::FieldRef,
};
use arrow_schema::DataType;

use crate::arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray};

use super::PgToArrowPerAttributeContext;

// Text
impl PgTypeToArrowArray<String> for Option<String> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let text_array = StringArray::from(vec![self]);
        (context.field, Arc::new(text_array))
    }
}

// Text[]
impl PgTypeToArrowArray<pgrx::Array<'_, String>> for Option<pgrx::Array<'_, String>> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let pg_array = if let Some(pg_array) = self {
            pg_array.iter().collect::<Vec<_>>()
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
