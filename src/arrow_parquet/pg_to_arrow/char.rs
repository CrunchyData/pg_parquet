use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, ListArray, StringArray},
    datatypes::FieldRef,
};
use arrow_schema::DataType;

use crate::arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray};

use super::PgToArrowPerAttributeContext;

// Char
impl PgTypeToArrowArray<i8> for Vec<Option<i8>> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let chars = self
            .into_iter()
            .map(|c| c.map(|c| (c as u8 as char).to_string()))
            .collect::<Vec<_>>();

        let char_array = StringArray::from(chars);

        (context.field, Arc::new(char_array))
    }
}

// "Char"[]
impl PgTypeToArrowArray<pgrx::Array<'_, i8>> for Vec<Option<pgrx::Array<'_, i8>>> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let pg_array = self
            .into_iter()
            .map(|v| v.map(|pg_array| pg_array.iter().collect::<Vec<_>>()))
            .collect::<Vec<_>>();

        let (offsets, nulls) = arrow_array_offsets(&pg_array);

        let chars = pg_array
            .into_iter()
            .flatten()
            .flatten()
            .map(|c| c.map(|c| (c as u8 as char).to_string()))
            .collect::<Vec<_>>();

        let char_array = StringArray::from(chars);

        let list_field = context.field;

        match list_field.data_type() {
            DataType::List(char_field) => {
                let list_array = ListArray::new(
                    char_field.clone(),
                    offsets,
                    Arc::new(char_array),
                    Some(nulls),
                );

                (list_field, make_array(list_array.into()))
            }
            _ => panic!("Expected List field"),
        }
    }
}
