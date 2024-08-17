use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, ListArray, StringArray},
    datatypes::FieldRef,
};
use arrow_schema::DataType;
use pgrx::JsonB;

use crate::arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray};

use super::PgTypeToArrowContext;

// Jsonb
impl PgTypeToArrowArray<JsonB> for Vec<Option<JsonB>> {
    fn to_arrow_array(self, context: PgTypeToArrowContext) -> (FieldRef, ArrayRef) {
        let jsonbs = self
            .into_iter()
            .map(|val| val.map(|val| serde_json::to_string(&val.0).unwrap()))
            .collect::<Vec<_>>();

        let jsonb_array = StringArray::from(jsonbs);

        (context.field, Arc::new(jsonb_array))
    }
}

// Jsonb[]
impl PgTypeToArrowArray<Vec<Option<JsonB>>> for Vec<Option<Vec<Option<JsonB>>>> {
    fn to_arrow_array(self, context: PgTypeToArrowContext) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let jsonbs = self
            .into_iter()
            .flatten()
            .flatten()
            .map(|val| val.map(|val| serde_json::to_string(&val.0).unwrap()))
            .collect::<Vec<_>>();

        let jsonb_array = StringArray::from(jsonbs);

        let list_field = context.field;

        match list_field.data_type() {
            DataType::List(jsonb_field) => {
                let list_array = ListArray::new(
                    jsonb_field.clone(),
                    offsets,
                    Arc::new(jsonb_array),
                    Some(nulls),
                );

                (list_field, make_array(list_array.into()))
            }
            _ => panic!("Expected List field"),
        }
    }
}
