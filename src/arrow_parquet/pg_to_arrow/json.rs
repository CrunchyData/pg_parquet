use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, ListArray, StringArray},
    datatypes::FieldRef,
};
use arrow_schema::DataType;
use pgrx::Json;

use crate::arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray};

use super::PgToArrowContext;

// Json
impl PgTypeToArrowArray<Json> for Vec<Option<Json>> {
    fn to_arrow_array(self, context: PgToArrowContext) -> (FieldRef, ArrayRef) {
        let jsons = self
            .into_iter()
            .map(|val| val.map(|val| serde_json::to_string(&val.0).unwrap()))
            .collect::<Vec<_>>();

        let json_array = StringArray::from(jsons);

        (context.field, Arc::new(json_array))
    }
}

// Json[]
impl PgTypeToArrowArray<Vec<Option<Json>>> for Vec<Option<Vec<Option<Json>>>> {
    fn to_arrow_array(self, context: PgToArrowContext) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let jsons = self
            .into_iter()
            .flatten()
            .flatten()
            .map(|val| val.map(|val| serde_json::to_string(&val.0).unwrap()))
            .collect::<Vec<_>>();

        let json_array = StringArray::from(jsons);

        let list_field = context.field;

        match list_field.data_type() {
            DataType::List(json_field) => {
                let list_array = ListArray::new(
                    json_field.clone(),
                    offsets,
                    Arc::new(json_array),
                    Some(nulls),
                );

                (list_field, make_array(list_array.into()))
            }
            _ => panic!("Expected List field"),
        }
    }
}
