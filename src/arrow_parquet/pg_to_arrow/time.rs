use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, ListArray, Time64MicrosecondArray},
    datatypes::FieldRef,
};
use arrow_schema::DataType;
use pgrx::datum::Time;

use crate::{
    arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray},
    type_compat::pg_arrow_type_conversions::time_to_i64,
};

use super::PgToArrowPerAttributeContext;

// Time
impl PgTypeToArrowArray<Time> for Option<Time> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let time = self.map(time_to_i64);

        let time_array = Time64MicrosecondArray::from(vec![time]);

        (context.field, Arc::new(time_array))
    }
}

// Time[]
impl PgTypeToArrowArray<pgrx::Array<'_, Time>> for Option<pgrx::Array<'_, Time>> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let pg_array = if let Some(pg_array) = self {
            pg_array
                .iter()
                .map(|time| time.map(time_to_i64))
                .collect::<Vec<_>>()
        } else {
            vec![]
        };

        let time_array = Time64MicrosecondArray::from(pg_array);

        let list_field = context.field;

        match list_field.data_type() {
            DataType::List(time_field) => {
                let list_array = ListArray::new(
                    time_field.clone(),
                    offsets,
                    Arc::new(time_array),
                    Some(nulls),
                );

                (list_field, make_array(list_array.into()))
            }
            _ => panic!("Expected List field"),
        }
    }
}
