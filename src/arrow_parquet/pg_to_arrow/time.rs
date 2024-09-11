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
impl PgTypeToArrowArray<Time> for Vec<Option<Time>> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let times = self
            .into_iter()
            .map(|time| time.and_then(time_to_i64))
            .collect::<Vec<_>>();

        let time_array = Time64MicrosecondArray::from(times);

        (context.field, Arc::new(time_array))
    }
}

// Time[]
impl PgTypeToArrowArray<pgrx::Array<'_, Time>> for Vec<Option<pgrx::Array<'_, Time>>> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let pg_array = self
            .into_iter()
            .map(|v| v.map(|pg_array| pg_array.iter().collect::<Vec<_>>()))
            .collect::<Vec<_>>();

        let (offsets, nulls) = arrow_array_offsets(&pg_array);

        let times = pg_array
            .into_iter()
            .flatten()
            .flatten()
            .map(|time| time.and_then(time_to_i64))
            .collect::<Vec<_>>();

        let time_array = Time64MicrosecondArray::from(times);

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
