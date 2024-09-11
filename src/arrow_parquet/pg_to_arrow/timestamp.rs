use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, ListArray, TimestampMicrosecondArray},
    datatypes::FieldRef,
};
use arrow_schema::DataType;
use pgrx::datum::Timestamp;

use crate::{
    arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray},
    type_compat::pg_arrow_type_conversions::timestamp_to_i64,
};

use super::PgToArrowPerAttributeContext;

// Timestamp
impl PgTypeToArrowArray<Timestamp> for Option<Timestamp> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let timestamp = self.map(timestamp_to_i64);

        let timestamp_array = TimestampMicrosecondArray::from(vec![timestamp]);

        (context.field, Arc::new(timestamp_array))
    }
}

// Timestamp[]
impl PgTypeToArrowArray<pgrx::Array<'_, Timestamp>> for Option<pgrx::Array<'_, Timestamp>> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let pg_array = if let Some(pg_array) = self {
            pg_array
                .iter()
                .map(|timestamp| timestamp.map(timestamp_to_i64))
                .collect::<Vec<_>>()
        } else {
            vec![]
        };

        let timestamp_array = TimestampMicrosecondArray::from(pg_array);

        let list_field = context.field;

        match list_field.data_type() {
            DataType::List(timestamp_field) => {
                let list_array = ListArray::new(
                    timestamp_field.clone(),
                    offsets,
                    Arc::new(timestamp_array),
                    Some(nulls),
                );

                (list_field, make_array(list_array.into()))
            }
            _ => panic!("Expected List field"),
        }
    }
}
