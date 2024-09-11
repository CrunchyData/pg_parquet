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
impl PgTypeToArrowArray<Timestamp> for Vec<Option<Timestamp>> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let timestamps = self
            .into_iter()
            .map(|timstamp| timstamp.and_then(timestamp_to_i64))
            .collect::<Vec<_>>();

        let timestamp_array = TimestampMicrosecondArray::from(timestamps);

        (context.field, Arc::new(timestamp_array))
    }
}

// Timestamp[]
impl PgTypeToArrowArray<pgrx::Array<'_, Timestamp>> for Vec<Option<pgrx::Array<'_, Timestamp>>> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let pg_array = self
            .into_iter()
            .map(|v| v.map(|pg_array| pg_array.iter().collect::<Vec<_>>()))
            .collect::<Vec<_>>();

        let (offsets, nulls) = arrow_array_offsets(&pg_array);

        let timestamps = pg_array
            .into_iter()
            .flatten()
            .flatten()
            .map(|timestamp| timestamp.and_then(timestamp_to_i64))
            .collect::<Vec<_>>();

        let timestamp_array = TimestampMicrosecondArray::from(timestamps);

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
