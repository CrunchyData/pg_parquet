use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, ListArray, TimestampMicrosecondArray},
    datatypes::FieldRef,
};
use arrow_schema::DataType;
use pgrx::datum::TimestampWithTimeZone;

use crate::{
    arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray},
    type_compat::pg_arrow_type_conversions::timestamptz_to_i64,
};

use super::PgToArrowPerAttributeContext;

// TimestampTz
impl PgTypeToArrowArray<TimestampWithTimeZone> for Vec<Option<TimestampWithTimeZone>> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let timestamptzs = self
            .into_iter()
            .map(|timestamptz| timestamptz.and_then(timestamptz_to_i64))
            .collect::<Vec<_>>();

        let timestamptz_array = TimestampMicrosecondArray::from(timestamptzs).with_timezone_utc();

        (context.field, Arc::new(timestamptz_array))
    }
}

// TimestampTz[]
impl PgTypeToArrowArray<pgrx::Array<'_, TimestampWithTimeZone>>
    for Vec<Option<pgrx::Array<'_, TimestampWithTimeZone>>>
{
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let pg_array = self
            .into_iter()
            .map(|v| v.map(|pg_array| pg_array.iter().collect::<Vec<_>>()))
            .collect::<Vec<_>>();

        let (offsets, nulls) = arrow_array_offsets(&pg_array);

        let timestamptzs = pg_array
            .into_iter()
            .flatten()
            .flatten()
            .map(|timestamptz| timestamptz.and_then(timestamptz_to_i64))
            .collect::<Vec<_>>();

        let timestamptz_array = TimestampMicrosecondArray::from(timestamptzs).with_timezone_utc();

        let list_field = context.field;

        match list_field.data_type() {
            DataType::List(timestamptz_field) => {
                let list_array = ListArray::new(
                    timestamptz_field.clone(),
                    offsets,
                    Arc::new(timestamptz_array),
                    Some(nulls),
                );

                (list_field, make_array(list_array.into()))
            }
            _ => panic!("Expected List field"),
        }
    }
}
