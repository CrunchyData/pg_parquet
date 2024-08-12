use std::sync::Arc;

use arrow::{
    array::{ArrayRef, TimestampMicrosecondArray},
    datatypes::{DataType, Field, FieldRef, TimeUnit},
};
use pgrx::{pg_sys::Oid, TimestampWithTimeZone};

use crate::{
    arrow_parquet::{
        arrow_utils::{arrow_array_offsets, create_arrow_list_array},
        pg_to_arrow::PgTypeToArrowArray,
    },
    type_compat::timestamptz_to_i64,
};

// TimestampTz
impl PgTypeToArrowArray<TimestampWithTimeZone> for Vec<Option<TimestampWithTimeZone>> {
    fn to_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let timestamptz_array = self
            .into_iter()
            .map(|timestamptz| timestamptz.and_then(timestamptz_to_i64))
            .collect::<Vec<_>>();

        let field = Field::new(
            name,
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
            true,
        );

        let array = TimestampMicrosecondArray::from(timestamptz_array).with_timezone_utc();
        (Arc::new(field), Arc::new(array))
    }
}

// TimestampTz[]
impl PgTypeToArrowArray<Vec<Option<TimestampWithTimeZone>>>
    for Vec<Option<Vec<Option<TimestampWithTimeZone>>>>
{
    fn to_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let field = Field::new(
            name,
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
            true,
        );

        let array = self
            .into_iter()
            .flatten()
            .flatten()
            .map(|timestamptz| timestamptz.and_then(timestamptz_to_i64))
            .collect::<Vec<_>>();
        let array = TimestampMicrosecondArray::from(array).with_timezone_utc();
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        create_arrow_list_array(name, field, primitive_array, offsets, nulls)
    }
}
