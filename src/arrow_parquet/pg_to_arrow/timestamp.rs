use std::sync::Arc;

use arrow::{
    array::{ArrayRef, TimestampMicrosecondArray},
    datatypes::{DataType, Field, FieldRef, TimeUnit},
};
use pgrx::{pg_sys::Oid, Timestamp};

use crate::{
    arrow_parquet::{
        pg_to_arrow::PgTypeToArrowArray,
        utils::{arrow_array_offsets, create_arrow_list_array, },
    },
    type_compat::timestamp_to_i64,
};

// Timestamp
impl PgTypeToArrowArray<Timestamp> for Vec<Option<Timestamp>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let timestamp_array = self
            .into_iter()
            .map(|timstamp| timstamp.and_then(timestamp_to_i64))
            .collect::<Vec<_>>();

        let field = Field::new(name, DataType::Timestamp(TimeUnit::Microsecond, None), true);

        let array = TimestampMicrosecondArray::from(timestamp_array);
        (Arc::new(field), Arc::new(array))
    }
}

// Timestamp[]
impl PgTypeToArrowArray<Vec<Option<Timestamp>>> for Vec<Option<Vec<Option<Timestamp>>>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let field = Field::new(name, DataType::Timestamp(TimeUnit::Microsecond, None), true);

        

        let array = self
            .into_iter()
            .flatten()
            .flatten()
            .map(|timestamp| timestamp.and_then(timestamp_to_i64))
            .collect::<Vec<_>>();
        let array = TimestampMicrosecondArray::from(array);
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        create_arrow_list_array(name, field, primitive_array, offsets, nulls)
    }
}
