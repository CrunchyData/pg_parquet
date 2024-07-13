use std::sync::Arc;

use arrow::{
    array::{ArrayRef, Time64MicrosecondArray},
    datatypes::{DataType, Field, FieldRef, TimeUnit},
};
use pgrx::{pg_sys::Oid, Time};

use crate::{
    arrow_parquet::{
        pg_to_arrow::PgTypeToArrowArray,
        utils::{array_offsets, create_arrow_list_array, create_arrow_null_list_array},
    },
    type_compat::time_to_i64,
};

// Time
impl PgTypeToArrowArray<Time> for Vec<Option<Time>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let time_array = self
            .into_iter()
            .map(|time| time.and_then(time_to_i64))
            .collect::<Vec<_>>();

        let field = Field::new(name, DataType::Time64(TimeUnit::Microsecond), true);

        let array = Time64MicrosecondArray::from(time_array);
        (Arc::new(field), Arc::new(array))
    }
}

// Time[]
impl PgTypeToArrowArray<Vec<Option<Time>>> for Vec<Option<Vec<Option<Time>>>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, all_nulls) = array_offsets(&self);

        let field = Field::new(name, DataType::Time64(TimeUnit::Microsecond), true);

        if all_nulls {
            return create_arrow_null_list_array(name, &field, self.len());
        }

        let array = self
            .into_iter()
            .flatten()
            .flatten()
            .map(|time| time.and_then(time_to_i64))
            .collect::<Vec<_>>();
        let array = Time64MicrosecondArray::from(array);
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        create_arrow_list_array(name, field, primitive_array, offsets)
    }
}
