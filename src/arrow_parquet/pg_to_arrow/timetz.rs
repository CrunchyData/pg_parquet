use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::{ArrayRef, Time64MicrosecondArray},
    datatypes::{DataType, Field, FieldRef, TimeUnit},
};
use pgrx::{pg_sys::Oid, TimeWithTimeZone};

use crate::{
    arrow_parquet::{
        arrow_utils::{arrow_array_offsets, create_arrow_list_array},
        pg_to_arrow::PgTypeToArrowArray,
    },
    type_compat::pg_arrow_type_conversions::timetz_to_i64,
};

// TimeTz
impl PgTypeToArrowArray<TimeWithTimeZone> for Vec<Option<TimeWithTimeZone>> {
    fn to_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let timetz_array = self
            .into_iter()
            .map(|timetz| timetz.and_then(timetz_to_i64))
            .collect::<Vec<_>>();

        let field = Field::new(name, DataType::Time64(TimeUnit::Microsecond), true).with_metadata(
            HashMap::from_iter(vec![("adjusted_to_utc".into(), "true".into())]),
        );

        let array = Time64MicrosecondArray::from(timetz_array);
        (Arc::new(field), Arc::new(array))
    }
}

// TimeTz[]
impl PgTypeToArrowArray<Vec<Option<TimeWithTimeZone>>>
    for Vec<Option<Vec<Option<TimeWithTimeZone>>>>
{
    fn to_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let field = Field::new(name, DataType::Time64(TimeUnit::Microsecond), true).with_metadata(
            HashMap::from_iter(vec![("adjusted_to_utc".into(), "true".into())]),
        );

        let array = self
            .into_iter()
            .flatten()
            .flatten()
            .map(|timetz| timetz.and_then(timetz_to_i64))
            .collect::<Vec<_>>();
        let array = Time64MicrosecondArray::from(array);
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        create_arrow_list_array(name, field, primitive_array, offsets, nulls)
    }
}
