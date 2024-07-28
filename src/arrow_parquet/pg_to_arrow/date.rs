use std::sync::Arc;

use arrow::{
    array::{ArrayRef, Date32Array},
    datatypes::{DataType, Field, FieldRef},
};
use pgrx::{pg_sys::Oid, Date};

use crate::{
    arrow_parquet::{
        pg_to_arrow::PgTypeToArrowArray,
        utils::{arrow_array_offsets, create_arrow_list_array},
    },
    type_compat::date_to_i32,
};

// Date
impl PgTypeToArrowArray<Date> for Vec<Option<Date>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let date_array = self
            .into_iter()
            .map(|date| date.and_then(date_to_i32))
            .collect::<Vec<_>>();

        let field = Field::new(name, DataType::Date32, true);
        let array = Date32Array::from(date_array);
        (Arc::new(field), Arc::new(array))
    }
}

// Date[]
impl PgTypeToArrowArray<Vec<Option<Date>>> for Vec<Option<Vec<Option<Date>>>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let field = Field::new(name, DataType::Date32, true);

        let array = self
            .into_iter()
            .flatten()
            .flatten()
            .map(|date| date.and_then(date_to_i32))
            .collect::<Vec<_>>();

        let array = Date32Array::from(array);
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        create_arrow_list_array(name, field, primitive_array, offsets, nulls)
    }
}
