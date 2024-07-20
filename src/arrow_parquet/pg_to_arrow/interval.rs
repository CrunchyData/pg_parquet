use std::sync::Arc;

use arrow::{
    array::{ArrayRef, IntervalMonthDayNanoArray},
    datatypes::{DataType, Field, FieldRef, IntervalUnit},
};
use pgrx::{pg_sys::Oid, Interval};

use crate::{
    arrow_parquet::{
        pg_to_arrow::PgTypeToArrowArray,
        utils::{arrow_array_offsets, create_arrow_list_array, create_arrow_null_list_array},
    },
    type_compat::interval_to_nano,
};

// Interval
impl PgTypeToArrowArray<Interval> for Vec<Option<Interval>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let interval_array = self
            .into_iter()
            .map(|interval| interval.and_then(interval_to_nano))
            .collect::<Vec<_>>();

        let field = Field::new(name, DataType::Interval(IntervalUnit::MonthDayNano), true);

        let array = IntervalMonthDayNanoArray::from(interval_array);
        (Arc::new(field), Arc::new(array))
    }
}

// Interval[]
impl PgTypeToArrowArray<Vec<Option<Interval>>> for Vec<Option<Vec<Option<Interval>>>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, all_nulls) = arrow_array_offsets(&self);

        let field = Field::new(name, DataType::Interval(IntervalUnit::MonthDayNano), true);

        if all_nulls {
            return create_arrow_null_list_array(name, &field, self.len());
        }

        let array = self
            .into_iter()
            .flatten()
            .flatten()
            .map(|interval| interval.and_then(interval_to_nano))
            .collect::<Vec<_>>();
        let array = IntervalMonthDayNanoArray::from(array);
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        create_arrow_list_array(name, field, primitive_array, offsets)
    }
}
