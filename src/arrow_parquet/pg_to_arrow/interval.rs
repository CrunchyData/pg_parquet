use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, IntervalMonthDayNanoArray, ListArray},
    datatypes::FieldRef,
};
use pgrx::{pg_sys::Oid, Interval};

use crate::{
    arrow_parquet::{
        arrow_utils::arrow_array_offsets,
        pg_to_arrow::PgTypeToArrowArray,
        schema_visitor::{visit_list_schema, visit_primitive_schema},
    },
    type_compat::pg_arrow_type_conversions::interval_to_nano,
};

// Interval
impl PgTypeToArrowArray<Interval> for Vec<Option<Interval>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let interval_field = visit_primitive_schema(typoid, typmod, name);

        let intervals = self
            .into_iter()
            .map(|interval| interval.and_then(interval_to_nano))
            .collect::<Vec<_>>();

        let interval_array = IntervalMonthDayNanoArray::from(intervals);

        (interval_field, Arc::new(interval_array))
    }
}

// Interval[]
impl PgTypeToArrowArray<Vec<Option<Interval>>> for Vec<Option<Vec<Option<Interval>>>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let interval_field = visit_primitive_schema(typoid, typmod, name);

        let intervals = self
            .into_iter()
            .flatten()
            .flatten()
            .map(|interval| interval.and_then(interval_to_nano))
            .collect::<Vec<_>>();

        let interval_array = IntervalMonthDayNanoArray::from(intervals);

        let list_field = visit_list_schema(typoid, typmod, name);
        let list_array = ListArray::new(
            interval_field,
            offsets,
            Arc::new(interval_array),
            Some(nulls),
        );
        (list_field, make_array(list_array.into()))
    }
}
