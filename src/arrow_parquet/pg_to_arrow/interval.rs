use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, IntervalMonthDayNanoArray, ListArray},
    datatypes::FieldRef,
};
use arrow_schema::DataType;
use pgrx::Interval;

use crate::{
    arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray},
    type_compat::pg_arrow_type_conversions::interval_to_nano,
};

use super::PgTypeToArrowContext;

// Interval
impl PgTypeToArrowArray<Interval> for Vec<Option<Interval>> {
    fn to_arrow_array(self, context: PgTypeToArrowContext) -> (FieldRef, ArrayRef) {
        let intervals = self
            .into_iter()
            .map(|interval| interval.and_then(interval_to_nano))
            .collect::<Vec<_>>();

        let interval_array = IntervalMonthDayNanoArray::from(intervals);

        (context.field, Arc::new(interval_array))
    }
}

// Interval[]
impl PgTypeToArrowArray<Vec<Option<Interval>>> for Vec<Option<Vec<Option<Interval>>>> {
    fn to_arrow_array(self, context: PgTypeToArrowContext) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let intervals = self
            .into_iter()
            .flatten()
            .flatten()
            .map(|interval| interval.and_then(interval_to_nano))
            .collect::<Vec<_>>();

        let interval_array = IntervalMonthDayNanoArray::from(intervals);

        let list_field = context.field;

        match list_field.data_type() {
            DataType::List(interval_field) => {
                let list_array = ListArray::new(
                    interval_field.clone(),
                    offsets,
                    Arc::new(interval_array),
                    Some(nulls),
                );

                (list_field, make_array(list_array.into()))
            }
            _ => panic!("Expected List field"),
        }
    }
}
