use arrow::array::{Array, IntervalMonthDayNanoArray};
use pgrx::{Interval, PgTupleDesc};

use crate::type_compat::nano_to_interval;

use super::ArrowArrayToPgType;

// Interval
impl<'a> ArrowArrayToPgType<'_, IntervalMonthDayNanoArray, Interval> for Interval {
    fn as_pg(arr: IntervalMonthDayNanoArray, _tupledesc: Option<PgTupleDesc>) -> Option<Interval> {
        if arr.is_null(0) {
            None
        } else {
            let val = arr.value(0);
            let val = nano_to_interval(val).unwrap();
            Some(val)
        }
    }
}

// Interval[]
impl<'a> ArrowArrayToPgType<'_, IntervalMonthDayNanoArray, Vec<Option<Interval>>>
    for Vec<Option<Interval>>
{
    fn as_pg(
        arr: IntervalMonthDayNanoArray,
        _tupledesc: Option<PgTupleDesc>,
    ) -> Option<Vec<Option<Interval>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            let val = val.and_then(nano_to_interval);
            vals.push(val);
        }
        Some(vals)
    }
}
