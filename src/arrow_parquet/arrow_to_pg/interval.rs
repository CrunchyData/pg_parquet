use arrow::array::{Array, IntervalMonthDayNanoArray};
use pgrx::Interval;

use crate::type_compat::pg_arrow_type_conversions::nano_to_interval;

use super::{ArrowArrayToPgType, ArrowToPgPerAttributeContext};

// Interval
impl ArrowArrayToPgType<'_, IntervalMonthDayNanoArray, Interval> for Interval {
    fn to_pg_type(
        arr: IntervalMonthDayNanoArray,
        _context: ArrowToPgPerAttributeContext<'_>,
    ) -> Option<Interval> {
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
impl ArrowArrayToPgType<'_, IntervalMonthDayNanoArray, Vec<Option<Interval>>>
    for Vec<Option<Interval>>
{
    fn to_pg_type(
        arr: IntervalMonthDayNanoArray,
        _context: ArrowToPgPerAttributeContext<'_>,
    ) -> Option<Vec<Option<Interval>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            let val = val.and_then(nano_to_interval);
            vals.push(val);
        }
        Some(vals)
    }
}
