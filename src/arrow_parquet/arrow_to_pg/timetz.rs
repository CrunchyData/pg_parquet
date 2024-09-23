use arrow::array::{Array, Time64MicrosecondArray};
use pgrx::datum::TimeWithTimeZone;

use crate::type_compat::pg_arrow_type_conversions::i64_to_timetz;

use super::{ArrowArrayToPgType, ArrowToPgAttributeContext};

// Timetz
impl ArrowArrayToPgType<Time64MicrosecondArray, TimeWithTimeZone> for TimeWithTimeZone {
    fn to_pg_type(
        arr: Time64MicrosecondArray,
        _context: &ArrowToPgAttributeContext,
    ) -> Option<TimeWithTimeZone> {
        if arr.is_null(0) {
            None
        } else {
            Some(i64_to_timetz(arr.value(0)))
        }
    }
}

// Timetz[]
impl ArrowArrayToPgType<Time64MicrosecondArray, Vec<Option<TimeWithTimeZone>>>
    for Vec<Option<TimeWithTimeZone>>
{
    fn to_pg_type(
        arr: Time64MicrosecondArray,
        _context: &ArrowToPgAttributeContext,
    ) -> Option<Vec<Option<TimeWithTimeZone>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            let val = val.map(i64_to_timetz);
            vals.push(val);
        }
        Some(vals)
    }
}
