use arrow::array::{Array, Time64MicrosecondArray};
use pgrx::datum::Time;

use crate::type_compat::pg_arrow_type_conversions::i64_to_time;

use super::{ArrowArrayToPgType, ArrowToPgAttributeContext};

// Time
impl ArrowArrayToPgType<Time64MicrosecondArray, Time> for Time {
    fn to_pg_type(
        arr: Time64MicrosecondArray,
        _context: &ArrowToPgAttributeContext,
    ) -> Option<Time> {
        if arr.is_null(0) {
            None
        } else {
            Some(i64_to_time(arr.value(0)))
        }
    }
}

// Time[]
impl ArrowArrayToPgType<Time64MicrosecondArray, Vec<Option<Time>>> for Vec<Option<Time>> {
    fn to_pg_type(
        arr: Time64MicrosecondArray,
        _context: &ArrowToPgAttributeContext,
    ) -> Option<Vec<Option<Time>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            let val = val.map(i64_to_time);
            vals.push(val);
        }
        Some(vals)
    }
}
