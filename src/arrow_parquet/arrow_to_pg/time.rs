use arrow::array::{Array, Time64MicrosecondArray};
use pgrx::datum::Time;

use crate::type_compat::pg_arrow_type_conversions::i64_to_time;

use super::{ArrowArrayToPgType, ArrowToPgPerAttributeContext};

// Time
impl ArrowArrayToPgType<'_, Time64MicrosecondArray, Time> for Time {
    fn to_pg_type(
        arr: Time64MicrosecondArray,
        _context: ArrowToPgPerAttributeContext<'_>,
    ) -> Option<Time> {
        if arr.is_null(0) {
            None
        } else {
            let val = arr.value(0);
            i64_to_time(val)
        }
    }
}

// Time[]
impl ArrowArrayToPgType<'_, Time64MicrosecondArray, Vec<Option<Time>>> for Vec<Option<Time>> {
    fn to_pg_type(
        arr: Time64MicrosecondArray,
        _context: ArrowToPgPerAttributeContext<'_>,
    ) -> Option<Vec<Option<Time>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            let val = val.and_then(i64_to_time);
            vals.push(val);
        }
        Some(vals)
    }
}
