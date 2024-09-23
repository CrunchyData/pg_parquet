use arrow::array::{Array, TimestampMicrosecondArray};
use pgrx::datum::TimestampWithTimeZone;

use crate::type_compat::pg_arrow_type_conversions::i64_to_timestamptz;

use super::{ArrowArrayToPgType, ArrowToPgAttributeContext};

// Timestamptz
impl ArrowArrayToPgType<TimestampMicrosecondArray, TimestampWithTimeZone>
    for TimestampWithTimeZone
{
    fn to_pg_type(
        arr: TimestampMicrosecondArray,
        _context: &ArrowToPgAttributeContext,
    ) -> Option<TimestampWithTimeZone> {
        if arr.is_null(0) {
            None
        } else {
            Some(i64_to_timestamptz(arr.value(0)))
        }
    }
}

// Timestamptz[]
impl ArrowArrayToPgType<TimestampMicrosecondArray, Vec<Option<TimestampWithTimeZone>>>
    for Vec<Option<TimestampWithTimeZone>>
{
    fn to_pg_type(
        arr: TimestampMicrosecondArray,
        _context: &ArrowToPgAttributeContext,
    ) -> Option<Vec<Option<TimestampWithTimeZone>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            let val = val.map(i64_to_timestamptz);
            vals.push(val);
        }
        Some(vals)
    }
}
