use arrow::array::{Array, TimestampMicrosecondArray};
use pgrx::datum::TimestampWithTimeZone;

use crate::type_compat::pg_arrow_type_conversions::i64_to_timestamptz;

use super::{ArrowArrayToPgType, ArrowToPgPerAttributeContext};

// Timestamptz
impl ArrowArrayToPgType<'_, TimestampMicrosecondArray, TimestampWithTimeZone>
    for TimestampWithTimeZone
{
    fn to_pg_type(
        arr: TimestampMicrosecondArray,
        _context: ArrowToPgPerAttributeContext<'_>,
    ) -> Option<TimestampWithTimeZone> {
        if arr.is_null(0) {
            None
        } else {
            let val = arr.value(0);
            i64_to_timestamptz(val)
        }
    }
}

// Timestamptz[]
impl ArrowArrayToPgType<'_, TimestampMicrosecondArray, Vec<Option<TimestampWithTimeZone>>>
    for Vec<Option<TimestampWithTimeZone>>
{
    fn to_pg_type(
        arr: TimestampMicrosecondArray,
        _context: ArrowToPgPerAttributeContext<'_>,
    ) -> Option<Vec<Option<TimestampWithTimeZone>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            let val = val.and_then(i64_to_timestamptz);
            vals.push(val);
        }
        Some(vals)
    }
}
