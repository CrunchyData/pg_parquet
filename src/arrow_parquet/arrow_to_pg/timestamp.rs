use arrow::array::{Array, TimestampMicrosecondArray};
use pgrx::datum::Timestamp;

use crate::type_compat::pg_arrow_type_conversions::i64_to_timestamp;

use super::{ArrowArrayToPgType, ArrowToPgAttributeContext};

// Timestamp
impl ArrowArrayToPgType<TimestampMicrosecondArray, Timestamp> for Timestamp {
    fn to_pg_type(
        arr: TimestampMicrosecondArray,
        _context: &ArrowToPgAttributeContext,
    ) -> Option<Timestamp> {
        if arr.is_null(0) {
            None
        } else {
            Some(i64_to_timestamp(arr.value(0)))
        }
    }
}

// Timestamp[]
impl ArrowArrayToPgType<TimestampMicrosecondArray, Vec<Option<Timestamp>>>
    for Vec<Option<Timestamp>>
{
    fn to_pg_type(
        arr: TimestampMicrosecondArray,
        _context: &ArrowToPgAttributeContext,
    ) -> Option<Vec<Option<Timestamp>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            let val = val.map(i64_to_timestamp);
            vals.push(val);
        }
        Some(vals)
    }
}
