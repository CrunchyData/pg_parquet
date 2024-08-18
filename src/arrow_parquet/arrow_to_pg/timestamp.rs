use arrow::array::{Array, TimestampMicrosecondArray};
use pgrx::Timestamp;

use crate::type_compat::pg_arrow_type_conversions::i64_to_timestamp;

use super::{ArrowArrayToPgType, ArrowToPgContext};

// Timestamp
impl ArrowArrayToPgType<'_, TimestampMicrosecondArray, Timestamp> for Timestamp {
    fn to_pg_type(
        arr: TimestampMicrosecondArray,
        _context: ArrowToPgContext<'_>,
    ) -> Option<Timestamp> {
        if arr.is_null(0) {
            None
        } else {
            let val = arr.value(0);
            i64_to_timestamp(val)
        }
    }
}

// Timestamp[]
impl ArrowArrayToPgType<'_, TimestampMicrosecondArray, Vec<Option<Timestamp>>>
    for Vec<Option<Timestamp>>
{
    fn to_pg_type(
        arr: TimestampMicrosecondArray,
        _context: ArrowToPgContext<'_>,
    ) -> Option<Vec<Option<Timestamp>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            let val = val.and_then(i64_to_timestamp);
            vals.push(val);
        }
        Some(vals)
    }
}
