use arrow::array::{Array, TimestampMicrosecondArray};
use pgrx::{PgTupleDesc, TimestampWithTimeZone};

use crate::type_compat::i64_to_timestamptz;

use super::ArrowArrayToPgType;

// Timestamptz
impl<'a> ArrowArrayToPgType<'_, TimestampMicrosecondArray, TimestampWithTimeZone>
    for TimestampWithTimeZone
{
    fn as_pg(
        arr: TimestampMicrosecondArray,
        _tupledesc: Option<PgTupleDesc>,
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
impl<'a> ArrowArrayToPgType<'_, TimestampMicrosecondArray, Vec<Option<TimestampWithTimeZone>>>
    for Vec<Option<TimestampWithTimeZone>>
{
    fn as_pg(
        arr: TimestampMicrosecondArray,
        _tupledesc: Option<PgTupleDesc>,
    ) -> Option<Vec<Option<TimestampWithTimeZone>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            let val = val.and_then(i64_to_timestamptz);
            vals.push(val);
        }
        Some(vals)
    }
}
