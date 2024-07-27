use arrow::array::{Array, TimestampMicrosecondArray};
use pgrx::{PgTupleDesc, Timestamp};

use crate::type_compat::i64_to_timestamp;

use super::ArrowArrayToPgType;

// Timestamp
impl<'a> ArrowArrayToPgType<'_, TimestampMicrosecondArray, Timestamp> for Timestamp {
    fn as_pg(arr: TimestampMicrosecondArray, _tupledesc: Option<PgTupleDesc>) -> Option<Timestamp> {
        if arr.is_null(0) {
            None
        } else {
            let val = arr.value(0);
            i64_to_timestamp(val)
        }
    }
}

// Timestamp[]
impl<'a> ArrowArrayToPgType<'_, TimestampMicrosecondArray, Vec<Option<Timestamp>>>
    for Vec<Option<Timestamp>>
{
    fn as_pg(
        arr: TimestampMicrosecondArray,
        _tupledesc: Option<PgTupleDesc>,
    ) -> Option<Vec<Option<Timestamp>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            let val = val.and_then(i64_to_timestamp);
            vals.push(val);
        }
        Some(vals)
    }
}
