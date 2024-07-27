use arrow::array::{Array, Time64MicrosecondArray};
use pgrx::{PgTupleDesc, Time};

use crate::type_compat::i64_to_time;

use super::ArrowArrayToPgType;

// Time
impl<'a> ArrowArrayToPgType<'_, Time64MicrosecondArray, Time> for Time {
    fn as_pg(arr: Time64MicrosecondArray, _tupledesc: Option<PgTupleDesc>) -> Option<Time> {
        if arr.is_null(0) {
            None
        } else {
            let val = arr.value(0);
            i64_to_time(val)
        }
    }
}

// Time[]
impl<'a> ArrowArrayToPgType<'_, Time64MicrosecondArray, Vec<Option<Time>>> for Vec<Option<Time>> {
    fn as_pg(
        arr: Time64MicrosecondArray,
        _tupledesc: Option<PgTupleDesc>,
    ) -> Option<Vec<Option<Time>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            let val = val.and_then(i64_to_time);
            vals.push(val);
        }
        Some(vals)
    }
}
