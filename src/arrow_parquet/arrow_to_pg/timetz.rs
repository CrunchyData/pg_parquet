use arrow::array::{Array, Time64MicrosecondArray};
use pgrx::{PgTupleDesc, TimeWithTimeZone};

use crate::type_compat::i64_to_timetz;

use super::ArrowArrayToPgType;

// Timetz
impl<'a> ArrowArrayToPgType<'_, Time64MicrosecondArray, TimeWithTimeZone> for TimeWithTimeZone {
    fn as_pg(
        arr: Time64MicrosecondArray,
        _tupledesc: Option<PgTupleDesc>,
    ) -> Option<TimeWithTimeZone> {
        if arr.is_null(0) {
            None
        } else {
            let val = arr.value(0);
            i64_to_timetz(val)
        }
    }
}

// Timetz[]
impl<'a> ArrowArrayToPgType<'_, Time64MicrosecondArray, Vec<Option<TimeWithTimeZone>>>
    for Vec<Option<TimeWithTimeZone>>
{
    fn as_pg(
        arr: Time64MicrosecondArray,
        _tupledesc: Option<PgTupleDesc>,
    ) -> Option<Vec<Option<TimeWithTimeZone>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            let val = val.and_then(i64_to_timetz);
            vals.push(val);
        }
        Some(vals)
    }
}
