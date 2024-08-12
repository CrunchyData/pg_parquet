use arrow::array::{Array, TimestampMicrosecondArray};
use pgrx::{pg_sys::Oid, PgTupleDesc, Timestamp};

use crate::type_compat::i64_to_timestamp;

use super::ArrowArrayToPgType;

// Timestamp
impl ArrowArrayToPgType<'_, TimestampMicrosecondArray, Timestamp> for Timestamp {
    fn to_pg_type(
        arr: TimestampMicrosecondArray,
        _typoid: Oid,
        _typmod: i32,
        _tupledesc: Option<PgTupleDesc<'_>>,
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
        _typoid: Oid,
        _typmod: i32,
        _tupledesc: Option<PgTupleDesc<'_>>,
    ) -> Option<Vec<Option<Timestamp>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            let val = val.and_then(i64_to_timestamp);
            vals.push(val);
        }
        Some(vals)
    }
}
