use arrow::array::{Array, BooleanArray};
use pgrx::{pg_sys::Oid, PgTupleDesc};

use super::ArrowArrayToPgType;

// Bool
impl<'a> ArrowArrayToPgType<'_, BooleanArray, bool> for bool {
    fn as_pg(
        arr: BooleanArray,
        _typoid: Oid,
        _typmod: i32,
        _tupledesc: Option<PgTupleDesc<'_>>,
    ) -> Option<bool> {
        if arr.is_null(0) {
            None
        } else {
            let val = arr.value(0);
            Some(val)
        }
    }
}

// Bool[]
impl<'a> ArrowArrayToPgType<'_, BooleanArray, Vec<Option<bool>>> for Vec<Option<bool>> {
    fn as_pg(
        arr: BooleanArray,
        _typoid: Oid,
        _typmod: i32,
        _tupledesc: Option<PgTupleDesc<'_>>,
    ) -> Option<Vec<Option<bool>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            vals.push(val);
        }

        Some(vals)
    }
}
