use arrow::array::{Array, Int64Array};
use pgrx::{pg_sys::Oid, PgTupleDesc};

use super::ArrowArrayToPgType;

// Int8
impl ArrowArrayToPgType<'_, Int64Array, i64> for i64 {
    fn to_pg_type(
        arr: Int64Array,
        _typoid: Oid,
        _typmod: i32,
        _tupledesc: Option<PgTupleDesc<'_>>,
    ) -> Option<i64> {
        if arr.is_null(0) {
            None
        } else {
            let val = arr.value(0);
            Some(val)
        }
    }
}

// Int8[]
impl ArrowArrayToPgType<'_, Int64Array, Vec<Option<i64>>> for Vec<Option<i64>> {
    fn to_pg_type(
        arr: Int64Array,
        _typoid: Oid,
        _typmod: i32,
        _tupledesc: Option<PgTupleDesc<'_>>,
    ) -> Option<Vec<Option<i64>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            vals.push(val);
        }
        Some(vals)
    }
}
