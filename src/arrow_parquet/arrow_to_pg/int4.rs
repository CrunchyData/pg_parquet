use arrow::array::{Array, Int32Array};
use pgrx::PgTupleDesc;

use super::ArrowArrayToPgType;

// Int4
impl<'a> ArrowArrayToPgType<'_, Int32Array, i32> for i32 {
    fn as_pg(arr: Int32Array, _tupledesc: Option<PgTupleDesc>) -> Option<i32> {
        if arr.is_null(0) {
            None
        } else {
            let val = arr.value(0);
            Some(val)
        }
    }
}

// Int4[]
impl<'a> ArrowArrayToPgType<'_, Int32Array, Vec<Option<i32>>> for Vec<Option<i32>> {
    fn as_pg(arr: Int32Array, _tupledesc: Option<PgTupleDesc>) -> Option<Vec<Option<i32>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            vals.push(val);
        }
        Some(vals)
    }
}
