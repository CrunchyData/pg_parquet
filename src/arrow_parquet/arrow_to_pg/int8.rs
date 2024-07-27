use arrow::array::{Array, Int64Array};
use pgrx::PgTupleDesc;

use super::ArrowArrayToPgType;

// Int8
impl<'a> ArrowArrayToPgType<'_, Int64Array, i64> for i64 {
    fn as_pg(arr: Int64Array, _tupledesc: Option<PgTupleDesc>) -> Option<i64> {
        if arr.is_null(0) {
            None
        } else {
            let val = arr.value(0);
            Some(val)
        }
    }
}

// Int8[]
impl<'a> ArrowArrayToPgType<'_, Int64Array, Vec<Option<i64>>> for Vec<Option<i64>> {
    fn as_pg(arr: Int64Array, _tupledesc: Option<PgTupleDesc>) -> Option<Vec<Option<i64>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            vals.push(val);
        }
        Some(vals)
    }
}
