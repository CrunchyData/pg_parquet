use arrow::array::{Array, Int16Array};
use pgrx::PgTupleDesc;

use super::ArrowArrayToPgType;

// Int2
impl<'a> ArrowArrayToPgType<'_, Int16Array, i16> for i16 {
    fn as_pg(arr: Int16Array, _tupledesc: Option<PgTupleDesc>) -> Option<i16> {
        if arr.is_null(0) {
            None
        } else {
            let val = arr.value(0);
            Some(val)
        }
    }
}

// Int2[]
impl<'a> ArrowArrayToPgType<'_, Int16Array, Vec<Option<i16>>> for Vec<Option<i16>> {
    fn as_pg(arr: Int16Array, _tupledesc: Option<PgTupleDesc>) -> Option<Vec<Option<i16>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            vals.push(val);
        }
        Some(vals)
    }
}
