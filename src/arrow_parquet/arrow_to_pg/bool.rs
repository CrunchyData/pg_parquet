use arrow::array::{Array, BooleanArray};
use pgrx::PgTupleDesc;

use super::ArrowArrayToPgType;

// Bool
impl<'a> ArrowArrayToPgType<'_, BooleanArray, bool> for bool {
    fn as_pg(arr: BooleanArray, _tupledesc: Option<PgTupleDesc>) -> Option<bool> {
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
    fn as_pg(arr: BooleanArray, _tupledesc: Option<PgTupleDesc>) -> Option<Vec<Option<bool>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            vals.push(val);
        }

        Some(vals)
    }
}
