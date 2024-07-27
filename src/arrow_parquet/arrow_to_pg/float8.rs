use arrow::array::{Array, Float64Array};
use pgrx::PgTupleDesc;

use super::ArrowArrayToPgType;

// Float8
impl<'a> ArrowArrayToPgType<'_, Float64Array, f64> for f64 {
    fn as_pg(arr: Float64Array, _tupledesc: Option<PgTupleDesc>) -> Option<f64> {
        if arr.is_null(0) {
            None
        } else {
            let val = arr.value(0);
            Some(val)
        }
    }
}

// Float8[]
impl<'a> ArrowArrayToPgType<'_, Float64Array, Vec<Option<f64>>> for Vec<Option<f64>> {
    fn as_pg(arr: Float64Array, _tupledesc: Option<PgTupleDesc>) -> Option<Vec<Option<f64>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            vals.push(val);
        }
        Some(vals)
    }
}
