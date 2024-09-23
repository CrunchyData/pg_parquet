use arrow::array::{Array, Float64Array};

use super::{ArrowArrayToPgType, ArrowToPgAttributeContext};

// Float8
impl ArrowArrayToPgType<Float64Array, f64> for f64 {
    fn to_pg_type(arr: Float64Array, _context: &ArrowToPgAttributeContext) -> Option<f64> {
        if arr.is_null(0) {
            None
        } else {
            let val = arr.value(0);
            Some(val)
        }
    }
}

// Float8[]
impl ArrowArrayToPgType<Float64Array, Vec<Option<f64>>> for Vec<Option<f64>> {
    fn to_pg_type(
        arr: Float64Array,
        _context: &ArrowToPgAttributeContext,
    ) -> Option<Vec<Option<f64>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            vals.push(val);
        }
        Some(vals)
    }
}
