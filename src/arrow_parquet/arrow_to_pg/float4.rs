use arrow::array::{Array, Float32Array};

use super::{ArrowArrayToPgType, ArrowToPgContext};

// Float4
impl ArrowArrayToPgType<'_, Float32Array, f32> for f32 {
    fn to_pg_type(arr: Float32Array, _context: ArrowToPgContext<'_>) -> Option<f32> {
        if arr.is_null(0) {
            None
        } else {
            let val = arr.value(0);
            Some(val)
        }
    }
}

// Float4[]
impl ArrowArrayToPgType<'_, Float32Array, Vec<Option<f32>>> for Vec<Option<f32>> {
    fn to_pg_type(arr: Float32Array, _context: ArrowToPgContext<'_>) -> Option<Vec<Option<f32>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            vals.push(val);
        }
        Some(vals)
    }
}
