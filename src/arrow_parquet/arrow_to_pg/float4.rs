use arrow::array::{Array, Float32Array};

use super::{ArrowArrayToPgType, ArrowToPgAttributeContext};

// Float4
impl ArrowArrayToPgType<Float32Array, f32> for f32 {
    fn to_pg_type(arr: Float32Array, _context: &ArrowToPgAttributeContext) -> Option<f32> {
        if arr.is_null(0) {
            None
        } else {
            let val = arr.value(0);
            Some(val)
        }
    }
}

// Float4[]
impl ArrowArrayToPgType<Float32Array, Vec<Option<f32>>> for Vec<Option<f32>> {
    fn to_pg_type(
        arr: Float32Array,
        _context: &ArrowToPgAttributeContext,
    ) -> Option<Vec<Option<f32>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            vals.push(val);
        }
        Some(vals)
    }
}
