use arrow::array::{Array, Int32Array};

use super::{ArrowArrayToPgType, ArrowToPgPerAttributeContext};

// Int4
impl ArrowArrayToPgType<'_, Int32Array, i32> for i32 {
    fn to_pg_type(arr: Int32Array, _context: ArrowToPgPerAttributeContext<'_>) -> Option<i32> {
        if arr.is_null(0) {
            None
        } else {
            let val = arr.value(0);
            Some(val)
        }
    }
}

// Int4[]
impl ArrowArrayToPgType<'_, Int32Array, Vec<Option<i32>>> for Vec<Option<i32>> {
    fn to_pg_type(
        arr: Int32Array,
        _context: ArrowToPgPerAttributeContext<'_>,
    ) -> Option<Vec<Option<i32>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            vals.push(val);
        }
        Some(vals)
    }
}
