use arrow::array::{Array, BooleanArray};

use super::{ArrowArrayToPgType, ArrowToPgAttributeContext};

// Bool
impl ArrowArrayToPgType<BooleanArray, bool> for bool {
    fn to_pg_type(arr: BooleanArray, _context: &ArrowToPgAttributeContext) -> Option<bool> {
        if arr.is_null(0) {
            None
        } else {
            let val = arr.value(0);
            Some(val)
        }
    }
}

// Bool[]
impl ArrowArrayToPgType<BooleanArray, Vec<Option<bool>>> for Vec<Option<bool>> {
    fn to_pg_type(
        arr: BooleanArray,
        _context: &ArrowToPgAttributeContext,
    ) -> Option<Vec<Option<bool>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            vals.push(val);
        }

        Some(vals)
    }
}
