use arrow::array::{Array, Int16Array};

use super::{ArrowArrayToPgType, ArrowToPgPerAttributeContext};

// Int2
impl ArrowArrayToPgType<'_, Int16Array, i16> for i16 {
    fn to_pg_type(arr: Int16Array, _context: ArrowToPgPerAttributeContext<'_>) -> Option<i16> {
        if arr.is_null(0) {
            None
        } else {
            let val = arr.value(0);
            Some(val)
        }
    }
}

// Int2[]
impl ArrowArrayToPgType<'_, Int16Array, Vec<Option<i16>>> for Vec<Option<i16>> {
    fn to_pg_type(
        arr: Int16Array,
        _context: ArrowToPgPerAttributeContext<'_>,
    ) -> Option<Vec<Option<i16>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            vals.push(val);
        }
        Some(vals)
    }
}
