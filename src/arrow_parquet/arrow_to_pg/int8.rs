use arrow::array::{Array, Int64Array};

use super::{ArrowArrayToPgType, ArrowToPgPerAttributeContext};

// Int8
impl ArrowArrayToPgType<'_, Int64Array, i64> for i64 {
    fn to_pg_type(arr: Int64Array, _context: ArrowToPgPerAttributeContext<'_>) -> Option<i64> {
        if arr.is_null(0) {
            None
        } else {
            let val = arr.value(0);
            Some(val)
        }
    }
}

// Int8[]
impl ArrowArrayToPgType<'_, Int64Array, Vec<Option<i64>>> for Vec<Option<i64>> {
    fn to_pg_type(
        arr: Int64Array,
        _context: ArrowToPgPerAttributeContext<'_>,
    ) -> Option<Vec<Option<i64>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            vals.push(val);
        }
        Some(vals)
    }
}
