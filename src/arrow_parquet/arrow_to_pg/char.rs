use arrow::array::{Array, StringArray};

use super::{ArrowArrayToPgType, ArrowToPgPerAttributeContext};

// Char
impl ArrowArrayToPgType<'_, StringArray, i8> for i8 {
    fn to_pg_type(arr: StringArray, _context: ArrowToPgPerAttributeContext<'_>) -> Option<i8> {
        if arr.is_null(0) {
            None
        } else {
            let val = arr.value(0);
            let val: i8 = val.chars().next().unwrap() as i8;
            Some(val)
        }
    }
}

// Char[]
impl ArrowArrayToPgType<'_, StringArray, Vec<Option<i8>>> for Vec<Option<i8>> {
    fn to_pg_type(
        arr: StringArray,
        _context: ArrowToPgPerAttributeContext<'_>,
    ) -> Option<Vec<Option<i8>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            let val = val.map(|val| {
                let val: i8 = val.chars().next().unwrap() as i8;
                val
            });
            vals.push(val);
        }
        Some(vals)
    }
}
