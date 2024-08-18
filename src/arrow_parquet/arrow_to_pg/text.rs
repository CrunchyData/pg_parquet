use arrow::array::{Array, StringArray};

use super::{ArrowArrayToPgType, ArrowToPgContext};

// Text
impl ArrowArrayToPgType<'_, StringArray, String> for String {
    fn to_pg_type(arr: StringArray, _context: ArrowToPgContext<'_>) -> Option<String> {
        if arr.is_null(0) {
            None
        } else {
            let val = arr.value(0);
            Some(val.to_string())
        }
    }
}

// Text[]
impl ArrowArrayToPgType<'_, StringArray, Vec<Option<String>>> for Vec<Option<String>> {
    fn to_pg_type(arr: StringArray, _context: ArrowToPgContext<'_>) -> Option<Vec<Option<String>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            let val = val.map(|val| val.to_string());
            vals.push(val);
        }
        Some(vals)
    }
}
