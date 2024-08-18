use arrow::array::{Array, StringArray};
use pgrx::JsonB;

use super::{ArrowArrayToPgType, ArrowToPgPerAttributeContext};

// Jsonb
impl ArrowArrayToPgType<'_, StringArray, JsonB> for JsonB {
    fn to_pg_type(arr: StringArray, _context: ArrowToPgPerAttributeContext<'_>) -> Option<JsonB> {
        if arr.is_null(0) {
            None
        } else {
            let val = arr.value(0);
            let val = JsonB(serde_json::from_str(val).unwrap());
            Some(val)
        }
    }
}

// Jsonb[]
impl ArrowArrayToPgType<'_, StringArray, Vec<Option<JsonB>>> for Vec<Option<JsonB>> {
    fn to_pg_type(
        arr: StringArray,
        _context: ArrowToPgPerAttributeContext<'_>,
    ) -> Option<Vec<Option<JsonB>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            let val = val.map(|val| JsonB(serde_json::from_str(val).unwrap()));
            vals.push(val);
        }
        Some(vals)
    }
}
