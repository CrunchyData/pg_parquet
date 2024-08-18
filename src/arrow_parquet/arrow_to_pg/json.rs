use arrow::array::{Array, StringArray};
use pgrx::Json;

use super::{ArrowArrayToPgType, ArrowToPgPerAttributeContext};

// Json
impl ArrowArrayToPgType<'_, StringArray, Json> for Json {
    fn to_pg_type(arr: StringArray, _context: ArrowToPgPerAttributeContext<'_>) -> Option<Json> {
        if arr.is_null(0) {
            None
        } else {
            let val = arr.value(0);
            let val = Json(serde_json::from_str(val).unwrap());
            Some(val)
        }
    }
}

// Json[]
impl ArrowArrayToPgType<'_, StringArray, Vec<Option<Json>>> for Vec<Option<Json>> {
    fn to_pg_type(arr: StringArray, _context: ArrowToPgPerAttributeContext<'_>) -> Option<Vec<Option<Json>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            let val = val.map(|val| Json(serde_json::from_str(val).unwrap()));
            vals.push(val);
        }
        Some(vals)
    }
}
