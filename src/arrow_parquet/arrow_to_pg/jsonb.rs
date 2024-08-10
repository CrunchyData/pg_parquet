use arrow::array::{Array, StringArray};
use pgrx::{JsonB, PgTupleDesc};

use super::ArrowArrayToPgType;

// Jsonb
impl<'a> ArrowArrayToPgType<'_, StringArray, JsonB> for JsonB {
    fn as_pg(arr: StringArray, _tupledesc: Option<PgTupleDesc>) -> Option<JsonB> {
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
impl<'a> ArrowArrayToPgType<'_, StringArray, Vec<Option<JsonB>>> for Vec<Option<JsonB>> {
    fn as_pg(arr: StringArray, _tupledesc: Option<PgTupleDesc>) -> Option<Vec<Option<JsonB>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            let val = val.map(|val| JsonB(serde_json::from_str(val).unwrap()));
            vals.push(val);
        }
        Some(vals)
    }
}
