use arrow::array::{Array, StringArray};
use pgrx::{Json, PgTupleDesc};

use super::ArrowArrayToPgType;

// Json
impl<'a> ArrowArrayToPgType<'_, StringArray, Json> for Json {
    fn as_pg(arr: StringArray, _tupledesc: Option<PgTupleDesc>) -> Option<Json> {
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
impl<'a> ArrowArrayToPgType<'_, StringArray, Vec<Option<Json>>> for Vec<Option<Json>> {
    fn as_pg(arr: StringArray, _tupledesc: Option<PgTupleDesc>) -> Option<Vec<Option<Json>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            let val = val.map(|val| Json(serde_json::from_str(val).unwrap()));
            vals.push(val);
        }
        Some(vals)
    }
}
