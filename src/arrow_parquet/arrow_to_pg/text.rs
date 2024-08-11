use arrow::array::{Array, StringArray};
use pgrx::{pg_sys::Oid, PgTupleDesc};

use super::ArrowArrayToPgType;

// Text
impl<'a> ArrowArrayToPgType<'_, StringArray, String> for String {
    fn as_pg(
        arr: StringArray,
        _typoid: Oid,
        _typmod: i32,
        _tupledesc: Option<PgTupleDesc<'_>>,
    ) -> Option<String> {
        if arr.is_null(0) {
            None
        } else {
            let val = arr.value(0);
            Some(val.to_string())
        }
    }
}

// Text[]
impl<'a> ArrowArrayToPgType<'_, StringArray, Vec<Option<String>>> for Vec<Option<String>> {
    fn as_pg(
        arr: StringArray,
        _typoid: Oid,
        _typmod: i32,
        _tupledesc: Option<PgTupleDesc<'_>>,
    ) -> Option<Vec<Option<String>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            let val = val.map(|val| val.to_string());
            vals.push(val);
        }
        Some(vals)
    }
}
