use arrow::array::{Array, StringArray};
use pgrx::PgTupleDesc;

use crate::type_compat::Bpchar;

use super::ArrowArrayToPgType;

// Bpchar
impl<'a> ArrowArrayToPgType<'_, StringArray, Bpchar> for Bpchar {
    fn as_pg(arr: StringArray, _tupledesc: Option<PgTupleDesc>) -> Option<Bpchar> {
        if arr.is_null(0) {
            None
        } else {
            let val = arr.value(0);
            Some(Bpchar(val.to_string()))
        }
    }
}

// Bpchar[]
impl<'a> ArrowArrayToPgType<'_, StringArray, Vec<Option<Bpchar>>> for Vec<Option<Bpchar>> {
    fn as_pg(arr: StringArray, _tupledesc: Option<PgTupleDesc>) -> Option<Vec<Option<Bpchar>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            let val = val.map(|val| Bpchar(val.to_string()));
            vals.push(val);
        }
        Some(vals)
    }
}
