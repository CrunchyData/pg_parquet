use arrow::array::{Array, StringArray};
use pgrx::PgTupleDesc;

use crate::type_compat::Name;

use super::ArrowArrayToPgType;

// Name
impl<'a> ArrowArrayToPgType<'_, StringArray, Name> for Name {
    fn as_pg(arr: StringArray, _tupledesc: Option<PgTupleDesc>) -> Option<Name> {
        if arr.is_null(0) {
            None
        } else {
            let val = arr.value(0);
            Some(Name(val.to_string()))
        }
    }
}

// Name[]
impl<'a> ArrowArrayToPgType<'_, StringArray, Vec<Option<Name>>> for Vec<Option<Name>> {
    fn as_pg(arr: StringArray, _tupledesc: Option<PgTupleDesc>) -> Option<Vec<Option<Name>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            let val = val.map(|val| Name(val.to_string()));
            vals.push(val);
        }
        Some(vals)
    }
}
