use arrow::array::{Array, StringArray};
use pgrx::PgTupleDesc;

use crate::type_compat::Bit;

use super::ArrowArrayToPgType;

// Bit
impl<'a> ArrowArrayToPgType<'_, StringArray, Bit> for Bit {
    fn as_pg(arr: StringArray, _tupledesc: Option<PgTupleDesc>) -> Option<Bit> {
        if arr.is_null(0) {
            None
        } else {
            let val = arr.value(0);
            Some(Bit(val.to_string()))
        }
    }
}

// Bit[]
impl<'a> ArrowArrayToPgType<'_, StringArray, Vec<Option<Bit>>> for Vec<Option<Bit>> {
    fn as_pg(arr: StringArray, _tupledesc: Option<PgTupleDesc>) -> Option<Vec<Option<Bit>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            let val = val.map(|val| Bit(val.to_string()));
            vals.push(val);
        }
        Some(vals)
    }
}
