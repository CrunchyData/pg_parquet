use arrow::array::{Array, StringArray};
use pgrx::PgTupleDesc;

use crate::type_compat::VarBit;

use super::ArrowArrayToPgType;

// VarBit
impl<'a> ArrowArrayToPgType<'_, StringArray, VarBit> for VarBit {
    fn as_pg(arr: StringArray, _tupledesc: Option<PgTupleDesc>) -> Option<VarBit> {
        if arr.is_null(0) {
            None
        } else {
            let val = arr.value(0);
            Some(VarBit(val.to_string()))
        }
    }
}

// VarBit[]
impl<'a> ArrowArrayToPgType<'_, StringArray, Vec<Option<VarBit>>> for Vec<Option<VarBit>> {
    fn as_pg(arr: StringArray, _tupledesc: Option<PgTupleDesc>) -> Option<Vec<Option<VarBit>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            let val = val.map(|val| VarBit(val.to_string()));
            vals.push(val);
        }
        Some(vals)
    }
}
