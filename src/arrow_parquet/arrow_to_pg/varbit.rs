use arrow::array::{Array, StringArray};
use pgrx::{pg_sys::Oid, PgTupleDesc};

use crate::type_compat::VarBit;

use super::ArrowArrayToPgType;

// VarBit
impl<'a> ArrowArrayToPgType<'_, StringArray, VarBit> for VarBit {
    fn as_pg(
        arr: StringArray,
        _typoid: Oid,
        _typmod: i32,
        _tupledesc: Option<PgTupleDesc<'_>>,
    ) -> Option<VarBit> {
        if arr.is_null(0) {
            None
        } else {
            let val = arr.value(0);
            Some(VarBit::new(val.to_string()))
        }
    }
}

// VarBit[]
impl<'a> ArrowArrayToPgType<'_, StringArray, Vec<Option<VarBit>>> for Vec<Option<VarBit>> {
    fn as_pg(
        arr: StringArray,
        _typoid: Oid,
        _typmod: i32,
        _tupledesc: Option<PgTupleDesc<'_>>,
    ) -> Option<Vec<Option<VarBit>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            let val = val.map(|val| VarBit::new(val.to_string()));
            vals.push(val);
        }
        Some(vals)
    }
}
