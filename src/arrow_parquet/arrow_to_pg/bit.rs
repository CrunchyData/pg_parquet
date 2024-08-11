use arrow::array::{Array, StringArray};
use pgrx::{pg_sys::Oid, PgTupleDesc};

use crate::type_compat::Bit;

use super::ArrowArrayToPgType;

// Bit
impl<'a> ArrowArrayToPgType<'_, StringArray, Bit> for Bit {
    fn as_pg(
        arr: StringArray,
        _typoid: Oid,
        _typmod: i32,
        _tupledesc: Option<PgTupleDesc<'_>>,
    ) -> Option<Bit> {
        if arr.is_null(0) {
            None
        } else {
            let val = arr.value(0);
            Some(Bit::new(val.to_string()))
        }
    }
}

// Bit[]
impl<'a> ArrowArrayToPgType<'_, StringArray, Vec<Option<Bit>>> for Vec<Option<Bit>> {
    fn as_pg(
        arr: StringArray,
        _typoid: Oid,
        _typmod: i32,
        _tupledesc: Option<PgTupleDesc<'_>>,
    ) -> Option<Vec<Option<Bit>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            let val = val.map(|val| Bit::new(val.to_string()));
            vals.push(val);
        }
        Some(vals)
    }
}
