use arrow::array::{Array, UInt32Array};
use pgrx::{pg_sys::Oid, PgTupleDesc};

use super::ArrowArrayToPgType;

// Oid
impl ArrowArrayToPgType<'_, UInt32Array, Oid> for Oid {
    fn to_pg_type(
        arr: UInt32Array,
        _typoid: Oid,
        _typmod: i32,
        _tupledesc: Option<PgTupleDesc<'_>>,
    ) -> Option<Oid> {
        if arr.is_null(0) {
            None
        } else {
            let val = arr.value(0);
            Some(val.into())
        }
    }
}

// Oid[]
impl ArrowArrayToPgType<'_, UInt32Array, Vec<Option<Oid>>> for Vec<Option<Oid>> {
    fn to_pg_type(
        arr: UInt32Array,
        _typoid: Oid,
        _typmod: i32,
        _tupledesc: Option<PgTupleDesc<'_>>,
    ) -> Option<Vec<Option<Oid>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            let val = val.map(|val| val.into());
            vals.push(val);
        }
        Some(vals)
    }
}