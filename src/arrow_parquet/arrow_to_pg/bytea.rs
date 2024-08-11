use arrow::array::{Array, BinaryArray};
use pgrx::{pg_sys::Oid, PgTupleDesc};

use super::ArrowArrayToPgType;

// Bytea
impl<'a> ArrowArrayToPgType<'_, BinaryArray, Vec<u8>> for Vec<u8> {
    fn as_pg(
        arr: BinaryArray,
        _typoid: Oid,
        _typmod: i32,
        _tupledesc: Option<PgTupleDesc<'_>>,
    ) -> Option<Vec<u8>> {
        if arr.is_null(0) {
            None
        } else {
            Some(arr.value(0).to_vec())
        }
    }
}

// Bytea[]
impl<'a> ArrowArrayToPgType<'_, BinaryArray, Vec<Option<Vec<u8>>>> for Vec<Option<Vec<u8>>> {
    fn as_pg(
        arr: BinaryArray,
        _typoid: Oid,
        _typmod: i32,
        _tupledesc: Option<PgTupleDesc<'_>>,
    ) -> Option<Vec<Option<Vec<u8>>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            if let Some(val) = val {
                vals.push(Some(val.to_vec()));
            } else {
                vals.push(None);
            }
        }

        Some(vals)
    }
}
