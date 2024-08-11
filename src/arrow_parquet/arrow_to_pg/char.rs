use arrow::array::{Array, StringArray};
use pgrx::{pg_sys::Oid, PgTupleDesc};

use super::ArrowArrayToPgType;

// Char
impl<'a> ArrowArrayToPgType<'_, StringArray, i8> for i8 {
    fn as_pg(
        arr: StringArray,
        _typoid: Oid,
        _typmod: i32,
        _tupledesc: Option<PgTupleDesc<'_>>,
    ) -> Option<i8> {
        if arr.is_null(0) {
            None
        } else {
            let val = arr.value(0);
            let val: i8 = val.chars().next().unwrap() as i8;
            Some(val)
        }
    }
}

// Char[]
impl<'a> ArrowArrayToPgType<'_, StringArray, Vec<Option<i8>>> for Vec<Option<i8>> {
    fn as_pg(
        arr: StringArray,
        _typoid: Oid,
        _typmod: i32,
        _tupledesc: Option<PgTupleDesc<'_>>,
    ) -> Option<Vec<Option<i8>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            let val = val.and_then(|val| {
                let val: i8 = val.chars().next().unwrap() as i8;
                Some(val)
            });
            vals.push(val);
        }
        Some(vals)
    }
}
