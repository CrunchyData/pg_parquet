use arrow::array::{Array, StringArray};
use pgrx::{pg_sys::Oid, PgTupleDesc};

use crate::type_compat::Enum;

use super::ArrowArrayToPgType;

// Enum
impl<'a> ArrowArrayToPgType<'_, StringArray, Enum> for Enum {
    fn as_pg(
        arr: StringArray,
        typoid: Oid,
        _typmod: i32,
        _tupledesc: Option<PgTupleDesc<'_>>,
    ) -> Option<Enum> {
        if arr.is_null(0) {
            None
        } else {
            let val = arr.value(0);
            Some(Enum::new(val.to_string(), typoid))
        }
    }
}

// Enum[]
impl<'a> ArrowArrayToPgType<'_, StringArray, Vec<Option<Enum>>> for Vec<Option<Enum>> {
    fn as_pg(
        arr: StringArray,
        typoid: Oid,
        _typmod: i32,
        _tupledesc: Option<PgTupleDesc<'_>>,
    ) -> Option<Vec<Option<Enum>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            let val = val.map(|val| Enum::new(val.to_string(), typoid));
            vals.push(val);
        }
        Some(vals)
    }
}
