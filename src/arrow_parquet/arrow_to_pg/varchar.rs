use arrow::array::{Array, StringArray};
use pgrx::{pg_sys::Oid, PgTupleDesc};

use crate::type_compat::Varchar;

use super::ArrowArrayToPgType;

// Varchar
impl<'a> ArrowArrayToPgType<'_, StringArray, Varchar> for Varchar {
    fn as_pg(
        arr: StringArray,
        _typoid: Oid,
        _typmod: i32,
        _tupledesc: Option<PgTupleDesc<'_>>,
    ) -> Option<Varchar> {
        if arr.is_null(0) {
            None
        } else {
            let val = arr.value(0);
            Some(Varchar(val.to_string()))
        }
    }
}

// Varchar[]
impl<'a> ArrowArrayToPgType<'_, StringArray, Vec<Option<Varchar>>> for Vec<Option<Varchar>> {
    fn as_pg(
        arr: StringArray,
        _typoid: Oid,
        _typmod: i32,
        _tupledesc: Option<PgTupleDesc<'_>>,
    ) -> Option<Vec<Option<Varchar>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            let val = val.map(|val| Varchar(val.to_string()));
            vals.push(val);
        }
        Some(vals)
    }
}
