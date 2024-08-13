use arrow::array::{Array, BinaryArray};
use pgrx::{pg_sys::Oid, PgTupleDesc};

use crate::type_compat::geometry::Geometry;

use super::ArrowArrayToPgType;

// Geometry
impl ArrowArrayToPgType<'_, BinaryArray, Geometry> for Geometry {
    fn to_pg_type(
        arr: BinaryArray,
        _typoid: Oid,
        _typmod: i32,
        _tupledesc: Option<PgTupleDesc<'_>>,
    ) -> Option<Geometry> {
        if arr.is_null(0) {
            None
        } else {
            Some(arr.value(0).to_vec().into())
        }
    }
}

// Geometry[]
impl ArrowArrayToPgType<'_, BinaryArray, Vec<Option<Geometry>>> for Vec<Option<Geometry>> {
    fn to_pg_type(
        arr: BinaryArray,
        _typoid: Oid,
        _typmod: i32,
        _tupledesc: Option<PgTupleDesc<'_>>,
    ) -> Option<Vec<Option<Geometry>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            if let Some(val) = val {
                vals.push(Some(val.to_vec().into()));
            } else {
                vals.push(None);
            }
        }

        Some(vals)
    }
}
