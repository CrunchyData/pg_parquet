use arrow::array::{Array, FixedSizeBinaryArray};
use pgrx::{pg_sys::Oid, PgTupleDesc, Uuid};

use super::ArrowArrayToPgType;

// Uuid
impl ArrowArrayToPgType<'_, FixedSizeBinaryArray, Uuid> for Uuid {
    fn to_pg_type(
        arr: FixedSizeBinaryArray,
        _typoid: Oid,
        _typmod: i32,
        _tupledesc: Option<PgTupleDesc<'_>>,
    ) -> Option<Uuid> {
        if arr.is_null(0) {
            None
        } else {
            let val = arr.value(0);
            let val = Uuid::from_slice(val).unwrap();
            Some(val)
        }
    }
}

// Uuid[]
impl ArrowArrayToPgType<'_, FixedSizeBinaryArray, Vec<Option<Uuid>>> for Vec<Option<Uuid>> {
    fn to_pg_type(
        arr: FixedSizeBinaryArray,
        _typoid: Oid,
        _typmod: i32,
        _tupledesc: Option<PgTupleDesc<'_>>,
    ) -> Option<Vec<Option<Uuid>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            let val = val.map(|val| Uuid::from_slice(val).unwrap());
            vals.push(val);
        }
        Some(vals)
    }
}
