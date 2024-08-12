use arrow::array::{Array, Float32Array};
use pgrx::{pg_sys::Oid, PgTupleDesc};

use super::ArrowArrayToPgType;

// Float4
impl ArrowArrayToPgType<'_, Float32Array, f32> for f32 {
    fn to_pg_type(
        arr: Float32Array,
        _typoid: Oid,
        _typmod: i32,
        _tupledesc: Option<PgTupleDesc<'_>>,
    ) -> Option<f32> {
        if arr.is_null(0) {
            None
        } else {
            let val = arr.value(0);
            Some(val)
        }
    }
}

// Float4[]
impl ArrowArrayToPgType<'_, Float32Array, Vec<Option<f32>>> for Vec<Option<f32>> {
    fn to_pg_type(
        arr: Float32Array,
        _typoid: Oid,
        _typmod: i32,
        _tupledesc: Option<PgTupleDesc<'_>>,
    ) -> Option<Vec<Option<f32>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            vals.push(val);
        }
        Some(vals)
    }
}
