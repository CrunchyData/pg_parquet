use arrow::array::{Array, Date32Array};
use pgrx::{pg_sys::Oid, Date, PgTupleDesc};

use crate::type_compat::i32_to_date;

use super::ArrowArrayToPgType;

// Date
impl ArrowArrayToPgType<'_, Date32Array, Date> for Date {
    fn to_pg_type(
        arr: Date32Array,
        _typoid: Oid,
        _typmod: i32,
        _tupledesc: Option<PgTupleDesc<'_>>,
    ) -> Option<Date> {
        if arr.is_null(0) {
            None
        } else {
            let val = arr.value(0);
            let val = i32_to_date(val).unwrap();
            Some(val)
        }
    }
}

// Date[]
impl ArrowArrayToPgType<'_, Date32Array, Vec<Option<Date>>> for Vec<Option<Date>> {
    fn to_pg_type(
        arr: Date32Array,
        _typoid: Oid,
        _typmod: i32,
        _tupledesc: Option<PgTupleDesc<'_>>,
    ) -> Option<Vec<Option<Date>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            let val = val.and_then(i32_to_date);
            vals.push(val);
        }
        Some(vals)
    }
}
