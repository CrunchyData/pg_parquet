use arrow::array::{Array, Date32Array};
use pgrx::{Date, PgTupleDesc};

use crate::type_compat::i32_to_date;

use super::ArrowArrayToPgType;

// Date
impl<'a> ArrowArrayToPgType<'_, Date32Array, Date> for Date {
    fn as_pg(arr: Date32Array, _tupledesc: Option<PgTupleDesc>) -> Option<Date> {
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
impl<'a> ArrowArrayToPgType<'_, Date32Array, Vec<Option<Date>>> for Vec<Option<Date>> {
    fn as_pg(arr: Date32Array, _tupledesc: Option<PgTupleDesc>) -> Option<Vec<Option<Date>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            let val = val.and_then(i32_to_date);
            vals.push(val);
        }
        Some(vals)
    }
}
