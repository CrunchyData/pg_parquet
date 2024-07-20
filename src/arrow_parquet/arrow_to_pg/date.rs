use arrow::array::{Array, Date32Array};
use pgrx::{
    pg_sys::{Datum, Oid},
    IntoDatum,
};

use crate::{pgrx_utils::is_array_type, type_compat::i32_to_date};

use super::ArrowArrayToPgType;

impl ArrowArrayToPgType<Date32Array> for Date32Array {
    fn as_pg_datum(self, typoid: Oid, _typmod: i32) -> Option<Datum> {
        if is_array_type(typoid) {
            let mut vals = vec![];
            for val in self.iter() {
                let val = val.and_then(i32_to_date);
                vals.push(val);
            }
            vals.into_datum()
        } else if self.is_null(0) {
            None
        } else {
            let val = self.value(0);
            let val = i32_to_date(val).unwrap();
            val.into_datum()
        }
    }
}
