use arrow::array::{Array, Int32Array};
use pgrx::{
    pg_sys::{Datum, Oid},
    IntoDatum,
};

use crate::pgrx_utils::is_array_type;

use super::ArrowArrayToPgType;

impl ArrowArrayToPgType<Int32Array> for Int32Array {
    fn as_pg_datum(self, typoid: Oid, _typmod: i32) -> Option<Datum> {
        if is_array_type(typoid) {
            let mut vals = vec![];
            for val in self.iter() {
                vals.push(val);
            }
            vals.into_datum()
        } else if self.is_null(0) {
            None
        } else {
            let val = self.value(0);
            val.into_datum()
        }
    }
}
