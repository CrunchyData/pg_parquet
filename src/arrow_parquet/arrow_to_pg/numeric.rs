use arrow::array::{Array, Decimal128Array};
use pgrx::{
    pg_sys::{Datum, Oid},
    IntoDatum,
};

use crate::{pgrx_utils::is_array_type, type_compat::i128_to_numeric};

use super::ArrowArrayToPgType;

impl ArrowArrayToPgType<Decimal128Array> for Decimal128Array {
    fn as_pg_datum(self, typoid: Oid, _typmod: i32) -> Option<Datum> {
        if is_array_type(typoid) {
            let mut vals = vec![];
            for val in self.iter() {
                let val = val.and_then(i128_to_numeric);
                vals.push(val);
            }
            vals.into_datum()
        } else if self.is_null(0) {
            None
        } else {
            let val = self.value(0);
            let val = i128_to_numeric(val).unwrap();
            val.into_datum()
        }
    }
}
