use arrow::array::{Array, IntervalMonthDayNanoArray};
use pgrx::{
    pg_sys::{Datum, Oid},
    IntoDatum,
};

use crate::{pgrx_utils::is_array_type, type_compat::nano_to_interval};

use super::ArrowArrayToPgType;

impl ArrowArrayToPgType<IntervalMonthDayNanoArray> for IntervalMonthDayNanoArray {
    fn as_pg_datum(self, typoid: Oid, _typmod: i32) -> Option<Datum> {
        if is_array_type(typoid) {
            let mut vals = vec![];
            for val in self.iter() {
                let val = val.and_then(nano_to_interval);
                vals.push(val);
            }
            vals.into_datum()
        } else if self.is_null(0) {
            None
        } else {
            let val = self.value(0);
            let val = nano_to_interval(val).unwrap();
            val.into_datum()
        }
    }
}
