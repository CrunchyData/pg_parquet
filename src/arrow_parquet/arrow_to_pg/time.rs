use arrow::array::{Array, Time64MicrosecondArray};
use pgrx::{
    pg_sys::{Datum, Oid, TIMEARRAYOID, TIMEOID, TIMETZARRAYOID, TIMETZOID},
    IntoDatum,
};

use crate::{
    pgrx_utils::is_array_type,
    type_compat::{i64_to_time, i64_to_timetz},
};

use super::ArrowArrayToPgType;

impl ArrowArrayToPgType<Time64MicrosecondArray> for Time64MicrosecondArray {
    fn as_pg_datum(self, typoid: Oid, _typmod: i32) -> Option<Datum> {
        if is_array_type(typoid) {
            if typoid == TIMEARRAYOID {
                let mut vals = vec![];
                for val in self.iter() {
                    let val = val.and_then(i64_to_time);
                    vals.push(val);
                }
                return vals.into_datum();
            } else {
                assert!(typoid == TIMETZARRAYOID);
                let mut vals = vec![];
                for val in self.iter() {
                    let val = val.and_then(i64_to_timetz);
                    vals.push(val);
                }
                return vals.into_datum();
            }
        }

        if self.is_null(0) {
            None
        } else if typoid == TIMEOID {
            let val = self.value(0);
            let val = i64_to_time(val).unwrap();
            val.into_datum()
        } else {
            assert!(typoid == TIMETZOID);
            let val = self.value(0);
            let val = i64_to_timetz(val).unwrap();
            val.into_datum()
        }
    }
}
