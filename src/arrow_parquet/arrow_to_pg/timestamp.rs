use arrow::array::{Array, TimestampMicrosecondArray};
use pgrx::{
    pg_sys::{Datum, Oid, TIMESTAMPARRAYOID, TIMESTAMPOID, TIMESTAMPTZARRAYOID, TIMESTAMPTZOID},
    IntoDatum,
};

use crate::{
    pgrx_utils::is_array_type,
    type_compat::{i64_to_timestamp, i64_to_timestamptz},
};

use super::ArrowArrayToPgType;

impl ArrowArrayToPgType<TimestampMicrosecondArray> for TimestampMicrosecondArray {
    fn as_pg_datum(self, typoid: Oid, _typmod: i32) -> Option<Datum> {
        if is_array_type(typoid) {
            if typoid == TIMESTAMPARRAYOID {
                let mut vals = vec![];
                for val in self.iter() {
                    let val = val.and_then(i64_to_timestamp);
                    vals.push(val);
                }
                return vals.into_datum();
            } else {
                assert!(typoid == TIMESTAMPTZARRAYOID);
                let mut vals = vec![];
                for val in self.iter() {
                    let val = val.and_then(i64_to_timestamptz);
                    vals.push(val);
                }
                return vals.into_datum();
            }
        }

        if self.is_null(0) {
            None
        } else if typoid == TIMESTAMPOID {
            let val = self.value(0);
            let val = i64_to_timestamp(val).unwrap();
            val.into_datum()
        } else {
            assert!(typoid == TIMESTAMPTZOID);
            let val = self.value(0);
            let val = i64_to_timestamptz(val).unwrap();
            val.into_datum()
        }
    }
}
