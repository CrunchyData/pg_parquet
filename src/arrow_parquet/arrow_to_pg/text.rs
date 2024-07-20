use arrow::array::{Array, StringArray};
use pgrx::{
    pg_sys::{
        Datum, Oid, CHARARRAYOID, CHAROID, TEXTARRAYOID, TEXTOID, VARCHARARRAYOID, VARCHAROID,
    },
    IntoDatum,
};

use crate::pgrx_utils::is_array_type;

use super::ArrowArrayToPgType;

impl ArrowArrayToPgType<StringArray> for StringArray {
    fn as_pg_datum(self, typoid: Oid, _typmod: i32) -> Option<Datum> {
        if is_array_type(typoid) {
            if typoid == CHARARRAYOID {
                let mut vals = vec![];
                for val in self.iter() {
                    let val = val.and_then(|val| {
                        let val: i8 = val.chars().next().unwrap() as i8;
                        Some(val)
                    });
                    vals.push(val);
                }
                return vals.into_datum();
            } else {
                assert!(typoid == TEXTARRAYOID || typoid == VARCHARARRAYOID);
                let mut vals = vec![];
                for val in self.iter() {
                    vals.push(val);
                }
                return vals.into_datum();
            }
        }

        if self.is_null(0) {
            return None;
        } else if typoid == CHAROID {
            let val = self.value(0);
            let val: i8 = val.chars().next().unwrap() as i8;
            val.into_datum()
        } else {
            assert!(typoid == TEXTOID || typoid == VARCHAROID);
            let val = self.value(0);
            val.into_datum()
        }
    }
}
