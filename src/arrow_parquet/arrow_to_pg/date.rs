use arrow::array::{Array, Date32Array};
use pgrx::Date;

use crate::type_compat::pg_arrow_type_conversions::i32_to_date;

use super::{ArrowArrayToPgType, ArrowToPgPerAttributeContext};

// Date
impl ArrowArrayToPgType<'_, Date32Array, Date> for Date {
    fn to_pg_type(arr: Date32Array, _context: ArrowToPgPerAttributeContext<'_>) -> Option<Date> {
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
    fn to_pg_type(arr: Date32Array, _context: ArrowToPgPerAttributeContext<'_>) -> Option<Vec<Option<Date>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            let val = val.and_then(i32_to_date);
            vals.push(val);
        }
        Some(vals)
    }
}
