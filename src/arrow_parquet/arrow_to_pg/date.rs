use arrow::array::{Array, Date32Array};
use pgrx::datum::Date;

use crate::type_compat::pg_arrow_type_conversions::i32_to_date;

use super::{ArrowArrayToPgType, ArrowToPgAttributeContext};

// Date
impl ArrowArrayToPgType<Date32Array, Date> for Date {
    fn to_pg_type(arr: Date32Array, _context: &ArrowToPgAttributeContext) -> Option<Date> {
        if arr.is_null(0) {
            None
        } else {
            let val = arr.value(0);
            Some(i32_to_date(val))
        }
    }
}

// Date[]
impl ArrowArrayToPgType<Date32Array, Vec<Option<Date>>> for Vec<Option<Date>> {
    fn to_pg_type(
        arr: Date32Array,
        _context: &ArrowToPgAttributeContext,
    ) -> Option<Vec<Option<Date>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            let val = val.map(i32_to_date);
            vals.push(val);
        }
        Some(vals)
    }
}
