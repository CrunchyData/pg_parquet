use arrow::array::{Array, Decimal128Array};
use pgrx::AnyNumeric;

use crate::type_compat::pg_arrow_type_conversions::i128_to_numeric;

use super::{ArrowArrayToPgType, ArrowToPgPerAttributeContext};

// Numeric
impl ArrowArrayToPgType<'_, Decimal128Array, AnyNumeric> for AnyNumeric {
    fn to_pg_type(
        arr: Decimal128Array,
        context: ArrowToPgPerAttributeContext<'_>,
    ) -> Option<AnyNumeric> {
        if arr.is_null(0) {
            None
        } else {
            let scale = context.scale.expect("Expected scale");
            Some(i128_to_numeric(arr.value(0), scale))
        }
    }
}

// Numeric[]
impl ArrowArrayToPgType<'_, Decimal128Array, Vec<Option<AnyNumeric>>> for Vec<Option<AnyNumeric>> {
    fn to_pg_type(
        arr: Decimal128Array,
        context: ArrowToPgPerAttributeContext<'_>,
    ) -> Option<Vec<Option<AnyNumeric>>> {
        let scale = context.scale.expect("Expected scale");
        let mut vals = vec![];
        for val in arr.iter() {
            let val = val.map(|v| i128_to_numeric(v, scale));
            vals.push(val);
        }
        Some(vals)
    }
}
