use arrow::array::{Array, Decimal128Array};
use pgrx::{pg_sys::Oid, AnyNumeric, PgTupleDesc};

use crate::type_compat::pg_arrow_type_conversions::{
    extract_scale_from_numeric_typmod, i128_to_numeric,
};

use super::ArrowArrayToPgType;

// Numeric
impl ArrowArrayToPgType<'_, Decimal128Array, AnyNumeric> for AnyNumeric {
    fn to_pg_type(
        arr: Decimal128Array,
        _typoid: Oid,
        typmod: i32,
        _tupledesc: Option<PgTupleDesc<'_>>,
    ) -> Option<AnyNumeric> {
        if arr.is_null(0) {
            None
        } else {
            let scale = extract_scale_from_numeric_typmod(typmod);
            let val = arr.value(0);
            let val = i128_to_numeric(val, scale).unwrap();
            Some(val)
        }
    }
}

// Numeric[]
impl ArrowArrayToPgType<'_, Decimal128Array, Vec<Option<AnyNumeric>>> for Vec<Option<AnyNumeric>> {
    fn to_pg_type(
        arr: Decimal128Array,
        _typoid: Oid,
        typmod: i32,
        _tupledesc: Option<PgTupleDesc<'_>>,
    ) -> Option<Vec<Option<AnyNumeric>>> {
        let scale = extract_scale_from_numeric_typmod(typmod);
        let mut vals = vec![];
        for val in arr.iter() {
            let val = val.and_then(|v| i128_to_numeric(v, scale));
            vals.push(val);
        }
        Some(vals)
    }
}
