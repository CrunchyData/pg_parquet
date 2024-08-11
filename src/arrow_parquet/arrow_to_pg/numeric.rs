use arrow::array::{Array, Decimal128Array};
use pgrx::{pg_sys::Oid, AnyNumeric, PgTupleDesc};

use crate::type_compat::i128_to_numeric;

use super::ArrowArrayToPgType;

// Numeric
impl<'a> ArrowArrayToPgType<'_, Decimal128Array, AnyNumeric> for AnyNumeric {
    fn as_pg(
        arr: Decimal128Array,
        _typoid: Oid,
        _typmod: i32,
        _tupledesc: Option<PgTupleDesc<'_>>,
    ) -> Option<AnyNumeric> {
        if arr.is_null(0) {
            None
        } else {
            let val = arr.value(0);
            let val = i128_to_numeric(val).unwrap();
            Some(val)
        }
    }
}

// Numeric[]
impl<'a> ArrowArrayToPgType<'_, Decimal128Array, Vec<Option<AnyNumeric>>>
    for Vec<Option<AnyNumeric>>
{
    fn as_pg(
        arr: Decimal128Array,
        _typoid: Oid,
        _typmod: i32,
        _tupledesc: Option<PgTupleDesc<'_>>,
    ) -> Option<Vec<Option<AnyNumeric>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            let val = val.and_then(i128_to_numeric);
            vals.push(val);
        }
        Some(vals)
    }
}
