use std::sync::Arc;

use arrow::{
    array::{ArrayRef, Decimal128Array},
    datatypes::{DataType, Field, FieldRef},
};
use pgrx::{pg_sys::Oid, AnyNumeric};

use crate::{
    arrow_parquet::{
        arrow_utils::{arrow_array_offsets, create_arrow_list_array},
        pg_to_arrow::PgTypeToArrowArray,
    },
    type_compat::{
        extract_precision_from_numeric_typmod, extract_scale_from_numeric_typmod, numeric_to_i128,
    },
};

// Numeric
impl PgTypeToArrowArray<AnyNumeric> for Vec<Option<AnyNumeric>> {
    fn to_arrow_array(self, name: &str, _typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let precision = extract_precision_from_numeric_typmod(typmod);
        let scale = extract_scale_from_numeric_typmod(typmod);

        let numeric_array = self
            .into_iter()
            .map(|numeric| numeric.and_then(|v| numeric_to_i128(v, scale)))
            .collect::<Vec<_>>();
        let field = Field::new(name, DataType::Decimal128(precision as _, scale as _), true);
        let array = Decimal128Array::from(numeric_array)
            .with_precision_and_scale(precision as _, scale as _)
            .unwrap();
        (Arc::new(field), Arc::new(array))
    }
}

// Int64[]
impl PgTypeToArrowArray<Vec<Option<AnyNumeric>>> for Vec<Option<Vec<Option<AnyNumeric>>>> {
    fn to_arrow_array(self, name: &str, _typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let precision = extract_precision_from_numeric_typmod(typmod);
        let scale = extract_scale_from_numeric_typmod(typmod);

        let (offsets, nulls) = arrow_array_offsets(&self);

        let field = Field::new(name, DataType::Decimal128(precision as _, scale as _), true);

        let array = self
            .into_iter()
            .flatten()
            .flatten()
            .map(|time| time.and_then(|v| numeric_to_i128(v, scale)))
            .collect::<Vec<_>>();
        let array = Decimal128Array::from(array)
            .with_precision_and_scale(precision as _, scale as _)
            .unwrap();
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        create_arrow_list_array(name, field, primitive_array, offsets, nulls)
    }
}
