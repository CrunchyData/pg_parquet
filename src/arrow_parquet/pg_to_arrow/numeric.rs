use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, Decimal128Array, ListArray},
    datatypes::FieldRef,
};
use pgrx::{pg_sys::Oid, AnyNumeric};

use crate::{
    arrow_parquet::{
        arrow_utils::arrow_array_offsets,
        pg_to_arrow::PgTypeToArrowArray,
        schema_visitor::{visit_list_schema, visit_primitive_schema},
    },
    type_compat::pg_arrow_type_conversions::{
        extract_precision_from_numeric_typmod, extract_scale_from_numeric_typmod, numeric_to_i128,
    },
};

// Numeric
impl PgTypeToArrowArray<AnyNumeric> for Vec<Option<AnyNumeric>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let numeric_field = visit_primitive_schema(typoid, typmod, name);

        let precision = extract_precision_from_numeric_typmod(typmod);
        let scale = extract_scale_from_numeric_typmod(typmod);

        let numerics = self
            .into_iter()
            .map(|numeric| numeric.and_then(|v| numeric_to_i128(v, scale)))
            .collect::<Vec<_>>();

        let numeric_array = Decimal128Array::from(numerics)
            .with_precision_and_scale(precision as _, scale as _)
            .unwrap();

        (numeric_field, Arc::new(numeric_array))
    }
}

// Int64[]
impl PgTypeToArrowArray<Vec<Option<AnyNumeric>>> for Vec<Option<Vec<Option<AnyNumeric>>>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let numeric_field = visit_primitive_schema(typoid, typmod, name);

        let precision = extract_precision_from_numeric_typmod(typmod);
        let scale = extract_scale_from_numeric_typmod(typmod);

        let numerics = self
            .into_iter()
            .flatten()
            .flatten()
            .map(|time| time.and_then(|v| numeric_to_i128(v, scale)))
            .collect::<Vec<_>>();

        let numeric_array = Decimal128Array::from(numerics)
            .with_precision_and_scale(precision as _, scale as _)
            .unwrap();

        let list_field = visit_list_schema(typoid, typmod, name);
        let list_array =
            ListArray::new(numeric_field, offsets, Arc::new(numeric_array), Some(nulls));
        (list_field, make_array(list_array.into()))
    }
}
