use std::sync::Arc;

use arrow::{
    array::{ArrayRef, Decimal128Array},
    datatypes::{DataType, Field, FieldRef},
};
use pgrx::{pg_sys::Oid, AnyNumeric};

use crate::{
    arrow_parquet::{
        pg_to_arrow::PgTypeToArrowArray,
        utils::{arrow_array_offsets, create_arrow_list_array, create_arrow_null_list_array},
    },
    type_compat::numeric_to_fixed,
};

// Numeric
impl PgTypeToArrowArray<AnyNumeric> for Vec<Option<AnyNumeric>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let numeric_array = self
            .into_iter()
            .map(|numeric| numeric.and_then(numeric_to_fixed))
            .collect::<Vec<_>>();
        let field = Field::new(name, DataType::Decimal128(30, 8), true);
        let array = Decimal128Array::from(numeric_array)
            .with_precision_and_scale(30, 8)
            .unwrap();
        (Arc::new(field), Arc::new(array))
    }
}

// Int64[]
impl PgTypeToArrowArray<Vec<Option<AnyNumeric>>> for Vec<Option<Vec<Option<AnyNumeric>>>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, all_nulls) = arrow_array_offsets(&self);

        let field = Field::new(name, DataType::Decimal128(30, 8), true);

        if all_nulls {
            return create_arrow_null_list_array(name, &field, self.len());
        }

        let array = self
            .into_iter()
            .flatten()
            .flatten()
            .map(|time| time.and_then(numeric_to_fixed))
            .collect::<Vec<_>>();
        let array = Decimal128Array::from(array)
            .with_precision_and_scale(30, 8)
            .unwrap();
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        create_arrow_list_array(name, field, primitive_array, offsets)
    }
}
