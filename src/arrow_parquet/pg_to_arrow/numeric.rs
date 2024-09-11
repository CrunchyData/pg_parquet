use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, Decimal128Array, ListArray},
    datatypes::FieldRef,
};
use arrow_schema::DataType;
use pgrx::AnyNumeric;

use crate::{
    arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray},
    type_compat::pg_arrow_type_conversions::{
        extract_precision_from_numeric_typmod, extract_scale_from_numeric_typmod, numeric_to_i128,
    },
};

use super::PgToArrowPerAttributeContext;

// Numeric
impl PgTypeToArrowArray<AnyNumeric> for Vec<Option<AnyNumeric>> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let precision = extract_precision_from_numeric_typmod(context.typmod);
        let scale = extract_scale_from_numeric_typmod(context.typmod);

        let numerics = self
            .into_iter()
            .map(|numeric| numeric.and_then(numeric_to_i128))
            .collect::<Vec<_>>();

        let numeric_array = Decimal128Array::from(numerics)
            .with_precision_and_scale(precision as _, scale as _)
            .unwrap();

        (context.field, Arc::new(numeric_array))
    }
}

// Numeric[]
impl PgTypeToArrowArray<pgrx::Array<'_, AnyNumeric>> for Vec<Option<pgrx::Array<'_, AnyNumeric>>> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let pg_array = self
            .into_iter()
            .map(|v| v.map(|pg_array| pg_array.iter().collect::<Vec<_>>()))
            .collect::<Vec<_>>();

        let (offsets, nulls) = arrow_array_offsets(&pg_array);

        let precision = extract_precision_from_numeric_typmod(context.typmod);
        let scale = extract_scale_from_numeric_typmod(context.typmod);

        let numerics = pg_array
            .into_iter()
            .flatten()
            .flatten()
            .map(|time| time.and_then(numeric_to_i128))
            .collect::<Vec<_>>();

        let numeric_array = Decimal128Array::from(numerics)
            .with_precision_and_scale(precision as _, scale as _)
            .unwrap();

        let list_field = context.field;

        match list_field.data_type() {
            DataType::List(numeric_field) => {
                let list_array = ListArray::new(
                    numeric_field.clone(),
                    offsets,
                    Arc::new(numeric_array),
                    Some(nulls),
                );

                (list_field, make_array(list_array.into()))
            }
            _ => panic!("Expected List field"),
        }
    }
}
