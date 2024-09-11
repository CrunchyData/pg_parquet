use std::sync::Arc;

use arrow::array::{ArrayRef, Decimal128Array, ListArray};
use pgrx::AnyNumeric;

use crate::{
    arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray},
    type_compat::pg_arrow_type_conversions::numeric_to_i128,
};

use super::PgToArrowAttributeContext;

// Numeric
impl PgTypeToArrowArray<AnyNumeric> for Option<AnyNumeric> {
    fn to_arrow_array(self, context: &PgToArrowAttributeContext) -> ArrayRef {
        let precision = context.precision.unwrap();
        let scale = context.scale.unwrap();

        let numeric = self.map(numeric_to_i128);

        let numeric_array = Decimal128Array::from(vec![numeric])
            .with_precision_and_scale(precision as _, scale as _)
            .unwrap();

        Arc::new(numeric_array)
    }
}

// Numeric[]
impl PgTypeToArrowArray<pgrx::Array<'_, AnyNumeric>> for Option<pgrx::Array<'_, AnyNumeric>> {
    fn to_arrow_array(self, context: &PgToArrowAttributeContext) -> ArrayRef {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let pg_array = if let Some(pg_array) = self {
            pg_array
                .iter()
                .map(|numeric| numeric.map(numeric_to_i128))
                .collect::<Vec<_>>()
        } else {
            vec![]
        };

        let precision = context.precision.unwrap();
        let scale = context.scale.unwrap();

        let numeric_array = Decimal128Array::from(pg_array)
            .with_precision_and_scale(precision as _, scale as _)
            .unwrap();

        let list_array = ListArray::new(
            context.field.clone(),
            offsets,
            Arc::new(numeric_array),
            Some(nulls),
        );

        Arc::new(list_array)
    }
}
