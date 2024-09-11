use std::sync::Arc;

use arrow::array::{ArrayRef, Float32Array, ListArray};

use crate::arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray};

use super::PgToArrowAttributeContext;

// Float32
impl PgTypeToArrowArray<f32> for Option<f32> {
    fn to_arrow_array(self, _context: &PgToArrowAttributeContext) -> ArrayRef {
        let float_array = Float32Array::from(vec![self]);
        Arc::new(float_array)
    }
}

// Float32[]
impl PgTypeToArrowArray<pgrx::Array<'_, f32>> for Option<pgrx::Array<'_, f32>> {
    fn to_arrow_array(self, context: &PgToArrowAttributeContext) -> ArrayRef {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let pg_array = if let Some(pg_array) = self {
            pg_array.iter().collect::<Vec<_>>()
        } else {
            vec![]
        };

        let float_array = Float32Array::from(pg_array);

        let list_array = ListArray::new(
            context.field.clone(),
            offsets,
            Arc::new(float_array),
            Some(nulls),
        );

        Arc::new(list_array)
    }
}
