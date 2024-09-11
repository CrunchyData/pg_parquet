use std::sync::Arc;

use arrow::array::{ArrayRef, Date32Array, ListArray};
use pgrx::datum::Date;

use crate::{
    arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray},
    type_compat::pg_arrow_type_conversions::date_to_i32,
};

use super::PgToArrowAttributeContext;

// Date
impl PgTypeToArrowArray<Date> for Option<Date> {
    fn to_arrow_array(self, _context: &PgToArrowAttributeContext) -> ArrayRef {
        let date = self.map(date_to_i32);
        let date_array = Date32Array::from(vec![date]);
        Arc::new(date_array)
    }
}

// Date[]
impl PgTypeToArrowArray<pgrx::Array<'_, Date>> for Option<pgrx::Array<'_, Date>> {
    fn to_arrow_array(self, context: &PgToArrowAttributeContext) -> ArrayRef {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let pg_array = if let Some(pg_array) = self {
            pg_array
                .iter()
                .map(|date| date.map(date_to_i32))
                .collect::<Vec<_>>()
        } else {
            vec![]
        };

        let date_array = Date32Array::from(pg_array);

        let list_array = ListArray::new(
            context.field.clone(),
            offsets,
            Arc::new(date_array),
            Some(nulls),
        );

        Arc::new(list_array)
    }
}
