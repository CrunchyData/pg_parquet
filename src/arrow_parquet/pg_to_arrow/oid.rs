use std::sync::Arc;

use arrow::array::{ArrayRef, ListArray, UInt32Array};
use pgrx::pg_sys::Oid;

use crate::arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray};

use super::PgToArrowAttributeContext;

// Oid
impl PgTypeToArrowArray<Oid> for Option<Oid> {
    fn to_arrow_array(self, _context: &PgToArrowAttributeContext) -> ArrayRef {
        let oid = self.map(|oid| oid.as_u32());
        let oid_array = UInt32Array::from(vec![oid]);
        Arc::new(oid_array)
    }
}

// Oid[]
impl PgTypeToArrowArray<pgrx::Array<'_, Oid>> for Option<pgrx::Array<'_, Oid>> {
    fn to_arrow_array(self, context: &PgToArrowAttributeContext) -> ArrayRef {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let pg_array = if let Some(pg_array) = self {
            pg_array
                .iter()
                .map(|oid| oid.map(|oid| oid.as_u32()))
                .collect::<Vec<_>>()
        } else {
            vec![]
        };

        let oid_array = UInt32Array::from(pg_array);

        let list_array = ListArray::new(
            context.field.clone(),
            offsets,
            Arc::new(oid_array),
            Some(nulls),
        );

        Arc::new(list_array)
    }
}
