use std::sync::Arc;

use arrow::array::{ArrayRef, ListArray, UInt32Array};
use pgrx::pg_sys::Oid;

use crate::arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray};

use super::PgToArrowAttributeContext;

// Oid
impl PgTypeToArrowArray<Oid> for Vec<Option<Oid>> {
    fn to_arrow_array(self, _context: &PgToArrowAttributeContext) -> ArrayRef {
        let oids = self
            .into_iter()
            .map(|oid| oid.map(|oid| oid.as_u32()))
            .collect::<Vec<_>>();
        let oid_array = UInt32Array::from(oids);
        Arc::new(oid_array)
    }
}

// Oid[]
impl PgTypeToArrowArray<pgrx::Array<'_, Oid>> for Vec<Option<pgrx::Array<'_, Oid>>> {
    fn to_arrow_array(self, context: &PgToArrowAttributeContext) -> ArrayRef {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let pg_array = self
            .into_iter()
            .flatten()
            .flat_map(|pg_array| pg_array.iter().collect::<Vec<_>>())
            .map(|oid| oid.map(|oid| oid.as_u32()))
            .collect::<Vec<_>>();

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
