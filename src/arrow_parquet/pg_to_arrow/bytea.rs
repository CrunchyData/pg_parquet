use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryArray, ListArray};

use crate::arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray};

use super::PgToArrowAttributeContext;

// Bytea
impl PgTypeToArrowArray<&[u8]> for Vec<Option<&[u8]>> {
    fn to_arrow_array(self, _context: &PgToArrowAttributeContext) -> ArrayRef {
        let byte_array = BinaryArray::from(self);
        Arc::new(byte_array)
    }
}

// Bytea[]
impl PgTypeToArrowArray<pgrx::Array<'_, &[u8]>> for Vec<Option<pgrx::Array<'_, &[u8]>>> {
    fn to_arrow_array(self, context: &PgToArrowAttributeContext) -> ArrayRef {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let pg_array = self
            .iter()
            .flatten()
            .flat_map(|pg_array| pg_array.iter().collect::<Vec<_>>())
            .collect::<Vec<_>>();

        let bytea_array = BinaryArray::from(pg_array);

        let list_array = ListArray::new(
            context.field.clone(),
            offsets,
            Arc::new(bytea_array),
            Some(nulls),
        );

        Arc::new(list_array)
    }
}
