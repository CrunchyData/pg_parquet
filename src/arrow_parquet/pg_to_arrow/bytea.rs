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
impl PgTypeToArrowArray<&[u8]> for Vec<Option<Vec<Option<&[u8]>>>> {
    fn to_arrow_array(self, context: &PgToArrowAttributeContext) -> ArrayRef {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let pg_array = self.into_iter().flatten().flatten().collect::<Vec<_>>();

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
