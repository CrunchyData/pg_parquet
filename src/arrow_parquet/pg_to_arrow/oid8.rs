use std::sync::Arc;

use arrow::array::{ArrayRef, ListArray, UInt64Array};

use crate::{
    arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray},
    type_compat::oid8::Oid8,
};

use super::PgToArrowAttributeContext;

// Oid8
impl PgTypeToArrowArray<Oid8> for Vec<Option<Oid8>> {
    fn to_arrow_array(self, _context: &PgToArrowAttributeContext) -> ArrayRef {
        let oids = self
            .into_iter()
            .map(|oid| oid.map(|oid| oid.0))
            .collect::<Vec<_>>();
        let oid_array = UInt64Array::from(oids);
        Arc::new(oid_array)
    }
}

// Oid8[]
impl PgTypeToArrowArray<Oid8> for Vec<Option<Vec<Option<Oid8>>>> {
    fn to_arrow_array(self, element_context: &PgToArrowAttributeContext) -> ArrayRef {
        let (offsets, nulls) = arrow_array_offsets(&self);

        // gets rid of the first level of Option, then flattens the inner Vec<Option<Oid8>>.
        let pg_array = self
            .into_iter()
            .flatten()
            .flatten()
            .map(|oid| oid.map(|oid| oid.0))
            .collect::<Vec<_>>();

        let oid_array = UInt64Array::from(pg_array);

        let list_array = ListArray::new(
            element_context.field(),
            offsets,
            Arc::new(oid_array),
            Some(nulls),
        );

        Arc::new(list_array)
    }
}
