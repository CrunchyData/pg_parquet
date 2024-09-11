use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, BinaryArray, ListArray},
    datatypes::FieldRef,
};
use arrow_schema::DataType;

use crate::arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray};

use super::PgToArrowPerAttributeContext;

// Bytea
impl PgTypeToArrowArray<&[u8]> for Option<&[u8]> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let byte_array = BinaryArray::from(vec![self]);
        (context.field, Arc::new(byte_array))
    }
}

// Bytea[]
impl PgTypeToArrowArray<pgrx::Array<'_, &[u8]>> for Option<pgrx::Array<'_, &[u8]>> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let pg_array = if let Some(pg_array) = &self {
            pg_array.iter().collect::<Vec<_>>()
        } else {
            vec![]
        };

        let bytea_array = BinaryArray::from(pg_array);

        let list_field = context.field;

        match list_field.data_type() {
            DataType::List(bytea_field) => {
                let list_array = ListArray::new(
                    bytea_field.clone(),
                    offsets,
                    Arc::new(bytea_array),
                    Some(nulls),
                );

                (list_field, make_array(list_array.into()))
            }
            _ => panic!("Expected List field"),
        }
    }
}
