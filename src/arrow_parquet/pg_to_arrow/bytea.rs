use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, BinaryArray, ListArray},
    datatypes::FieldRef,
};
use arrow_schema::DataType;

use crate::arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray};

use super::PgToArrowPerAttributeContext;

// Bytea
impl PgTypeToArrowArray<&[u8]> for Vec<Option<&[u8]>> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let byte_array = BinaryArray::from(self);
        (context.field, Arc::new(byte_array))
    }
}

// Bytea[]
impl PgTypeToArrowArray<Vec<Option<&[u8]>>> for Vec<Option<Vec<Option<&[u8]>>>> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let byteas = self.into_iter().flatten().flatten().collect::<Vec<_>>();
        let bytea_array = BinaryArray::from(byteas);

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
