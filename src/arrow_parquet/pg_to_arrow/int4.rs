use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, Int32Array, ListArray},
    datatypes::FieldRef,
};
use arrow_schema::DataType;

use crate::arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray};

use super::PgToArrowPerAttributeContext;

// Int32
impl PgTypeToArrowArray<i32> for Option<i32> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let int32_array = Int32Array::from(vec![self]);
        (context.field, Arc::new(int32_array))
    }
}

// Int32[]
impl PgTypeToArrowArray<pgrx::Array<'_, i32>> for Option<pgrx::Array<'_, i32>> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let pg_array = if let Some(pg_array) = self {
            pg_array.iter().collect::<Vec<_>>()
        } else {
            vec![]
        };

        let int32_array = Int32Array::from(pg_array);

        let list_field = context.field;

        match list_field.data_type() {
            DataType::List(int32_field) => {
                let list_array = ListArray::new(
                    int32_field.clone(),
                    offsets,
                    Arc::new(int32_array),
                    Some(nulls),
                );

                (list_field, make_array(list_array.into()))
            }
            _ => panic!("Expected List field"),
        }
    }
}
