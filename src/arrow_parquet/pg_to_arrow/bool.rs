use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, BooleanArray, ListArray},
    datatypes::FieldRef,
};
use arrow_schema::DataType;

use crate::arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray};

use super::PgToArrowPerAttributeContext;

// Bool
impl PgTypeToArrowArray<bool> for Option<bool> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let bool_array = BooleanArray::from(vec![self]);
        (context.field, Arc::new(bool_array))
    }
}

// Bool[]
impl<'a> PgTypeToArrowArray<pgrx::Array<'a, bool>> for Option<pgrx::Array<'a, bool>> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let pg_array = if let Some(pg_array) = self {
            pg_array.iter().collect::<Vec<_>>()
        } else {
            vec![]
        };

        let bool_array = BooleanArray::from(pg_array);

        let list_field = context.field;

        match list_field.data_type() {
            DataType::List(bool_field) => {
                let list_array = ListArray::new(
                    bool_field.clone(),
                    offsets,
                    Arc::new(bool_array),
                    Some(nulls),
                );

                (list_field, make_array(list_array.into()))
            }
            _ => panic!("Expected List field"),
        }
    }
}
