use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, BooleanArray, ListArray},
    datatypes::FieldRef,
};
use arrow_schema::DataType;

use crate::arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray};

use super::PgToArrowPerAttributeContext;

// Bool
impl PgTypeToArrowArray<bool> for Vec<Option<bool>> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let bool_array = BooleanArray::from(self);
        (context.field, Arc::new(bool_array))
    }
}

// Bool[]
impl PgTypeToArrowArray<pgrx::Array<'_, bool>> for Vec<Option<pgrx::Array<'_, bool>>> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let pg_array = self
            .into_iter()
            .map(|v| v.map(|pg_array| pg_array.iter().collect::<Vec<_>>()))
            .collect::<Vec<_>>();

        let (offsets, nulls) = arrow_array_offsets(&pg_array);

        let bools = pg_array.into_iter().flatten().flatten().collect::<Vec<_>>();
        let bool_array = BooleanArray::from(bools);

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
