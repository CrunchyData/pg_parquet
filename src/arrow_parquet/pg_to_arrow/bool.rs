use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, BooleanArray, ListArray},
    datatypes::FieldRef,
};
use arrow_schema::DataType;

use crate::arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray};

use super::PgToArrowContext;

// Bool
impl PgTypeToArrowArray<bool> for Vec<Option<bool>> {
    fn to_arrow_array(self, context: PgToArrowContext) -> (FieldRef, ArrayRef) {
        let bool_array = BooleanArray::from(self);
        (context.field, Arc::new(bool_array))
    }
}

// Bool[]
impl PgTypeToArrowArray<Vec<Option<bool>>> for Vec<Option<Vec<Option<bool>>>> {
    fn to_arrow_array(self, context: PgToArrowContext) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let bools = self.into_iter().flatten().flatten().collect::<Vec<_>>();
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
