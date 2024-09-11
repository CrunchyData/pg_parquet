use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, Int32Array, ListArray},
    datatypes::FieldRef,
};
use arrow_schema::DataType;

use crate::arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray};

use super::PgToArrowPerAttributeContext;

// Int32
impl PgTypeToArrowArray<i32> for Vec<Option<i32>> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let int32_array = Int32Array::from(self);
        (context.field, Arc::new(int32_array))
    }
}

// Int32[]
impl PgTypeToArrowArray<pgrx::Array<'_, i32>> for Vec<Option<pgrx::Array<'_, i32>>> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let pg_array = self
            .into_iter()
            .map(|v| v.map(|pg_array| pg_array.iter().collect::<Vec<_>>()))
            .collect::<Vec<_>>();

        let (offsets, nulls) = arrow_array_offsets(&pg_array);

        let int32s = pg_array.into_iter().flatten().flatten().collect::<Vec<_>>();

        let int32_array = Int32Array::from(int32s);

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
