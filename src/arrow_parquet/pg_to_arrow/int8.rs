use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, Int64Array, ListArray},
    datatypes::FieldRef,
};
use arrow_schema::DataType;

use crate::arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray};

use super::PgTypeToArrowContext;

// Int64
impl PgTypeToArrowArray<i64> for Vec<Option<i64>> {
    fn to_arrow_array(self, context: PgTypeToArrowContext) -> (FieldRef, ArrayRef) {
        let int64_array = Int64Array::from(self);
        (context.field, Arc::new(int64_array))
    }
}

// Int64[]
impl PgTypeToArrowArray<Vec<Option<i64>>> for Vec<Option<Vec<Option<i64>>>> {
    fn to_arrow_array(self, context: PgTypeToArrowContext) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let int64s = self.into_iter().flatten().flatten().collect::<Vec<_>>();

        let int64_array = Int64Array::from(int64s);

        let list_field = context.field;

        match list_field.data_type() {
            DataType::List(int64_field) => {
                let list_array = ListArray::new(
                    int64_field.clone(),
                    offsets,
                    Arc::new(int64_array),
                    Some(nulls),
                );

                (list_field, make_array(list_array.into()))
            }
            _ => panic!("Expected List field"),
        }
    }
}
