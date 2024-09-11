use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, Int64Array, ListArray},
    datatypes::FieldRef,
};
use arrow_schema::DataType;

use crate::arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray};

use super::PgToArrowPerAttributeContext;

// Int64
impl PgTypeToArrowArray<i64> for Option<i64> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let int64_array = Int64Array::from(vec![self]);
        (context.field, Arc::new(int64_array))
    }
}

// Int64[]
impl PgTypeToArrowArray<pgrx::Array<'_, i64>> for Option<pgrx::Array<'_, i64>> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let pg_array = if let Some(pg_array) = self {
            pg_array.iter().collect::<Vec<_>>()
        } else {
            vec![]
        };

        let int64_array = Int64Array::from(pg_array);

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
