use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, Float64Array, ListArray},
    datatypes::FieldRef,
};
use arrow_schema::DataType;

use crate::arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray};

use super::PgToArrowPerAttributeContext;

// Float64
impl PgTypeToArrowArray<f64> for Option<f64> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let double_array = Float64Array::from(vec![self]);
        (context.field, Arc::new(double_array))
    }
}

// Float64[]
impl PgTypeToArrowArray<pgrx::Array<'_, f64>> for Option<pgrx::Array<'_, f64>> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let pg_array = if let Some(pg_array) = self {
            pg_array.iter().collect::<Vec<_>>()
        } else {
            vec![]
        };

        let double_array = Float64Array::from(pg_array);

        let list_field = context.field;

        match list_field.data_type() {
            DataType::List(double_field) => {
                let list_array = ListArray::new(
                    double_field.clone(),
                    offsets,
                    Arc::new(double_array),
                    Some(nulls),
                );

                (list_field, make_array(list_array.into()))
            }
            _ => panic!("Expected List field"),
        }
    }
}
