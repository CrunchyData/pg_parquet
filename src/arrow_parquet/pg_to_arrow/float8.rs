use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, Float64Array, ListArray},
    datatypes::FieldRef,
};
use arrow_schema::DataType;

use crate::arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray};

use super::PgTypeToArrowContext;

// Float64
impl PgTypeToArrowArray<f64> for Vec<Option<f64>> {
    fn to_arrow_array(self, context: PgTypeToArrowContext) -> (FieldRef, ArrayRef) {
        let double_array = Float64Array::from(self);
        (context.field, Arc::new(double_array))
    }
}

// Float64[]
impl PgTypeToArrowArray<Vec<Option<f64>>> for Vec<Option<Vec<Option<f64>>>> {
    fn to_arrow_array(self, context: PgTypeToArrowContext) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let doubles = self.into_iter().flatten().flatten().collect::<Vec<_>>();

        let double_array = Float64Array::from(doubles);

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
