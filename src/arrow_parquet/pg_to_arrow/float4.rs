use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, Float32Array, ListArray},
    datatypes::FieldRef,
};
use arrow_schema::DataType;

use crate::arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray};

use super::PgToArrowPerAttributeContext;

// Float32
impl PgTypeToArrowArray<f32> for Vec<Option<f32>> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let float_array = Float32Array::from(self);
        (context.field, Arc::new(float_array))
    }
}

// Float32[]
impl PgTypeToArrowArray<Vec<Option<f32>>> for Vec<Option<Vec<Option<f32>>>> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let floats = self.into_iter().flatten().flatten().collect::<Vec<_>>();
        let float_array = Float32Array::from(floats);

        let list_field = context.field;

        match list_field.data_type() {
            DataType::List(float_field) => {
                let list_array = ListArray::new(
                    float_field.clone(),
                    offsets,
                    Arc::new(float_array),
                    Some(nulls),
                );

                (list_field, make_array(list_array.into()))
            }
            _ => panic!("Expected List field"),
        }
    }
}
