use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, Int16Array, ListArray},
    datatypes::FieldRef,
};
use arrow_schema::DataType;

use crate::arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray};

use super::PgToArrowPerAttributeContext;

// Int16
impl PgTypeToArrowArray<i16> for Vec<Option<i16>> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let int16_array = Int16Array::from(self);
        (context.field, Arc::new(int16_array))
    }
}

// Int16[]
impl PgTypeToArrowArray<Vec<Option<i16>>> for Vec<Option<Vec<Option<i16>>>> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let int16s = self.into_iter().flatten().flatten().collect::<Vec<_>>();

        let int16_array = Int16Array::from(int16s);

        let list_field = context.field;

        match list_field.data_type() {
            DataType::List(int16_field) => {
                let list_array = ListArray::new(
                    int16_field.clone(),
                    offsets,
                    Arc::new(int16_array),
                    Some(nulls),
                );

                (list_field, make_array(list_array.into()))
            }
            _ => panic!("Expected List field"),
        }
    }
}
