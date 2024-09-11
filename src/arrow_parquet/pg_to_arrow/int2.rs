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
impl PgTypeToArrowArray<pgrx::Array<'_, i16>> for Vec<Option<pgrx::Array<'_, i16>>> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let pg_array = self
            .into_iter()
            .map(|v| v.map(|pg_array| pg_array.iter().collect::<Vec<_>>()))
            .collect::<Vec<_>>();

        let (offsets, nulls) = arrow_array_offsets(&pg_array);

        let int16s = pg_array.into_iter().flatten().flatten().collect::<Vec<_>>();

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
