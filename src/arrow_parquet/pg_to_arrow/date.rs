use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, Date32Array, ListArray},
    datatypes::FieldRef,
};
use arrow_schema::DataType;
use pgrx::Date;

use crate::{
    arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray},
    type_compat::pg_arrow_type_conversions::date_to_i32,
};

use super::PgToArrowPerAttributeContext;

// Date
impl PgTypeToArrowArray<Date> for Vec<Option<Date>> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let dates = self
            .into_iter()
            .map(|date| date.and_then(date_to_i32))
            .collect::<Vec<_>>();

        let date_array = Date32Array::from(dates);

        (context.field, Arc::new(date_array))
    }
}

// Date[]
impl PgTypeToArrowArray<Vec<Option<Date>>> for Vec<Option<Vec<Option<Date>>>> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let dates = self
            .into_iter()
            .flatten()
            .flatten()
            .map(|date| date.and_then(date_to_i32))
            .collect::<Vec<_>>();

        let date_array = Date32Array::from(dates);

        let list_field = context.field;

        match list_field.data_type() {
            DataType::List(date_field) => {
                let list_array = ListArray::new(
                    date_field.clone(),
                    offsets,
                    Arc::new(date_array),
                    Some(nulls),
                );

                (list_field, make_array(list_array.into()))
            }
            _ => panic!("Expected List field"),
        }
    }
}
