use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, Date32Array, ListArray},
    datatypes::FieldRef,
};
use arrow_schema::DataType;
use pgrx::datum::Date;

use crate::{
    arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray},
    type_compat::pg_arrow_type_conversions::date_to_i32,
};

use super::PgToArrowPerAttributeContext;

// Date
impl PgTypeToArrowArray<Date> for Option<Date> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let date = self.map(date_to_i32);

        let date_array = Date32Array::from(vec![date]);

        (context.field, Arc::new(date_array))
    }
}

// Date[]
impl PgTypeToArrowArray<pgrx::Array<'_, Date>> for Option<pgrx::Array<'_, Date>> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let pg_array = if let Some(pg_array) = self {
            pg_array
                .iter()
                .map(|date| date.map(date_to_i32))
                .collect::<Vec<_>>()
        } else {
            vec![]
        };

        let date_array = Date32Array::from(pg_array);

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
