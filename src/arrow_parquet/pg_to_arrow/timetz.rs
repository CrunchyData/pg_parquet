use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, ListArray, Time64MicrosecondArray},
    datatypes::FieldRef,
};
use arrow_schema::DataType;
use pgrx::datum::TimeWithTimeZone;

use crate::{
    arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray},
    type_compat::pg_arrow_type_conversions::timetz_to_i64,
};

use super::PgToArrowPerAttributeContext;

// TimeTz
impl PgTypeToArrowArray<TimeWithTimeZone> for Vec<Option<TimeWithTimeZone>> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let timetzs = self
            .into_iter()
            .map(|timetz| timetz.and_then(timetz_to_i64))
            .collect::<Vec<_>>();

        let timetz_array = Time64MicrosecondArray::from(timetzs);

        (context.field, Arc::new(timetz_array))
    }
}

// TimeTz[]
impl PgTypeToArrowArray<pgrx::Array<'_, TimeWithTimeZone>>
    for Vec<Option<pgrx::Array<'_, TimeWithTimeZone>>>
{
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let pg_array = self
            .into_iter()
            .map(|v| v.map(|pg_array| pg_array.iter().collect::<Vec<_>>()))
            .collect::<Vec<_>>();

        let (offsets, nulls) = arrow_array_offsets(&pg_array);

        let timetzs = pg_array
            .into_iter()
            .flatten()
            .flatten()
            .map(|timetz| timetz.and_then(timetz_to_i64))
            .collect::<Vec<_>>();

        let timetz_array = Time64MicrosecondArray::from(timetzs);

        let list_field = context.field;

        match list_field.data_type() {
            DataType::List(timetz_field) => {
                let list_array = ListArray::new(
                    timetz_field.clone(),
                    offsets,
                    Arc::new(timetz_array),
                    Some(nulls),
                );

                (list_field, make_array(list_array.into()))
            }
            _ => panic!("Expected List field"),
        }
    }
}
