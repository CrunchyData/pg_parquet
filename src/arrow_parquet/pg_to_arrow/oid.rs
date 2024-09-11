use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, ListArray, UInt32Array},
    datatypes::FieldRef,
};
use arrow_schema::DataType;
use pgrx::pg_sys::Oid;

use crate::arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray};

use super::PgToArrowPerAttributeContext;

// Oid
impl PgTypeToArrowArray<Oid> for Option<Oid> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let oid = self.map(|oid| oid.as_u32());

        let oid_array = UInt32Array::from(vec![oid]);

        (context.field, Arc::new(oid_array))
    }
}

// Oid[]
impl PgTypeToArrowArray<pgrx::Array<'_, Oid>> for Option<pgrx::Array<'_, Oid>> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let pg_array = if let Some(pg_array) = self {
            pg_array
                .iter()
                .map(|oid| oid.map(|oid| oid.as_u32()))
                .collect::<Vec<_>>()
        } else {
            vec![]
        };

        let oid_array = UInt32Array::from(pg_array);

        let list_field = context.field;

        match list_field.data_type() {
            DataType::List(oid_field) => {
                let list_array =
                    ListArray::new(oid_field.clone(), offsets, Arc::new(oid_array), Some(nulls));

                (list_field, make_array(list_array.into()))
            }
            _ => panic!("Expected List field"),
        }
    }
}
