use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, ListArray, UInt32Array},
    datatypes::FieldRef,
};
use arrow_schema::DataType;
use pgrx::pg_sys::Oid;

use crate::arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray};

use super::PgToArrowContext;

// Oid
impl PgTypeToArrowArray<Oid> for Vec<Option<Oid>> {
    fn to_arrow_array(self, context: PgToArrowContext) -> (FieldRef, ArrayRef) {
        let oids = self
            .into_iter()
            .map(|x| x.map(|x| x.as_u32()))
            .collect::<Vec<_>>();

        let oid_array = UInt32Array::from(oids);

        (context.field, Arc::new(oid_array))
    }
}

// Oid[]
impl PgTypeToArrowArray<Vec<Option<Oid>>> for Vec<Option<Vec<Option<Oid>>>> {
    fn to_arrow_array(self, context: PgToArrowContext) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let oids = self.into_iter().flatten().flatten().collect::<Vec<_>>();
        let oids = oids
            .into_iter()
            .map(|x| x.map(|x| x.as_u32()))
            .collect::<Vec<_>>();

        let oid_array = UInt32Array::from(oids);

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
