use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, FixedSizeBinaryArray, ListArray},
    datatypes::FieldRef,
};
use arrow_schema::DataType;
use pgrx::Uuid;

use crate::arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray};

use super::PgTypeToArrowContext;

// Uuid
impl PgTypeToArrowArray<Uuid> for Vec<Option<Uuid>> {
    fn to_arrow_array(self, context: PgTypeToArrowContext) -> (FieldRef, ArrayRef) {
        let uuids = self
            .iter()
            .map(|uuid| uuid.map(|uuid| uuid.to_vec()))
            .collect::<Vec<_>>();
        let uuids = uuids
            .iter()
            .map(|uuid| uuid.as_ref().map(|uuid| uuid.as_slice()))
            .collect::<Vec<_>>();

        let uuid_array = FixedSizeBinaryArray::from(uuids);

        (context.field, Arc::new(uuid_array))
    }
}

// Uuid[]
impl PgTypeToArrowArray<Vec<Option<Uuid>>> for Vec<Option<Vec<Option<Uuid>>>> {
    fn to_arrow_array(self, context: PgTypeToArrowContext) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let uuids = self
            .into_iter()
            .flatten()
            .flatten()
            .map(|uuid| uuid.map(|uuid| uuid.to_vec()))
            .collect::<Vec<_>>();

        let uuids = uuids
            .iter()
            .map(|uuid| uuid.as_ref().map(|uuid| uuid.as_slice()))
            .collect::<Vec<_>>();

        let uuid_array = FixedSizeBinaryArray::from(uuids);

        let list_field = context.field;

        match list_field.data_type() {
            DataType::List(uuid_field) => {
                let list_array = ListArray::new(
                    uuid_field.clone(),
                    offsets,
                    Arc::new(uuid_array),
                    Some(nulls),
                );

                (list_field, make_array(list_array.into()))
            }
            _ => panic!("Expected List field"),
        }
    }
}
