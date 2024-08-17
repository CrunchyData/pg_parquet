use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, FixedSizeBinaryArray, ListArray},
    datatypes::FieldRef,
};
use pgrx::{pg_sys::Oid, Uuid};

use crate::arrow_parquet::{
    arrow_utils::arrow_array_offsets,
    pg_to_arrow::PgTypeToArrowArray,
    schema_visitor::{visit_list_schema, visit_primitive_schema},
};

// Uuid
impl PgTypeToArrowArray<Uuid> for Vec<Option<Uuid>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let uuid_field = visit_primitive_schema(typoid, typmod, name);

        let uuids = self
            .iter()
            .map(|uuid| uuid.map(|uuid| uuid.to_vec()))
            .collect::<Vec<_>>();
        let uuids = uuids
            .iter()
            .map(|uuid| uuid.as_ref().map(|uuid| uuid.as_slice()))
            .collect::<Vec<_>>();

        let uuid_array = FixedSizeBinaryArray::from(uuids);

        (uuid_field, Arc::new(uuid_array))
    }
}

// Uuid[]
impl PgTypeToArrowArray<Vec<Option<Uuid>>> for Vec<Option<Vec<Option<Uuid>>>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let uuid_field = visit_primitive_schema(typoid, typmod, name);

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

        let list_field = visit_list_schema(typoid, typmod, name);
        let list_array = ListArray::new(uuid_field, offsets, Arc::new(uuid_array), Some(nulls));
        (list_field, make_array(list_array.into()))
    }
}
