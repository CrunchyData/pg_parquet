use std::sync::Arc;

use arrow::{
    array::{ArrayRef, FixedSizeBinaryArray},
    datatypes::{DataType, Field, FieldRef},
};
use arrow_schema::ExtensionType;
use pgrx::{pg_sys::Oid, Uuid};

use crate::arrow_parquet::{
    pg_to_arrow::PgTypeToArrowArray,
    utils::{arrow_array_offsets, create_arrow_list_array},
};

// Uuid
impl PgTypeToArrowArray<Uuid> for Vec<Option<Uuid>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let uuid_array = self
            .iter()
            .map(|uuid| uuid.map(|uuid| uuid.to_vec()))
            .collect::<Vec<_>>();

        let uuid_array = uuid_array
            .iter()
            .map(|uuid| uuid.as_ref().map(|uuid| uuid.as_slice()))
            .collect::<Vec<_>>();

        let field = Field::new(name, DataType::FixedSizeBinary(16), true)
            .with_extension_type(ExtensionType::Uuid);

        let array = FixedSizeBinaryArray::from(uuid_array);
        (Arc::new(field), Arc::new(array))
    }
}

// Uuid[]
impl PgTypeToArrowArray<Vec<Option<Uuid>>> for Vec<Option<Vec<Option<Uuid>>>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let field = Field::new(name, DataType::FixedSizeBinary(16), true)
            .with_extension_type(ExtensionType::Uuid);

        let uuid_array = self
            .into_iter()
            .flatten()
            .flatten()
            .map(|uuid| uuid.map(|uuid| uuid.to_vec()))
            .collect::<Vec<_>>();

        let uuid_array = uuid_array
            .iter()
            .map(|uuid| uuid.as_ref().map(|uuid| uuid.as_slice()))
            .collect::<Vec<_>>();

        let array = FixedSizeBinaryArray::from(uuid_array);
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        create_arrow_list_array(name, field, primitive_array, offsets, nulls)
    }
}
