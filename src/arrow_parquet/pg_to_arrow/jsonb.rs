use std::sync::Arc;

use arrow::{
    array::{ArrayRef, StringArray},
    datatypes::{DataType, Field, FieldRef},
};
use arrow_schema::ExtensionType;
use pgrx::{pg_sys::Oid, JsonB};

use crate::arrow_parquet::{
    arrow_utils::{arrow_array_offsets, create_arrow_list_array},
    pg_to_arrow::PgTypeToArrowArray,
};

// Jsonb
impl PgTypeToArrowArray<JsonB> for Vec<Option<JsonB>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let field = Field::new(name, DataType::Utf8, true).with_extension_type(ExtensionType::Json);

        let array = self
            .into_iter()
            .map(|val| val.map(|val| serde_json::to_string(&val.0).unwrap()))
            .collect::<Vec<_>>();

        let array = StringArray::from(array);
        (Arc::new(field), Arc::new(array))
    }
}

// Jsonb[]
impl PgTypeToArrowArray<Vec<Option<JsonB>>> for Vec<Option<Vec<Option<JsonB>>>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let field = Field::new(name, DataType::Utf8, true).with_extension_type(ExtensionType::Json);

        let array = self
            .into_iter()
            .flatten()
            .flatten()
            .map(|val| val.map(|val| serde_json::to_string(&val.0).unwrap()))
            .collect::<Vec<_>>();

        let array = StringArray::from(array);
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        create_arrow_list_array(name, field, primitive_array, offsets, nulls)
    }
}
