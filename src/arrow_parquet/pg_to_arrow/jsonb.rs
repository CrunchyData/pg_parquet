use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, ListArray, StringArray},
    datatypes::FieldRef,
};
use pgrx::{pg_sys::Oid, JsonB};

use crate::arrow_parquet::{
    arrow_utils::arrow_array_offsets,
    pg_to_arrow::PgTypeToArrowArray,
    schema_visitor::{visit_list_schema, visit_primitive_schema},
};

// Jsonb
impl PgTypeToArrowArray<JsonB> for Vec<Option<JsonB>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let jsonb_field = visit_primitive_schema(typoid, typmod, name);

        let jsonbs = self
            .into_iter()
            .map(|val| val.map(|val| serde_json::to_string(&val.0).unwrap()))
            .collect::<Vec<_>>();

        let jsonb_array = StringArray::from(jsonbs);

        (jsonb_field, Arc::new(jsonb_array))
    }
}

// Jsonb[]
impl PgTypeToArrowArray<Vec<Option<JsonB>>> for Vec<Option<Vec<Option<JsonB>>>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let jsonb_field = visit_primitive_schema(typoid, typmod, name);

        let jsonbs = self
            .into_iter()
            .flatten()
            .flatten()
            .map(|val| val.map(|val| serde_json::to_string(&val.0).unwrap()))
            .collect::<Vec<_>>();

        let jsonb_array = StringArray::from(jsonbs);

        let list_field = visit_list_schema(typoid, typmod, name);
        let list_array = ListArray::new(jsonb_field, offsets, Arc::new(jsonb_array), Some(nulls));
        (list_field, make_array(list_array.into()))
    }
}
