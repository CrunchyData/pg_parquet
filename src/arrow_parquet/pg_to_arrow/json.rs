use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, ListArray, StringArray},
    datatypes::FieldRef,
};
use pgrx::{pg_sys::Oid, Json};

use crate::arrow_parquet::{
    arrow_utils::arrow_array_offsets,
    pg_to_arrow::PgTypeToArrowArray,
    schema_visitor::{visit_list_schema, visit_primitive_schema},
};

// Json
impl PgTypeToArrowArray<Json> for Vec<Option<Json>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let json_field = visit_primitive_schema(typoid, typmod, name);

        let jsons = self
            .into_iter()
            .map(|val| val.map(|val| serde_json::to_string(&val.0).unwrap()))
            .collect::<Vec<_>>();

        let json_array = StringArray::from(jsons);

        (json_field, Arc::new(json_array))
    }
}

// Json[]
impl PgTypeToArrowArray<Vec<Option<Json>>> for Vec<Option<Vec<Option<Json>>>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let json_field = visit_primitive_schema(typoid, typmod, name);

        let jsons = self
            .into_iter()
            .flatten()
            .flatten()
            .map(|val| val.map(|val| serde_json::to_string(&val.0).unwrap()))
            .collect::<Vec<_>>();

        let json_array = StringArray::from(jsons);

        let list_field = visit_list_schema(typoid, typmod, name);
        let list_array = ListArray::new(json_field, offsets, Arc::new(json_array), Some(nulls));
        (list_field, make_array(list_array.into()))
    }
}
