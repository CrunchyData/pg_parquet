use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, ListArray, StringArray},
    datatypes::FieldRef,
};
use pgrx::pg_sys::Oid;

use crate::{
    arrow_parquet::{
        arrow_utils::arrow_array_offsets,
        pg_to_arrow::PgTypeToArrowArray,
        schema_visitor::{visit_list_schema, visit_primitive_schema},
    },
    type_compat::fallback_to_text::FallbackToText,
};

// Text representation of any type
impl PgTypeToArrowArray<FallbackToText> for Vec<Option<FallbackToText>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let text_field = visit_primitive_schema(typoid, typmod, name);

        let texts = self
            .into_iter()
            .map(|val| val.map(String::from))
            .collect::<Vec<_>>();

        let text_array = StringArray::from(texts);

        (text_field, Arc::new(text_array))
    }
}

// Text[] representation of any type
impl PgTypeToArrowArray<Vec<Option<FallbackToText>>> for Vec<Option<Vec<Option<FallbackToText>>>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let text_field = visit_primitive_schema(typoid, typmod, name);

        let texts = self.into_iter().flatten().flatten().collect::<Vec<_>>();

        let texts = texts
            .into_iter()
            .map(|val| val.map(String::from))
            .collect::<Vec<_>>();

        let text_array = StringArray::from(texts);

        let list_field = visit_list_schema(typoid, typmod, name);
        let list_array = ListArray::new(text_field, offsets, Arc::new(text_array), Some(nulls));
        (list_field, make_array(list_array.into()))
    }
}
