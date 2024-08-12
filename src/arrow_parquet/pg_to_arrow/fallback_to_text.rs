use std::sync::Arc;

use arrow::{
    array::{ArrayRef, StringArray},
    datatypes::{DataType, Field, FieldRef},
};
use pgrx::pg_sys::Oid;

use crate::{
    arrow_parquet::{
        arrow_utils::{arrow_array_offsets, create_arrow_list_array},
        pg_to_arrow::PgTypeToArrowArray,
    },
    type_compat::FallbackToText,
};

// Text representation of any type
impl PgTypeToArrowArray<FallbackToText> for Vec<Option<FallbackToText>> {
    fn to_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let field = Field::new(name, DataType::Utf8, true);
        let array = self
            .into_iter()
            .map(|val| val.map(String::from))
            .collect::<Vec<_>>();
        let array = StringArray::from(array);
        (Arc::new(field), Arc::new(array))
    }
}

// Text[] representation of any type
impl PgTypeToArrowArray<Vec<Option<FallbackToText>>> for Vec<Option<Vec<Option<FallbackToText>>>> {
    fn to_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let field = Field::new(name, DataType::Utf8, true);

        let array = self.into_iter().flatten().flatten().collect::<Vec<_>>();
        let array = array
            .into_iter()
            .map(|val| val.map(String::from))
            .collect::<Vec<_>>();

        let array = StringArray::from(array);
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        create_arrow_list_array(name, field, primitive_array, offsets, nulls)
    }
}