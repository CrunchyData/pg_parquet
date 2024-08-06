use std::sync::Arc;

use arrow::{
    array::{ArrayRef, StringArray},
    datatypes::{DataType, Field, FieldRef},
};
use pgrx::pg_sys::Oid;

use crate::{
    arrow_parquet::{
        pg_to_arrow::PgTypeToArrowArray,
        utils::{arrow_array_offsets, create_arrow_list_array},
    },
    type_compat::Bpchar,
};

// Bpchar
impl PgTypeToArrowArray<Bpchar> for Vec<Option<Bpchar>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let field = Field::new(name, DataType::Utf8, true);
        let array = self.into_iter().map(|x| x.map(|x| x.0)).collect::<Vec<_>>();
        let array = StringArray::from(array);
        (Arc::new(field), Arc::new(array))
    }
}

// Bpchar[]
impl PgTypeToArrowArray<Vec<Option<Bpchar>>> for Vec<Option<Vec<Option<Bpchar>>>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let field = Field::new(name, DataType::Utf8, true);

        let array = self
            .into_iter()
            .flatten()
            .flatten()
            .map(|x| x.and_then(|x| Some(x.0)))
            .collect::<Vec<_>>();
        let array = StringArray::from(array);
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        create_arrow_list_array(name, field, primitive_array, offsets, nulls)
    }
}
