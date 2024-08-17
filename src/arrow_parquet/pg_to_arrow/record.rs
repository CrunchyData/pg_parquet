use arrow::{
    array::{make_array, ArrayRef, ListArray, StructArray},
    buffer::{BooleanBuffer, NullBuffer},
    datatypes::{FieldRef, Fields},
};
use pgrx::{heap_tuple::PgHeapTuple, pg_sys::Oid, AllocatedByRust};

use crate::{
    arrow_parquet::{
        arrow_utils::arrow_array_offsets,
        pg_to_arrow::PgTypeToArrowArray,
        schema_visitor::{visit_list_schema, visit_struct_schema},
    },
    pgrx_utils::{collect_valid_attributes, tuple_desc},
};

use super::collect_attribute_array_from_tuples;

// PgHeapTuple
impl PgTypeToArrowArray<PgHeapTuple<'_, AllocatedByRust>>
    for Vec<Option<PgHeapTuple<'_, AllocatedByRust>>>
{
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let mut struct_attribute_arrays = vec![];
        let mut struct_attribute_fields = vec![];

        let tupledesc = tuple_desc(typoid, typmod);

        let include_generated_columns = true;
        let attributes = collect_valid_attributes(&tupledesc, include_generated_columns);

        let mut tuples = self;

        for attribute in attributes {
            let attribute_name = attribute.name();
            let attribute_typoid = attribute.type_oid().value();
            let attribute_typmod = attribute.type_mod();

            let (field, array, tups) = collect_attribute_array_from_tuples(
                tuples,
                tupledesc.clone(),
                attribute_name,
                attribute_typoid,
                attribute_typmod,
            );

            tuples = tups;
            struct_attribute_fields.push(field);
            struct_attribute_arrays.push(array);
        }

        let struct_field = visit_struct_schema(tupledesc, name);

        // determines which structs in the array are null
        let is_null_buffer =
            BooleanBuffer::collect_bool(tuples.len(), |idx| tuples.get(idx).unwrap().is_some());
        let struct_null_buffer = NullBuffer::new(is_null_buffer);

        let struct_array = StructArray::new(
            Fields::from(struct_attribute_fields),
            struct_attribute_arrays,
            Some(struct_null_buffer),
        );
        let struct_array = make_array(struct_array.into());

        (struct_field, struct_array)
    }
}

// PgHeapTuple[]
impl PgTypeToArrowArray<Vec<Option<PgHeapTuple<'_, AllocatedByRust>>>>
    for Vec<Option<Vec<Option<PgHeapTuple<'_, AllocatedByRust>>>>>
{
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let tuples = self.into_iter().flatten().flatten().collect::<Vec<_>>();
        let (struct_field, struct_array) = tuples.to_arrow_array(name, typoid, typmod);

        let list_field = visit_list_schema(typoid, typmod, name);
        let list_array = ListArray::new(struct_field, offsets, struct_array, Some(nulls));
        (list_field, make_array(list_array.into()))
    }
}
