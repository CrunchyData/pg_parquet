use arrow::{
    array::{make_array, ArrayRef, ListArray, StructArray},
    buffer::{BooleanBuffer, NullBuffer},
    datatypes::FieldRef,
};
use arrow_schema::DataType;
use pgrx::{heap_tuple::PgHeapTuple, AllocatedByRust};

use crate::{
    arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray},
    pgrx_utils::collect_valid_attributes,
};

use super::{collect_attribute_array_from_tuples, PgToArrowContext};

// PgHeapTuple
impl PgTypeToArrowArray<PgHeapTuple<'_, AllocatedByRust>>
    for Vec<Option<PgHeapTuple<'_, AllocatedByRust>>>
{
    fn to_arrow_array(self, context: PgToArrowContext) -> (FieldRef, ArrayRef) {
        let struct_field = context.field;

        let fields = match struct_field.data_type() {
            DataType::Struct(fields) => fields.clone(),
            _ => panic!("Expected Struct field"),
        };

        let tupledesc = context.tupledesc.expect("Expected tuple descriptor");

        let include_generated_columns = true;
        let attributes = collect_valid_attributes(&tupledesc, include_generated_columns);

        let mut tuples = self;

        let mut struct_attribute_arrays = vec![];

        for attribute in attributes {
            let attribute_name = attribute.name();
            let attribute_typoid = attribute.type_oid().value();
            let attribute_typmod = attribute.type_mod();

            let attribute_field = fields
                .iter()
                .find(|field| field.name() == attribute_name)
                .expect("Expected attribute field");

            let (_, array, tups) = collect_attribute_array_from_tuples(
                tuples,
                tupledesc.clone(),
                attribute_name,
                attribute_typoid,
                attribute_typmod,
                attribute_field.clone(),
            );

            tuples = tups;
            struct_attribute_arrays.push(array);
        }

        // determines which structs in the array are null
        let is_null_buffer =
            BooleanBuffer::collect_bool(tuples.len(), |idx| tuples.get(idx).unwrap().is_some());
        let struct_null_buffer = NullBuffer::new(is_null_buffer);

        let struct_array =
            StructArray::new(fields, struct_attribute_arrays, Some(struct_null_buffer));

        (struct_field, make_array(struct_array.into()))
    }
}

// PgHeapTuple[]
impl PgTypeToArrowArray<Vec<Option<PgHeapTuple<'_, AllocatedByRust>>>>
    for Vec<Option<Vec<Option<PgHeapTuple<'_, AllocatedByRust>>>>>
{
    fn to_arrow_array(self, context: PgToArrowContext) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let list_field = context.field;

        let struct_field = match list_field.data_type() {
            DataType::List(struct_field) => struct_field.clone(),
            _ => panic!("Expected List field"),
        };

        let tuples = self.into_iter().flatten().flatten().collect::<Vec<_>>();

        let tuples_context = PgToArrowContext::new(
            context.name,
            context.typoid,
            context.typmod,
            struct_field.clone(),
        )
        .with_tupledesc(context.tupledesc.expect("Expected tuple descriptor"));

        let (struct_field, struct_array) = tuples.to_arrow_array(tuples_context);

        let list_array = ListArray::new(struct_field, offsets, struct_array, Some(nulls));

        (list_field, make_array(list_array.into()))
    }
}
