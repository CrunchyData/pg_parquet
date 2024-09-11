use arrow::{
    array::{make_array, new_empty_array, ArrayRef, ListArray, StructArray},
    datatypes::FieldRef,
};
use arrow_schema::DataType;
use pgrx::{heap_tuple::PgHeapTuple, AllocatedByRust};

use crate::{
    arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray},
    pgrx_utils::{collect_valid_attributes, tuple_desc},
};

use super::{to_arrow_array, PgToArrowPerAttributeContext};

// PgHeapTuple
impl PgTypeToArrowArray<PgHeapTuple<'_, AllocatedByRust>>
    for Option<PgHeapTuple<'_, AllocatedByRust>>
{
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let struct_field = context.field;

        let fields = match struct_field.data_type() {
            DataType::Struct(fields) => fields.clone(),
            _ => panic!("Expected Struct field"),
        };

        let tupledesc = tuple_desc(context.typoid, context.typmod);

        let include_generated_columns = true;
        let attributes = collect_valid_attributes(&tupledesc, include_generated_columns);

        if let Some(tuple) = &self {
            let mut attribute_arrow_arrays = vec![];

            for attribute in attributes {
                let attribute_name = attribute.name();
                let attribute_typoid = attribute.type_oid().value();
                let attribute_typmod = attribute.type_mod();

                let attribute_field = fields
                    .iter()
                    .find(|field| field.name() == attribute_name)
                    .expect("Expected attribute field");

                let (_field, array) = to_arrow_array(
                    tuple,
                    attribute_name,
                    attribute_typoid,
                    attribute_typmod,
                    attribute_field.clone(),
                );

                attribute_arrow_arrays.push(array);
            }

            let struct_array = StructArray::new(fields.clone(), attribute_arrow_arrays, None);

            (struct_field, make_array(struct_array.into()))
        } else {
            let null_array = make_array(StructArray::new_null(fields.clone(), 1).into());
            (struct_field, null_array)
        }
    }
}

// PgHeapTuple[]
impl PgTypeToArrowArray<pgrx::Array<'_, PgHeapTuple<'_, AllocatedByRust>>>
    for Option<pgrx::Array<'_, PgHeapTuple<'_, AllocatedByRust>>>
{
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let pg_array = if let Some(pg_array) = &self {
            pg_array.iter().collect::<Vec<_>>()
        } else {
            vec![]
        };

        let list_field = context.field;

        let struct_field = match list_field.data_type() {
            DataType::List(struct_field) => struct_field.clone(),
            _ => panic!("Expected List field"),
        };

        let tuples_context = PgToArrowPerAttributeContext::new(
            context.name,
            context.typoid,
            context.typmod,
            struct_field.clone(),
        );

        // collect struct arrays
        let mut struct_arrays = vec![];

        for tuple in pg_array {
            let (_, struct_array) = tuple.to_arrow_array(tuples_context.clone());
            struct_arrays.push(struct_array);
        }

        let struct_arrays = struct_arrays
            .iter()
            .map(|array| array.as_ref())
            .collect::<Vec<_>>();

        let struct_array = if struct_arrays.is_empty() {
            new_empty_array(struct_field.data_type())
        } else {
            // concatenate struct arrays
            arrow::compute::concat(&struct_arrays).unwrap()
        };

        let list_array = ListArray::new(struct_field, offsets, struct_array, Some(nulls));

        (list_field, make_array(list_array.into()))
    }
}
