use std::sync::Arc;

use arrow::array::{new_empty_array, ArrayRef, ListArray, StructArray};
use arrow_schema::DataType;
use pgrx::{heap_tuple::PgHeapTuple, AllocatedByRust};

use crate::arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray};

use super::{to_arrow_array, PgToArrowAttributeContext};

// PgHeapTuple
impl PgTypeToArrowArray<PgHeapTuple<'_, AllocatedByRust>>
    for Option<PgHeapTuple<'_, AllocatedByRust>>
{
    fn to_arrow_array(self, context: &PgToArrowAttributeContext) -> ArrayRef {
        let struct_field = context.field.clone();

        let fields = match struct_field.data_type() {
            DataType::Struct(fields) => fields.clone(),
            _ => panic!("Expected Struct field"),
        };

        if let Some(tuple) = &self {
            let mut attribute_arrow_arrays = vec![];

            let attribute_contexts = context.attribute_contexts.as_ref().unwrap();

            for attribute_context in attribute_contexts {
                let array = to_arrow_array(tuple, attribute_context);

                attribute_arrow_arrays.push(array);
            }

            let struct_array = StructArray::new(fields.clone(), attribute_arrow_arrays, None);

            Arc::new(struct_array)
        } else {
            Arc::new(StructArray::new_null(fields.clone(), 1))
        }
    }
}

// PgHeapTuple[]
impl PgTypeToArrowArray<pgrx::Array<'_, PgHeapTuple<'_, AllocatedByRust>>>
    for Option<pgrx::Array<'_, PgHeapTuple<'_, AllocatedByRust>>>
{
    fn to_arrow_array(self, context: &PgToArrowAttributeContext) -> ArrayRef {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let pg_array = if let Some(pg_array) = &self {
            pg_array.iter().collect::<Vec<_>>()
        } else {
            vec![]
        };

        // collect struct arrays
        let mut struct_arrays = vec![];

        for tuple in pg_array {
            let struct_array = tuple.to_arrow_array(context);
            struct_arrays.push(struct_array);
        }

        let struct_arrays = struct_arrays
            .iter()
            .map(|array| array.as_ref())
            .collect::<Vec<_>>();

        let struct_array = if struct_arrays.is_empty() {
            new_empty_array(context.field.data_type())
        } else {
            // concatenate struct arrays
            arrow::compute::concat(&struct_arrays).unwrap()
        };

        let list_array = ListArray::new(context.field.clone(), offsets, struct_array, Some(nulls));

        Arc::new(list_array)
    }
}
