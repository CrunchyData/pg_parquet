use std::{num::NonZeroUsize, sync::Arc};

use arrow::{
    array::{ArrayRef, ListArray, StructArray},
    buffer::{BooleanBuffer, NullBuffer},
};
use arrow_schema::DataType;
use pgrx::{heap_tuple::PgHeapTuple, htup::heap_getattr_raw, pg_sys::Datum, AllocatedByRust};

use crate::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray};

use super::{to_arrow_array, PgToArrowAttributeContext};

// PgHeapTuple
impl PgTypeToArrowArray<PgHeapTuple<'_, AllocatedByRust>>
    for Vec<Option<PgHeapTuple<'_, AllocatedByRust>>>
{
    fn to_arrow_array(self, context: &PgToArrowAttributeContext) -> ArrayRef {
        let struct_field = context.field();

        let fields = match struct_field.data_type() {
            DataType::Struct(fields) => fields.clone(),
            _ => panic!("Expected Struct field"),
        };

        let tupledesc = context.tupledesc();

        // the child arrays are built column by column, so the raw tuple pointers have to stay
        // alive across all of them. they are palloc'd in the current (per batch) memory
        // context, which outlives this call.
        let tuple_ptrs = self
            .into_iter()
            .map(|tuple| tuple.map(|tuple| tuple.into_pg()))
            .collect::<Vec<_>>();

        let mut struct_attribute_arrays = vec![];

        for attribute_context in context.attribute_contexts() {
            let attnum = NonZeroUsize::new(attribute_context.attnum() as usize)
                .expect("invalid attribute number");

            let attribute_datums = tuple_ptrs
                .iter()
                .map(|tuple| match tuple {
                    Some(tuple) => unsafe { heap_getattr_raw(*tuple, attnum, tupledesc.as_ptr()) },
                    None => None,
                })
                .collect::<Vec<Option<Datum>>>();

            let attribute_array = to_arrow_array(&attribute_datums, attribute_context);
            struct_attribute_arrays.push(attribute_array);
        }

        let is_null_buffer = BooleanBuffer::collect_bool(tuple_ptrs.len(), |idx| {
            tuple_ptrs.get(idx).expect("invalid tuple idx").is_some()
        });
        let struct_null_buffer = NullBuffer::new(is_null_buffer);

        let struct_array =
            StructArray::new(fields, struct_attribute_arrays, Some(struct_null_buffer));

        Arc::new(struct_array)
    }
}

// PgHeapTuple[]
impl PgTypeToArrowArray<PgHeapTuple<'_, AllocatedByRust>>
    for Vec<Option<Vec<Option<PgHeapTuple<'_, AllocatedByRust>>>>>
{
    fn to_arrow_array(self, element_context: &PgToArrowAttributeContext) -> ArrayRef {
        let (offsets, nulls) = arrow_array_offsets(&self);

        // gets rid of the first level of Option, then flattens the inner Vec<Option<bool>>.
        let tuples = self.into_iter().flatten().flatten().collect::<Vec<_>>();

        let struct_array = tuples.to_arrow_array(element_context);

        let list_array = ListArray::new(
            element_context.field(),
            offsets,
            Arc::new(struct_array),
            Some(nulls),
        );

        Arc::new(list_array)
    }
}
