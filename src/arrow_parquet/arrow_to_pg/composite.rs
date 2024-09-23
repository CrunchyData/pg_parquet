use arrow::array::{Array, StructArray};
use pgrx::{prelude::PgHeapTuple, AllocatedByRust};

use super::{to_pg_datum, ArrowArrayToPgType, ArrowToPgAttributeContext};

// PgHeapTuple
impl<'a> ArrowArrayToPgType<StructArray, PgHeapTuple<'a, AllocatedByRust>>
    for PgHeapTuple<'a, AllocatedByRust>
{
    fn to_pg_type(
        arr: StructArray,
        context: &ArrowToPgAttributeContext,
    ) -> Option<PgHeapTuple<'a, AllocatedByRust>> {
        if arr.is_null(0) {
            return None;
        }

        let mut datums = vec![];

        for attribute_context in context
            .attribute_contexts
            .as_ref()
            .expect("each attribute of the tuple should have a context")
        {
            let column_data = arr
                .column_by_name(&attribute_context.name)
                .unwrap_or_else(|| panic!("column {} not found", &attribute_context.name));

            let datum = to_pg_datum(column_data.into_data(), attribute_context);

            datums.push(datum);
        }

        let tupledesc = context
            .attribute_tupledesc
            .as_ref()
            .expect("Expected attribute tupledesc");

        Some(
            unsafe { PgHeapTuple::from_datums(tupledesc.clone(), datums) }.unwrap_or_else(|e| {
                panic!("failed to create heap tuple: {}", e);
            }),
        )
    }
}

// PgHeapTuple[]
impl<'a> ArrowArrayToPgType<StructArray, Vec<Option<PgHeapTuple<'a, AllocatedByRust>>>>
    for Vec<Option<PgHeapTuple<'a, AllocatedByRust>>>
{
    fn to_pg_type(
        arr: StructArray,
        context: &ArrowToPgAttributeContext,
    ) -> Option<Vec<Option<PgHeapTuple<'a, AllocatedByRust>>>> {
        let len = arr.len();
        let mut values = Vec::with_capacity(len);

        for i in 0..len {
            let tuple = arr.slice(i, 1);

            let tuple = <PgHeapTuple<AllocatedByRust> as ArrowArrayToPgType<
                StructArray,
                PgHeapTuple<AllocatedByRust>,
            >>::to_pg_type(tuple, &context.clone());

            values.push(tuple);
        }

        Some(values)
    }
}
