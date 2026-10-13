use arrow::array::{Array, StructArray};
use pgrx::{prelude::PgHeapTuple, AllocatedByRust};

use super::{to_pg_datum, ArrowArrayToPgType, ArrowToPgAttributeContext};

// PgHeapTuple
impl<'a> ArrowArrayToPgType<PgHeapTuple<'a, AllocatedByRust>> for StructArray {
    fn to_pg_type(
        self,
        context: &ArrowToPgAttributeContext,
    ) -> Option<PgHeapTuple<'a, AllocatedByRust>> {
        if self.is_null(0) {
            return None;
        }

        let tupledesc = context.tupledesc();
        let attribute_contexts = context.attribute_contexts();

        // the heap tuple needs one datum per tuple descriptor attribute, and the
        // descriptor still keeps the dropped attributes as null placeholders
        let mut datums = Vec::with_capacity(tupledesc.len());
        let mut attribute_context_idx = 0;

        for attribute_idx in 0..tupledesc.len() {
            let attribute = tupledesc
                .get(attribute_idx)
                .expect("failed to get attribute");

            if attribute.is_dropped() {
                datums.push(None);
                continue;
            }

            let attribute_context = &attribute_contexts[attribute_context_idx];
            attribute_context_idx += 1;

            let column_data = self
                .column_by_name(attribute_context.name())
                .unwrap_or_else(|| panic!("column {} not found", attribute_context.name()));

            let datum = to_pg_datum(column_data.into_data(), attribute_context);

            datums.push(datum);
        }

        Some(
            unsafe { PgHeapTuple::from_datums(tupledesc.clone(), datums) }.unwrap_or_else(|e| {
                panic!("failed to create heap tuple: {e}");
            }),
        )
    }
}

// PgHeapTuple[]
impl<'a> ArrowArrayToPgType<Vec<Option<PgHeapTuple<'a, AllocatedByRust>>>> for StructArray {
    fn to_pg_type(
        self,
        element_context: &ArrowToPgAttributeContext,
    ) -> Option<Vec<Option<PgHeapTuple<'a, AllocatedByRust>>>> {
        let len = self.len();
        let mut values = Vec::with_capacity(len);

        for i in 0..len {
            let tuple = self.slice(i, 1);

            let tuple = tuple.to_pg_type(element_context);

            values.push(tuple);
        }

        Some(values)
    }
}
