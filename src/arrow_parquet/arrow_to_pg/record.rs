use arrow::array::{Array, StructArray};
use pgrx::{prelude::PgHeapTuple, AllocatedByRust};

use crate::pgrx_utils::collect_valid_attributes;

use super::{to_pg_datum, ArrowArrayToPgType, ArrowToPgPerAttributeContext};

// PgHeapTuple
impl<'a> ArrowArrayToPgType<'a, StructArray, PgHeapTuple<'a, AllocatedByRust>>
    for PgHeapTuple<'a, AllocatedByRust>
{
    fn to_pg_type(
        arr: StructArray,
        context: ArrowToPgPerAttributeContext<'a>,
    ) -> Option<PgHeapTuple<'a, AllocatedByRust>> {
        if arr.is_null(0) {
            return None;
        }

        let tupledesc = context.tupledesc.expect("Expected tupledesc");

        let mut datums = vec![];

        let include_generated_columns = false;
        let attributes = collect_valid_attributes(&tupledesc, include_generated_columns);

        for attribute in attributes {
            let name = attribute.name();
            let typoid = attribute.type_oid().value();
            let typmod = attribute.type_mod();

            let column_data = arr.column_by_name(name).unwrap();

            let datum = to_pg_datum(
                column_data.into_data(),
                typoid,
                typmod,
            );
            datums.push(datum);
        }

        Some(unsafe { PgHeapTuple::from_datums(tupledesc, datums) }.unwrap())
    }
}

// PgHeapTuple[]
impl<'a> ArrowArrayToPgType<'a, StructArray, Vec<Option<PgHeapTuple<'a, AllocatedByRust>>>>
    for Vec<Option<PgHeapTuple<'a, AllocatedByRust>>>
{
    fn to_pg_type(
        arr: StructArray,
        context: ArrowToPgPerAttributeContext<'a>,
    ) -> Option<Vec<Option<PgHeapTuple<'a, AllocatedByRust>>>> {
        let len = arr.len();
        let mut values = Vec::with_capacity(len);

        for i in 0..len {
            let tuple = arr.slice(i, 1);

            let tuple = <PgHeapTuple<AllocatedByRust> as ArrowArrayToPgType<
                StructArray,
                PgHeapTuple<AllocatedByRust>,
            >>::to_pg_type(tuple, context.clone());

            values.push(tuple);
        }

        Some(values)
    }
}
