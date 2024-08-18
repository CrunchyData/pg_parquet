use arrow::array::{Array, MapArray, StructArray};
use pgrx::{prelude::PgHeapTuple, AllocatedByRust};

use crate::{
    pgrx_utils::{domain_array_base_elem_typoid, tuple_desc},
    type_compat::map::PGMap,
};

use super::{ArrowArrayToPgType, ArrowToPgContext};

// crunchy_map.key_<type1>_val_<type2>
impl<'b, 'a: 'b> ArrowArrayToPgType<'b, MapArray, PGMap<'a>> for PGMap<'a> {
    fn to_pg_type(arr: MapArray, context: ArrowToPgContext<'_>) -> Option<PGMap<'a>> {
        if arr.is_null(0) {
            None
        } else {
            let entries_array = arr.value(0);

            let entries_typoid = domain_array_base_elem_typoid(context.typoid);

            let entries_tupledesc = tuple_desc(entries_typoid, context.typmod);

            let entries_context = ArrowToPgContext::new(entries_typoid, context.typmod)
                .with_tupledesc(entries_tupledesc);

            let entries = <Vec<Option<PgHeapTuple<AllocatedByRust>>> as ArrowArrayToPgType<
                StructArray,
                Vec<Option<PgHeapTuple<AllocatedByRust>>>,
            >>::to_pg_type(entries_array, entries_context);

            if let Some(entries) = entries {
                // entries cannot be null if the map is not null (arrow does not allow it)
                let entries = entries.into_iter().flatten().collect();
                Some(PGMap { entries })
            } else {
                None
            }
        }
    }
}

// crunchy_map.key_<type1>_val_<type2>[]
impl<'b, 'a: 'b> ArrowArrayToPgType<'b, MapArray, Vec<Option<PGMap<'a>>>>
    for Vec<Option<PGMap<'a>>>
{
    fn to_pg_type(
        array: MapArray,
        context: ArrowToPgContext<'_>,
    ) -> Option<Vec<Option<PGMap<'a>>>> {
        let mut maps = vec![];

        let entries_typoid = domain_array_base_elem_typoid(context.typoid);

        let entries_tupledesc = tuple_desc(entries_typoid, context.typmod);

        let entries_context =
            ArrowToPgContext::new(entries_typoid, context.typmod).with_tupledesc(entries_tupledesc);

        for entries_array in array.iter() {
            if let Some(entries_array) = entries_array {
                let entries = <Vec<Option<PgHeapTuple<AllocatedByRust>>> as ArrowArrayToPgType<
                    StructArray,
                    Vec<Option<PgHeapTuple<AllocatedByRust>>>,
                >>::to_pg_type(entries_array, entries_context.clone());

                if let Some(entries) = entries {
                    // entries cannot be null if the map is not null (arrow does not allow it)
                    let entries = entries.into_iter().flatten().collect();
                    maps.push(Some(PGMap { entries }));
                } else {
                    maps.push(None);
                }
            } else {
                maps.push(None);
            }
        }

        Some(maps)
    }
}
