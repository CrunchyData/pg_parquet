use arrow::array::{Array, MapArray, StructArray};
use pgrx::{prelude::PgHeapTuple, AllocatedByRust, FromDatum, IntoDatum};

use crate::{
    pgrx_utils::{domain_array_base_elem_typoid, tuple_desc},
    type_compat::map::CrunchyMap,
};

use super::{ArrowArrayToPgType, ArrowToPgPerAttributeContext};

// crunchy_map.key_<type1>_val_<type2>
impl<'b, 'a: 'b> ArrowArrayToPgType<'b, MapArray, CrunchyMap<'a>> for CrunchyMap<'a> {
    fn to_pg_type(
        arr: MapArray,
        context: ArrowToPgPerAttributeContext<'_>,
    ) -> Option<CrunchyMap<'a>> {
        if arr.is_null(0) {
            None
        } else {
            let entries_array = arr.value(0);

            let entries_typoid = domain_array_base_elem_typoid(context.typoid);

            let entries_tupledesc = tuple_desc(entries_typoid, context.typmod);

            let entries_context = ArrowToPgPerAttributeContext::new(entries_typoid, context.typmod)
                .with_tupledesc(entries_tupledesc);

            let entries = <Vec<Option<PgHeapTuple<AllocatedByRust>>> as ArrowArrayToPgType<
                StructArray,
                Vec<Option<PgHeapTuple<AllocatedByRust>>>,
            >>::to_pg_type(entries_array, entries_context);

            if let Some(entries) = entries {
                let entries_datum = entries.into_datum();

                if let Some(entries_datum) = entries_datum {
                    let entries = unsafe { pgrx::Array::from_datum(entries_datum, false).unwrap() };
                    Some(CrunchyMap { entries })
                } else {
                    None
                }
            } else {
                None
            }
        }
    }
}

// crunchy_map.key_<type1>_val_<type2>[]
impl<'b, 'a: 'b> ArrowArrayToPgType<'b, MapArray, Vec<Option<CrunchyMap<'a>>>>
    for Vec<Option<CrunchyMap<'a>>>
{
    fn to_pg_type(
        array: MapArray,
        context: ArrowToPgPerAttributeContext<'_>,
    ) -> Option<Vec<Option<CrunchyMap<'a>>>> {
        let mut maps = vec![];

        let entries_typoid = domain_array_base_elem_typoid(context.typoid);

        let entries_tupledesc = tuple_desc(entries_typoid, context.typmod);

        let entries_context = ArrowToPgPerAttributeContext::new(entries_typoid, context.typmod)
            .with_tupledesc(entries_tupledesc);

        for entries_array in array.iter() {
            if let Some(entries_array) = entries_array {
                let entries = <Vec<Option<PgHeapTuple<AllocatedByRust>>> as ArrowArrayToPgType<
                    StructArray,
                    Vec<Option<PgHeapTuple<AllocatedByRust>>>,
                >>::to_pg_type(entries_array, entries_context.clone());

                if let Some(entries) = entries {
                    let entries_datum = entries.into_datum();

                    if let Some(entries_datum) = entries_datum {
                        let entries =
                            unsafe { pgrx::Array::from_datum(entries_datum, false).unwrap() };
                        maps.push(Some(CrunchyMap { entries }))
                    } else {
                        maps.push(None);
                    }
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
