use arrow::array::{Array, MapArray, StructArray};
use pgrx::{pg_sys::Oid, prelude::PgHeapTuple, AllocatedByRust, PgTupleDesc};

use crate::{
    pgrx_utils::{domain_array_base_elem_typoid, tuple_desc},
    type_compat::map::PGMap,
};

use super::ArrowArrayToPgType;

// crunchy_map.key_<type1>_val_<type2>
impl<'b, 'a: 'b> ArrowArrayToPgType<'b, MapArray, PGMap<'a>> for PGMap<'a> {
    fn to_pg_type(
        arr: MapArray,
        typoid: Oid,
        typmod: i32,
        _tupledesc: Option<PgTupleDesc<'b>>,
    ) -> Option<PGMap<'a>> {
        if arr.is_null(0) {
            None
        } else {
            let entries_array = arr.value(0);

            let base_elem_typoid = domain_array_base_elem_typoid(typoid);

            let tupledesc = tuple_desc(base_elem_typoid, typmod);

            let entries = <Vec<Option<PgHeapTuple<AllocatedByRust>>> as ArrowArrayToPgType<
                StructArray,
                Vec<Option<PgHeapTuple<AllocatedByRust>>>,
            >>::to_pg_type(
                entries_array, base_elem_typoid, typmod, Some(tupledesc)
            );

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
        typoid: Oid,
        typmod: i32,
        _tupledesc: Option<PgTupleDesc<'b>>,
    ) -> Option<Vec<Option<PGMap<'a>>>> {
        let mut maps = vec![];

        let base_elem_typoid = domain_array_base_elem_typoid(typoid);

        let tupledesc = tuple_desc(base_elem_typoid, typmod);

        for entries_array in array.iter() {
            if let Some(entries_array) = entries_array {
                let entries = <Vec<Option<PgHeapTuple<AllocatedByRust>>> as ArrowArrayToPgType<
                    StructArray,
                    Vec<Option<PgHeapTuple<AllocatedByRust>>>,
                >>::to_pg_type(
                    entries_array, typoid, typmod, Some(tupledesc.clone())
                );

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
