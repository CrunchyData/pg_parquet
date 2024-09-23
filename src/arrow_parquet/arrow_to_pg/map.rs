use arrow::array::{Array, MapArray, StructArray};
use pgrx::{prelude::PgHeapTuple, AllocatedByRust, FromDatum, IntoDatum};

use crate::type_compat::map::CrunchyMap;

use super::{ArrowArrayToPgType, ArrowToPgAttributeContext};

// crunchy_map.key_<type1>_val_<type2>
impl<'a> ArrowArrayToPgType<MapArray, CrunchyMap<'a>> for CrunchyMap<'a> {
    fn to_pg_type(arr: MapArray, context: &ArrowToPgAttributeContext) -> Option<CrunchyMap<'a>> {
        if arr.is_null(0) {
            None
        } else {
            let entries_array = arr.value(0);

            let entries = <Vec<Option<PgHeapTuple<AllocatedByRust>>> as ArrowArrayToPgType<
                StructArray,
                Vec<Option<PgHeapTuple<AllocatedByRust>>>,
            >>::to_pg_type(entries_array, context);

            if let Some(entries) = entries {
                let entries_datum = entries.into_datum();

                if let Some(entries_datum) = entries_datum {
                    let entries = unsafe {
                        let is_null = false;
                        pgrx::Array::from_datum(entries_datum, is_null)
                            .expect("map entries should be an array")
                    };
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
impl<'a> ArrowArrayToPgType<MapArray, Vec<Option<CrunchyMap<'a>>>> for Vec<Option<CrunchyMap<'a>>> {
    fn to_pg_type(
        array: MapArray,
        context: &ArrowToPgAttributeContext,
    ) -> Option<Vec<Option<CrunchyMap<'a>>>> {
        let mut maps = vec![];

        for entries_array in array.iter() {
            if let Some(entries_array) = entries_array {
                let entries = <Vec<Option<PgHeapTuple<AllocatedByRust>>> as ArrowArrayToPgType<
                    StructArray,
                    Vec<Option<PgHeapTuple<AllocatedByRust>>>,
                >>::to_pg_type(entries_array, context);

                if let Some(entries) = entries {
                    let entries_datum = entries.into_datum();

                    if let Some(entries_datum) = entries_datum {
                        let entries = unsafe {
                            let is_null = false;
                            pgrx::Array::from_datum(entries_datum, is_null)
                                .expect("map entries should be an array")
                        };
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
