use std::sync::Arc;

use arrow::array::{new_empty_array, ArrayRef, AsArray, ListArray, MapArray};
use arrow_schema::DataType;

use crate::{
    arrow_parquet::{
        arrow_utils::{arrow_array_offsets, arrow_map_offsets},
        pg_to_arrow::PgTypeToArrowArray,
    },
    type_compat::map::CrunchyMap,
};

use super::PgToArrowAttributeContext;

// crunchy_map.key_<type1>_val_<type2>
impl<'b> PgTypeToArrowArray<CrunchyMap<'b>> for Option<CrunchyMap<'b>> {
    fn to_arrow_array(self, context: &PgToArrowAttributeContext) -> ArrayRef {
        let maps = vec![self];

        let (map_offsets, map_nulls) = arrow_map_offsets(&maps);

        let map_field = context.field.clone();

        let entries_field = match map_field.data_type() {
            DataType::Map(entries_field, false) => entries_field.clone(),
            _ => panic!("Expected Map field"),
        };

        let map = maps.into_iter().flatten().next();

        let entries = if let Some(map) = map {
            Some(map.entries)
        } else {
            None
        };

        let mut entries_context = context.clone();
        entries_context.field = entries_field.clone();

        let entries_array = if let Some(entries) = entries {
            let mut entries_arrays = vec![];

            for entries in entries.iter() {
                let entries_array = entries.to_arrow_array(&entries_context);
                entries_arrays.push(entries_array);
            }

            let entries_arrays = entries_arrays
                .iter()
                .map(|array| array.as_ref())
                .collect::<Vec<_>>();

            if entries_arrays.is_empty() {
                new_empty_array(entries_field.data_type())
            } else {
                // concatenate entries arrays
                arrow::compute::concat(&entries_arrays).unwrap()
            }
        } else {
            new_empty_array(entries_field.data_type())
        };

        let entries_array = entries_array.as_struct().to_owned();

        let map_array = MapArray::new(
            entries_field,
            map_offsets,
            entries_array,
            Some(map_nulls),
            false,
        );

        Arc::new(map_array)
    }
}

// crunchy_map.key_<type1>_val_<type2>[]
impl<'b> PgTypeToArrowArray<pgrx::Array<'_, CrunchyMap<'b>>>
    for Option<pgrx::Array<'_, CrunchyMap<'b>>>
{
    fn to_arrow_array(self, context: &PgToArrowAttributeContext) -> ArrayRef {
        let (list_offsets, list_nulls) = arrow_array_offsets(&self);

        let pg_array = if let Some(pg_array) = &self {
            pg_array.iter().collect::<Vec<_>>()
        } else {
            vec![]
        };

        let map_field = context.field.clone();

        let (map_offsets, map_nulls) = arrow_map_offsets(&pg_array);

        let entries_field = match map_field.data_type() {
            DataType::Map(entries_field, false) => entries_field.clone(),
            _ => panic!("Expected Map field"),
        };

        let mut entries = vec![];

        for map in &pg_array {
            if let Some(map) = &map {
                entries.push(Some(map.entries.iter().collect::<Vec<_>>()));
            } else {
                entries.push(None);
            }
        }

        let entries = entries.into_iter().flatten().flatten().collect::<Vec<_>>();

        let mut entries_context = context.clone();
        entries_context.field = entries_field.clone();

        // collect entries arrays
        let mut entries_arrays = vec![];

        for entries in entries {
            let entries_array = entries.to_arrow_array(&entries_context);
            entries_arrays.push(entries_array);
        }

        let entries_arrays = entries_arrays
            .iter()
            .map(|array| array.as_ref())
            .collect::<Vec<_>>();

        let entries_array = if entries_arrays.is_empty() {
            new_empty_array(entries_field.data_type())
        } else {
            // concatenate entries arrays
            arrow::compute::concat(&entries_arrays).unwrap()
        };

        let entries_array = entries_array.as_struct().to_owned();

        let map_array = MapArray::new(
            entries_field,
            map_offsets,
            entries_array,
            Some(map_nulls),
            false,
        );

        let list_array = ListArray::new(
            map_field,
            list_offsets,
            Arc::new(map_array),
            Some(list_nulls),
        );

        Arc::new(list_array)
    }
}
