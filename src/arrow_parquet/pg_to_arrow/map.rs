use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, ListArray, MapArray};
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
impl<'b> PgTypeToArrowArray<CrunchyMap<'b>> for Vec<Option<CrunchyMap<'b>>> {
    fn to_arrow_array(self, context: &PgToArrowAttributeContext) -> ArrayRef {
        let (map_offsets, map_nulls) = arrow_map_offsets(&self);

        let map_field = context.field.clone();

        let entries_field = match map_field.data_type() {
            DataType::Map(entries_field, false) => entries_field.clone(),
            _ => panic!("Expected Map field"),
        };

        let maps = self;

        let mut entries = vec![];

        for map in &maps {
            if let Some(map) = map {
                entries.push(Some(map.entries.iter().collect::<Vec<_>>()));
            } else {
                entries.push(None)
            };
        }

        let entries = entries.into_iter().flatten().flatten().collect::<Vec<_>>();

        let mut entries_context = context.clone();
        entries_context.field = entries_field.clone();

        let entries_array = entries.to_arrow_array(&entries_context);
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
    for Vec<Option<pgrx::Array<'_, CrunchyMap<'b>>>>
{
    fn to_arrow_array(self, context: &PgToArrowAttributeContext) -> ArrayRef {
        let (list_offsets, list_nulls) = arrow_array_offsets(&self);

        let maps = self
            .into_iter()
            .flatten()
            .flat_map(|pg_array| pg_array.iter().collect::<Vec<_>>())
            .collect::<Vec<_>>();

        let map_field = context.field.clone();

        let map_array = maps.to_arrow_array(context);

        let list_array = ListArray::new(map_field, list_offsets, map_array, Some(list_nulls));

        Arc::new(list_array)
    }
}
