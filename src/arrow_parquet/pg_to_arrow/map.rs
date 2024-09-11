use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, AsArray, ListArray, MapArray},
    datatypes::FieldRef,
};
use arrow_schema::DataType;

use crate::{
    arrow_parquet::{
        arrow_utils::{arrow_array_offsets, arrow_map_offsets},
        pg_to_arrow::PgTypeToArrowArray,
    },
    pgrx_utils::domain_array_base_elem_typoid,
    type_compat::map::CrunchyMap,
};

use super::PgToArrowPerAttributeContext;

// crunchy_map.key_<type1>_val_<type2>
impl<'b> PgTypeToArrowArray<CrunchyMap<'b>> for Vec<Option<CrunchyMap<'b>>> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let (map_offsets, map_nulls) = arrow_map_offsets(&self);

        let map_field = context.field;

        let entries_field = match map_field.data_type() {
            DataType::Map(entries_field, false) => entries_field.clone(),
            _ => panic!("Expected Map field"),
        };

        let mut entries = vec![];

        for map in self {
            if let Some(map) = map {
                entries.push(Some(map.entries));
            } else {
                entries.push(None);
            }
        }

        let entries = entries
            .iter()
            .map(|v| {
                v.as_ref()
                    .map(|pg_array| pg_array.iter().collect::<Vec<_>>())
            })
            .collect::<Vec<_>>();

        let entries = entries.into_iter().flatten().flatten().collect::<Vec<_>>();

        let entries_typoid = domain_array_base_elem_typoid(context.typoid);

        let entries_context = PgToArrowPerAttributeContext::new(
            context.name,
            entries_typoid,
            context.typmod,
            entries_field.clone(),
        );

        let (_, entries_array) = entries.to_arrow_array(entries_context);

        let entries_array = entries_array.as_struct().to_owned();

        let map_array = MapArray::new(
            entries_field,
            map_offsets,
            entries_array,
            Some(map_nulls),
            false,
        );

        (map_field, Arc::new(map_array))
    }
}

// crunchy_map.key_<type1>_val_<type2>[]
impl<'b> PgTypeToArrowArray<pgrx::Array<'_, CrunchyMap<'b>>>
    for Vec<Option<pgrx::Array<'_, CrunchyMap<'b>>>>
{
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef) {
        let pg_array = self
            .iter()
            .map(|v| {
                v.as_ref()
                    .map(|pg_array| pg_array.iter().collect::<Vec<_>>())
            })
            .collect::<Vec<_>>();

        let (list_offsets, list_nulls) = arrow_array_offsets(&pg_array);

        let list_field = context.field;

        let map_field = match list_field.data_type() {
            DataType::List(map_field) => map_field.clone(),
            _ => panic!("Expected List field"),
        };

        let maps = pg_array.into_iter().flatten().flatten().collect::<Vec<_>>();

        let (map_offsets, map_nulls) = arrow_map_offsets(&maps);

        let entries_field = match map_field.data_type() {
            DataType::Map(entries_field, false) => entries_field.clone(),
            _ => panic!("Expected Map field"),
        };

        let mut entries = vec![];

        for map in maps {
            if let Some(map) = map {
                entries.push(Some(map.entries));
            } else {
                entries.push(None);
            }
        }

        let entries = entries.into_iter().flatten().map(Some).collect::<Vec<_>>();

        let entries_typoid = domain_array_base_elem_typoid(context.typoid);

        let entries_context = PgToArrowPerAttributeContext::new(
            context.name,
            entries_typoid,
            context.typmod,
            entries_field.clone(),
        );

        let (_, entries_array) = entries.to_arrow_array(entries_context);

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

        (list_field, make_array(list_array.into()))
    }
}
