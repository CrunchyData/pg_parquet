use std::sync::Arc;

use arrow::{
    array::{ArrayRef, AsArray, MapArray},
    datatypes::{DataType, Field, FieldRef},
};
use pgrx::pg_sys::Oid;

use crate::{
    arrow_parquet::{
        arrow_utils::{
            arrow_array_offsets, arrow_map_offsets, create_arrow_list_array, to_not_nullable_field,
        },
        pg_to_arrow::PgTypeToArrowArray,
    },
    pgrx_utils::domain_array_base_elem_typoid,
    type_compat::map::PGMap,
};

// crunchy_map.key_<type1>_val_<type2>
impl<'a> PgTypeToArrowArray<PGMap<'a>> for Vec<Option<PGMap<'a>>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_map_offsets(&self);

        let mut entries = vec![];

        for map in self {
            if let Some(map) = map {
                entries.push(map.entries);
            } else {
                entries.push(vec![]);
            }
        }

        let entries = entries.into_iter().flatten().map(Some).collect::<Vec<_>>();

        let base_elem_typoid = domain_array_base_elem_typoid(typoid);

        let (entries_field, entries_array) = entries.to_arrow_array(name, base_elem_typoid, typmod);

        let entries_field = to_not_nullable_field(entries_field);

        let entries_array = entries_array.as_struct().to_owned();

        let array = MapArray::new(
            entries_field.clone(),
            offsets,
            entries_array,
            Some(nulls),
            false,
        );

        let map_field = Field::new(name, DataType::Map(entries_field, false), true);

        (map_field.into(), Arc::new(array))
    }
}

// crunchy_map.key_<type1>_val_<type2>[]
impl<'a> PgTypeToArrowArray<Vec<Option<PGMap<'a>>>> for Vec<Option<Vec<Option<PGMap<'a>>>>> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let (list_offsets, list_nulls) = arrow_array_offsets(&self);

        let map_array = self.into_iter().flatten().flatten().collect::<Vec<_>>();

        let (map_offsets, map_nulls) = arrow_map_offsets(&map_array);

        let mut entries = vec![];

        for map in map_array {
            if let Some(map) = map {
                entries.push(map.entries);
            } else {
                entries.push(vec![]);
            }
        }

        let entries = entries.into_iter().flatten().map(Some).collect::<Vec<_>>();

        let base_elem_typoid = domain_array_base_elem_typoid(typoid);

        let (entries_field, entries_array) = entries.to_arrow_array(name, base_elem_typoid, typmod);

        let entries_field = to_not_nullable_field(entries_field);

        let entries_array = entries_array.as_struct().to_owned();

        let map_field = Arc::new(Field::new(
            name,
            DataType::Map(entries_field.clone(), false),
            true,
        ));

        let map_array = MapArray::new(
            entries_field,
            map_offsets,
            entries_array,
            Some(map_nulls),
            false,
        );
        let map_array = Arc::new(map_array);

        create_arrow_list_array(name, map_field, map_array, list_offsets, list_nulls)
    }
}
