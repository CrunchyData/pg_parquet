use std::{ops::Deref, sync::Arc};

use arrow::{
    buffer::{NullBuffer, OffsetBuffer, ScalarBuffer},
    datatypes::{Field, FieldRef},
};

use crate::type_compat::map::CrunchyMap;

pub(crate) fn to_not_nullable_field(field: FieldRef) -> FieldRef {
    let name = field.deref().name();
    let data_type = field.deref().data_type();
    let metadata = field.deref().metadata().clone();

    let field = Field::new(name, data_type.clone(), false).with_metadata(metadata);
    Arc::new(field)
}

pub(crate) fn arrow_map_offsets(maps: &Vec<Option<CrunchyMap>>) -> (OffsetBuffer<i32>, NullBuffer) {
    let mut offsets = vec![0];
    let mut nulls = vec![];

    for map in maps {
        if let Some(map) = map {
            let len = map.entries.len() as i32;
            offsets.push(offsets.last().unwrap() + len);
            nulls.push(true);
        } else {
            offsets.push(*offsets.last().unwrap());
            nulls.push(false);
        }
    }

    let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets));
    let nulls = NullBuffer::from(nulls);

    (offsets, nulls)
}

pub(crate) fn arrow_array_offsets<T>(
    pg_array: &Option<pgrx::Array<T>>,
) -> (OffsetBuffer<i32>, NullBuffer) {
    let mut nulls = vec![];
    let mut offsets = vec![0];

    if let Some(pg_array) = pg_array {
        let len = pg_array.len() as i32;
        offsets.push(len);
        nulls.push(true);
    } else {
        offsets.push(0);
        nulls.push(false);
    };

    let offsets = arrow::buffer::OffsetBuffer::new(arrow::buffer::ScalarBuffer::from(offsets));
    let nulls = arrow::buffer::NullBuffer::from(nulls);

    (offsets, nulls)
}
