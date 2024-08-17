use std::{ops::Deref, sync::Arc};

use arrow::{
    buffer::{NullBuffer, OffsetBuffer, ScalarBuffer},
    datatypes::{Field, FieldRef},
};

use crate::type_compat::map::PGMap;

pub(crate) fn to_not_nullable_field(field: FieldRef) -> FieldRef {
    let name = field.deref().name();
    let data_type = field.deref().data_type();
    let field = Field::new(name, data_type.clone(), false);
    Arc::new(field)
}

pub(crate) fn arrow_map_offsets(maps: &Vec<Option<PGMap>>) -> (OffsetBuffer<i32>, NullBuffer) {
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
    arrays: &Vec<Option<Vec<Option<T>>>>,
) -> (OffsetBuffer<i32>, NullBuffer) {
    pgrx::pg_sys::check_for_interrupts!();

    let mut nulls = vec![];
    let mut offsets = vec![0];
    let mut current_offset = 0;
    for array in arrays {
        if let Some(array) = array {
            let len = array.len() as i32;
            current_offset += len;
            offsets.push(current_offset);
            nulls.push(true);
        } else {
            offsets.push(current_offset);
            nulls.push(false);
        }
    }

    let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets));
    let nulls = NullBuffer::from(nulls);

    (offsets, nulls)
}
