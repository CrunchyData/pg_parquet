use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, ListArray},
    buffer::{OffsetBuffer, ScalarBuffer},
    datatypes::{DataType, Field, FieldRef},
};

pub(crate) fn create_arrow_null_list_array(
    name: &str,
    field: &Field,
    len: usize,
) -> (FieldRef, ArrayRef) {
    let list_array = ListArray::new_null(field.clone().into(), len);
    let list_array = make_array(list_array.into());
    let list_field = Arc::new(Field::new(name, DataType::List(field.clone().into()), true));
    (list_field, list_array)
}

pub(crate) fn create_arrow_list_array(
    name: &str,
    field: FieldRef,
    array: ArrayRef,
    offsets: OffsetBuffer<i32>,
) -> (FieldRef, ArrayRef) {
    let list_array = ListArray::new(field.clone(), offsets, array, None);
    let list_array = make_array(list_array.into());
    let list_field = Arc::new(Field::new(name, DataType::List(field), true));
    (list_field, list_array)
}

pub(crate) fn array_offsets<T>(arrays: &Vec<Option<Vec<Option<T>>>>) -> (OffsetBuffer<i32>, bool) {
    let mut has_some = false;
    let mut offsets = vec![0];
    let mut current_offset = 0;
    for array in arrays {
        if let Some(array) = array {
            let len = array.len() as i32;
            current_offset += len;
            offsets.push(current_offset);
            has_some = true;
        } else {
            offsets.push(0);
        }
    }

    let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets));
    let all_nulls = !arrays.is_empty() && !has_some;

    (offsets, all_nulls)
}
