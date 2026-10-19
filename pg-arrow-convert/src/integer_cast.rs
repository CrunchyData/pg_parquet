use std::sync::Arc;

use arrow::{
    array::{make_array, Array, ArrayRef, AsArray, Float32Array, Float64Array},
    compute::kernels::arity::unary,
    datatypes::{Float32Type, Float64Type, DECIMAL256_MAX_PRECISION},
};
use arrow_cast::{cast_with_options, CastOptions};
use arrow_schema::DataType;

// round_for_integer_cast pre-rounds the values of a column that is about to be cast to an integer
// type. arrow's decimal and float to integer casts truncate toward zero, but PostgreSQL's casts
// round, so the cast is handed values that are already integral. A decimal is rounded by rescaling
// it to scale 0, which rounds half away from zero like numeric::int4 does, and a float is rounded
// half to even, like float8::int4 does through rint. A rounded value keeps the type of its column,
// and the target type is followed into lists, maps and structs, so that the elements of an array
// column round the same way.
pub fn round_for_integer_cast(array: &ArrayRef, target_type: &DataType) -> ArrayRef {
    match (array.data_type(), target_type) {
        (source, target) if target.is_integer() && decimal_scale(source).unwrap_or(0) > 0 => {
            // the widest decimal type holds any value rescaled to scale 0, and the way back to
            // the type of the column cannot overflow it, since the value only lost digits
            let integral = cast_to(array, &DataType::Decimal256(DECIMAL256_MAX_PRECISION, 0));

            cast_to(&integral, source)
        }
        (DataType::Float16, target) if target.is_integer() => {
            // f16 has no rounding method of its own. widening it to f32 is lossless, and an
            // integral value that came from an f16 is representable as one again
            let widened = cast_to(array, &DataType::Float32);

            cast_to(
                &round_for_integer_cast(&widened, target),
                &DataType::Float16,
            )
        }
        (DataType::Float32, target) if target.is_integer() => {
            let rounded: Float32Array =
                unary(array.as_primitive::<Float32Type>(), f32::round_ties_even);

            Arc::new(rounded)
        }
        (DataType::Float64, target) if target.is_integer() => {
            let rounded: Float64Array =
                unary(array.as_primitive::<Float64Type>(), f64::round_ties_even);

            Arc::new(rounded)
        }
        (DataType::Struct(fields), DataType::Struct(target_fields))
            if fields.len() == target_fields.len() =>
        {
            let field_targets = target_fields
                .iter()
                .map(|field| field.data_type())
                .collect::<Vec<_>>();

            with_rounded_children(array, &field_targets)
        }
        (source, target) => match (element_type(source), element_type(target)) {
            (Some(_), Some(element_target)) => with_rounded_children(array, &[element_target]),
            _ => array.clone(),
        },
    }
}

// with_rounded_children rebuilds array with each of its children rounded for the target type at
// the same position. A rounded child keeps its type, its length and its place, so the type, the
// offsets and the nulls of the array stay valid.
fn with_rounded_children(array: &ArrayRef, child_targets: &[&DataType]) -> ArrayRef {
    let array_data = array.to_data();

    let mut rounded_children = vec![];
    let mut rounded_any = false;

    for (child_data, child_target) in array_data.child_data().iter().zip(child_targets) {
        let child = make_array(child_data.clone());
        let rounded_child = round_for_integer_cast(&child, child_target);

        rounded_any |= !Arc::ptr_eq(&rounded_child, &child);

        rounded_children.push(rounded_child.to_data());
    }

    if !rounded_any {
        return array.clone();
    }

    let rounded_data = array_data
        .into_builder()
        .child_data(rounded_children)
        .build()
        .unwrap_or_else(|e| panic!("failed to round the values of a column: {e}"));

    make_array(rounded_data)
}

// element_type is the type of the only child of a container type.
fn element_type(data_type: &DataType) -> Option<&DataType> {
    match data_type {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => Some(field.data_type()),
        _ => None,
    }
}

// decimal_scale is the scale of a decimal type, and None for any other type.
fn decimal_scale(data_type: &DataType) -> Option<i8> {
    match data_type {
        DataType::Decimal32(_, scale)
        | DataType::Decimal64(_, scale)
        | DataType::Decimal128(_, scale)
        | DataType::Decimal256(_, scale) => Some(*scale),
        _ => None,
    }
}

fn cast_to(array: &ArrayRef, data_type: &DataType) -> ArrayRef {
    // should fail instead of returning None if the cast fails at runtime
    let cast_options = CastOptions {
        safe: false,
        ..Default::default()
    };

    cast_with_options(array, data_type, &cast_options)
        .unwrap_or_else(|e| panic!("failed to round the values of a column: {e}"))
}
