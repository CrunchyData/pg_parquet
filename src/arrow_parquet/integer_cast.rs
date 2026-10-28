use std::sync::Arc;

use arrow::{
    array::{make_array, Array, ArrayData, ArrayRef, AsArray, Float32Array, Float64Array},
    compute::kernels::arity::unary,
    datatypes::{Float32Type, Float64Type, DECIMAL256_MAX_PRECISION},
};
use arrow_cast::{cast_with_options, CastOptions};
use arrow_schema::{DataType, FieldRef};

// round_for_integer_cast pre-rounds the values of a column that is about to be cast to an integer
// type. arrow's decimal and float to integer casts truncate toward zero, but PostgreSQL's casts
// round, so the cast is handed values that are already integral. A decimal is rounded by rescaling
// it to scale 0, which rounds half away from zero like numeric::int4 does, and a float is rounded
// half to even, like float8::int4 does through rint. A rounded decimal keeps the width and the
// precision of its column at scale 0, since a carry out of the rounding does not fit its own
// scale, and the target type is followed into lists, maps and structs, so that the elements of an
// array column round the same way. The name of the column is only carried to name it in the
// error of a file whose values do not fit the precision they are declared with.
pub(crate) fn round_for_integer_cast(
    array: &ArrayRef,
    target_type: &DataType,
    name: &str,
) -> ArrayRef {
    match (array.data_type(), target_type) {
        (source, target) if target.is_integer() && decimal_scale(source).unwrap_or(0) > 0 => {
            // the widest decimal type holds any value rescaled to scale 0, and the way back to the
            // decimal type of the column at scale 0 cannot overflow it: the integral part of the
            // value has at most precision - scale digits, so a carry out of the rounding still
            // fits in precision digits
            let integral = cast_to(
                array,
                &DataType::Decimal256(DECIMAL256_MAX_PRECISION, 0),
                name,
            );

            cast_to(&integral, &decimal_at_scale_0(source), name)
        }
        (DataType::Float16, target) if target.is_integer() => {
            // f16 has no rounding method of its own. widening it to f32 is lossless, and an
            // integral value that came from an f16 is representable as one again
            let widened = cast_to(array, &DataType::Float32, name);

            cast_to(
                &round_for_integer_cast(&widened, target, name),
                &DataType::Float16,
                name,
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

            with_rounded_children(array, &field_targets, name)
        }
        (source, target) => match (element_type(source), element_type(target)) {
            (Some(_), Some(element_target)) => {
                with_rounded_children(array, &[element_target], name)
            }
            _ => array.clone(),
        },
    }
}

// with_rounded_children rebuilds array with each of its children rounded for the target type at
// the same position. A rounded child keeps its length and its place, so the offsets and the nulls
// of the array stay valid, and the type of the array follows the types of its rounded children.
fn with_rounded_children(array: &ArrayRef, child_targets: &[&DataType], name: &str) -> ArrayRef {
    let array_data = array.to_data();

    let mut rounded_children = vec![];
    let mut rounded_any = false;

    for (child_data, child_target) in array_data.child_data().iter().zip(child_targets) {
        let child = make_array(child_data.clone());
        let rounded_child = round_for_integer_cast(&child, child_target, name);

        rounded_any |= !Arc::ptr_eq(&rounded_child, &child);

        rounded_children.push(rounded_child.to_data());
    }

    if !rounded_any {
        return array.clone();
    }

    let rounded_type = with_child_types(array.data_type(), &rounded_children);

    let rounded_data = array_data
        .into_builder()
        .data_type(rounded_type)
        .child_data(rounded_children)
        .build()
        .unwrap_or_else(|e| panic!("failed to round column {name}: {e}"));

    make_array(rounded_data)
}

// with_child_types is the container type whose child fields carry the types of the rounded
// children, which a rounding that rescales a decimal to scale 0 changes.
fn with_child_types(data_type: &DataType, children: &[ArrayData]) -> DataType {
    let retyped = |field: &FieldRef, position: usize| {
        let child_type = match children.get(position) {
            Some(child) => child.data_type().clone(),
            None => return field.clone(),
        };

        Arc::new(field.as_ref().clone().with_data_type(child_type))
    };

    match data_type {
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .enumerate()
                .map(|(position, field)| retyped(field, position))
                .collect(),
        ),
        DataType::List(field) => DataType::List(retyped(field, 0)),
        DataType::LargeList(field) => DataType::LargeList(retyped(field, 0)),
        DataType::ListView(field) => DataType::ListView(retyped(field, 0)),
        DataType::LargeListView(field) => DataType::LargeListView(retyped(field, 0)),
        DataType::FixedSizeList(field, length) => {
            DataType::FixedSizeList(retyped(field, 0), *length)
        }
        DataType::Map(field, sorted) => DataType::Map(retyped(field, 0), *sorted),
        other => other.clone(),
    }
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

// decimal_at_scale_0 is the decimal type of the same width and precision, at scale 0.
fn decimal_at_scale_0(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Decimal32(precision, _) => DataType::Decimal32(*precision, 0),
        DataType::Decimal64(precision, _) => DataType::Decimal64(*precision, 0),
        DataType::Decimal128(precision, _) => DataType::Decimal128(*precision, 0),
        DataType::Decimal256(precision, _) => DataType::Decimal256(*precision, 0),
        other => other.clone(),
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

fn cast_to(array: &ArrayRef, data_type: &DataType, name: &str) -> ArrayRef {
    // should fail instead of returning None if the cast fails at runtime
    let cast_options = CastOptions {
        safe: false,
        ..Default::default()
    };

    cast_with_options(array, data_type, &cast_options)
        .unwrap_or_else(|e| panic!("failed to round column {name}: {e}"))
}
