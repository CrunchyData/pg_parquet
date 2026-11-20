use arrow::array::{
    Array, ArrayData, BinaryArray, BooleanArray, Date32Array, Decimal128Array,
    DurationMicrosecondArray, DurationMillisecondArray, DurationNanosecondArray,
    DurationSecondArray, FixedSizeBinaryArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, ListArray, MapArray, StringArray, StructArray, Time64MicrosecondArray,
    TimestampMicrosecondArray, UInt32Array,
};
use arrow_schema::{DataType, TimeUnit};
use context::ArrowToPgAttributeContext;
use pgrx::{
    datum::{Date, Interval, Timestamp, TimestampWithTimeZone},
    pg_sys::{Datum, Oid, CHAROID, TEXTOID, TIMEOID, UUIDOID},
    prelude::PgHeapTuple,
    AllocatedByRust, AnyNumeric, IntoDatum, Uuid,
};

use crate::{
    pgrx_utils::{
        array_element_typoid, collect_attributes_for, domain_array_base_elem_type, is_array_type,
        is_composite_type, resolve_domain_type, tuple_desc, CollectAttributesFor,
    },
    type_compat::{
        end_of_day_time::{EndOfDayTime, EndOfDayTimeTz},
        fallback_to_text::{reset_fallback_to_text_context, FallbackToText},
        geometry::{is_postgis_geography_type, is_postgis_geometry_type, Geography, Geometry},
        map::{is_map_type, reset_map_type_context, Map},
    },
};

pub(crate) mod bool;
pub(crate) mod bytea;
pub(crate) mod char;
pub(crate) mod composite;
pub(crate) mod context;
pub(crate) mod date;
pub(crate) mod fallback_to_text;
pub(crate) mod float4;
pub(crate) mod float8;
pub(crate) mod geography;
pub(crate) mod geometry;
pub(crate) mod int2;
pub(crate) mod int4;
pub(crate) mod int8;
pub(crate) mod interval;
pub(crate) mod map;
pub(crate) mod numeric;
pub(crate) mod oid;
#[cfg(not(pre_pg19))]
pub(crate) mod oid8;
pub(crate) mod text;
pub(crate) mod time;
pub(crate) mod timestamp;
pub(crate) mod timestamptz;
pub(crate) mod timetz;
pub(crate) mod uuid;

pub(crate) trait ArrowArrayToPgType<T: IntoDatum>: From<ArrayData> {
    fn to_pg_type(self, context: &ArrowToPgAttributeContext) -> Option<T>;
}

pub(crate) fn to_pg_datum(
    attribute_array: ArrayData,
    attribute_context: &ArrowToPgAttributeContext,
) -> Option<Datum> {
    if matches!(attribute_array.data_type(), DataType::List(_)) {
        to_pg_array_datum(attribute_array, attribute_context)
    } else {
        to_pg_nonarray_datum(attribute_array, attribute_context)
    }
}

macro_rules! to_pg_datum {
    ($arrow_array_type:ty, $pg_type:ty, $arrow_array:expr, $attribute_context:expr) => {{
        let arrow_array: $arrow_array_type = $arrow_array.into();

        let val: Option<$pg_type> = arrow_array.to_pg_type($attribute_context);

        val.into_datum()
    }};
}

// the duration arrays differ only in the unit their count is in, so the unit picks the array
// the values are read from
fn to_pg_duration_datum(
    unit: TimeUnit,
    primitive_array: ArrayData,
    attribute_context: &ArrowToPgAttributeContext,
) -> Option<Datum> {
    match unit {
        TimeUnit::Second => {
            to_pg_datum!(
                DurationSecondArray,
                Interval,
                primitive_array,
                attribute_context
            )
        }
        TimeUnit::Millisecond => {
            to_pg_datum!(
                DurationMillisecondArray,
                Interval,
                primitive_array,
                attribute_context
            )
        }
        TimeUnit::Microsecond => {
            to_pg_datum!(
                DurationMicrosecondArray,
                Interval,
                primitive_array,
                attribute_context
            )
        }
        TimeUnit::Nanosecond => {
            to_pg_datum!(
                DurationNanosecondArray,
                Interval,
                primitive_array,
                attribute_context
            )
        }
    }
}

fn to_pg_duration_list_datum(
    unit: TimeUnit,
    list_array: ArrayData,
    element_context: &ArrowToPgAttributeContext,
) -> Option<Datum> {
    match unit {
        TimeUnit::Second => {
            to_pg_datum!(
                DurationSecondArray,
                Vec<Option<Interval>>,
                list_array,
                element_context
            )
        }
        TimeUnit::Millisecond => {
            to_pg_datum!(
                DurationMillisecondArray,
                Vec<Option<Interval>>,
                list_array,
                element_context
            )
        }
        TimeUnit::Microsecond => {
            to_pg_datum!(
                DurationMicrosecondArray,
                Vec<Option<Interval>>,
                list_array,
                element_context
            )
        }
        TimeUnit::Nanosecond => {
            to_pg_datum!(
                DurationNanosecondArray,
                Vec<Option<Interval>>,
                list_array,
                element_context
            )
        }
    }
}

fn to_pg_nonarray_datum(
    primitive_array: ArrayData,
    attribute_context: &ArrowToPgAttributeContext,
) -> Option<Datum> {
    match attribute_context.data_type() {
        DataType::Float32 => {
            to_pg_datum!(Float32Array, f32, primitive_array, attribute_context)
        }
        DataType::Float64 => {
            to_pg_datum!(Float64Array, f64, primitive_array, attribute_context)
        }
        DataType::Int16 => {
            to_pg_datum!(Int16Array, i16, primitive_array, attribute_context)
        }
        DataType::Int32 => {
            to_pg_datum!(Int32Array, i32, primitive_array, attribute_context)
        }
        DataType::Int64 => {
            to_pg_datum!(Int64Array, i64, primitive_array, attribute_context)
        }
        DataType::UInt32 => {
            to_pg_datum!(UInt32Array, Oid, primitive_array, attribute_context)
        }
        #[cfg(not(pre_pg19))]
        DataType::UInt64 => {
            to_pg_datum!(
                arrow::array::UInt64Array,
                crate::type_compat::oid8::Oid8,
                primitive_array,
                attribute_context
            )
        }
        DataType::Boolean => {
            to_pg_datum!(BooleanArray, bool, primitive_array, attribute_context)
        }
        // a time span that another writer put in the file, which pg_parquet writes as text
        DataType::Duration(unit) => to_pg_duration_datum(*unit, primitive_array, attribute_context),
        DataType::Utf8 => {
            if attribute_context.typoid() == CHAROID {
                to_pg_datum!(StringArray, i8, primitive_array, attribute_context)
            } else if attribute_context.typoid() == TEXTOID {
                to_pg_datum!(StringArray, String, primitive_array, attribute_context)
            } else {
                reset_fallback_to_text_context(
                    attribute_context.typoid(),
                    attribute_context.typmod(),
                );

                to_pg_datum!(
                    StringArray,
                    FallbackToText,
                    primitive_array,
                    attribute_context
                )
            }
        }
        DataType::Binary => {
            if attribute_context.is_geometry() {
                to_pg_datum!(BinaryArray, Geometry, primitive_array, attribute_context)
            } else if attribute_context.is_geography() {
                to_pg_datum!(BinaryArray, Geography, primitive_array, attribute_context)
            } else {
                to_pg_datum!(BinaryArray, Vec<u8>, primitive_array, attribute_context)
            }
        }
        DataType::FixedSizeBinary(16) if attribute_context.typoid() == UUIDOID => {
            to_pg_datum!(
                FixedSizeBinaryArray,
                Uuid,
                primitive_array,
                attribute_context
            )
        }
        DataType::Decimal128(_, _) => {
            to_pg_datum!(
                Decimal128Array,
                AnyNumeric,
                primitive_array,
                attribute_context
            )
        }
        DataType::Date32 => {
            to_pg_datum!(Date32Array, Date, primitive_array, attribute_context)
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            if attribute_context.typoid() == TIMEOID {
                to_pg_datum!(
                    Time64MicrosecondArray,
                    EndOfDayTime,
                    primitive_array,
                    attribute_context
                )
            } else {
                to_pg_datum!(
                    Time64MicrosecondArray,
                    EndOfDayTimeTz,
                    primitive_array,
                    attribute_context
                )
            }
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            to_pg_datum!(
                TimestampMicrosecondArray,
                Timestamp,
                primitive_array,
                attribute_context
            )
        }
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
            to_pg_datum!(
                TimestampMicrosecondArray,
                TimestampWithTimeZone,
                primitive_array,
                attribute_context
            )
        }
        DataType::Struct(_) => {
            to_pg_datum!(
                StructArray,
                PgHeapTuple<AllocatedByRust>,
                primitive_array,
                attribute_context
            )
        }
        DataType::Map(_, _) => {
            reset_map_type_context(attribute_context.typoid());

            to_pg_datum!(MapArray, Map, primitive_array, attribute_context)
        }
        _ => {
            panic!("unsupported data type: {:?}", attribute_context.data_type());
        }
    }
}

fn to_pg_array_datum(
    list_array: ArrayData,
    attribute_context: &ArrowToPgAttributeContext,
) -> Option<Datum> {
    let list_array: ListArray = list_array.into();

    if list_array.is_null(0) {
        return None;
    }

    let list_array = list_array.value(0).to_data();

    let element_context = attribute_context.element_context();

    match element_context.data_type() {
        DataType::Float32 => {
            to_pg_datum!(Float32Array, Vec<Option<f32>>, list_array, element_context)
        }
        DataType::Float64 => {
            to_pg_datum!(Float64Array, Vec<Option<f64>>, list_array, element_context)
        }
        DataType::Int16 => {
            to_pg_datum!(Int16Array, Vec<Option<i16>>, list_array, element_context)
        }
        DataType::Int32 => {
            to_pg_datum!(Int32Array, Vec<Option<i32>>, list_array, element_context)
        }
        DataType::Int64 => {
            to_pg_datum!(Int64Array, Vec<Option<i64>>, list_array, element_context)
        }
        DataType::UInt32 => {
            to_pg_datum!(UInt32Array, Vec<Option<Oid>>, list_array, element_context)
        }
        DataType::Duration(unit) => to_pg_duration_list_datum(*unit, list_array, element_context),
        #[cfg(not(pre_pg19))]
        DataType::UInt64 => {
            to_pg_datum!(
                arrow::array::UInt64Array,
                Vec<Option<crate::type_compat::oid8::Oid8>>,
                list_array,
                element_context
            )
        }
        DataType::Boolean => {
            to_pg_datum!(BooleanArray, Vec<Option<bool>>, list_array, element_context)
        }
        DataType::Utf8 => {
            if element_context.typoid() == CHAROID {
                to_pg_datum!(StringArray, Vec<Option<i8>>, list_array, element_context)
            } else if element_context.typoid() == TEXTOID {
                to_pg_datum!(
                    StringArray,
                    Vec<Option<String>>,
                    list_array,
                    element_context
                )
            } else {
                reset_fallback_to_text_context(element_context.typoid(), element_context.typmod());

                to_pg_datum!(
                    StringArray,
                    Vec<Option<FallbackToText>>,
                    list_array,
                    element_context
                )
            }
        }
        DataType::Binary => {
            if element_context.is_geometry() {
                to_pg_datum!(
                    BinaryArray,
                    Vec<Option<Geometry>>,
                    list_array,
                    element_context
                )
            } else if element_context.is_geography() {
                to_pg_datum!(
                    BinaryArray,
                    Vec<Option<Geography>>,
                    list_array,
                    element_context
                )
            } else {
                to_pg_datum!(
                    BinaryArray,
                    Vec<Option<Vec<u8>>>,
                    list_array,
                    element_context
                )
            }
        }
        DataType::FixedSizeBinary(16) if element_context.typoid() == UUIDOID => {
            to_pg_datum!(
                FixedSizeBinaryArray,
                Vec<Option<Uuid>>,
                list_array,
                element_context
            )
        }
        DataType::Decimal128(_, _) => {
            to_pg_datum!(
                Decimal128Array,
                Vec<Option<AnyNumeric>>,
                list_array,
                element_context
            )
        }
        DataType::Date32 => {
            to_pg_datum!(Date32Array, Vec<Option<Date>>, list_array, element_context)
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            if element_context.typoid() == TIMEOID {
                to_pg_datum!(
                    Time64MicrosecondArray,
                    Vec<Option<EndOfDayTime>>,
                    list_array,
                    element_context
                )
            } else {
                to_pg_datum!(
                    Time64MicrosecondArray,
                    Vec<Option<EndOfDayTimeTz>>,
                    list_array,
                    element_context
                )
            }
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            to_pg_datum!(
                TimestampMicrosecondArray,
                Vec<Option<Timestamp>>,
                list_array,
                element_context
            )
        }
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
            to_pg_datum!(
                TimestampMicrosecondArray,
                Vec<Option<TimestampWithTimeZone>>,
                list_array,
                element_context
            )
        }
        DataType::Struct(_) => {
            to_pg_datum!(
                StructArray,
                Vec<Option<PgHeapTuple<AllocatedByRust>>>,
                list_array,
                element_context
            )
        }
        DataType::Map(_, _) => {
            reset_map_type_context(element_context.typoid());

            to_pg_datum!(MapArray, Vec<Option<Map>>, list_array, element_context)
        }
        _ => {
            panic!("unsupported data type: {:?}", element_context.data_type());
        }
    }
}
