use arrow::array::{
    Array, ArrayData, BinaryArray, BooleanArray, Date32Array, Decimal128Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    IntervalMonthDayNanoArray, ListArray, StringArray, StructArray, Time64MicrosecondArray,
    TimestampMicrosecondArray, UInt32Array,
};
use pgrx::{
    pg_sys::{
        Datum, Oid, BOOLARRAYOID, BOOLOID, BYTEAARRAYOID, BYTEAOID, CHARARRAYOID, CHAROID,
        DATEARRAYOID, DATEOID, FLOAT4ARRAYOID, FLOAT4OID, FLOAT8ARRAYOID, FLOAT8OID, INT2ARRAYOID,
        INT2OID, INT4ARRAYOID, INT4OID, INT8ARRAYOID, INT8OID, INTERVALARRAYOID, INTERVALOID,
        JSONARRAYOID, JSONBARRAYOID, JSONBOID, JSONOID, NUMERICARRAYOID, NUMERICOID, OIDARRAYOID,
        OIDOID, TEXTARRAYOID, TEXTOID, TIMEARRAYOID, TIMEOID, TIMESTAMPARRAYOID, TIMESTAMPOID,
        TIMESTAMPTZARRAYOID, TIMESTAMPTZOID, TIMETZARRAYOID, TIMETZOID, UUIDARRAYOID, UUIDOID,
    },
    prelude::PgHeapTuple,
    AllocatedByRust, AnyNumeric, Date, Interval, IntoDatum, Json, JsonB, PgTupleDesc, Time,
    TimeWithTimeZone, Timestamp, TimestampWithTimeZone, Uuid,
};

use crate::{
    pgrx_utils::{array_element_typoid, is_array_type, is_composite_type, tuple_desc},
    type_compat::{
        fallback_to_text::{set_fallback_typoid, FallbackToText},
        geometry::{is_postgis_geometry_type, set_geometry_typoid, Geometry},
        pg_arrow_type_conversions::{extract_precision_from_numeric_typmod, MAX_DECIMAL_PRECISION},
    },
};

pub(crate) mod bool;
pub(crate) mod bytea;
pub(crate) mod char;
pub(crate) mod date;
pub(crate) mod fallback_to_text;
pub(crate) mod float4;
pub(crate) mod float8;
pub(crate) mod geometry;
pub(crate) mod int2;
pub(crate) mod int4;
pub(crate) mod int8;
pub(crate) mod interval;
pub(crate) mod json;
pub(crate) mod jsonb;
pub(crate) mod numeric;
pub(crate) mod oid;
pub(crate) mod record;
pub(crate) mod text;
pub(crate) mod time;
pub(crate) mod timestamp;
pub(crate) mod timestamptz;
pub(crate) mod timetz;
pub(crate) mod uuid;

pub(crate) trait ArrowArrayToPgType<'a, A: From<ArrayData>, T: 'a + IntoDatum> {
    fn to_pg_type(
        array: A,
        typoid: Oid,
        typmod: i32,
        tupledesc: Option<PgTupleDesc<'a>>,
    ) -> Option<T>;
}

pub(crate) fn to_pg_datum(row: ArrayData, typoid: Oid, typmod: i32) -> Option<Datum> {
    if is_array_type(typoid) {
        to_pg_array_datum(row, typoid, typmod)
    } else {
        to_pg_primitive_datum(row, typoid, typmod)
    }
}

fn to_pg_primitive_datum(primitive_array: ArrayData, typoid: Oid, typmod: i32) -> Option<Datum> {
    match typoid {
        FLOAT4OID => {
            let val = <f32 as ArrowArrayToPgType<Float32Array, f32>>::to_pg_type(
                primitive_array.into(),
                typoid,
                typmod,
                None,
            );
            val.into_datum()
        }
        FLOAT8OID => {
            let val = <f64 as ArrowArrayToPgType<Float64Array, f64>>::to_pg_type(
                primitive_array.into(),
                typoid,
                typmod,
                None,
            );
            val.into_datum()
        }
        INT2OID => {
            let val = <i16 as ArrowArrayToPgType<Int16Array, i16>>::to_pg_type(
                primitive_array.into(),
                typoid,
                typmod,
                None,
            );
            val.into_datum()
        }
        INT4OID => {
            let val = <i32 as ArrowArrayToPgType<Int32Array, i32>>::to_pg_type(
                primitive_array.into(),
                typoid,
                typmod,
                None,
            );
            val.into_datum()
        }
        INT8OID => {
            let val = <i64 as ArrowArrayToPgType<Int64Array, i64>>::to_pg_type(
                primitive_array.into(),
                typoid,
                typmod,
                None,
            );
            val.into_datum()
        }
        BOOLOID => {
            let val = <bool as ArrowArrayToPgType<BooleanArray, bool>>::to_pg_type(
                primitive_array.into(),
                typoid,
                typmod,
                None,
            );
            val.into_datum()
        }
        CHAROID => {
            let val = <i8 as ArrowArrayToPgType<StringArray, i8>>::to_pg_type(
                primitive_array.into(),
                typoid,
                typmod,
                None,
            );
            val.into_datum()
        }
        TEXTOID => {
            let val = <String as ArrowArrayToPgType<StringArray, String>>::to_pg_type(
                primitive_array.into(),
                typoid,
                typmod,
                None,
            );
            val.into_datum()
        }
        BYTEAOID => {
            let val = <Vec<u8> as ArrowArrayToPgType<BinaryArray, Vec<u8>>>::to_pg_type(
                primitive_array.into(),
                typoid,
                typmod,
                None,
            );
            val.into_datum()
        }
        OIDOID => {
            let val = <Oid as ArrowArrayToPgType<UInt32Array, Oid>>::to_pg_type(
                primitive_array.into(),
                typoid,
                typmod,
                None,
            );
            val.into_datum()
        }
        NUMERICOID => {
            let precision = extract_precision_from_numeric_typmod(typmod);
            if precision > MAX_DECIMAL_PRECISION {
                set_fallback_typoid(typoid);
                let val =
                    <FallbackToText as ArrowArrayToPgType<StringArray, FallbackToText>>::to_pg_type(
                        primitive_array.into(),
                        typoid,
                        typmod,
                        None,
                    );
                val.into_datum()
            } else {
                let val =
                    <AnyNumeric as ArrowArrayToPgType<Decimal128Array, AnyNumeric>>::to_pg_type(
                        primitive_array.into(),
                        typoid,
                        typmod,
                        None,
                    );
                val.into_datum()
            }
        }
        DATEOID => {
            let val = <Date as ArrowArrayToPgType<Date32Array, Date>>::to_pg_type(
                primitive_array.into(),
                typoid,
                typmod,
                None,
            );
            val.into_datum()
        }
        TIMEOID => {
            let val = <Time as ArrowArrayToPgType<Time64MicrosecondArray, Time>>::to_pg_type(
                primitive_array.into(),
                typoid,
                typmod,
                None,
            );
            val.into_datum()
        }
        TIMETZOID => {
            let val = <TimeWithTimeZone as ArrowArrayToPgType<
                Time64MicrosecondArray,
                TimeWithTimeZone,
            >>::to_pg_type(primitive_array.into(), typoid, typmod, None);
            val.into_datum()
        }
        TIMESTAMPOID => {
            let val =
                <Timestamp as ArrowArrayToPgType<TimestampMicrosecondArray, Timestamp>>::to_pg_type(
                    primitive_array.into(),
                    typoid,
                    typmod,
                    None,
                );
            val.into_datum()
        }
        TIMESTAMPTZOID => {
            let val = <TimestampWithTimeZone as ArrowArrayToPgType<
                TimestampMicrosecondArray,
                TimestampWithTimeZone,
            >>::to_pg_type(primitive_array.into(), typoid, typmod, None);
            val.into_datum()
        }
        INTERVALOID => {
            let val =
                <Interval as ArrowArrayToPgType<IntervalMonthDayNanoArray, Interval>>::to_pg_type(
                    primitive_array.into(),
                    typoid,
                    typmod,
                    None,
                );
            val.into_datum()
        }
        UUIDOID => {
            let val = <Uuid as ArrowArrayToPgType<FixedSizeBinaryArray, Uuid>>::to_pg_type(
                primitive_array.into(),
                typoid,
                typmod,
                None,
            );
            val.into_datum()
        }
        JSONOID => {
            let val = <Json as ArrowArrayToPgType<StringArray, Json>>::to_pg_type(
                primitive_array.into(),
                typoid,
                typmod,
                None,
            );
            val.into_datum()
        }
        JSONBOID => {
            let val = <JsonB as ArrowArrayToPgType<StringArray, JsonB>>::to_pg_type(
                primitive_array.into(),
                typoid,
                typmod,
                None,
            );
            val.into_datum()
        }
        _ => {
            if is_composite_type(typoid) {
                let tupledesc = tuple_desc(typoid, typmod);

                let val = <PgHeapTuple<AllocatedByRust> as ArrowArrayToPgType<
                    StructArray,
                    PgHeapTuple<AllocatedByRust>,
                >>::to_pg_type(
                    primitive_array.into(), typoid, typmod, Some(tupledesc)
                );

                val.into_datum()
            } else if is_postgis_geometry_type(typoid) {
                set_geometry_typoid(typoid);
                let val = <Geometry as ArrowArrayToPgType<BinaryArray, Geometry>>::to_pg_type(
                    primitive_array.into(),
                    typoid,
                    typmod,
                    None,
                );
                val.into_datum()
            } else {
                set_fallback_typoid(typoid);
                let val =
                    <FallbackToText as ArrowArrayToPgType<StringArray, FallbackToText>>::to_pg_type(
                        primitive_array.into(),
                        typoid,
                        typmod,
                        None,
                    );
                val.into_datum()
            }
        }
    }
}

fn to_pg_array_datum(list_array: ArrayData, typoid: Oid, typmod: i32) -> Option<Datum> {
    let list_array: ListArray = list_array.into();

    if list_array.is_null(0) {
        return None;
    }

    let list_array = list_array.value(0).to_data();

    let element_typoid = array_element_typoid(typoid);

    match typoid {
        FLOAT4ARRAYOID => {
            let val =
                <Vec<Option<f32>> as ArrowArrayToPgType<Float32Array, Vec<Option<f32>>>>::to_pg_type(
                    list_array.into(),
                    element_typoid,
                    typmod,
                    None,
                );
            val.into_datum()
        }
        FLOAT8ARRAYOID => {
            let val =
                <Vec<Option<f64>> as ArrowArrayToPgType<Float64Array, Vec<Option<f64>>>>::to_pg_type(
                    list_array.into(),
                    element_typoid,
                    typmod,
                    None,
                );
            val.into_datum()
        }
        INT2ARRAYOID => {
            let val =
                <Vec<Option<i16>> as ArrowArrayToPgType<Int16Array, Vec<Option<i16>>>>::to_pg_type(
                    list_array.into(),
                    element_typoid,
                    typmod,
                    None,
                );
            val.into_datum()
        }
        INT4ARRAYOID => {
            let val =
                <Vec<Option<i32>> as ArrowArrayToPgType<Int32Array, Vec<Option<i32>>>>::to_pg_type(
                    list_array.into(),
                    element_typoid,
                    typmod,
                    None,
                );
            val.into_datum()
        }
        INT8ARRAYOID => {
            let val =
                <Vec<Option<i64>> as ArrowArrayToPgType<Int64Array, Vec<Option<i64>>>>::to_pg_type(
                    list_array.into(),
                    element_typoid,
                    typmod,
                    None,
                );
            val.into_datum()
        }
        BOOLARRAYOID => {
            let val =
                <Vec<Option<bool>> as ArrowArrayToPgType<BooleanArray, Vec<Option<bool>>>>::to_pg_type(
                    list_array.into(),
                    element_typoid,
                    typmod,
                    None,
                );
            val.into_datum()
        }
        CHARARRAYOID => {
            let val =
                <Vec<Option<i8>> as ArrowArrayToPgType<StringArray, Vec<Option<i8>>>>::to_pg_type(
                    list_array.into(),
                    element_typoid,
                    typmod,
                    None,
                );
            val.into_datum()
        }
        TEXTARRAYOID => {
            let val = <Vec<Option<String>> as ArrowArrayToPgType<
                StringArray,
                Vec<Option<String>>,
            >>::to_pg_type(list_array.into(), element_typoid, typmod, None);
            val.into_datum()
        }
        BYTEAARRAYOID => {
            let val = <Vec<Option<Vec<u8>>> as ArrowArrayToPgType<
                BinaryArray,
                Vec<Option<Vec<u8>>>,
            >>::to_pg_type(list_array.into(), element_typoid, typmod, None);
            val.into_datum()
        }
        OIDARRAYOID => {
            let val =
                <Vec<Option<Oid>> as ArrowArrayToPgType<UInt32Array, Vec<Option<Oid>>>>::to_pg_type(
                    list_array.into(),
                    element_typoid,
                    typmod,
                    None,
                );
            val.into_datum()
        }
        NUMERICARRAYOID => {
            let precision = extract_precision_from_numeric_typmod(typmod);
            if precision > MAX_DECIMAL_PRECISION {
                set_fallback_typoid(element_typoid);
                let val = <Vec<Option<FallbackToText>> as ArrowArrayToPgType<
                    StringArray,
                    Vec<Option<FallbackToText>>,
                >>::to_pg_type(
                    list_array.into(), element_typoid, typmod, None
                );
                val.into_datum()
            } else {
                let val = <Vec<Option<AnyNumeric>> as ArrowArrayToPgType<
                    Decimal128Array,
                    Vec<Option<AnyNumeric>>,
                >>::to_pg_type(
                    list_array.into(), element_typoid, typmod, None
                );
                val.into_datum()
            }
        }
        DATEARRAYOID => {
            let val =
                <Vec<Option<Date>> as ArrowArrayToPgType<Date32Array, Vec<Option<Date>>>>::to_pg_type(
                    list_array.into(),
                    element_typoid,
                    typmod,
                    None,
                );
            val.into_datum()
        }
        TIMEARRAYOID => {
            let val = <Vec<Option<Time>> as ArrowArrayToPgType<
                Time64MicrosecondArray,
                Vec<Option<Time>>,
            >>::to_pg_type(list_array.into(), element_typoid, typmod, None);
            val.into_datum()
        }
        TIMETZARRAYOID => {
            let val = <Vec<Option<TimeWithTimeZone>> as ArrowArrayToPgType<
                Time64MicrosecondArray,
                Vec<Option<TimeWithTimeZone>>,
            >>::to_pg_type(list_array.into(), element_typoid, typmod, None);
            val.into_datum()
        }
        TIMESTAMPARRAYOID => {
            let val = <Vec<Option<Timestamp>> as ArrowArrayToPgType<
                TimestampMicrosecondArray,
                Vec<Option<Timestamp>>,
            >>::to_pg_type(list_array.into(), element_typoid, typmod, None);
            val.into_datum()
        }
        TIMESTAMPTZARRAYOID => {
            let val = <Vec<Option<TimestampWithTimeZone>> as ArrowArrayToPgType<
                TimestampMicrosecondArray,
                Vec<Option<TimestampWithTimeZone>>,
            >>::to_pg_type(list_array.into(), element_typoid, typmod, None);
            val.into_datum()
        }
        INTERVALARRAYOID => {
            let val = <Vec<Option<Interval>> as ArrowArrayToPgType<
                IntervalMonthDayNanoArray,
                Vec<Option<Interval>>,
            >>::to_pg_type(list_array.into(), element_typoid, typmod, None);
            val.into_datum()
        }
        UUIDARRAYOID => {
            let val = <Vec<Option<Uuid>> as ArrowArrayToPgType<
                FixedSizeBinaryArray,
                Vec<Option<Uuid>>,
            >>::to_pg_type(list_array.into(), element_typoid, typmod, None);
            val.into_datum()
        }
        JSONARRAYOID => {
            let val =
                <Vec<Option<Json>> as ArrowArrayToPgType<StringArray, Vec<Option<Json>>>>::to_pg_type(
                    list_array.into(),
                    element_typoid,
                    typmod,
                    None,
                );
            val.into_datum()
        }
        JSONBARRAYOID => {
            let val =
                <Vec<Option<JsonB>> as ArrowArrayToPgType<StringArray, Vec<Option<JsonB>>>>::to_pg_type(
                    list_array.into(),
                    element_typoid,
                    typmod,
                    None,
                );
            val.into_datum()
        }
        _ => {
            if is_composite_type(element_typoid) {
                let tupledesc = tuple_desc(element_typoid, typmod);

                let val = <Vec<Option<PgHeapTuple<AllocatedByRust>>> as ArrowArrayToPgType<
                    StructArray,
                    Vec<Option<PgHeapTuple<AllocatedByRust>>>,
                >>::to_pg_type(
                    list_array.into(), element_typoid, typmod, Some(tupledesc)
                );

                val.into_datum()
            } else if is_postgis_geometry_type(element_typoid) {
                set_geometry_typoid(element_typoid);
                let val = <Vec<Option<Geometry>> as ArrowArrayToPgType<
                    BinaryArray,
                    Vec<Option<Geometry>>,
                >>::to_pg_type(
                    list_array.into(), element_typoid, typmod, None
                );
                val.into_datum()
            } else {
                set_fallback_typoid(element_typoid);
                let val = <Vec<Option<FallbackToText>> as ArrowArrayToPgType<
                    StringArray,
                    Vec<Option<FallbackToText>>,
                >>::to_pg_type(
                    list_array.into(), element_typoid, typmod, None
                );
                val.into_datum()
            }
        }
    }
}
