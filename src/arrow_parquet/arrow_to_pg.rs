use arrow::array::{
    Array, ArrayData, BinaryArray, BooleanArray, Date32Array, Decimal128Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    IntervalMonthDayNanoArray, ListArray, StringArray, StructArray, Time64MicrosecondArray,
    TimestampMicrosecondArray, UInt32Array,
};
use pgrx::{
    pg_sys::{
        Datum, Oid, BITARRAYOID, BITOID, BOOLARRAYOID, BOOLOID, BPCHARARRAYOID, BPCHAROID,
        BYTEAARRAYOID, BYTEAOID, CHARARRAYOID, CHAROID, DATEARRAYOID, DATEOID, FLOAT4ARRAYOID,
        FLOAT4OID, FLOAT8ARRAYOID, FLOAT8OID, INT2ARRAYOID, INT2OID, INT4ARRAYOID, INT4OID,
        INT8ARRAYOID, INT8OID, INTERVALARRAYOID, INTERVALOID, JSONARRAYOID, JSONBARRAYOID,
        JSONBOID, JSONOID, NAMEARRAYOID, NAMEOID, NUMERICARRAYOID, NUMERICOID, OIDARRAYOID, OIDOID,
        TEXTARRAYOID, TEXTOID, TIMEARRAYOID, TIMEOID, TIMESTAMPARRAYOID, TIMESTAMPOID,
        TIMESTAMPTZARRAYOID, TIMESTAMPTZOID, TIMETZARRAYOID, TIMETZOID, UUIDARRAYOID, UUIDOID,
        VARBITARRAYOID, VARBITOID, VARCHARARRAYOID, VARCHAROID,
    },
    prelude::PgHeapTuple,
    AllocatedByRust, AnyNumeric, Date, Interval, IntoDatum, Json, JsonB, PgTupleDesc, Time,
    TimeWithTimeZone, Timestamp, TimestampWithTimeZone, Uuid,
};

use crate::{
    pgrx_utils::{
        array_element_typoid, is_array_type, is_composite_type, is_enum_typoid, tuple_desc,
    },
    type_compat::{Bit, Bpchar, Enum, Name, VarBit, Varchar},
};

pub(crate) mod bit;
pub(crate) mod bool;
pub(crate) mod bpchar;
pub(crate) mod bytea;
pub(crate) mod char;
pub(crate) mod date;
pub(crate) mod enumeration;
pub(crate) mod float4;
pub(crate) mod float8;
pub(crate) mod int2;
pub(crate) mod int4;
pub(crate) mod int8;
pub(crate) mod interval;
pub(crate) mod json;
pub(crate) mod jsonb;
pub(crate) mod name;
pub(crate) mod numeric;
pub(crate) mod oid;
pub(crate) mod record;
pub(crate) mod text;
pub(crate) mod time;
pub(crate) mod timestamp;
pub(crate) mod timestamptz;
pub(crate) mod timetz;
pub(crate) mod uuid;
pub(crate) mod varbit;
pub(crate) mod varchar;

pub(crate) trait ArrowArrayToPgType<'a, A: From<ArrayData>, T: 'a + IntoDatum> {
    fn as_pg(array: A, typoid: Oid, typmod: i32, tupledesc: Option<PgTupleDesc<'a>>) -> Option<T>;
}

pub(crate) fn as_pg_datum(row: ArrayData, typoid: Oid, typmod: i32) -> Option<Datum> {
    if is_array_type(typoid) {
        as_pg_array_datum(row, typoid, typmod)
    } else {
        as_pg_primitive_datum(row, typoid, typmod)
    }
}

fn as_pg_primitive_datum(primitive_array: ArrayData, typoid: Oid, typmod: i32) -> Option<Datum> {
    match typoid {
        FLOAT4OID => {
            let val = <f32 as ArrowArrayToPgType<Float32Array, f32>>::as_pg(
                primitive_array.into(),
                typoid,
                typmod,
                None,
            );
            val.into_datum()
        }
        FLOAT8OID => {
            let val = <f64 as ArrowArrayToPgType<Float64Array, f64>>::as_pg(
                primitive_array.into(),
                typoid,
                typmod,
                None,
            );
            val.into_datum()
        }
        INT2OID => {
            let val = <i16 as ArrowArrayToPgType<Int16Array, i16>>::as_pg(
                primitive_array.into(),
                typoid,
                typmod,
                None,
            );
            val.into_datum()
        }
        INT4OID => {
            let val = <i32 as ArrowArrayToPgType<Int32Array, i32>>::as_pg(
                primitive_array.into(),
                typoid,
                typmod,
                None,
            );
            val.into_datum()
        }
        INT8OID => {
            let val = <i64 as ArrowArrayToPgType<Int64Array, i64>>::as_pg(
                primitive_array.into(),
                typoid,
                typmod,
                None,
            );
            val.into_datum()
        }
        BOOLOID => {
            let val = <bool as ArrowArrayToPgType<BooleanArray, bool>>::as_pg(
                primitive_array.into(),
                typoid,
                typmod,
                None,
            );
            val.into_datum()
        }
        CHAROID => {
            let val = <i8 as ArrowArrayToPgType<StringArray, i8>>::as_pg(
                primitive_array.into(),
                typoid,
                typmod,
                None,
            );
            val.into_datum()
        }
        TEXTOID => {
            let val = <String as ArrowArrayToPgType<StringArray, String>>::as_pg(
                primitive_array.into(),
                typoid,
                typmod,
                None,
            );
            val.into_datum()
        }
        VARCHAROID => {
            let val = <Varchar as ArrowArrayToPgType<StringArray, Varchar>>::as_pg(
                primitive_array.into(),
                typoid,
                typmod,
                None,
            );
            val.into_datum()
        }
        NAMEOID => {
            let val = <Name as ArrowArrayToPgType<StringArray, Name>>::as_pg(
                primitive_array.into(),
                typoid,
                typmod,
                None,
            );
            val.into_datum()
        }
        BPCHAROID => {
            let val = <Bpchar as ArrowArrayToPgType<StringArray, Bpchar>>::as_pg(
                primitive_array.into(),
                typoid,
                typmod,
                None,
            );
            val.into_datum()
        }
        BITOID => {
            let val = <Bit as ArrowArrayToPgType<StringArray, Bit>>::as_pg(
                primitive_array.into(),
                typoid,
                typmod,
                None,
            );
            val.into_datum()
        }
        VARBITOID => {
            let val = <VarBit as ArrowArrayToPgType<StringArray, VarBit>>::as_pg(
                primitive_array.into(),
                typoid,
                typmod,
                None,
            );
            val.into_datum()
        }
        BYTEAOID => {
            let val = <Vec<u8> as ArrowArrayToPgType<BinaryArray, Vec<u8>>>::as_pg(
                primitive_array.into(),
                typoid,
                typmod,
                None,
            );
            val.into_datum()
        }
        OIDOID => {
            let val = <Oid as ArrowArrayToPgType<UInt32Array, Oid>>::as_pg(
                primitive_array.into(),
                typoid,
                typmod,
                None,
            );
            val.into_datum()
        }
        NUMERICOID => {
            let val = <AnyNumeric as ArrowArrayToPgType<Decimal128Array, AnyNumeric>>::as_pg(
                primitive_array.into(),
                typoid,
                typmod,
                None,
            );
            val.into_datum()
        }
        DATEOID => {
            let val = <Date as ArrowArrayToPgType<Date32Array, Date>>::as_pg(
                primitive_array.into(),
                typoid,
                typmod,
                None,
            );
            val.into_datum()
        }
        TIMEOID => {
            let val = <Time as ArrowArrayToPgType<Time64MicrosecondArray, Time>>::as_pg(
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
            >>::as_pg(primitive_array.into(), typoid, typmod, None);
            val.into_datum()
        }
        TIMESTAMPOID => {
            let val =
                <Timestamp as ArrowArrayToPgType<TimestampMicrosecondArray, Timestamp>>::as_pg(
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
            >>::as_pg(primitive_array.into(), typoid, typmod, None);
            val.into_datum()
        }
        INTERVALOID => {
            let val = <Interval as ArrowArrayToPgType<IntervalMonthDayNanoArray, Interval>>::as_pg(
                primitive_array.into(),
                typoid,
                typmod,
                None,
            );
            val.into_datum()
        }
        UUIDOID => {
            let val = <Uuid as ArrowArrayToPgType<FixedSizeBinaryArray, Uuid>>::as_pg(
                primitive_array.into(),
                typoid,
                typmod,
                None,
            );
            val.into_datum()
        }
        JSONOID => {
            let val = <Json as ArrowArrayToPgType<StringArray, Json>>::as_pg(
                primitive_array.into(),
                typoid,
                typmod,
                None,
            );
            val.into_datum()
        }
        JSONBOID => {
            let val = <JsonB as ArrowArrayToPgType<StringArray, JsonB>>::as_pg(
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
                >>::as_pg(
                    primitive_array.into(), typoid, typmod, Some(tupledesc)
                );

                val.into_datum()
            } else if is_enum_typoid(typoid) {
                let val = <Enum as ArrowArrayToPgType<StringArray, Enum>>::as_pg(
                    primitive_array.into(),
                    typoid,
                    typmod,
                    None,
                );
                val.into_datum()
            } else {
                panic!("unsupported primitive type {:?}", primitive_array)
            }
        }
    }
}

fn as_pg_array_datum(list_array: ArrayData, typoid: Oid, typmod: i32) -> Option<Datum> {
    let list_array: ListArray = list_array.into();

    if list_array.is_null(0) {
        return None;
    }

    let list_array = list_array.value(0).to_data();

    let element_typoid = array_element_typoid(typoid);

    match typoid {
        FLOAT4ARRAYOID => {
            let val =
                <Vec<Option<f32>> as ArrowArrayToPgType<Float32Array, Vec<Option<f32>>>>::as_pg(
                    list_array.into(),
                    element_typoid,
                    typmod,
                    None,
                );
            val.into_datum()
        }
        FLOAT8ARRAYOID => {
            let val =
                <Vec<Option<f64>> as ArrowArrayToPgType<Float64Array, Vec<Option<f64>>>>::as_pg(
                    list_array.into(),
                    element_typoid,
                    typmod,
                    None,
                );
            val.into_datum()
        }
        INT2ARRAYOID => {
            let val = <Vec<Option<i16>> as ArrowArrayToPgType<Int16Array, Vec<Option<i16>>>>::as_pg(
                list_array.into(),
                element_typoid,
                typmod,
                None,
            );
            val.into_datum()
        }
        INT4ARRAYOID => {
            let val = <Vec<Option<i32>> as ArrowArrayToPgType<Int32Array, Vec<Option<i32>>>>::as_pg(
                list_array.into(),
                element_typoid,
                typmod,
                None,
            );
            val.into_datum()
        }
        INT8ARRAYOID => {
            let val = <Vec<Option<i64>> as ArrowArrayToPgType<Int64Array, Vec<Option<i64>>>>::as_pg(
                list_array.into(),
                element_typoid,
                typmod,
                None,
            );
            val.into_datum()
        }
        BOOLARRAYOID => {
            let val =
                <Vec<Option<bool>> as ArrowArrayToPgType<BooleanArray, Vec<Option<bool>>>>::as_pg(
                    list_array.into(),
                    element_typoid,
                    typmod,
                    None,
                );
            val.into_datum()
        }
        CHARARRAYOID => {
            let val = <Vec<Option<i8>> as ArrowArrayToPgType<StringArray, Vec<Option<i8>>>>::as_pg(
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
            >>::as_pg(list_array.into(), element_typoid, typmod, None);
            val.into_datum()
        }
        VARCHARARRAYOID => {
            let val = <Vec<Option<Varchar>> as ArrowArrayToPgType<
                StringArray,
                Vec<Option<Varchar>>,
            >>::as_pg(list_array.into(), element_typoid, typmod, None);
            val.into_datum()
        }
        NAMEARRAYOID => {
            let val =
                <Vec<Option<Name>> as ArrowArrayToPgType<StringArray, Vec<Option<Name>>>>::as_pg(
                    list_array.into(),
                    element_typoid,
                    typmod,
                    None,
                );
            val.into_datum()
        }
        BPCHARARRAYOID => {
            let val = <Vec<Option<Bpchar>> as ArrowArrayToPgType<
                StringArray,
                Vec<Option<Bpchar>>,
            >>::as_pg(list_array.into(), element_typoid, typmod, None);
            val.into_datum()
        }
        BITARRAYOID => {
            let val =
                <Vec<Option<Bit>> as ArrowArrayToPgType<StringArray, Vec<Option<Bit>>>>::as_pg(
                    list_array.into(),
                    element_typoid,
                    typmod,
                    None,
                );
            val.into_datum()
        }
        VARBITARRAYOID => {
            let val = <Vec<Option<VarBit>> as ArrowArrayToPgType<
                StringArray,
                Vec<Option<VarBit>>,
            >>::as_pg(list_array.into(), element_typoid, typmod, None);
            val.into_datum()
        }
        BYTEAARRAYOID => {
            let val = <Vec<Option<Vec<u8>>> as ArrowArrayToPgType<
                BinaryArray,
                Vec<Option<Vec<u8>>>,
            >>::as_pg(list_array.into(), element_typoid, typmod, None);
            val.into_datum()
        }
        OIDARRAYOID => {
            let val =
                <Vec<Option<Oid>> as ArrowArrayToPgType<UInt32Array, Vec<Option<Oid>>>>::as_pg(
                    list_array.into(),
                    element_typoid,
                    typmod,
                    None,
                );
            val.into_datum()
        }
        NUMERICARRAYOID => {
            let val = <Vec<Option<AnyNumeric>> as ArrowArrayToPgType<
                Decimal128Array,
                Vec<Option<AnyNumeric>>,
            >>::as_pg(list_array.into(), element_typoid, typmod, None);
            val.into_datum()
        }
        DATEARRAYOID => {
            let val =
                <Vec<Option<Date>> as ArrowArrayToPgType<Date32Array, Vec<Option<Date>>>>::as_pg(
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
            >>::as_pg(list_array.into(), element_typoid, typmod, None);
            val.into_datum()
        }
        TIMETZARRAYOID => {
            let val = <Vec<Option<TimeWithTimeZone>> as ArrowArrayToPgType<
                Time64MicrosecondArray,
                Vec<Option<TimeWithTimeZone>>,
            >>::as_pg(list_array.into(), element_typoid, typmod, None);
            val.into_datum()
        }
        TIMESTAMPARRAYOID => {
            let val = <Vec<Option<Timestamp>> as ArrowArrayToPgType<
                TimestampMicrosecondArray,
                Vec<Option<Timestamp>>,
            >>::as_pg(list_array.into(), element_typoid, typmod, None);
            val.into_datum()
        }
        TIMESTAMPTZARRAYOID => {
            let val = <Vec<Option<TimestampWithTimeZone>> as ArrowArrayToPgType<
                TimestampMicrosecondArray,
                Vec<Option<TimestampWithTimeZone>>,
            >>::as_pg(list_array.into(), element_typoid, typmod, None);
            val.into_datum()
        }
        INTERVALARRAYOID => {
            let val = <Vec<Option<Interval>> as ArrowArrayToPgType<
                IntervalMonthDayNanoArray,
                Vec<Option<Interval>>,
            >>::as_pg(list_array.into(), element_typoid, typmod, None);
            val.into_datum()
        }
        UUIDARRAYOID => {
            let val = <Vec<Option<Uuid>> as ArrowArrayToPgType<
                FixedSizeBinaryArray,
                Vec<Option<Uuid>>,
            >>::as_pg(list_array.into(), element_typoid, typmod, None);
            val.into_datum()
        }
        JSONARRAYOID => {
            let val =
                <Vec<Option<Json>> as ArrowArrayToPgType<StringArray, Vec<Option<Json>>>>::as_pg(
                    list_array.into(),
                    element_typoid,
                    typmod,
                    None,
                );
            val.into_datum()
        }
        JSONBARRAYOID => {
            let val =
                <Vec<Option<JsonB>> as ArrowArrayToPgType<StringArray, Vec<Option<JsonB>>>>::as_pg(
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
                >>::as_pg(
                    list_array.into(), element_typoid, typmod, Some(tupledesc)
                );

                val.into_datum()
            } else if is_enum_typoid(element_typoid) {
                let val = <Vec<Option<Enum>> as ArrowArrayToPgType<
                    StringArray,
                    Vec<Option<Enum>>,
                >>::as_pg(list_array.into(), element_typoid, typmod, None);
                val.into_datum()
            } else {
                panic!("unsupported array type {:?}", typoid)
            }
        }
    }
}
