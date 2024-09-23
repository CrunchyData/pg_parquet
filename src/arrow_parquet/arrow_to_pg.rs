use arrow::array::{
    Array, ArrayData, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, ListArray, MapArray, StringArray,
    StructArray, Time64MicrosecondArray, TimestampMicrosecondArray, UInt32Array,
};
use pgrx::{
    datum::{Date, Time, TimeWithTimeZone, Timestamp, TimestampWithTimeZone},
    pg_sys::{
        Datum, Oid, BOOLOID, BYTEAOID, CHAROID, DATEOID, FLOAT4OID, FLOAT8OID, INT2OID, INT4OID,
        INT8OID, NUMERICOID, OIDOID, TEXTOID, TIMEOID, TIMESTAMPOID, TIMESTAMPTZOID, TIMETZOID,
    },
    prelude::PgHeapTuple,
    AllocatedByRust, AnyNumeric, IntoDatum, PgTupleDesc,
};

use crate::{
    pgrx_utils::{array_element_typoid, is_array_type, is_composite_type, tuple_desc},
    type_compat::{
        fallback_to_text::{reset_fallback_to_text_context, FallbackToText},
        geometry::{is_postgis_geometry_typoid, Geometry},
        map::{is_crunchy_map_typoid, CrunchyMap},
        pg_arrow_type_conversions::{
            extract_precision_from_numeric_typmod, extract_scale_from_numeric_typmod,
            MAX_DECIMAL_PRECISION,
        },
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
pub(crate) mod map;
pub(crate) mod numeric;
pub(crate) mod oid;
pub(crate) mod record;
pub(crate) mod text;
pub(crate) mod time;
pub(crate) mod timestamp;
pub(crate) mod timestamptz;
pub(crate) mod timetz;

pub(crate) trait ArrowArrayToPgType<'a, A: From<ArrayData>, T: 'a + IntoDatum> {
    fn to_pg_type(array: A, context: ArrowToPgPerAttributeContext<'a>) -> Option<T>;
}

#[derive(Clone)]
pub(crate) struct ArrowToPgPerAttributeContext<'a> {
    typoid: Oid,
    typmod: i32,
    tupledesc: Option<PgTupleDesc<'a>>,
    precision: Option<usize>,
    scale: Option<usize>,
}

impl<'a> ArrowToPgPerAttributeContext<'a> {
    pub(crate) fn new(typoid: Oid, typmod: i32) -> Self {
        Self {
            typoid,
            typmod,
            tupledesc: None,
            precision: None,
            scale: None,
        }
    }

    pub(crate) fn with_tupledesc(mut self, tupledesc: PgTupleDesc<'a>) -> Self {
        self.tupledesc = Some(tupledesc);
        self
    }

    pub(crate) fn with_precision(mut self, precision: usize) -> Self {
        self.precision = Some(precision);
        self
    }

    pub(crate) fn with_scale(mut self, scale: usize) -> Self {
        self.scale = Some(scale);
        self
    }
}

pub(crate) fn to_pg_datum(
    attribute_array: ArrayData,
    attribute_typoid: Oid,
    attribute_typmod: i32,
) -> Option<Datum> {
    if is_array_type(attribute_typoid) {
        let attribute_element_typoid = array_element_typoid(attribute_typoid);

        let attribute_context =
            ArrowToPgPerAttributeContext::new(attribute_element_typoid, attribute_typmod);

        to_pg_array_datum(attribute_array.into(), attribute_context)
    } else {
        let attribute_context =
            ArrowToPgPerAttributeContext::new(attribute_typoid, attribute_typmod);

        to_pg_nonarray_datum(attribute_array, attribute_context)
    }
}

fn to_pg_nonarray_datum(
    primitive_array: ArrayData,
    attribute_context: ArrowToPgPerAttributeContext,
) -> Option<Datum> {
    match attribute_context.typoid {
        FLOAT4OID => {
            let val = <f32 as ArrowArrayToPgType<Float32Array, f32>>::to_pg_type(
                primitive_array.into(),
                attribute_context,
            );
            val.into_datum()
        }
        FLOAT8OID => {
            let val = <f64 as ArrowArrayToPgType<Float64Array, f64>>::to_pg_type(
                primitive_array.into(),
                attribute_context,
            );
            val.into_datum()
        }
        INT2OID => {
            let val = <i16 as ArrowArrayToPgType<Int16Array, i16>>::to_pg_type(
                primitive_array.into(),
                attribute_context,
            );
            val.into_datum()
        }
        INT4OID => {
            let val = <i32 as ArrowArrayToPgType<Int32Array, i32>>::to_pg_type(
                primitive_array.into(),
                attribute_context,
            );
            val.into_datum()
        }
        INT8OID => {
            let val = <i64 as ArrowArrayToPgType<Int64Array, i64>>::to_pg_type(
                primitive_array.into(),
                attribute_context,
            );
            val.into_datum()
        }
        BOOLOID => {
            let val = <bool as ArrowArrayToPgType<BooleanArray, bool>>::to_pg_type(
                primitive_array.into(),
                attribute_context,
            );
            val.into_datum()
        }
        CHAROID => {
            let val = <i8 as ArrowArrayToPgType<StringArray, i8>>::to_pg_type(
                primitive_array.into(),
                attribute_context,
            );
            val.into_datum()
        }
        TEXTOID => {
            let val = <String as ArrowArrayToPgType<StringArray, String>>::to_pg_type(
                primitive_array.into(),
                attribute_context,
            );
            val.into_datum()
        }
        BYTEAOID => {
            let val = <Vec<u8> as ArrowArrayToPgType<BinaryArray, Vec<u8>>>::to_pg_type(
                primitive_array.into(),
                attribute_context,
            );
            val.into_datum()
        }
        OIDOID => {
            let val = <Oid as ArrowArrayToPgType<UInt32Array, Oid>>::to_pg_type(
                primitive_array.into(),
                attribute_context,
            );
            val.into_datum()
        }
        NUMERICOID => to_pg_numeric_datum(primitive_array, attribute_context),
        DATEOID => {
            let val = <Date as ArrowArrayToPgType<Date32Array, Date>>::to_pg_type(
                primitive_array.into(),
                attribute_context,
            );
            val.into_datum()
        }
        TIMEOID => {
            let val = <Time as ArrowArrayToPgType<Time64MicrosecondArray, Time>>::to_pg_type(
                primitive_array.into(),
                attribute_context,
            );
            val.into_datum()
        }
        TIMETZOID => {
            let val = <TimeWithTimeZone as ArrowArrayToPgType<
                Time64MicrosecondArray,
                TimeWithTimeZone,
            >>::to_pg_type(primitive_array.into(), attribute_context);
            val.into_datum()
        }
        TIMESTAMPOID => {
            let val =
                <Timestamp as ArrowArrayToPgType<TimestampMicrosecondArray, Timestamp>>::to_pg_type(
                    primitive_array.into(),
                    attribute_context,
                );
            val.into_datum()
        }
        TIMESTAMPTZOID => {
            let val = <TimestampWithTimeZone as ArrowArrayToPgType<
                TimestampMicrosecondArray,
                TimestampWithTimeZone,
            >>::to_pg_type(primitive_array.into(), attribute_context);
            val.into_datum()
        }
        _ => {
            if is_composite_type(attribute_context.typoid) {
                let attribute_tupledesc =
                    tuple_desc(attribute_context.typoid, attribute_context.typmod);
                let attribute_context = attribute_context.with_tupledesc(attribute_tupledesc);

                let val = <PgHeapTuple<AllocatedByRust> as ArrowArrayToPgType<
                    StructArray,
                    PgHeapTuple<AllocatedByRust>,
                >>::to_pg_type(primitive_array.into(), attribute_context);

                val.into_datum()
            } else if is_crunchy_map_typoid(attribute_context.typoid) {
                let val = <CrunchyMap as ArrowArrayToPgType<MapArray, CrunchyMap>>::to_pg_type(
                    primitive_array.into(),
                    attribute_context,
                );

                val.into_datum()
            } else if is_postgis_geometry_typoid(attribute_context.typoid) {
                let val = <Geometry as ArrowArrayToPgType<BinaryArray, Geometry>>::to_pg_type(
                    primitive_array.into(),
                    attribute_context,
                );

                val.into_datum()
            } else {
                reset_fallback_to_text_context(attribute_context.typoid, attribute_context.typmod);

                let val =
                    <FallbackToText as ArrowArrayToPgType<StringArray, FallbackToText>>::to_pg_type(
                        primitive_array.into(),
                        attribute_context,
                    );

                val.into_datum()
            }
        }
    }
}

fn to_pg_array_datum(
    list_array: ListArray,
    attribute_context: ArrowToPgPerAttributeContext,
) -> Option<Datum> {
    if list_array.is_null(0) {
        return None;
    }

    let list_array = list_array.value(0).to_data();

    match attribute_context.typoid {
        FLOAT4OID => {
            let val =
                <Vec<Option<f32>> as ArrowArrayToPgType<Float32Array, Vec<Option<f32>>>>::to_pg_type(
                    list_array.into(),
                    attribute_context
                );
            val.into_datum()
        }
        FLOAT8OID => {
            let val =
                <Vec<Option<f64>> as ArrowArrayToPgType<Float64Array, Vec<Option<f64>>>>::to_pg_type(
                    list_array.into(),
                    attribute_context
                );
            val.into_datum()
        }
        INT2OID => {
            let val =
                <Vec<Option<i16>> as ArrowArrayToPgType<Int16Array, Vec<Option<i16>>>>::to_pg_type(
                    list_array.into(),
                    attribute_context,
                );
            val.into_datum()
        }
        INT4OID => {
            let val =
                <Vec<Option<i32>> as ArrowArrayToPgType<Int32Array, Vec<Option<i32>>>>::to_pg_type(
                    list_array.into(),
                    attribute_context,
                );
            val.into_datum()
        }
        INT8OID => {
            let val =
                <Vec<Option<i64>> as ArrowArrayToPgType<Int64Array, Vec<Option<i64>>>>::to_pg_type(
                    list_array.into(),
                    attribute_context,
                );
            val.into_datum()
        }
        BOOLOID => {
            let val =
                <Vec<Option<bool>> as ArrowArrayToPgType<BooleanArray, Vec<Option<bool>>>>::to_pg_type(
                    list_array.into(),
                    attribute_context
                );
            val.into_datum()
        }
        CHAROID => {
            let val =
                <Vec<Option<i8>> as ArrowArrayToPgType<StringArray, Vec<Option<i8>>>>::to_pg_type(
                    list_array.into(),
                    attribute_context,
                );
            val.into_datum()
        }
        TEXTOID => {
            let val = <Vec<Option<String>> as ArrowArrayToPgType<
                StringArray,
                Vec<Option<String>>,
            >>::to_pg_type(list_array.into(), attribute_context);
            val.into_datum()
        }
        BYTEAOID => {
            let val = <Vec<Option<Vec<u8>>> as ArrowArrayToPgType<
                BinaryArray,
                Vec<Option<Vec<u8>>>,
            >>::to_pg_type(list_array.into(), attribute_context);
            val.into_datum()
        }
        OIDOID => {
            let val =
                <Vec<Option<Oid>> as ArrowArrayToPgType<UInt32Array, Vec<Option<Oid>>>>::to_pg_type(
                    list_array.into(),
                    attribute_context,
                );
            val.into_datum()
        }
        NUMERICOID => to_pg_numeric_array_datum(list_array, attribute_context),
        DATEOID => {
            let val =
                <Vec<Option<Date>> as ArrowArrayToPgType<Date32Array, Vec<Option<Date>>>>::to_pg_type(
                    list_array.into(),
                    attribute_context
                );
            val.into_datum()
        }
        TIMEOID => {
            let val = <Vec<Option<Time>> as ArrowArrayToPgType<
                Time64MicrosecondArray,
                Vec<Option<Time>>,
            >>::to_pg_type(list_array.into(), attribute_context);
            val.into_datum()
        }
        TIMETZOID => {
            let val = <Vec<Option<TimeWithTimeZone>> as ArrowArrayToPgType<
                Time64MicrosecondArray,
                Vec<Option<TimeWithTimeZone>>,
            >>::to_pg_type(list_array.into(), attribute_context);
            val.into_datum()
        }
        TIMESTAMPOID => {
            let val = <Vec<Option<Timestamp>> as ArrowArrayToPgType<
                TimestampMicrosecondArray,
                Vec<Option<Timestamp>>,
            >>::to_pg_type(list_array.into(), attribute_context);
            val.into_datum()
        }
        TIMESTAMPTZOID => {
            let val = <Vec<Option<TimestampWithTimeZone>> as ArrowArrayToPgType<
                TimestampMicrosecondArray,
                Vec<Option<TimestampWithTimeZone>>,
            >>::to_pg_type(list_array.into(), attribute_context);
            val.into_datum()
        }
        _ => {
            if is_composite_type(attribute_context.typoid) {
                let attribute_tupledesc =
                    tuple_desc(attribute_context.typoid, attribute_context.typmod);
                let attribute_context = attribute_context.with_tupledesc(attribute_tupledesc);

                let val = <Vec<Option<PgHeapTuple<AllocatedByRust>>> as ArrowArrayToPgType<
                    StructArray,
                    Vec<Option<PgHeapTuple<AllocatedByRust>>>,
                >>::to_pg_type(list_array.into(), attribute_context);

                val.into_datum()
            } else if is_crunchy_map_typoid(attribute_context.typoid) {
                let val = <Vec<Option<CrunchyMap>> as ArrowArrayToPgType<
                    MapArray,
                    Vec<Option<CrunchyMap>>,
                >>::to_pg_type(list_array.into(), attribute_context);

                val.into_datum()
            } else if is_postgis_geometry_typoid(attribute_context.typoid) {
                let val = <Vec<Option<Geometry>> as ArrowArrayToPgType<
                    BinaryArray,
                    Vec<Option<Geometry>>,
                >>::to_pg_type(list_array.into(), attribute_context);

                val.into_datum()
            } else {
                reset_fallback_to_text_context(attribute_context.typoid, attribute_context.typmod);

                let val = <Vec<Option<FallbackToText>> as ArrowArrayToPgType<
                    StringArray,
                    Vec<Option<FallbackToText>>,
                >>::to_pg_type(list_array.into(), attribute_context);

                val.into_datum()
            }
        }
    }
}

fn to_pg_numeric_datum(
    numeric_array: ArrayData,
    attribute_context: ArrowToPgPerAttributeContext,
) -> Option<Datum> {
    let precision = extract_precision_from_numeric_typmod(attribute_context.typmod);

    if precision > MAX_DECIMAL_PRECISION {
        reset_fallback_to_text_context(attribute_context.typoid, attribute_context.typmod);

        let val = <FallbackToText as ArrowArrayToPgType<StringArray, FallbackToText>>::to_pg_type(
            numeric_array.into(),
            attribute_context,
        );

        val.into_datum()
    } else {
        let scale = extract_scale_from_numeric_typmod(attribute_context.typmod);

        let val = <AnyNumeric as ArrowArrayToPgType<Decimal128Array, AnyNumeric>>::to_pg_type(
            numeric_array.into(),
            attribute_context
                .with_precision(precision)
                .with_scale(scale),
        );

        val.into_datum()
    }
}

fn to_pg_numeric_array_datum(
    numeric_list_array: ArrayData,
    attribute_context: ArrowToPgPerAttributeContext,
) -> Option<Datum> {
    let precision = extract_precision_from_numeric_typmod(attribute_context.typmod);

    if precision > MAX_DECIMAL_PRECISION {
        reset_fallback_to_text_context(attribute_context.typoid, attribute_context.typmod);

        let val = <Vec<Option<FallbackToText>> as ArrowArrayToPgType<
            StringArray,
            Vec<Option<FallbackToText>>,
        >>::to_pg_type(numeric_list_array.into(), attribute_context);

        val.into_datum()
    } else {
        let scale = extract_scale_from_numeric_typmod(attribute_context.typmod);

        let val = <Vec<Option<AnyNumeric>> as ArrowArrayToPgType<
            Decimal128Array,
            Vec<Option<AnyNumeric>>,
        >>::to_pg_type(
            numeric_list_array.into(),
            attribute_context
                .with_precision(precision)
                .with_scale(scale),
        );

        val.into_datum()
    }
}
