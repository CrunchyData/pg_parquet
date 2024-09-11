use arrow::{array::ArrayRef, datatypes::FieldRef};
use pgrx::{
    datum::{Date, Time, TimeWithTimeZone, Timestamp, TimestampWithTimeZone, UnboxDatum},
    heap_tuple::PgHeapTuple,
    pg_sys::{
        Oid, BOOLOID, BYTEAOID, CHAROID, DATEOID, FLOAT4OID, FLOAT8OID, INT2OID, INT4OID, INT8OID,
        NUMERICOID, OIDOID, TEXTOID, TIMEOID, TIMESTAMPOID, TIMESTAMPTZOID, TIMETZOID,
    },
    AllocatedByRust, AnyNumeric, FromDatum, IntoDatum,
};

use crate::{
    pgrx_utils::{array_element_typoid, is_array_type, is_composite_type},
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

pub(crate) trait PgTypeToArrowArray<T: IntoDatum + FromDatum + UnboxDatum> {
    fn to_arrow_array(self, context: PgToArrowPerAttributeContext) -> (FieldRef, ArrayRef);
}

#[derive(Clone)]
pub(crate) struct PgToArrowPerAttributeContext<'a> {
    name: &'a str,
    field: FieldRef,
    typoid: Oid,
    typmod: i32,
    scale: Option<usize>,
    precision: Option<usize>,
}

impl<'a> PgToArrowPerAttributeContext<'a> {
    fn new(name: &'a str, typoid: Oid, typmod: i32, field: FieldRef) -> Self {
        Self {
            name,
            field,
            typoid,
            typmod,
            scale: None,
            precision: None,
        }
    }

    fn with_scale(mut self, scale: usize) -> Self {
        self.scale = Some(scale);
        self
    }

    fn with_precision(mut self, precision: usize) -> Self {
        self.precision = Some(precision);
        self
    }
}

pub(crate) fn to_arrow_array(
    tuple: &PgHeapTuple<AllocatedByRust>,
    attribute_name: &str,
    attribute_typoid: Oid,
    attribute_typmod: i32,
    attribute_field: FieldRef,
) -> (FieldRef, ArrayRef) {
    if is_array_type(attribute_typoid) {
        let attribute_element_typoid = array_element_typoid(attribute_typoid);

        let attribute_context = PgToArrowPerAttributeContext::new(
            attribute_name,
            attribute_element_typoid,
            attribute_typmod,
            attribute_field,
        );

        to_arrow_list_array(tuple, attribute_context)
    } else {
        let attribute_context = PgToArrowPerAttributeContext::new(
            attribute_name,
            attribute_typoid,
            attribute_typmod,
            attribute_field,
        );

        to_arrow_primitive_array(tuple, attribute_context)
    }
}

fn to_arrow_primitive_array(
    tuple: &PgHeapTuple<AllocatedByRust>,
    attribute_context: PgToArrowPerAttributeContext,
) -> (FieldRef, ArrayRef) {
    match attribute_context.typoid {
        FLOAT4OID => {
            let attribute_val: Option<f32> = tuple.get_by_name(attribute_context.name).unwrap();

            let (field, array) = attribute_val.to_arrow_array(attribute_context);
            (field, array)
        }
        FLOAT8OID => {
            let attribute_val: Option<f64> = tuple.get_by_name(attribute_context.name).unwrap();

            let (field, array) = attribute_val.to_arrow_array(attribute_context);
            (field, array)
        }
        INT2OID => {
            let attribute_val: Option<i16> = tuple.get_by_name(attribute_context.name).unwrap();

            let (field, array) = attribute_val.to_arrow_array(attribute_context);
            (field, array)
        }
        INT4OID => {
            let attribute_val: Option<i32> = tuple.get_by_name(attribute_context.name).unwrap();

            let (field, array) = attribute_val.to_arrow_array(attribute_context);
            (field, array)
        }
        INT8OID => {
            let attribute_val: Option<i64> = tuple.get_by_name(attribute_context.name).unwrap();

            let (field, array) = attribute_val.to_arrow_array(attribute_context);
            (field, array)
        }
        NUMERICOID => {
            let precision = extract_precision_from_numeric_typmod(attribute_context.typmod);

            if precision > MAX_DECIMAL_PRECISION {
                reset_fallback_to_text_context(attribute_context.typoid, attribute_context.typmod);

                let attribute_val: Option<FallbackToText> =
                    tuple.get_by_name(attribute_context.name).unwrap();

                let (field, array) = attribute_val.to_arrow_array(attribute_context);
                (field, array)
            } else {
                let scale = extract_scale_from_numeric_typmod(attribute_context.typmod);

                let attribute_context = attribute_context
                    .with_scale(scale)
                    .with_precision(precision);

                let attribute_val: Option<AnyNumeric> =
                    tuple.get_by_name(attribute_context.name).unwrap();

                let (field, array) = attribute_val.to_arrow_array(attribute_context);
                (field, array)
            }
        }
        BOOLOID => {
            let attribute_val: Option<bool> = tuple.get_by_name(attribute_context.name).unwrap();

            let (field, array) = attribute_val.to_arrow_array(attribute_context);
            (field, array)
        }
        DATEOID => {
            let attribute_val: Option<Date> = tuple.get_by_name(attribute_context.name).unwrap();

            let (field, array) = attribute_val.to_arrow_array(attribute_context);
            (field, array)
        }
        TIMEOID => {
            let attribute_val: Option<Time> = tuple.get_by_name(attribute_context.name).unwrap();

            let (field, array) = attribute_val.to_arrow_array(attribute_context);
            (field, array)
        }
        TIMETZOID => {
            let attribute_val: Option<TimeWithTimeZone> =
                tuple.get_by_name(attribute_context.name).unwrap();

            let (field, array) = attribute_val.to_arrow_array(attribute_context);
            (field, array)
        }
        TIMESTAMPOID => {
            let attribute_val: Option<Timestamp> =
                tuple.get_by_name(attribute_context.name).unwrap();

            let (field, array) = attribute_val.to_arrow_array(attribute_context);
            (field, array)
        }
        TIMESTAMPTZOID => {
            let attribute_val: Option<TimestampWithTimeZone> =
                tuple.get_by_name(attribute_context.name).unwrap();

            let (field, array) = attribute_val.to_arrow_array(attribute_context);
            (field, array)
        }
        CHAROID => {
            let attribute_val: Option<i8> = tuple.get_by_name(attribute_context.name).unwrap();

            let (field, array) = attribute_val.to_arrow_array(attribute_context);
            (field, array)
        }
        TEXTOID => {
            let attribute_val: Option<String> = tuple.get_by_name(attribute_context.name).unwrap();

            let (field, array) = attribute_val.to_arrow_array(attribute_context);
            (field, array)
        }
        BYTEAOID => {
            let attribute_val: Option<&[u8]> = tuple.get_by_name(attribute_context.name).unwrap();

            let (field, array) = attribute_val.to_arrow_array(attribute_context);
            (field, array)
        }
        OIDOID => {
            let attribute_val: Option<Oid> = tuple.get_by_name(attribute_context.name).unwrap();

            let (field, array) = attribute_val.to_arrow_array(attribute_context);
            (field, array)
        }
        _ => {
            if is_composite_type(attribute_context.typoid) {
                let attribute_val: Option<PgHeapTuple<AllocatedByRust>> =
                    tuple.get_by_name(attribute_context.name).unwrap();

                let (field, array) = attribute_val.to_arrow_array(attribute_context);
                (field, array)
            } else if is_crunchy_map_typoid(attribute_context.typoid) {
                let attribute_val: Option<CrunchyMap> =
                    tuple.get_by_name(attribute_context.name).unwrap();

                let (field, array) = attribute_val.to_arrow_array(attribute_context);
                (field, array)
            } else if is_postgis_geometry_typoid(attribute_context.typoid) {
                let attribute_val: Option<Geometry> =
                    tuple.get_by_name(attribute_context.name).unwrap();

                let (field, array) = attribute_val.to_arrow_array(attribute_context);
                (field, array)
            } else {
                reset_fallback_to_text_context(attribute_context.typoid, attribute_context.typmod);

                let attribute_val: Option<FallbackToText> =
                    tuple.get_by_name(attribute_context.name).unwrap();

                let (field, array) = attribute_val.to_arrow_array(attribute_context);
                (field, array)
            }
        }
    }
}

pub(crate) fn to_arrow_list_array(
    tuple: &PgHeapTuple<AllocatedByRust>,
    attribute_context: PgToArrowPerAttributeContext,
) -> (FieldRef, ArrayRef) {
    match attribute_context.typoid {
        FLOAT4OID => {
            let attribute_val: Option<pgrx::Array<f32>> =
                tuple.get_by_name(attribute_context.name).unwrap();

            let (field, array) = attribute_val.to_arrow_array(attribute_context);
            (field, array)
        }
        FLOAT8OID => {
            let attribute_val: Option<pgrx::Array<f64>> =
                tuple.get_by_name(attribute_context.name).unwrap();

            let (field, array) = attribute_val.to_arrow_array(attribute_context);
            (field, array)
        }
        INT2OID => {
            let attribute_val: Option<pgrx::Array<i16>> =
                tuple.get_by_name(attribute_context.name).unwrap();

            let (field, array) = attribute_val.to_arrow_array(attribute_context);
            (field, array)
        }
        INT4OID => {
            let attribute_val: Option<pgrx::Array<i32>> =
                tuple.get_by_name(attribute_context.name).unwrap();

            let (field, array) = attribute_val.to_arrow_array(attribute_context);
            (field, array)
        }
        INT8OID => {
            let attribute_val: Option<pgrx::Array<i64>> =
                tuple.get_by_name(attribute_context.name).unwrap();

            let (field, array) = attribute_val.to_arrow_array(attribute_context);
            (field, array)
        }
        NUMERICOID => {
            let precision = extract_precision_from_numeric_typmod(attribute_context.typmod);

            if precision > MAX_DECIMAL_PRECISION {
                reset_fallback_to_text_context(attribute_context.typoid, attribute_context.typmod);

                let attribute_val: Option<pgrx::Array<FallbackToText>> =
                    tuple.get_by_name(attribute_context.name).unwrap();

                let (field, array) = attribute_val.to_arrow_array(attribute_context);
                (field, array)
            } else {
                let scale = extract_scale_from_numeric_typmod(attribute_context.typmod);

                let attribute_context = attribute_context
                    .with_scale(scale)
                    .with_precision(precision);

                let attribute_val: Option<pgrx::Array<AnyNumeric>> =
                    tuple.get_by_name(attribute_context.name).unwrap();

                let (field, array) = attribute_val.to_arrow_array(attribute_context);
                (field, array)
            }
        }
        BOOLOID => {
            let attribute_val: Option<pgrx::Array<bool>> =
                tuple.get_by_name(attribute_context.name).unwrap();

            let (field, array) = attribute_val.to_arrow_array(attribute_context);
            (field, array)
        }
        DATEOID => {
            let attribute_val: Option<pgrx::Array<Date>> =
                tuple.get_by_name(attribute_context.name).unwrap();

            let (field, array) = attribute_val.to_arrow_array(attribute_context);
            (field, array)
        }
        TIMEOID => {
            let attribute_val: Option<pgrx::Array<Time>> =
                tuple.get_by_name(attribute_context.name).unwrap();

            let (field, array) = attribute_val.to_arrow_array(attribute_context);
            (field, array)
        }
        TIMETZOID => {
            let attribute_val: Option<pgrx::Array<TimeWithTimeZone>> =
                tuple.get_by_name(attribute_context.name).unwrap();

            let (field, array) = attribute_val.to_arrow_array(attribute_context);
            (field, array)
        }
        TIMESTAMPOID => {
            let attribute_val: Option<pgrx::Array<Timestamp>> =
                tuple.get_by_name(attribute_context.name).unwrap();

            let (field, array) = attribute_val.to_arrow_array(attribute_context);
            (field, array)
        }
        TIMESTAMPTZOID => {
            let attribute_val: Option<pgrx::Array<TimestampWithTimeZone>> =
                tuple.get_by_name(attribute_context.name).unwrap();

            let (field, array) = attribute_val.to_arrow_array(attribute_context);
            (field, array)
        }
        CHAROID => {
            let attribute_val: Option<pgrx::Array<i8>> =
                tuple.get_by_name(attribute_context.name).unwrap();

            let (field, array) = attribute_val.to_arrow_array(attribute_context);
            (field, array)
        }
        TEXTOID => {
            let attribute_val: Option<pgrx::Array<String>> =
                tuple.get_by_name(attribute_context.name).unwrap();

            let (field, array) = attribute_val.to_arrow_array(attribute_context);
            (field, array)
        }
        BYTEAOID => {
            let attribute_val: Option<pgrx::Array<&[u8]>> =
                tuple.get_by_name(attribute_context.name).unwrap();

            let (field, array) = attribute_val.to_arrow_array(attribute_context);
            (field, array)
        }
        OIDOID => {
            let attribute_val: Option<pgrx::Array<Oid>> =
                tuple.get_by_name(attribute_context.name).unwrap();

            let (field, array) = attribute_val.to_arrow_array(attribute_context);
            (field, array)
        }
        _ => {
            if is_composite_type(attribute_context.typoid) {
                let attribute_val: Option<pgrx::Array<PgHeapTuple<AllocatedByRust>>> =
                    tuple.get_by_name(attribute_context.name).unwrap();

                let (field, array) = attribute_val.to_arrow_array(attribute_context);
                (field, array)
            } else if is_crunchy_map_typoid(attribute_context.typoid) {
                let attribute_val: Option<pgrx::Array<CrunchyMap>> =
                    tuple.get_by_name(attribute_context.name).unwrap();

                let (field, array) = attribute_val.to_arrow_array(attribute_context);
                (field, array)
            } else if is_postgis_geometry_typoid(attribute_context.typoid) {
                let attribute_val: Option<pgrx::Array<Geometry>> =
                    tuple.get_by_name(attribute_context.name).unwrap();

                let (field, array) = attribute_val.to_arrow_array(attribute_context);
                (field, array)
            } else {
                reset_fallback_to_text_context(attribute_context.typoid, attribute_context.typmod);

                let attribute_val: Option<pgrx::Array<FallbackToText>> =
                    tuple.get_by_name(attribute_context.name).unwrap();

                let (field, array) = attribute_val.to_arrow_array(attribute_context);
                (field, array)
            }
        }
    }
}
