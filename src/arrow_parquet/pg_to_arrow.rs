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

pub(crate) fn collect_attribute_array_from_tuples<'tup>(
    tuples: &'tup [Option<PgHeapTuple<'tup, AllocatedByRust>>],
    attribute_name: &'tup str,
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

        collect_array_attribute_array_from_tuples(tuples, attribute_context)
    } else {
        let attribute_context = PgToArrowPerAttributeContext::new(
            attribute_name,
            attribute_typoid,
            attribute_typmod,
            attribute_field,
        );

        collect_nonarray_attribute_array_from_tuples(tuples, attribute_context)
    }
}

fn collect_nonarray_attribute_array_from_tuples<'tup>(
    tuples: &'tup [Option<PgHeapTuple<'tup, AllocatedByRust>>],
    attribute_context: PgToArrowPerAttributeContext<'tup>,
) -> (FieldRef, ArrayRef) {
    match attribute_context.typoid {
        FLOAT4OID => {
            let mut attribute_values: Vec<Option<f32>> = Vec::new();

            collect_attribute_array_from_tuples_helper(
                tuples,
                &attribute_context,
                &mut attribute_values,
            );

            let (field, array) = attribute_values.to_arrow_array(attribute_context);
            (field, array)
        }
        FLOAT8OID => {
            let mut attribute_values: Vec<Option<f64>> = Vec::new();

            collect_attribute_array_from_tuples_helper(
                tuples,
                &attribute_context,
                &mut attribute_values,
            );

            let (field, array) = attribute_values.to_arrow_array(attribute_context);
            (field, array)
        }
        INT2OID => {
            let mut attribute_values: Vec<Option<i16>> = Vec::new();

            collect_attribute_array_from_tuples_helper(
                tuples,
                &attribute_context,
                &mut attribute_values,
            );

            let (field, array) = attribute_values.to_arrow_array(attribute_context);
            (field, array)
        }
        INT4OID => {
            let mut attribute_values: Vec<Option<i32>> = Vec::new();

            collect_attribute_array_from_tuples_helper(
                tuples,
                &attribute_context,
                &mut attribute_values,
            );

            let (field, array) = attribute_values.to_arrow_array(attribute_context);
            (field, array)
        }
        INT8OID => {
            let mut attribute_values: Vec<Option<i64>> = Vec::new();

            collect_attribute_array_from_tuples_helper(
                tuples,
                &attribute_context,
                &mut attribute_values,
            );

            let (field, array) = attribute_values.to_arrow_array(attribute_context);
            (field, array)
        }
        NUMERICOID => {
            let precision = extract_precision_from_numeric_typmod(attribute_context.typmod);
            if precision > MAX_DECIMAL_PRECISION {
                reset_fallback_to_text_context(attribute_context.typoid, attribute_context.typmod);

                let mut attribute_values: Vec<Option<FallbackToText>> = Vec::new();

                collect_attribute_array_from_tuples_helper(
                    tuples,
                    &attribute_context,
                    &mut attribute_values,
                );

                let (field, array) = attribute_values.to_arrow_array(attribute_context);
                (field, array)
            } else {
                let mut attribute_values: Vec<Option<AnyNumeric>> = Vec::new();

                collect_attribute_array_from_tuples_helper(
                    tuples,
                    &attribute_context,
                    &mut attribute_values,
                );

                let (field, array) = attribute_values.to_arrow_array(attribute_context);
                (field, array)
            }
        }
        BOOLOID => {
            let mut attribute_values: Vec<Option<bool>> = Vec::new();

            collect_attribute_array_from_tuples_helper(
                tuples,
                &attribute_context,
                &mut attribute_values,
            );

            let (field, array) = attribute_values.to_arrow_array(attribute_context);
            (field, array)
        }
        DATEOID => {
            let mut attribute_values: Vec<Option<Date>> = Vec::new();

            collect_attribute_array_from_tuples_helper(
                tuples,
                &attribute_context,
                &mut attribute_values,
            );

            let (field, array) = attribute_values.to_arrow_array(attribute_context);
            (field, array)
        }
        TIMEOID => {
            let mut attribute_values: Vec<Option<Time>> = Vec::new();

            collect_attribute_array_from_tuples_helper(
                tuples,
                &attribute_context,
                &mut attribute_values,
            );

            let (field, array) = attribute_values.to_arrow_array(attribute_context);
            (field, array)
        }
        TIMETZOID => {
            let mut attribute_values: Vec<Option<TimeWithTimeZone>> = Vec::new();

            collect_attribute_array_from_tuples_helper(
                tuples,
                &attribute_context,
                &mut attribute_values,
            );

            let (field, array) = attribute_values.to_arrow_array(attribute_context);
            (field, array)
        }
        TIMESTAMPOID => {
            let mut attribute_values: Vec<Option<Timestamp>> = Vec::new();

            collect_attribute_array_from_tuples_helper(
                tuples,
                &attribute_context,
                &mut attribute_values,
            );

            let (field, array) = attribute_values.to_arrow_array(attribute_context);
            (field, array)
        }
        TIMESTAMPTZOID => {
            let mut attribute_values: Vec<Option<TimestampWithTimeZone>> = Vec::new();

            collect_attribute_array_from_tuples_helper(
                tuples,
                &attribute_context,
                &mut attribute_values,
            );

            let (field, array) = attribute_values.to_arrow_array(attribute_context);
            (field, array)
        }
        CHAROID => {
            let mut attribute_values: Vec<Option<i8>> = Vec::new();

            collect_attribute_array_from_tuples_helper(
                tuples,
                &attribute_context,
                &mut attribute_values,
            );

            let (field, array) = attribute_values.to_arrow_array(attribute_context);
            (field, array)
        }
        TEXTOID => {
            let mut attribute_values: Vec<Option<String>> = Vec::new();

            collect_attribute_array_from_tuples_helper(
                tuples,
                &attribute_context,
                &mut attribute_values,
            );

            let (field, array) = attribute_values.to_arrow_array(attribute_context);
            (field, array)
        }
        BYTEAOID => {
            let mut attribute_values: Vec<Option<&[u8]>> = Vec::new();

            collect_attribute_array_from_tuples_helper(
                tuples,
                &attribute_context,
                &mut attribute_values,
            );

            let (field, array) = attribute_values.to_arrow_array(attribute_context);
            (field, array)
        }
        OIDOID => {
            let mut attribute_values: Vec<Option<Oid>> = Vec::new();

            collect_attribute_array_from_tuples_helper(
                tuples,
                &attribute_context,
                &mut attribute_values,
            );

            let (field, array) = attribute_values.to_arrow_array(attribute_context);
            (field, array)
        }
        _ => {
            if is_composite_type(attribute_context.typoid) {
                let mut attribute_values: Vec<Option<PgHeapTuple<AllocatedByRust>>> = Vec::new();

                collect_attribute_array_from_tuples_helper(
                    tuples,
                    &attribute_context,
                    &mut attribute_values,
                );

                let (field, array) = attribute_values.to_arrow_array(attribute_context);
                (field, array)
            } else if is_crunchy_map_typoid(attribute_context.typoid) {
                let mut attribute_values: Vec<Option<CrunchyMap>> = Vec::new();

                collect_attribute_array_from_tuples_helper(
                    tuples,
                    &attribute_context,
                    &mut attribute_values,
                );

                let (field, array) = attribute_values.to_arrow_array(attribute_context);
                (field, array)
            } else if is_postgis_geometry_typoid(attribute_context.typoid) {
                let mut attribute_values: Vec<Option<Geometry>> = Vec::new();

                collect_attribute_array_from_tuples_helper(
                    tuples,
                    &attribute_context,
                    &mut attribute_values,
                );

                let (field, array) = attribute_values.to_arrow_array(attribute_context);
                (field, array)
            } else {
                reset_fallback_to_text_context(attribute_context.typoid, attribute_context.typmod);

                let mut attribute_values: Vec<Option<FallbackToText>> = Vec::new();

                collect_attribute_array_from_tuples_helper(
                    tuples,
                    &attribute_context,
                    &mut attribute_values,
                );

                let (field, array) = attribute_values.to_arrow_array(attribute_context);
                (field, array)
            }
        }
    }
}

pub(crate) fn collect_array_attribute_array_from_tuples<'tup>(
    tuples: &'tup [Option<PgHeapTuple<'tup, AllocatedByRust>>],
    attribute_context: PgToArrowPerAttributeContext<'tup>,
) -> (FieldRef, ArrayRef) {
    match attribute_context.typoid {
        FLOAT4OID => {
            let mut attribute_values: Vec<Option<pgrx::Array<f32>>> = Vec::new();

            collect_attribute_array_from_tuples_helper(
                tuples,
                &attribute_context,
                &mut attribute_values,
            );

            let (field, array) = attribute_values.to_arrow_array(attribute_context);
            (field, array)
        }
        FLOAT8OID => {
            let mut attribute_values: Vec<Option<pgrx::Array<f64>>> = Vec::new();

            collect_attribute_array_from_tuples_helper(
                tuples,
                &attribute_context,
                &mut attribute_values,
            );

            let (field, array) = attribute_values.to_arrow_array(attribute_context);
            (field, array)
        }
        INT2OID => {
            let mut attribute_values: Vec<Option<pgrx::Array<i16>>> = Vec::new();

            collect_attribute_array_from_tuples_helper(
                tuples,
                &attribute_context,
                &mut attribute_values,
            );

            let (field, array) = attribute_values.to_arrow_array(attribute_context);
            (field, array)
        }
        INT4OID => {
            let mut attribute_values: Vec<Option<pgrx::Array<i32>>> = Vec::new();

            collect_attribute_array_from_tuples_helper(
                tuples,
                &attribute_context,
                &mut attribute_values,
            );

            let (field, array) = attribute_values.to_arrow_array(attribute_context);
            (field, array)
        }
        INT8OID => {
            let mut attribute_values: Vec<Option<pgrx::Array<i64>>> = Vec::new();

            collect_attribute_array_from_tuples_helper(
                tuples,
                &attribute_context,
                &mut attribute_values,
            );

            let (field, array) = attribute_values.to_arrow_array(attribute_context);
            (field, array)
        }
        NUMERICOID => {
            let precision = extract_precision_from_numeric_typmod(attribute_context.typmod);

            if precision > MAX_DECIMAL_PRECISION {
                reset_fallback_to_text_context(attribute_context.typoid, attribute_context.typmod);

                let mut attribute_values: Vec<Option<pgrx::Array<FallbackToText>>> = Vec::new();

                collect_attribute_array_from_tuples_helper(
                    tuples,
                    &attribute_context,
                    &mut attribute_values,
                );

                let (field, array) = attribute_values.to_arrow_array(attribute_context);
                (field, array)
            } else {
                let mut attribute_values: Vec<Option<pgrx::Array<AnyNumeric>>> = Vec::new();

                let scale = extract_scale_from_numeric_typmod(attribute_context.typmod);

                let attribute_context = attribute_context
                    .with_scale(scale)
                    .with_precision(precision);

                collect_attribute_array_from_tuples_helper(
                    tuples,
                    &attribute_context,
                    &mut attribute_values,
                );

                let (field, array) = attribute_values.to_arrow_array(attribute_context);
                (field, array)
            }
        }
        BOOLOID => {
            let mut attribute_values: Vec<Option<pgrx::Array<bool>>> = Vec::new();

            collect_attribute_array_from_tuples_helper(
                tuples,
                &attribute_context,
                &mut attribute_values,
            );

            let (field, array) = attribute_values.to_arrow_array(attribute_context);
            (field, array)
        }
        DATEOID => {
            let mut attribute_values: Vec<Option<pgrx::Array<Date>>> = Vec::new();

            collect_attribute_array_from_tuples_helper(
                tuples,
                &attribute_context,
                &mut attribute_values,
            );

            let (field, array) = attribute_values.to_arrow_array(attribute_context);
            (field, array)
        }
        TIMEOID => {
            let mut attribute_values: Vec<Option<pgrx::Array<Time>>> = Vec::new();

            collect_attribute_array_from_tuples_helper(
                tuples,
                &attribute_context,
                &mut attribute_values,
            );

            let (field, array) = attribute_values.to_arrow_array(attribute_context);
            (field, array)
        }
        TIMETZOID => {
            let mut attribute_values: Vec<Option<pgrx::Array<TimeWithTimeZone>>> = Vec::new();

            collect_attribute_array_from_tuples_helper(
                tuples,
                &attribute_context,
                &mut attribute_values,
            );

            let (field, array) = attribute_values.to_arrow_array(attribute_context);
            (field, array)
        }
        TIMESTAMPOID => {
            let mut attribute_values: Vec<Option<pgrx::Array<Timestamp>>> = Vec::new();

            collect_attribute_array_from_tuples_helper(
                tuples,
                &attribute_context,
                &mut attribute_values,
            );

            let (field, array) = attribute_values.to_arrow_array(attribute_context);
            (field, array)
        }
        TIMESTAMPTZOID => {
            let mut attribute_values: Vec<Option<pgrx::Array<TimestampWithTimeZone>>> = Vec::new();

            collect_attribute_array_from_tuples_helper(
                tuples,
                &attribute_context,
                &mut attribute_values,
            );

            let (field, array) = attribute_values.to_arrow_array(attribute_context);
            (field, array)
        }
        CHAROID => {
            let mut attribute_values: Vec<Option<pgrx::Array<i8>>> = Vec::new();

            collect_attribute_array_from_tuples_helper(
                tuples,
                &attribute_context,
                &mut attribute_values,
            );

            let (field, array) = attribute_values.to_arrow_array(attribute_context);
            (field, array)
        }
        TEXTOID => {
            let mut attribute_values: Vec<Option<pgrx::Array<String>>> = Vec::new();

            collect_attribute_array_from_tuples_helper(
                tuples,
                &attribute_context,
                &mut attribute_values,
            );

            let (field, array) = attribute_values.to_arrow_array(attribute_context);
            (field, array)
        }
        BYTEAOID => {
            let mut attribute_values: Vec<Option<pgrx::Array<&[u8]>>> = Vec::new();

            collect_attribute_array_from_tuples_helper(
                tuples,
                &attribute_context,
                &mut attribute_values,
            );

            let (field, array) = attribute_values.to_arrow_array(attribute_context);
            (field, array)
        }
        OIDOID => {
            let mut attribute_values: Vec<Option<pgrx::Array<Oid>>> = Vec::new();

            collect_attribute_array_from_tuples_helper(
                tuples,
                &attribute_context,
                &mut attribute_values,
            );

            let (field, array) = attribute_values.to_arrow_array(attribute_context);
            (field, array)
        }
        _ => {
            if is_composite_type(attribute_context.typoid) {
                let mut attribute_values: Vec<Option<pgrx::Array<PgHeapTuple<AllocatedByRust>>>> =
                    Vec::new();

                collect_attribute_array_from_tuples_helper(
                    tuples,
                    &attribute_context,
                    &mut attribute_values,
                );

                let (field, array) = attribute_values.to_arrow_array(attribute_context);
                (field, array)
            } else if is_crunchy_map_typoid(attribute_context.typoid) {
                let mut attribute_values: Vec<Option<pgrx::Array<CrunchyMap>>> = Vec::new();

                collect_attribute_array_from_tuples_helper(
                    tuples,
                    &attribute_context,
                    &mut attribute_values,
                );

                let (field, array) = attribute_values.to_arrow_array(attribute_context);
                (field, array)
            } else if is_postgis_geometry_typoid(attribute_context.typoid) {
                let mut attribute_values: Vec<Option<pgrx::Array<Geometry>>> = Vec::new();

                collect_attribute_array_from_tuples_helper(
                    tuples,
                    &attribute_context,
                    &mut attribute_values,
                );

                let (field, array) = attribute_values.to_arrow_array(attribute_context);
                (field, array)
            } else {
                reset_fallback_to_text_context(attribute_context.typoid, attribute_context.typmod);

                let mut attribute_values: Vec<Option<pgrx::Array<FallbackToText>>> = Vec::new();

                collect_attribute_array_from_tuples_helper(
                    tuples,
                    &attribute_context,
                    &mut attribute_values,
                );

                let (field, array) = attribute_values.to_arrow_array(attribute_context);
                (field, array)
            }
        }
    }
}

fn collect_attribute_array_from_tuples_helper<'tup, T>(
    tuples: &'tup [Option<PgHeapTuple<'tup, AllocatedByRust>>],
    attribute_context: &PgToArrowPerAttributeContext<'tup>,
    attribute_values: &mut Vec<Option<T>>,
) where
    T: IntoDatum + FromDatum + UnboxDatum<As<'tup> = T> + 'tup,
{
    for record in tuples.iter() {
        pgrx::pg_sys::check_for_interrupts!();

        if let Some(record) = record {
            let attribute_val: Option<T> = record.get_by_name(attribute_context.name).unwrap();
            attribute_values.push(attribute_val);
        } else {
            attribute_values.push(None);
        }
    }
}
