use arrow::array::{ArrayRef, Decimal128Array};
use context::PgToArrowAttributeContext;
use pgrx::{
    check_for_interrupts,
    datum::{Date, Time, TimeWithTimeZone, Timestamp, TimestampWithTimeZone, UnboxDatum},
    heap_tuple::PgHeapTuple,
    pg_sys::{
        Datum, Oid, BOOLOID, BYTEAOID, CHAROID, DATEOID, FLOAT4OID, FLOAT8OID, INT2OID, INT4OID,
        INT8OID, NUMERICOID, OIDOID, TEXTOID, TIMEOID, TIMESTAMPOID, TIMESTAMPTZOID, TIMETZOID,
        UUIDOID,
    },
    AllocatedByRust, AnyNumeric, FromDatum, Uuid,
};
use std::sync::Arc;

use crate::{
    pgrx_utils::{
        array_element_typoid, collect_attributes_for, domain_array_base_elem_type, is_array_type,
        is_composite_type, tuple_desc, AnyArrayDatum, CollectAttributesFor,
    },
    type_compat::{
        fallback_to_text::{reset_fallback_to_text_context, FallbackToText},
        geometry::{is_postgis_geography_type, is_postgis_geometry_type, Geography, Geometry},
        map::{is_map_type, reset_map_type_context, Map},
        pg_arrow_type_conversions::{
            extract_precision_and_scale_from_numeric_typmod, is_unbounded_numeric_typmod,
            numeric_datum_to_i128, numeric_to_i128, should_write_numeric_as_text,
        },
    },
};

#[cfg(not(pre_pg19))]
use crate::type_compat::oid8::Oid8;

pub mod bool;
pub mod bytea;
pub mod char;
pub mod composite;
pub mod context;
pub mod date;
pub mod fallback_to_text;
pub mod float4;
pub mod float8;
pub mod geography;
pub mod geometry;
pub mod int2;
pub mod int4;
pub mod int8;
pub mod map;
pub mod numeric;
pub mod oid;
#[cfg(not(pre_pg19))]
pub mod oid8;
pub mod text;
pub mod time;
pub mod timestamp;
pub mod timestamptz;
pub mod timetz;
pub mod uuid;

pub trait PgTypeToArrowArray<T: FromDatum + UnboxDatum> {
    fn to_arrow_array(self, context: &PgToArrowAttributeContext) -> ArrayRef;
}

pub fn to_arrow_array(
    datums: &[Option<Datum>],
    attribute_context: &PgToArrowAttributeContext,
) -> ArrayRef {
    if attribute_context.is_array() {
        to_arrow_list_array(datums, attribute_context)
    } else {
        to_arrow_primitive_array(datums, attribute_context)
    }
}

macro_rules! to_arrow_primitive_array {
    ($pg_type:ty, $datums:expr, $attribute_context:expr) => {{
        let mut attribute_vals = Vec::with_capacity($datums.len());

        for datum in $datums {
            check_for_interrupts!();

            let attribute_val: Option<$pg_type> = match datum {
                Some(datum) => unsafe { <$pg_type>::from_datum(*datum, false) },
                None => None,
            };

            attribute_vals.push(attribute_val);
        }

        return attribute_vals.to_arrow_array($attribute_context);
    }};
}

macro_rules! to_arrow_list_array {
    ($pg_type:ty, $datums:expr, $attribute_context:expr) => {{
        let mut attribute_vals = vec![];

        for datum in $datums {
            check_for_interrupts!();

            {
                let attribute_val: Option<$pg_type> = match datum {
                    Some(datum) => unsafe { <$pg_type>::from_datum(*datum, false) },
                    None => None,
                };

                attribute_vals.push(attribute_val);
            }
        }

        let attribute_vals = attribute_vals
            .iter()
            .map(|val| val.as_ref().map(|val| val.iter().collect::<Vec<_>>()))
            .collect::<Vec<_>>();

        return attribute_vals.to_arrow_array($attribute_context);
    }};
}

// reject_multi_dimensional_arrays panics for an array value with more than one dimension. A
// Arrow list is one dimensional, so writing such a value would flatten it and lose its shape.
fn reject_multi_dimensional_arrays(
    datums: &[Option<Datum>],
    attribute_context: &PgToArrowAttributeContext,
) {
    for datum in datums {
        check_for_interrupts!();

        let Some(datum) = datum else {
            continue;
        };

        // a null value has no dimensions
        let ndim = unsafe { AnyArrayDatum::from_datum(*datum, false) }
            .map(|array| array.ndim())
            .unwrap_or(1);

        if ndim > 1 {
            panic!(
                "column \"{}\" has a {}-dimensional array value, but an Arrow list is one-dimensional",
                attribute_context.field().name(),
                ndim
            );
        }
    }
}

fn to_arrow_primitive_array(
    datums: &[Option<Datum>],
    attribute_context: &PgToArrowAttributeContext,
) -> ArrayRef {
    match attribute_context.typoid() {
        FLOAT4OID => to_arrow_primitive_array!(f32, datums, attribute_context),
        FLOAT8OID => to_arrow_primitive_array!(f64, datums, attribute_context),
        INT2OID => to_arrow_primitive_array!(i16, datums, attribute_context),
        INT4OID => to_arrow_primitive_array!(i32, datums, attribute_context),
        INT8OID => to_arrow_primitive_array!(i64, datums, attribute_context),
        UUIDOID => to_arrow_primitive_array!(Uuid, datums, attribute_context),
        NUMERICOID => {
            let precision = attribute_context.precision();

            if should_write_numeric_as_text(precision) {
                reset_fallback_to_text_context(
                    attribute_context.typoid(),
                    attribute_context.typmod(),
                );

                to_arrow_primitive_array!(FallbackToText, datums, attribute_context)
            } else {
                to_arrow_decimal_array(datums, attribute_context)
            }
        }
        BOOLOID => to_arrow_primitive_array!(bool, datums, attribute_context),
        DATEOID => to_arrow_primitive_array!(Date, datums, attribute_context),
        TIMEOID => to_arrow_primitive_array!(Time, datums, attribute_context),
        TIMETZOID => to_arrow_primitive_array!(TimeWithTimeZone, datums, attribute_context),
        TIMESTAMPOID => to_arrow_primitive_array!(Timestamp, datums, attribute_context),
        TIMESTAMPTZOID => {
            to_arrow_primitive_array!(TimestampWithTimeZone, datums, attribute_context)
        }
        CHAROID => to_arrow_primitive_array!(i8, datums, attribute_context),
        TEXTOID => to_arrow_primitive_array!(String, datums, attribute_context),
        BYTEAOID => to_arrow_primitive_array!(&[u8], datums, attribute_context),
        OIDOID => to_arrow_primitive_array!(Oid, datums, attribute_context),
        #[cfg(not(pre_pg19))]
        pgrx::pg_sys::OID8OID => to_arrow_primitive_array!(Oid8, datums, attribute_context),
        _ => {
            if attribute_context.is_composite() {
                let mut attribute_vals = vec![];

                let attribute_tupledesc = attribute_context.tupledesc();

                for datum in datums {
                    check_for_interrupts!();

                    if let Some(datum) = datum {
                        let attribute_val: Option<PgHeapTuple<AllocatedByRust>> =
                            unsafe { PgHeapTuple::from_datum(*datum, false) };

                        // this trick is needed to avoid having a bunch of
                        // reference counted tupledesc which comes from pgrx's "get_by_name".
                        // we first convert PgHeapTuple into unsafe HeapTuple to drop
                        // the reference counted tupledesc and then convert it back to
                        // PgHeapTuple by reusing the same tupledesc that we created
                        // before the loop. Only overhead is 1 "heap_copy_tuple" call.
                        let attribute_val = attribute_val.map(|tuple| tuple.into_pg());
                        let attribute_val = attribute_val.map(|tuple| unsafe {
                            PgHeapTuple::from_heap_tuple(attribute_tupledesc.clone(), tuple)
                                .into_owned()
                        });

                        attribute_vals.push(attribute_val);
                    } else {
                        attribute_vals.push(None);
                    }
                }

                attribute_vals.to_arrow_array(attribute_context)
            } else if attribute_context.is_map() {
                reset_map_type_context(attribute_context.typoid());

                to_arrow_primitive_array!(Map, datums, attribute_context)
            } else if attribute_context.is_geometry() {
                to_arrow_primitive_array!(Geometry, datums, attribute_context)
            } else if attribute_context.is_geography() {
                to_arrow_primitive_array!(Geography, datums, attribute_context)
            } else {
                reset_fallback_to_text_context(
                    attribute_context.typoid(),
                    attribute_context.typmod(),
                );

                to_arrow_primitive_array!(FallbackToText, datums, attribute_context)
            }
        }
    }
}

// to_arrow_decimal_array converts numeric datums into an arrow decimal array. It decodes
// the numeric digits in place and only falls back to the numeric_out based conversion for
// the values that need it (NaN, +-Inf, unbounded numerics, rounding, i128 overflow).
fn to_arrow_decimal_array(
    datums: &[Option<Datum>],
    attribute_context: &PgToArrowAttributeContext,
) -> ArrayRef {
    let precision = attribute_context.precision();
    let scale = attribute_context.scale();
    let typmod = attribute_context.typmod();
    let needs_general_path = is_unbounded_numeric_typmod(typmod);

    let mut decimals = Vec::with_capacity(datums.len());

    for datum in datums {
        check_for_interrupts!();

        let decimal = datum.map(|datum| {
            let fast_decimal = if needs_general_path {
                None
            } else {
                numeric_datum_to_i128(datum, scale)
            };

            fast_decimal.unwrap_or_else(|| {
                let numeric: AnyNumeric =
                    unsafe { AnyNumeric::from_datum(datum, false) }.expect("numeric is null");

                numeric_to_i128(numeric, typmod, attribute_context.field().name())
            })
        });

        decimals.push(decimal);
    }

    let decimal_array = Decimal128Array::from(decimals)
        .with_precision_and_scale(precision as _, scale as _)
        .unwrap_or_else(|e| panic!("failed to create Decimal128Array: {e}"));

    Arc::new(decimal_array)
}

fn to_arrow_list_array(
    datums: &[Option<Datum>],
    attribute_context: &PgToArrowAttributeContext,
) -> ArrayRef {
    reject_multi_dimensional_arrays(datums, attribute_context);

    let element_context = attribute_context.element_context();
    let element_typoid = element_context.typoid();
    let element_typmod = element_context.typmod();

    match element_typoid {
        FLOAT4OID => to_arrow_list_array!(pgrx::Array<f32>, datums, element_context),
        FLOAT8OID => to_arrow_list_array!(pgrx::Array<f64>, datums, element_context),
        INT2OID => to_arrow_list_array!(pgrx::Array<i16>, datums, element_context),
        INT4OID => to_arrow_list_array!(pgrx::Array<i32>, datums, element_context),
        INT8OID => to_arrow_list_array!(pgrx::Array<i64>, datums, element_context),
        UUIDOID => to_arrow_list_array!(pgrx::Array<Uuid>, datums, element_context),
        NUMERICOID => {
            let precision = element_context.precision();

            if should_write_numeric_as_text(precision) {
                reset_fallback_to_text_context(element_typoid, element_typmod);

                to_arrow_list_array!(pgrx::Array<FallbackToText>, datums, element_context)
            } else {
                to_arrow_list_array!(pgrx::Array<AnyNumeric>, datums, element_context)
            }
        }
        BOOLOID => to_arrow_list_array!(pgrx::Array<bool>, datums, element_context),
        DATEOID => to_arrow_list_array!(pgrx::Array<Date>, datums, element_context),
        TIMEOID => to_arrow_list_array!(pgrx::Array<Time>, datums, element_context),
        TIMETZOID => {
            to_arrow_list_array!(pgrx::Array<TimeWithTimeZone>, datums, element_context)
        }
        TIMESTAMPOID => {
            to_arrow_list_array!(pgrx::Array<Timestamp>, datums, element_context)
        }
        TIMESTAMPTZOID => {
            to_arrow_list_array!(pgrx::Array<TimestampWithTimeZone>, datums, element_context)
        }
        CHAROID => to_arrow_list_array!(pgrx::Array<i8>, datums, element_context),
        TEXTOID => to_arrow_list_array!(pgrx::Array<String>, datums, element_context),
        BYTEAOID => to_arrow_list_array!(pgrx::Array<&[u8]>, datums, element_context),
        OIDOID => to_arrow_list_array!(pgrx::Array<Oid>, datums, element_context),
        #[cfg(not(pre_pg19))]
        pgrx::pg_sys::OID8OID => {
            to_arrow_list_array!(pgrx::Array<Oid8>, datums, element_context)
        }
        _ => {
            if element_context.is_composite() {
                let mut attribute_vals = vec![];

                let attribute_tupledesc = element_context.tupledesc();

                for datum in datums {
                    check_for_interrupts!();

                    if let Some(datum) = datum {
                        let attribute_val: Option<pgrx::Array<PgHeapTuple<AllocatedByRust>>> =
                            unsafe { pgrx::Array::from_datum(*datum, false) };

                        if let Some(attribute_val) = attribute_val {
                            let attribute_val = attribute_val
                                .iter()
                                .map(|tuple| tuple.map(|tuple| tuple.into_pg()))
                                .collect::<Vec<_>>();

                            let attribute_val = attribute_val
                                .iter()
                                .map(|tuple| {
                                    tuple.map(|tuple| unsafe {
                                        PgHeapTuple::from_heap_tuple(
                                            attribute_tupledesc.clone(),
                                            tuple,
                                        )
                                        .into_owned()
                                    })
                                })
                                .collect::<Vec<_>>();

                            attribute_vals.push(Some(attribute_val));
                        } else {
                            attribute_vals.push(None);
                        }
                    } else {
                        attribute_vals.push(None);
                    }
                }

                attribute_vals.to_arrow_array(element_context)
            } else if element_context.is_map() {
                reset_map_type_context(element_typoid);

                to_arrow_list_array!(pgrx::Array<Map>, datums, element_context)
            } else if element_context.is_geometry() {
                to_arrow_list_array!(pgrx::Array<Geometry>, datums, element_context)
            } else if element_context.is_geography() {
                to_arrow_list_array!(pgrx::Array<Geography>, datums, element_context)
            } else {
                reset_fallback_to_text_context(element_typoid, element_typmod);

                to_arrow_list_array!(pgrx::Array<FallbackToText>, datums, element_context)
            }
        }
    }
}
