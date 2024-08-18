use arrow::{array::ArrayRef, datatypes::FieldRef};
use pgrx::{
    heap_tuple::PgHeapTuple,
    pg_sys::{
        self, deconstruct_array, heap_getattr, Datum, Oid, BOOLOID, BYTEAOID, CHAROID, DATEOID,
        FLOAT4OID, FLOAT8OID, INT2OID, INT4OID, INT8OID, INTERVALOID, JSONBOID, JSONOID,
        NUMERICOID, OIDOID, TEXTOID, TIMEOID, TIMESTAMPOID, TIMESTAMPTZOID, TIMETZOID, UUIDOID,
    },
    AllocatedByRust, AnyNumeric, Date, FromDatum, Interval, IntoDatum, Json, JsonB, PgBox,
    PgTupleDesc, Time, TimeWithTimeZone, Timestamp, TimestampWithTimeZone, Uuid,
};

use crate::{
    pgrx_utils::{array_element_typoid, is_array_type, is_composite_type, tuple_desc},
    type_compat::{
        fallback_to_text::{set_fallback_typoid, FallbackToText},
        geometry::{is_postgis_geometry_type, set_geometry_typoid, Geometry},
        map::{is_crunchy_map_type, set_crunchy_map_typoid, PGMap},
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
pub(crate) mod interval;
pub(crate) mod json;
pub(crate) mod jsonb;
pub(crate) mod map;
pub(crate) mod numeric;
pub(crate) mod oid;
pub(crate) mod record;
pub(crate) mod text;
pub(crate) mod time;
pub(crate) mod timestamp;
pub(crate) mod timestamptz;
pub(crate) mod timetz;
pub(crate) mod uuid;

pub(crate) trait PgTypeToArrowArray<T: IntoDatum + FromDatum> {
    fn to_arrow_array(self, context: PgToArrowContext) -> (FieldRef, ArrayRef);
}

pub(crate) struct PgToArrowContext<'a> {
    name: &'a str,
    typoid: Oid,
    typmod: i32,
    field: FieldRef,
    scale: Option<usize>,
    precision: Option<usize>,
    tupledesc: Option<PgTupleDesc<'a>>,
}

impl<'a> PgToArrowContext<'a> {
    fn new(name: &'a str, typoid: Oid, typmod: i32, field: FieldRef) -> Self {
        Self {
            name,
            field,
            typoid,
            typmod,
            scale: None,
            precision: None,
            tupledesc: None,
        }
    }

    fn with_tupledesc(mut self, tupledesc: PgTupleDesc<'a>) -> Self {
        self.tupledesc = Some(tupledesc);
        self
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

pub(crate) fn collect_attribute_array_from_tuples<'a>(
    tuples: Vec<Option<PgHeapTuple<'a, AllocatedByRust>>>,
    tupledesc: PgTupleDesc<'a>,
    attribute_name: &'a str,
    attribute_typoid: Oid,
    attribute_typmod: i32,
    attribute_field: FieldRef,
) -> (
    FieldRef,
    ArrayRef,
    Vec<Option<PgHeapTuple<'a, AllocatedByRust>>>,
) {
    if is_composite_type(attribute_typoid) {
        let attribute_tupledesc = tuple_desc(attribute_typoid, attribute_typmod);

        let attribute_context = PgToArrowContext::new(
            attribute_name,
            attribute_typoid,
            attribute_typmod,
            attribute_field,
        )
        .with_tupledesc(attribute_tupledesc);

        collect_tuple_attribute_array_from_tuples_helper(tuples, tupledesc, attribute_context)
    } else if is_array_type(attribute_typoid) {
        let attribute_element_typoid = array_element_typoid(attribute_typoid);

        let attribute_context = PgToArrowContext::new(
            attribute_name,
            attribute_element_typoid,
            attribute_typmod,
            attribute_field,
        );

        collect_array_attribute_array_from_tuples(tuples, tupledesc, attribute_context)
    } else if is_crunchy_map_type(attribute_typoid) {
        set_crunchy_map_typoid(attribute_typoid);

        let attribute_context = PgToArrowContext::new(
            attribute_name,
            attribute_typoid,
            attribute_typmod,
            attribute_field,
        );

        collect_array_attribute_array_from_tuples_helper::<PGMap<'_>>(tuples, attribute_context)
    } else {
        let attribute_context = PgToArrowContext::new(
            attribute_name,
            attribute_typoid,
            attribute_typmod,
            attribute_field,
        );

        collect_primitive_attribute_array_from_tuples(tuples, attribute_context)
    }
}

fn collect_primitive_attribute_array_from_tuples<'a>(
    tuples: Vec<Option<PgHeapTuple<'a, AllocatedByRust>>>,
    attribute_context: PgToArrowContext<'a>,
) -> (
    FieldRef,
    ArrayRef,
    Vec<Option<PgHeapTuple<'a, AllocatedByRust>>>,
) {
    match attribute_context.typoid {
        FLOAT4OID => {
            collect_array_attribute_array_from_tuples_helper::<f32>(tuples, attribute_context)
        }
        FLOAT8OID => {
            collect_array_attribute_array_from_tuples_helper::<f64>(tuples, attribute_context)
        }
        INT2OID => {
            collect_array_attribute_array_from_tuples_helper::<i16>(tuples, attribute_context)
        }
        INT4OID => {
            collect_array_attribute_array_from_tuples_helper::<i32>(tuples, attribute_context)
        }
        INT8OID => {
            collect_array_attribute_array_from_tuples_helper::<i64>(tuples, attribute_context)
        }
        NUMERICOID => {
            let precision = extract_precision_from_numeric_typmod(attribute_context.typmod);
            if precision > MAX_DECIMAL_PRECISION {
                set_fallback_typoid(attribute_context.typoid);
                collect_array_attribute_array_from_tuples_helper::<FallbackToText>(
                    tuples,
                    attribute_context,
                )
            } else {
                collect_array_attribute_array_from_tuples_helper::<AnyNumeric>(
                    tuples,
                    attribute_context,
                )
            }
        }
        BOOLOID => {
            collect_array_attribute_array_from_tuples_helper::<bool>(tuples, attribute_context)
        }
        DATEOID => {
            collect_array_attribute_array_from_tuples_helper::<Date>(tuples, attribute_context)
        }
        TIMEOID => {
            collect_array_attribute_array_from_tuples_helper::<Time>(tuples, attribute_context)
        }
        TIMETZOID => collect_array_attribute_array_from_tuples_helper::<TimeWithTimeZone>(
            tuples,
            attribute_context,
        ),
        INTERVALOID => {
            collect_array_attribute_array_from_tuples_helper::<Interval>(tuples, attribute_context)
        }
        UUIDOID => {
            collect_array_attribute_array_from_tuples_helper::<Uuid>(tuples, attribute_context)
        }
        JSONOID => {
            collect_array_attribute_array_from_tuples_helper::<Json>(tuples, attribute_context)
        }
        JSONBOID => {
            collect_array_attribute_array_from_tuples_helper::<JsonB>(tuples, attribute_context)
        }
        TIMESTAMPOID => {
            collect_array_attribute_array_from_tuples_helper::<Timestamp>(tuples, attribute_context)
        }
        TIMESTAMPTZOID => {
            collect_array_attribute_array_from_tuples_helper::<TimestampWithTimeZone>(
                tuples,
                attribute_context,
            )
        }
        CHAROID => {
            collect_array_attribute_array_from_tuples_helper::<i8>(tuples, attribute_context)
        }
        TEXTOID => {
            collect_array_attribute_array_from_tuples_helper::<String>(tuples, attribute_context)
        }
        BYTEAOID => {
            collect_array_attribute_array_from_tuples_helper::<&[u8]>(tuples, attribute_context)
        }
        OIDOID => {
            collect_array_attribute_array_from_tuples_helper::<Oid>(tuples, attribute_context)
        }
        _ => {
            if is_postgis_geometry_type(attribute_context.typoid) {
                set_geometry_typoid(attribute_context.typoid);
                collect_array_attribute_array_from_tuples_helper::<Geometry>(
                    tuples,
                    attribute_context,
                )
            } else {
                set_fallback_typoid(attribute_context.typoid);
                collect_array_attribute_array_from_tuples_helper::<FallbackToText>(
                    tuples,
                    attribute_context,
                )
            }
        }
    }
}

pub(crate) fn collect_array_attribute_array_from_tuples<'a>(
    tuples: Vec<Option<PgHeapTuple<'a, AllocatedByRust>>>,
    tupledesc: PgTupleDesc<'a>,
    attribute_context: PgToArrowContext<'a>,
) -> (
    FieldRef,
    ArrayRef,
    Vec<Option<PgHeapTuple<'a, AllocatedByRust>>>,
) {
    match attribute_context.typoid {
        FLOAT4OID => collect_array_attribute_array_from_tuples_helper::<Vec<Option<f32>>>(
            tuples,
            attribute_context,
        ),
        FLOAT8OID => collect_array_attribute_array_from_tuples_helper::<Vec<Option<f64>>>(
            tuples,
            attribute_context,
        ),
        INT2OID => collect_array_attribute_array_from_tuples_helper::<Vec<Option<i16>>>(
            tuples,
            attribute_context,
        ),
        INT4OID => collect_array_attribute_array_from_tuples_helper::<Vec<Option<i32>>>(
            tuples,
            attribute_context,
        ),
        INT8OID => collect_array_attribute_array_from_tuples_helper::<Vec<Option<i64>>>(
            tuples,
            attribute_context,
        ),
        NUMERICOID => {
            let precision = extract_precision_from_numeric_typmod(attribute_context.typmod);

            if precision > MAX_DECIMAL_PRECISION {
                set_fallback_typoid(attribute_context.typoid);

                collect_array_attribute_array_from_tuples_helper::<Vec<Option<FallbackToText>>>(
                    tuples,
                    attribute_context,
                )
            } else {
                let scale = extract_scale_from_numeric_typmod(attribute_context.typmod);

                collect_array_attribute_array_from_tuples_helper::<Vec<Option<AnyNumeric>>>(
                    tuples,
                    attribute_context
                        .with_precision(precision)
                        .with_scale(scale),
                )
            }
        }
        BOOLOID => collect_array_attribute_array_from_tuples_helper::<Vec<Option<bool>>>(
            tuples,
            attribute_context,
        ),
        DATEOID => collect_array_attribute_array_from_tuples_helper::<Vec<Option<Date>>>(
            tuples,
            attribute_context,
        ),
        TIMEOID => collect_array_attribute_array_from_tuples_helper::<Vec<Option<Time>>>(
            tuples,
            attribute_context,
        ),
        TIMETZOID => collect_array_attribute_array_from_tuples_helper::<
            Vec<Option<TimeWithTimeZone>>,
        >(tuples, attribute_context),
        INTERVALOID => collect_array_attribute_array_from_tuples_helper::<Vec<Option<Interval>>>(
            tuples,
            attribute_context,
        ),
        UUIDOID => collect_array_attribute_array_from_tuples_helper::<Vec<Option<Uuid>>>(
            tuples,
            attribute_context,
        ),
        JSONOID => collect_array_attribute_array_from_tuples_helper::<Vec<Option<Json>>>(
            tuples,
            attribute_context,
        ),
        JSONBOID => collect_array_attribute_array_from_tuples_helper::<Vec<Option<JsonB>>>(
            tuples,
            attribute_context,
        ),
        TIMESTAMPOID => collect_array_attribute_array_from_tuples_helper::<Vec<Option<Timestamp>>>(
            tuples,
            attribute_context,
        ),
        TIMESTAMPTZOID => collect_array_attribute_array_from_tuples_helper::<
            Vec<Option<TimestampWithTimeZone>>,
        >(tuples, attribute_context),
        CHAROID => collect_array_attribute_array_from_tuples_helper::<Vec<Option<i8>>>(
            tuples,
            attribute_context,
        ),
        TEXTOID => collect_array_attribute_array_from_tuples_helper::<Vec<Option<String>>>(
            tuples,
            attribute_context,
        ),
        BYTEAOID => collect_array_attribute_array_from_tuples_helper::<Vec<Option<&[u8]>>>(
            tuples,
            attribute_context,
        ),
        OIDOID => collect_array_attribute_array_from_tuples_helper::<Vec<Option<Oid>>>(
            tuples,
            attribute_context,
        ),
        _ => {
            if is_composite_type(attribute_context.typoid) {
                let attribute_tupledesc =
                    tuple_desc(attribute_context.typoid, attribute_context.typmod);

                collect_array_of_tuple_attribute_array_from_tuples_helper(
                    tuples,
                    tupledesc,
                    attribute_context.with_tupledesc(attribute_tupledesc),
                )
            } else if is_crunchy_map_type(attribute_context.typoid) {
                set_crunchy_map_typoid(attribute_context.typoid);
                collect_array_attribute_array_from_tuples_helper::<Vec<Option<PGMap<'_>>>>(
                    tuples,
                    attribute_context,
                )
            } else if is_postgis_geometry_type(attribute_context.typoid) {
                set_geometry_typoid(attribute_context.typoid);
                collect_array_attribute_array_from_tuples_helper::<Vec<Option<Geometry>>>(
                    tuples,
                    attribute_context,
                )
            } else {
                set_fallback_typoid(attribute_context.typoid);
                collect_array_attribute_array_from_tuples_helper::<Vec<Option<FallbackToText>>>(
                    tuples,
                    attribute_context,
                )
            }
        }
    }
}

fn collect_array_attribute_array_from_tuples_helper<'a, T>(
    tuples: Vec<Option<PgHeapTuple<'a, AllocatedByRust>>>,
    attribute_context: PgToArrowContext<'a>,
) -> (
    FieldRef,
    ArrayRef,
    Vec<Option<PgHeapTuple<'a, AllocatedByRust>>>,
)
where
    T: IntoDatum + FromDatum + 'static,
    Vec<Option<T>>: PgTypeToArrowArray<T>,
{
    let mut attribute_values = vec![];

    for record in tuples.iter() {
        pgrx::pg_sys::check_for_interrupts!();

        if let Some(record) = record {
            let attribute_val: Option<T> = record.get_by_name(attribute_context.name).unwrap();
            attribute_values.push(attribute_val);
        } else {
            attribute_values.push(None);
        }
    }

    let (field, array) = attribute_values.to_arrow_array(attribute_context);
    (field, array, tuples)
}

fn collect_tuple_attribute_array_from_tuples_helper<'a>(
    tuples: Vec<Option<PgHeapTuple<'a, AllocatedByRust>>>,
    tuple_tupledesc: PgTupleDesc<'a>,
    attribute_context: PgToArrowContext<'a>,
) -> (
    FieldRef,
    ArrayRef,
    Vec<Option<PgHeapTuple<'a, AllocatedByRust>>>,
) {
    let mut attribute_values = vec![];

    let att_tupledesc = attribute_context
        .tupledesc
        .clone()
        .expect("Expected tuple descriptor");

    let attnum = tuple_tupledesc
        .iter()
        .find(|attr| attr.name() == attribute_context.name)
        .unwrap()
        .attnum;

    let mut tuples_restored = vec![];

    for record in tuples.into_iter() {
        pgrx::pg_sys::check_for_interrupts!();

        if let Some(record) = record {
            unsafe {
                let mut isnull = false;
                let record = record.into_pg();
                let att_datum =
                    heap_getattr(record, attnum as _, tuple_tupledesc.as_ptr(), &mut isnull);

                if isnull {
                    attribute_values.push(None);
                } else {
                    let att_val = tuple_from_datum_and_tupdesc(att_datum, att_tupledesc.clone());
                    attribute_values.push(Some(att_val));
                }

                let record = PgHeapTuple::from_heap_tuple(tuple_tupledesc.clone(), record);
                tuples_restored.push(Some(record.into_owned()));
            }
        } else {
            attribute_values.push(None);
            tuples_restored.push(None);
        }
    }

    let (field, array) = attribute_values.to_arrow_array(attribute_context);
    (field, array, tuples_restored)
}

fn collect_array_of_tuple_attribute_array_from_tuples_helper<'a>(
    tuples: Vec<Option<PgHeapTuple<'a, AllocatedByRust>>>,
    tuple_tupledesc: PgTupleDesc<'a>,
    attribute_context: PgToArrowContext<'a>,
) -> (
    FieldRef,
    ArrayRef,
    Vec<Option<PgHeapTuple<'a, AllocatedByRust>>>,
) {
    let mut attribute_values = vec![];

    let attnum = tuple_tupledesc
        .iter()
        .find(|attr| attr.name() == attribute_context.name)
        .unwrap()
        .attnum;

    let att_tupledesc = attribute_context
        .tupledesc
        .clone()
        .expect("Expected tuple descriptor");

    let mut tuples_restored = vec![];

    for record in tuples.into_iter() {
        pgrx::pg_sys::check_for_interrupts!();

        if let Some(record) = record {
            unsafe {
                let mut isnull = false;
                let record = record.into_pg();
                let att_datum =
                    heap_getattr(record, attnum as _, tuple_tupledesc.as_ptr(), &mut isnull);

                if isnull {
                    attribute_values.push(None);
                } else {
                    let att_val =
                        tuple_array_from_datum_and_tupdesc(att_datum, att_tupledesc.clone());
                    attribute_values.push(Some(att_val));
                }

                let record = PgHeapTuple::from_heap_tuple(tuple_tupledesc.clone(), record);
                tuples_restored.push(Some(record.into_owned()));
            }
        } else {
            attribute_values.push(None);
            tuples_restored.push(None);
        }
    }

    let (field, array) = attribute_values.to_arrow_array(attribute_context);

    (field, array, tuples_restored)
}

fn tuple_from_datum_and_tupdesc(
    datum: Datum,
    tupledesc: PgTupleDesc,
) -> PgHeapTuple<AllocatedByRust> {
    unsafe {
        let htup_header = pg_sys::pg_detoast_datum(datum.cast_mut_ptr()) as pg_sys::HeapTupleHeader;

        let mut data = PgBox::<pg_sys::HeapTupleData>::alloc0();
        data.t_len = pgrx::heap_tuple_header_get_datum_length(htup_header) as u32;
        data.t_data = htup_header;

        PgHeapTuple::from_heap_tuple(tupledesc.clone(), data.as_ptr()).into_owned()
    }
}

fn tuple_array_from_datum_and_tupdesc(
    datum: Datum,
    tupledesc: PgTupleDesc,
) -> Vec<Option<PgHeapTuple<AllocatedByRust>>> {
    unsafe {
        let arraytype = pg_sys::pg_detoast_datum(datum.cast_mut_ptr()) as *mut pg_sys::ArrayType;

        let element_oid = tupledesc.oid();

        let mut num_elem = 0;
        let mut datums = std::ptr::null_mut();
        let mut nulls = std::ptr::null_mut();

        let mut type_len = 0;
        let mut type_by_val = false;
        let mut type_by_align = 0;

        pg_sys::get_typlenbyvalalign(
            element_oid,
            &mut type_len,
            &mut type_by_val,
            &mut type_by_align,
        );

        let arr_elem_type = (*arraytype).elemtype;
        debug_assert!(arr_elem_type == element_oid);

        deconstruct_array(
            arraytype,
            element_oid,
            type_len as _,
            type_by_val,
            type_by_align,
            &mut datums as _,
            &mut nulls as _,
            &mut num_elem,
        );

        let datums = std::slice::from_raw_parts(datums as *const Datum, num_elem as _);
        let nulls = std::slice::from_raw_parts(nulls as *const bool, num_elem as _);

        let mut tuples = vec![];

        for (datum, isnull) in datums.iter().zip(nulls.iter()) {
            if *isnull {
                tuples.push(None);
            } else {
                let tuple = tuple_from_datum_and_tupdesc(datum.to_owned(), tupledesc.clone());
                tuples.push(Some(tuple));
            }
        }

        tuples
    }
}
