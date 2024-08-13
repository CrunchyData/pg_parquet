use arrow::{array::ArrayRef, datatypes::FieldRef};
use pgrx::{
    heap_tuple::PgHeapTuple,
    pg_sys::{
        self, deconstruct_array, heap_getattr, Datum, InvalidOid, Oid, BOOLARRAYOID, BOOLOID,
        BYTEAARRAYOID, BYTEAOID, CHARARRAYOID, CHAROID, DATEARRAYOID, DATEOID, FLOAT4ARRAYOID,
        FLOAT4OID, FLOAT8ARRAYOID, FLOAT8OID, INT2ARRAYOID, INT2OID, INT4ARRAYOID, INT4OID,
        INT8ARRAYOID, INT8OID, INTERVALARRAYOID, INTERVALOID, JSONARRAYOID, JSONBARRAYOID,
        JSONBOID, JSONOID, NUMERICARRAYOID, NUMERICOID, OIDARRAYOID, OIDOID, TEXTARRAYOID, TEXTOID,
        TIMEARRAYOID, TIMEOID, TIMESTAMPARRAYOID, TIMESTAMPOID, TIMESTAMPTZARRAYOID,
        TIMESTAMPTZOID, TIMETZARRAYOID, TIMETZOID, UUIDARRAYOID, UUIDOID,
    },
    AllocatedByRust, AnyNumeric, Date, FromDatum, Interval, IntoDatum, Json, JsonB, PgBox,
    PgTupleDesc, Time, TimeWithTimeZone, Timestamp, TimestampWithTimeZone, Uuid,
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

pub(crate) trait PgTypeToArrowArray<T: IntoDatum + FromDatum> {
    fn to_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef);
}

pub(crate) fn collect_attribute_array_from_tuples<'a>(
    tuples: Vec<Option<PgHeapTuple<'a, AllocatedByRust>>>,
    tupledesc: PgTupleDesc<'a>,
    attribute_name: &str,
    attribute_typoid: Oid,
    attribute_typmod: i32,
) -> (
    FieldRef,
    ArrayRef,
    Vec<Option<PgHeapTuple<'a, AllocatedByRust>>>,
) {
    let attribute_element_typoid = if is_array_type(attribute_typoid) {
        array_element_typoid(attribute_typoid)
    } else {
        InvalidOid
    };

    match attribute_typoid {
        FLOAT4OID => collect_attribute_array_from_tuples_helper::<f32>(
            tuples,
            attribute_name,
            attribute_typoid,
            attribute_typmod,
        ),
        FLOAT4ARRAYOID => collect_attribute_array_from_tuples_helper::<Vec<Option<f32>>>(
            tuples,
            attribute_name,
            attribute_element_typoid,
            attribute_typmod,
        ),
        FLOAT8OID => collect_attribute_array_from_tuples_helper::<f64>(
            tuples,
            attribute_name,
            attribute_typoid,
            attribute_typmod,
        ),
        FLOAT8ARRAYOID => collect_attribute_array_from_tuples_helper::<Vec<Option<f64>>>(
            tuples,
            attribute_name,
            attribute_element_typoid,
            attribute_typmod,
        ),
        INT2OID => collect_attribute_array_from_tuples_helper::<i16>(
            tuples,
            attribute_name,
            attribute_typoid,
            attribute_typmod,
        ),
        INT2ARRAYOID => collect_attribute_array_from_tuples_helper::<Vec<Option<i16>>>(
            tuples,
            attribute_name,
            attribute_element_typoid,
            attribute_typmod,
        ),
        INT4OID => collect_attribute_array_from_tuples_helper::<i32>(
            tuples,
            attribute_name,
            attribute_typoid,
            attribute_typmod,
        ),
        INT4ARRAYOID => collect_attribute_array_from_tuples_helper::<Vec<Option<i32>>>(
            tuples,
            attribute_name,
            attribute_element_typoid,
            attribute_typmod,
        ),
        INT8OID => collect_attribute_array_from_tuples_helper::<i64>(
            tuples,
            attribute_name,
            attribute_typoid,
            attribute_typmod,
        ),
        INT8ARRAYOID => collect_attribute_array_from_tuples_helper::<Vec<Option<i64>>>(
            tuples,
            attribute_name,
            attribute_element_typoid,
            attribute_typmod,
        ),
        NUMERICOID => {
            let precision = extract_precision_from_numeric_typmod(attribute_typmod);
            if precision > MAX_DECIMAL_PRECISION {
                set_fallback_typoid(attribute_typoid);
                collect_attribute_array_from_tuples_helper::<FallbackToText>(
                    tuples,
                    attribute_name,
                    attribute_typoid,
                    attribute_typmod,
                )
            } else {
                collect_attribute_array_from_tuples_helper::<AnyNumeric>(
                    tuples,
                    attribute_name,
                    attribute_typoid,
                    attribute_typmod,
                )
            }
        }
        NUMERICARRAYOID => {
            let precision = extract_precision_from_numeric_typmod(attribute_typmod);
            if precision > MAX_DECIMAL_PRECISION {
                set_fallback_typoid(attribute_element_typoid);
                collect_attribute_array_from_tuples_helper::<Vec<Option<FallbackToText>>>(
                    tuples,
                    attribute_name,
                    attribute_element_typoid,
                    attribute_typmod,
                )
            } else {
                collect_attribute_array_from_tuples_helper::<Vec<Option<AnyNumeric>>>(
                    tuples,
                    attribute_name,
                    attribute_element_typoid,
                    attribute_typmod,
                )
            }
        }
        BOOLOID => collect_attribute_array_from_tuples_helper::<bool>(
            tuples,
            attribute_name,
            attribute_typoid,
            attribute_typmod,
        ),
        BOOLARRAYOID => collect_attribute_array_from_tuples_helper::<Vec<Option<bool>>>(
            tuples,
            attribute_name,
            attribute_element_typoid,
            attribute_typmod,
        ),
        DATEOID => collect_attribute_array_from_tuples_helper::<Date>(
            tuples,
            attribute_name,
            attribute_typoid,
            attribute_typmod,
        ),
        DATEARRAYOID => collect_attribute_array_from_tuples_helper::<Vec<Option<Date>>>(
            tuples,
            attribute_name,
            attribute_element_typoid,
            attribute_typmod,
        ),
        TIMEOID => collect_attribute_array_from_tuples_helper::<Time>(
            tuples,
            attribute_name,
            attribute_typoid,
            attribute_typmod,
        ),
        TIMEARRAYOID => collect_attribute_array_from_tuples_helper::<Vec<Option<Time>>>(
            tuples,
            attribute_name,
            attribute_element_typoid,
            attribute_typmod,
        ),
        TIMETZOID => collect_attribute_array_from_tuples_helper::<TimeWithTimeZone>(
            tuples,
            attribute_name,
            attribute_typoid,
            attribute_typmod,
        ),
        TIMETZARRAYOID => {
            collect_attribute_array_from_tuples_helper::<Vec<Option<TimeWithTimeZone>>>(
                tuples,
                attribute_name,
                attribute_element_typoid,
                attribute_typmod,
            )
        }
        INTERVALOID => collect_attribute_array_from_tuples_helper::<Interval>(
            tuples,
            attribute_name,
            attribute_typoid,
            attribute_typmod,
        ),
        INTERVALARRAYOID => collect_attribute_array_from_tuples_helper::<Vec<Option<Interval>>>(
            tuples,
            attribute_name,
            attribute_element_typoid,
            attribute_typmod,
        ),
        UUIDOID => collect_attribute_array_from_tuples_helper::<Uuid>(
            tuples,
            attribute_name,
            attribute_typoid,
            attribute_typmod,
        ),
        UUIDARRAYOID => collect_attribute_array_from_tuples_helper::<Vec<Option<Uuid>>>(
            tuples,
            attribute_name,
            attribute_element_typoid,
            attribute_typmod,
        ),
        JSONOID => collect_attribute_array_from_tuples_helper::<Json>(
            tuples,
            attribute_name,
            attribute_typoid,
            attribute_typmod,
        ),
        JSONARRAYOID => collect_attribute_array_from_tuples_helper::<Vec<Option<Json>>>(
            tuples,
            attribute_name,
            attribute_element_typoid,
            attribute_typmod,
        ),
        JSONBOID => collect_attribute_array_from_tuples_helper::<JsonB>(
            tuples,
            attribute_name,
            attribute_typoid,
            attribute_typmod,
        ),
        JSONBARRAYOID => collect_attribute_array_from_tuples_helper::<Vec<Option<JsonB>>>(
            tuples,
            attribute_name,
            attribute_element_typoid,
            attribute_typmod,
        ),
        TIMESTAMPOID => collect_attribute_array_from_tuples_helper::<Timestamp>(
            tuples,
            attribute_name,
            attribute_typoid,
            attribute_typmod,
        ),
        TIMESTAMPARRAYOID => collect_attribute_array_from_tuples_helper::<Vec<Option<Timestamp>>>(
            tuples,
            attribute_name,
            attribute_element_typoid,
            attribute_typmod,
        ),
        TIMESTAMPTZOID => collect_attribute_array_from_tuples_helper::<TimestampWithTimeZone>(
            tuples,
            attribute_name,
            attribute_typoid,
            attribute_typmod,
        ),
        TIMESTAMPTZARRAYOID => {
            collect_attribute_array_from_tuples_helper::<Vec<Option<TimestampWithTimeZone>>>(
                tuples,
                attribute_name,
                attribute_element_typoid,
                attribute_typmod,
            )
        }
        CHAROID => collect_attribute_array_from_tuples_helper::<i8>(
            tuples,
            attribute_name,
            attribute_typoid,
            attribute_typmod,
        ),
        CHARARRAYOID => collect_attribute_array_from_tuples_helper::<Vec<Option<i8>>>(
            tuples,
            attribute_name,
            attribute_element_typoid,
            attribute_typmod,
        ),
        TEXTOID => collect_attribute_array_from_tuples_helper::<String>(
            tuples,
            attribute_name,
            attribute_typoid,
            attribute_typmod,
        ),
        TEXTARRAYOID => collect_attribute_array_from_tuples_helper::<Vec<Option<String>>>(
            tuples,
            attribute_name,
            attribute_element_typoid,
            attribute_typmod,
        ),
        BYTEAOID => collect_attribute_array_from_tuples_helper::<&[u8]>(
            tuples,
            attribute_name,
            attribute_typoid,
            attribute_typmod,
        ),
        BYTEAARRAYOID => collect_attribute_array_from_tuples_helper::<Vec<Option<&[u8]>>>(
            tuples,
            attribute_name,
            attribute_element_typoid,
            attribute_typmod,
        ),
        OIDOID => collect_attribute_array_from_tuples_helper::<Oid>(
            tuples,
            attribute_name,
            attribute_typoid,
            attribute_typmod,
        ),
        OIDARRAYOID => collect_attribute_array_from_tuples_helper::<Vec<Option<Oid>>>(
            tuples,
            attribute_name,
            attribute_element_typoid,
            attribute_typmod,
        ),
        _ => {
            if attribute_element_typoid != InvalidOid {
                if is_composite_type(attribute_element_typoid) {
                    collect_array_of_tuple_attribute_array_from_tuples_helper(
                        tuples,
                        tupledesc,
                        attribute_name,
                        attribute_element_typoid,
                        attribute_typmod,
                    )
                } else if is_postgis_geometry_type(attribute_element_typoid) {
                    set_geometry_typoid(attribute_element_typoid);
                    collect_attribute_array_from_tuples_helper::<Vec<Option<Geometry>>>(
                        tuples,
                        attribute_name,
                        attribute_element_typoid,
                        attribute_typmod,
                    )
                } else {
                    set_fallback_typoid(attribute_element_typoid);
                    collect_attribute_array_from_tuples_helper::<Vec<Option<FallbackToText>>>(
                        tuples,
                        attribute_name,
                        attribute_element_typoid,
                        attribute_typmod,
                    )
                }
            } else if is_composite_type(attribute_typoid) {
                collect_tuple_attribute_array_from_tuples_helper(
                    tuples,
                    tupledesc,
                    attribute_name,
                    attribute_typoid,
                    attribute_typmod,
                )
            } else if is_postgis_geometry_type(attribute_typoid) {
                set_geometry_typoid(attribute_typoid);
                collect_attribute_array_from_tuples_helper::<Geometry>(
                    tuples,
                    attribute_name,
                    attribute_typoid,
                    attribute_typmod,
                )
            } else {
                set_fallback_typoid(attribute_typoid);
                collect_attribute_array_from_tuples_helper::<FallbackToText>(
                    tuples,
                    attribute_name,
                    attribute_typoid,
                    attribute_typmod,
                )
            }
        }
    }
}

fn collect_attribute_array_from_tuples_helper<'a, T>(
    tuples: Vec<Option<PgHeapTuple<'a, AllocatedByRust>>>,
    attribute_name: &str,
    attribute_typoid: Oid,
    attribute_typmod: i32,
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
            let attribute_val: Option<T> = record.get_by_name(attribute_name).unwrap();
            attribute_values.push(attribute_val);
        } else {
            attribute_values.push(None);
        }
    }

    let (field, array) =
        attribute_values.to_arrow_array(attribute_name, attribute_typoid, attribute_typmod);
    (field, array, tuples)
}

fn collect_tuple_attribute_array_from_tuples_helper<'a>(
    tuples: Vec<Option<PgHeapTuple<'a, AllocatedByRust>>>,
    tuple_tupledesc: PgTupleDesc<'a>,
    attribute_name: &str,
    attribute_typoid: Oid,
    attribute_typmod: i32,
) -> (
    FieldRef,
    ArrayRef,
    Vec<Option<PgHeapTuple<'a, AllocatedByRust>>>,
) {
    let mut attribute_values = vec![];

    let att_tupledesc = tuple_desc(attribute_typoid, attribute_typmod);

    let attnum = tuple_tupledesc
        .iter()
        .find(|attr| attr.name() == attribute_name)
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

    let (field, array) =
        attribute_values.to_arrow_array(attribute_name, attribute_typoid, attribute_typmod);
    (field, array, tuples_restored)
}

fn collect_array_of_tuple_attribute_array_from_tuples_helper<'a>(
    tuples: Vec<Option<PgHeapTuple<'a, AllocatedByRust>>>,
    tuple_tupledesc: PgTupleDesc<'a>,
    attribute_name: &str,
    attribute_typoid: Oid,
    attribute_typmod: i32,
) -> (
    FieldRef,
    ArrayRef,
    Vec<Option<PgHeapTuple<'a, AllocatedByRust>>>,
) {
    let mut attribute_values = vec![];

    let att_tupledesc = tuple_desc(attribute_typoid, attribute_typmod);

    let attnum = tuple_tupledesc
        .iter()
        .find(|attr| attr.name() == attribute_name)
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

    let (field, array) =
        attribute_values.to_arrow_array(attribute_name, attribute_typoid, attribute_typmod);
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
