use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, StructArray},
    buffer::{BooleanBuffer, NullBuffer},
    datatypes::{DataType, Field, FieldRef, Fields},
};
use pgrx::{
    heap_tuple::PgHeapTuple,
    pg_sys::{
        self, deconstruct_array, heap_getattr, Datum, InvalidOid, Oid, BOOLARRAYOID, BOOLOID,
        BPCHARARRAYOID, BPCHAROID, BYTEAARRAYOID, BYTEAOID, CHARARRAYOID, CHAROID, DATEARRAYOID,
        DATEOID, FLOAT4ARRAYOID, FLOAT4OID, FLOAT8ARRAYOID, FLOAT8OID, INT2ARRAYOID, INT2OID,
        INT4ARRAYOID, INT4OID, INT8ARRAYOID, INT8OID, INTERVALARRAYOID, INTERVALOID,
        NUMERICARRAYOID, NUMERICOID, OIDARRAYOID, OIDOID, TEXTARRAYOID, TEXTOID, TIMEARRAYOID,
        TIMEOID, TIMESTAMPARRAYOID, TIMESTAMPOID, TIMESTAMPTZARRAYOID, TIMESTAMPTZOID,
        TIMETZARRAYOID, TIMETZOID, VARCHARARRAYOID, VARCHAROID,
    },
    AllocatedByRust, AnyNumeric, Date, FromDatum, Interval, IntoDatum, PgBox, PgTupleDesc, Time,
    TimeWithTimeZone, Timestamp, TimestampWithTimeZone,
};

use crate::{
    arrow_parquet::{
        pg_to_arrow::PgTypeToArrowArray,
        utils::{arrow_array_offsets, create_arrow_list_array},
    },
    pgrx_utils::{
        array_element_typoid, collect_valid_attributes, is_array_type, is_composite_type,
        tuple_desc,
    },
    type_compat::{Bpchar, Varchar},
};

// PgHeapTuple
impl PgTypeToArrowArray<PgHeapTuple<'_, AllocatedByRust>>
    for Vec<Option<PgHeapTuple<'_, AllocatedByRust>>>
{
    fn as_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let mut struct_attribute_arrays = vec![];
        let mut struct_attribute_fields = vec![];

        let tupledesc = tuple_desc(typoid, typmod);

        let include_generated_columns = true;
        let attributes = collect_valid_attributes(&tupledesc, include_generated_columns);

        let mut tuples = self;

        for attribute in attributes {
            let attribute_name = attribute.name();
            let attribute_typoid = attribute.type_oid().value();
            let attribute_typmod = attribute.type_mod();

            let (field, array, tups) = collect_attribute_array_from_tuples(
                tuples,
                tupledesc.clone(),
                attribute_name,
                attribute_typoid,
                attribute_typmod,
            );

            tuples = tups;
            struct_attribute_fields.push(field);
            struct_attribute_arrays.push(array);
        }

        let struct_field = Arc::new(Field::new(
            name,
            DataType::Struct(Fields::from(struct_attribute_fields.clone())),
            true,
        ));

        // determines which structs in the array are null
        let is_null_buffer =
            BooleanBuffer::collect_bool(tuples.len(), |idx| tuples.get(idx).unwrap().is_some());
        let struct_null_buffer = NullBuffer::new(is_null_buffer);

        let struct_array = StructArray::new(
            Fields::from(struct_attribute_fields),
            struct_attribute_arrays,
            Some(struct_null_buffer),
        );
        let struct_array = make_array(struct_array.into());

        (struct_field, struct_array)
    }
}

// PgHeapTuple[]
impl PgTypeToArrowArray<Vec<Option<PgHeapTuple<'_, AllocatedByRust>>>>
    for Vec<Option<Vec<Option<PgHeapTuple<'_, AllocatedByRust>>>>>
{
    fn as_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, nulls) = arrow_array_offsets(&self);

        let array = self.into_iter().flatten().flatten().collect::<Vec<_>>();
        let (field, primitive_array) = array.as_arrow_array(name, typoid, typmod);

        create_arrow_list_array(name, field, primitive_array, offsets, nulls)
    }
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
        NUMERICOID => collect_attribute_array_from_tuples_helper::<AnyNumeric>(
            tuples,
            attribute_name,
            attribute_typoid,
            attribute_typmod,
        ),
        NUMERICARRAYOID => collect_attribute_array_from_tuples_helper::<Vec<Option<AnyNumeric>>>(
            tuples,
            attribute_name,
            attribute_typoid,
            attribute_typmod,
        ),
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
        VARCHAROID => collect_attribute_array_from_tuples_helper::<Varchar>(
            tuples,
            attribute_name,
            attribute_typoid,
            attribute_typmod,
        ),
        BPCHAROID => collect_attribute_array_from_tuples_helper::<Bpchar>(
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
        VARCHARARRAYOID => collect_attribute_array_from_tuples_helper::<Vec<Option<Varchar>>>(
            tuples,
            attribute_name,
            attribute_element_typoid,
            attribute_typmod,
        ),
        BPCHARARRAYOID => collect_attribute_array_from_tuples_helper::<Vec<Option<Bpchar>>>(
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
            if is_composite_type(attribute_typoid) {
                collect_tuple_attribute_array_from_tuples_helper(
                    tuples,
                    tupledesc,
                    attribute_name,
                    attribute_typoid,
                    attribute_typmod,
                )
            } else if is_composite_type(attribute_element_typoid) {
                collect_array_of_tuple_attribute_array_from_tuples_helper(
                    tuples,
                    tupledesc,
                    attribute_name,
                    attribute_element_typoid,
                    attribute_typmod,
                )
            } else {
                panic!("unsupported type {}", attribute_typoid);
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
        attribute_values.as_arrow_array(attribute_name, attribute_typoid, attribute_typmod);
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
        attribute_values.as_arrow_array(attribute_name, attribute_typoid, attribute_typmod);
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
        attribute_values.as_arrow_array(attribute_name, attribute_typoid, attribute_typmod);
    (field, array, tuples_restored)
}

fn tuple_from_datum_and_tupdesc<'a>(
    datum: Datum,
    tupledesc: PgTupleDesc<'a>,
) -> PgHeapTuple<'a, AllocatedByRust> {
    unsafe {
        let htup_header = pg_sys::pg_detoast_datum(datum.cast_mut_ptr()) as pg_sys::HeapTupleHeader;

        let mut data = PgBox::<pg_sys::HeapTupleData>::alloc0();
        data.t_len = pgrx::heap_tuple_header_get_datum_length(htup_header) as u32;
        data.t_data = htup_header;

        PgHeapTuple::from_heap_tuple(tupledesc.clone(), data.as_ptr()).into_owned()
    }
}

fn tuple_array_from_datum_and_tupdesc<'a>(
    datum: Datum,
    tupledesc: PgTupleDesc<'a>,
) -> Vec<Option<PgHeapTuple<'a, AllocatedByRust>>> {
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

        for (datum, isnull) in datums.into_iter().zip(nulls.into_iter()) {
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
