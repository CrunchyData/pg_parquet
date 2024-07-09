use std::collections::HashSet;
use std::fs::File;

use parquet::{
    data_type::{
        ByteArray, ByteArrayType, DoubleType, FixedLenByteArrayType, FloatType, Int32Type,
        Int64Type,
    },
    file::writer::SerializedRowGroupWriter,
    schema::types::ColumnDescriptor,
};

use pg_sys::{
    InputFunctionCall, Oid, BOOLARRAYOID, BPCHARARRAYOID, CHARARRAYOID, DATEARRAYOID,
    FLOAT4ARRAYOID, FLOAT8ARRAYOID, INT2ARRAYOID, INT4ARRAYOID, INT8ARRAYOID, INTERVALARRAYOID,
    JSONARRAYOID, NUMERICARRAYOID, TEXTARRAYOID, TIMEARRAYOID, TIMESTAMPARRAYOID,
    TIMESTAMPTZARRAYOID, TIMETZARRAYOID, UUIDARRAYOID, VARCHARARRAYOID,
};
use pgrx::prelude::*;
use pgrx::{direct_function_call, Json, PgTupleDesc, Uuid};

use crate::conversion::{
    date_to_i32, interval_to_fixed_byte_array, json_to_byte_array, time_to_i64, timestamp_to_i64,
    timestamptz_to_i64, timetz_to_i64, uuid_to_fixed_byte_array,
};

type Strides = Vec<usize>;

fn definition_vector_from_col_desc(col_desc: &ColumnDescriptor, length: usize) -> Vec<i16> {
    vec![col_desc.max_def_level(); length]
}

fn repetition_vector_from_col_desc(
    col_desc: &ColumnDescriptor,
    level_strides: &Vec<Strides>,
    length: usize,
) -> Vec<i16> {
    let mut repetition_vector = vec![col_desc.max_rep_level(); length];

    if level_strides.is_empty() {
        repetition_vector[0] = 0;
        return repetition_vector;
    }

    let mut level_strides = level_strides.clone();
    let mut normalized_level_strides = vec![];

    // e.g. first level contains [2, 1]
    //      second level contains [1, 1, 1]
    //      last level contains [2, 1, 5]
    // last level is equal to total element length, all previous levels contains partition indexes
    // I want to convert all previous levels to summation of partition indexes like below
    // previous level should be converted to [2, 1, 5]
    // the first level should be converted to [3, 5]

    let mut last_level_sum_vector = level_strides.pop().unwrap();
    normalized_level_strides.push(last_level_sum_vector.clone());

    while !level_strides.is_empty() {
        let previous_level_partition_indexes = level_strides.pop().unwrap();

        let mut previous_level_sum_vector = vec![];

        let mut idx = 0;
        for partition_index in previous_level_partition_indexes {
            previous_level_sum_vector.push(
                last_level_sum_vector
                    .iter()
                    .skip(idx)
                    .take(partition_index)
                    .sum(),
            );
            idx += partition_index;
        }

        last_level_sum_vector = previous_level_sum_vector.clone();
        normalized_level_strides.push(previous_level_sum_vector);
    }

    let mut idx_set = HashSet::new();
    let mut level_identifier = 1;
    while !normalized_level_strides.is_empty() {
        let mut strides = normalized_level_strides.pop().unwrap();
        strides.pop().unwrap();

        let mut idx = 0;
        for stride in strides {
            idx += stride;
            if !idx_set.contains(&idx) {
                repetition_vector[idx] = level_identifier;
                idx_set.insert(idx);
            }
        }

        level_identifier += 1;
    }

    repetition_vector[0] = 0;

    repetition_vector
}

fn array_default(array_typoid: Oid) -> pgrx::AnyArray {
    let arr_str = std::ffi::CString::new("{}").unwrap();

    unsafe {
        let mut type_input_funcoid = pg_sys::InvalidOid;
        let mut typioparam = pg_sys::InvalidOid;
        pg_sys::getTypeInputInfo(array_typoid, &mut type_input_funcoid, &mut typioparam);

        let arg_flinfo =
            pg_sys::palloc0(std::mem::size_of::<pg_sys::FmgrInfo>()) as *mut pg_sys::FmgrInfo;
        pg_sys::fmgr_info(type_input_funcoid, arg_flinfo);

        let arg_str_ = arr_str.as_ptr() as *mut _;
        let arg_typioparam = typioparam;
        let arg_typmod = -1;
        let datum = InputFunctionCall(arg_flinfo, arg_str_, arg_typioparam, arg_typmod);
        pgrx::AnyArray::from_polymorphic_datum(datum, false, array_typoid).unwrap()
    }
}

fn flatten_arrays(
    arrays: Vec<pgrx::AnyElement>,
    array_typoid: Oid,
) -> (pgrx::AnyArray, Strides, Option<PgTupleDesc<'static>>) {
    assert!(unsafe { pg_sys::type_is_array(array_typoid) });

    let array_element_typoid = unsafe { pg_sys::get_element_type(array_typoid) };
    let tupledesc = tupledesc_for_typeoid(array_element_typoid);

    if arrays.is_empty() {
        return (array_default(array_typoid), vec![], tupledesc);
    }

    let array_datums = arrays.into_iter().map(|x| x.datum()).collect::<Vec<_>>();

    let mut lengths = Vec::<usize>::new();
    for array_datum in &array_datums {
        let a_len: i32 = unsafe {
            direct_function_call(
                pg_sys::array_length,
                &[array_datum.into_datum(), 1.into_datum()],
            )
            .unwrap()
        };
        lengths.push(a_len as _);
    }

    let flatten_array_datum = array_datums
        .into_iter()
        .reduce(|a, b| unsafe {
            direct_function_call(pg_sys::array_cat, &[a.into_datum(), b.into_datum()]).unwrap()
        })
        .unwrap();

    let flatten_array = unsafe {
        pgrx::AnyArray::from_polymorphic_datum(flatten_array_datum, false, array_typoid).unwrap()
    };

    (flatten_array, lengths, tupledesc)
}

fn elements_to_anyarray_helper<T: IntoDatum + FromDatum>(
    elements: Vec<pgrx::AnyElement>,
    array_typoid: Oid,
) -> pgrx::AnyArray {
    let elem_typoid = unsafe { pg_sys::get_element_type(array_typoid) };
    let elements = elements
        .into_iter()
        .map(|x| unsafe { T::from_polymorphic_datum(x.datum(), false, elem_typoid).unwrap() })
        .collect::<Vec<_>>();

    unsafe {
        pgrx::AnyArray::from_polymorphic_datum(elements.into_datum().unwrap(), false, array_typoid)
            .unwrap()
    }
}

fn elements_to_anyarray(
    elements: Vec<pgrx::AnyElement>,
    element_typoid: Oid,
) -> (pgrx::AnyArray, Strides, Option<PgTupleDesc<'static>>) {
    let lengths = vec![1; elements.len()];

    let tupledesc = tupledesc_for_typeoid(element_typoid);

    let array_typoid = unsafe { pg_sys::get_array_type(element_typoid) };
    let array = match array_typoid {
        FLOAT4ARRAYOID => elements_to_anyarray_helper::<f32>(elements, array_typoid),
        FLOAT8ARRAYOID => elements_to_anyarray_helper::<f64>(elements, array_typoid),
        BOOLARRAYOID => elements_to_anyarray_helper::<bool>(elements, array_typoid),
        INT2ARRAYOID => elements_to_anyarray_helper::<i16>(elements, array_typoid),
        INT4ARRAYOID => elements_to_anyarray_helper::<i32>(elements, array_typoid),
        INT8ARRAYOID => elements_to_anyarray_helper::<i64>(elements, array_typoid),
        NUMERICARRAYOID => {
            unimplemented!("numeric type is not supported yet");
        }
        DATEARRAYOID => elements_to_anyarray_helper::<Date>(elements, array_typoid),
        TIMESTAMPARRAYOID => elements_to_anyarray_helper::<Timestamp>(elements, array_typoid),
        TIMESTAMPTZARRAYOID => {
            elements_to_anyarray_helper::<TimestampWithTimeZone>(elements, array_typoid)
        }
        TIMEARRAYOID => elements_to_anyarray_helper::<Time>(elements, array_typoid),
        TIMETZARRAYOID => elements_to_anyarray_helper::<TimeWithTimeZone>(elements, array_typoid),
        INTERVALARRAYOID => elements_to_anyarray_helper::<Interval>(elements, array_typoid),
        CHARARRAYOID => elements_to_anyarray_helper::<i8>(elements, array_typoid),
        TEXTARRAYOID | VARCHARARRAYOID | BPCHARARRAYOID => {
            elements_to_anyarray_helper::<String>(elements, array_typoid)
        }
        UUIDARRAYOID => elements_to_anyarray_helper::<Uuid>(elements, array_typoid),
        JSONARRAYOID => elements_to_anyarray_helper::<Json>(elements, array_typoid),
        _ => {
            let elem_typoid = unsafe { pg_sys::get_element_type(array_typoid) };
            let is_composite_type = unsafe { pg_sys::type_is_rowtype(elem_typoid) };
            if is_composite_type {
                elements_to_anyarray_helper::<PgHeapTuple<'_, AllocatedByRust>>(
                    elements,
                    array_typoid,
                )
            } else {
                panic!("unsupported array type {}", array_typoid)
            }
        }
    };

    (array, lengths, tupledesc)
}

fn collect_attribute_array_from_tuples(
    tuples: &[PgHeapTuple<'_, AllocatedByRust>],
    attribute_name: &str,
) -> Vec<pgrx::AnyElement> {
    let mut attribute_values = vec![];

    for record in tuples {
        // todo: verify no duplicates
        let attribute_val = record.get_by_name(attribute_name).unwrap().unwrap();
        attribute_values.push(attribute_val);
    }

    attribute_values
}

pub(crate) fn tupledesc_for_typeoid(typoid: Oid) -> Option<PgTupleDesc<'static>> {
    let is_composite_type = unsafe { pg_sys::type_is_rowtype(typoid) };
    if !is_composite_type {
        return None;
    }

    PgTupleDesc::for_composite_type_by_oid(typoid)
}

pub(crate) fn tupledesc_for_tuples(
    tuples: Vec<PgHeapTuple<'_, AllocatedByRust>>,
) -> (Vec<PgHeapTuple<'_, AllocatedByRust>>, PgTupleDesc) {
    unsafe {
        let tuples = tuples
            .into_iter()
            .map(|x| x.into_datum().unwrap())
            .collect::<Vec<_>>();

        let tuple_datum = tuples.first().unwrap();
        let htup_header =
            pg_sys::pg_detoast_datum(tuple_datum.cast_mut_ptr()) as pg_sys::HeapTupleHeader;
        let tup_type = htup_header.as_ref().unwrap().t_choice.t_datum.datum_typeid;
        let tup_typmod = htup_header.as_ref().unwrap().t_choice.t_datum.datum_typmod;
        let tup_desc = pg_sys::lookup_rowtype_tupdesc(tup_type, tup_typmod);

        let tuples = tuples
            .into_iter()
            .map(|x| PgHeapTuple::from_datum(x, false).unwrap())
            .collect::<Vec<_>>();
        (tuples, PgTupleDesc::from_pg(tup_desc))
    }
}

fn heap_tuple_array_to_columnar_arrays(
    tuples: Vec<PgHeapTuple<'static, AllocatedByRust>>,
    tuple_desc: PgTupleDesc,
) -> Vec<(pgrx::AnyArray, Strides, Option<PgTupleDesc<'static>>)> {
    let mut columnar_arrays_with_strides_and_tupdescs = vec![];

    if tuples.is_empty() {
        return columnar_arrays_with_strides_and_tupdescs;
    }

    for attribute_idx in 0..tuple_desc.len() {
        let attribute = tuple_desc.get(attribute_idx).unwrap();

        if attribute.is_dropped() {
            continue;
        }

        let attribute_name = attribute.name();
        let attribute_values = collect_attribute_array_from_tuples(&tuples, attribute_name);

        let attribute_typoid = attribute.type_oid().value();
        let attribute_is_array = unsafe { pg_sys::type_is_array(attribute_typoid) };

        let attribute_array_with_strides_and_tupdesc = if attribute_is_array {
            flatten_arrays(attribute_values, attribute_typoid)
        } else {
            elements_to_anyarray(attribute_values, attribute_typoid)
        };

        columnar_arrays_with_strides_and_tupdescs.push(attribute_array_with_strides_and_tupdesc);
    }

    columnar_arrays_with_strides_and_tupdescs
}

fn serialize_array_internal<D: parquet::data_type::DataType>(
    row_group_writer: &mut SerializedRowGroupWriter<File>,
    value: Vec<D::T>,
    level_strides: &mut Vec<Strides>,
) {
    let mut column_writer = row_group_writer.next_column().unwrap().unwrap();

    let col_desc = column_writer.typed::<D>().get_descriptor();
    let def_levels = definition_vector_from_col_desc(col_desc, value.len());
    let rep_levels = repetition_vector_from_col_desc(col_desc, level_strides, value.len());

    column_writer
        .typed::<D>()
        .write_batch(&value, Some(&def_levels), Some(&rep_levels))
        .unwrap();
    column_writer.close().unwrap();
}

pub(crate) fn serialize_array(
    array: pgrx::AnyArray,
    tupledesc: Option<PgTupleDesc>,
    level_strides: &mut Vec<Strides>,
    row_group_writer: &mut SerializedRowGroupWriter<File>,
) {
    let is_array_of_composite = tupledesc.is_some();
    if is_array_of_composite {
        let tupledesc = tupledesc.unwrap();

        let tuples = unsafe {
            Vec::<PgHeapTuple<'_, AllocatedByRust>>::from_polymorphic_datum(
                array.datum(),
                false,
                array.oid(),
            )
            .unwrap()
        };

        // each attribute belongs to a separate column, so we need to
        // serialize them separately as column chunks
        let columnar_attribute_arrays_with_strides =
            heap_tuple_array_to_columnar_arrays(tuples, tupledesc);

        for (columnar_attribute_array, strides, tupledesc) in
            columnar_attribute_arrays_with_strides.into_iter()
        {
            level_strides.push(strides);
            serialize_array(
                columnar_attribute_array,
                tupledesc,
                level_strides,
                row_group_writer,
            );
            level_strides.pop();
        }

        return;
    }

    match array.oid() {
        FLOAT4ARRAYOID => {
            let value = unsafe {
                Vec::<f32>::from_polymorphic_datum(array.datum(), false, array.oid()).unwrap()
            };
            serialize_array_internal::<FloatType>(row_group_writer, value, level_strides);
        }
        FLOAT8ARRAYOID => {
            let value = unsafe {
                Vec::<f64>::from_polymorphic_datum(array.datum(), false, array.oid()).unwrap()
            };
            serialize_array_internal::<DoubleType>(row_group_writer, value, level_strides);
        }
        BOOLARRAYOID => {
            let value = unsafe {
                Vec::<bool>::from_polymorphic_datum(array.datum(), false, array.oid()).unwrap()
            };
            let value: Vec<i32> = value.into_iter().map(|x| x as i32).collect();
            serialize_array_internal::<Int32Type>(row_group_writer, value, level_strides);
        }
        INT2ARRAYOID => {
            let value = unsafe {
                Vec::<i16>::from_polymorphic_datum(array.datum(), false, array.oid()).unwrap()
            };
            let value = value.into_iter().map(|x| x as i32).collect();
            serialize_array_internal::<Int32Type>(row_group_writer, value, level_strides);
        }
        INT4ARRAYOID => {
            let value = unsafe {
                Vec::<i32>::from_polymorphic_datum(array.datum(), false, array.oid()).unwrap()
            };
            serialize_array_internal::<Int32Type>(row_group_writer, value, level_strides);
        }
        INT8ARRAYOID => {
            let value = unsafe {
                Vec::<i64>::from_polymorphic_datum(array.datum(), false, array.oid()).unwrap()
            };
            serialize_array_internal::<Int64Type>(row_group_writer, value, level_strides);
        }
        NUMERICARRAYOID => {
            unimplemented!("numeric type is not supported yet");
        }
        DATEARRAYOID => {
            let value = unsafe {
                Vec::<Date>::from_polymorphic_datum(array.datum(), false, array.oid()).unwrap()
            };
            serialize_array_internal::<Int32Type>(
                row_group_writer,
                value.into_iter().map(date_to_i32).collect(),
                level_strides,
            );
        }
        TIMESTAMPARRAYOID => {
            let value = unsafe {
                Vec::<Timestamp>::from_polymorphic_datum(array.datum(), false, array.oid()).unwrap()
            };
            serialize_array_internal::<Int64Type>(
                row_group_writer,
                value.into_iter().map(timestamp_to_i64).collect(),
                level_strides,
            );
        }
        TIMESTAMPTZARRAYOID => {
            let value = unsafe {
                Vec::<TimestampWithTimeZone>::from_polymorphic_datum(
                    array.datum(),
                    false,
                    array.oid(),
                )
                .unwrap()
            };
            serialize_array_internal::<Int64Type>(
                row_group_writer,
                value.into_iter().map(timestamptz_to_i64).collect(),
                level_strides,
            );
        }
        TIMEARRAYOID => {
            let value = unsafe {
                Vec::<Time>::from_polymorphic_datum(array.datum(), false, array.oid()).unwrap()
            };
            serialize_array_internal::<Int64Type>(
                row_group_writer,
                value.into_iter().map(time_to_i64).collect(),
                level_strides,
            );
        }
        TIMETZARRAYOID => {
            let value = unsafe {
                Vec::<TimeWithTimeZone>::from_polymorphic_datum(array.datum(), false, array.oid())
                    .unwrap()
            };
            serialize_array_internal::<Int64Type>(
                row_group_writer,
                value.into_iter().map(timetz_to_i64).collect(),
                level_strides,
            );
        }
        INTERVALARRAYOID => {
            let value = unsafe {
                Vec::<Interval>::from_polymorphic_datum(array.datum(), false, array.oid()).unwrap()
            };
            serialize_array_internal::<FixedLenByteArrayType>(
                row_group_writer,
                value
                    .into_iter()
                    .map(interval_to_fixed_byte_array)
                    .collect(),
                level_strides,
            );
        }
        CHARARRAYOID => {
            let value = unsafe {
                Vec::<i8>::from_polymorphic_datum(array.datum(), false, array.oid()).unwrap()
            };
            let value: Vec<ByteArray> = value.into_iter().map(|x| vec![x as u8].into()).collect();
            serialize_array_internal::<ByteArrayType>(row_group_writer, value, level_strides);
        }
        TEXTARRAYOID | VARCHARARRAYOID | BPCHARARRAYOID => {
            let value = unsafe {
                Vec::<String>::from_polymorphic_datum(array.datum(), false, array.oid()).unwrap()
            };
            let value: Vec<ByteArray> = value.into_iter().map(|x| x.as_bytes().into()).collect();
            serialize_array_internal::<ByteArrayType>(row_group_writer, value, level_strides);
        }
        UUIDARRAYOID => {
            let value = unsafe {
                Vec::<Uuid>::from_polymorphic_datum(array.datum(), false, array.oid()).unwrap()
            };
            serialize_array_internal::<FixedLenByteArrayType>(
                row_group_writer,
                value.into_iter().map(uuid_to_fixed_byte_array).collect(),
                level_strides,
            );
        }
        JSONARRAYOID => {
            let value = unsafe {
                Vec::<Json>::from_polymorphic_datum(array.datum(), false, array.oid()).unwrap()
            };
            serialize_array_internal::<ByteArrayType>(
                row_group_writer,
                value.into_iter().map(json_to_byte_array).collect(),
                level_strides,
            );
        }
        _ => {
            panic!("unsupported array type {}", array.oid());
        }
    }
}
