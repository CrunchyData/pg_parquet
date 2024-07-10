use std::sync::Arc;

use arrow::{
    array::{
        make_array, Array, ArrayRef, BooleanArray, Date32Array, FixedSizeBinaryArray, Float32Array,
        Float64Array, Int16Array, Int32Array, Int64Array, ListArray, StringArray, StructArray,
        Time64MicrosecondArray, TimestampMicrosecondArray,
    },
    buffer::{OffsetBuffer, ScalarBuffer},
    datatypes::{Field, FieldRef, Fields},
};

use pg_sys::{
    Datum, InputFunctionCall, Oid, BOOLARRAYOID, BPCHARARRAYOID, CHARARRAYOID, DATEARRAYOID,
    FLOAT4ARRAYOID, FLOAT8ARRAYOID, INT2ARRAYOID, INT4ARRAYOID, INT8ARRAYOID, TEXTARRAYOID,
    TIMEARRAYOID, TIMESTAMPARRAYOID, TIMESTAMPTZARRAYOID, TIMETZARRAYOID, VARCHARARRAYOID,
};
use pgrx::prelude::*;
use pgrx::{direct_function_call, PgTupleDesc};

use crate::{
    conversion::{date_to_i32, time_to_i64, timestamp_to_i64, timestamptz_to_i64, timetz_to_i64},
    pgrx_utils::{collect_attributes, tupledesc_for_typeoid},
};

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

fn flatten_arrays_helper<T: IntoDatum + FromDatum>(
    array_typoid: Oid,
    array_datums: Vec<Datum>,
) -> pgrx::AnyArray {
    let mut array_vectors = Vec::with_capacity(array_datums.len());
    for datum in array_datums.into_iter() {
        let array =
            unsafe { Vec::<T>::from_polymorphic_datum(datum, false, array_typoid).unwrap() };
        array_vectors.push(array);
    }

    let flatten_array = array_vectors.into_iter().flatten().collect::<Vec<_>>();

    unsafe {
        pgrx::AnyArray::from_polymorphic_datum(
            flatten_array.into_datum().unwrap(),
            false,
            array_typoid,
        )
        .unwrap()
    }
}

fn flatten_arrays(
    arrays: Vec<pgrx::AnyElement>,
    array_typoid: Oid,
) -> (
    pgrx::AnyArray,
    Option<PgTupleDesc<'static>>,
    OffsetBuffer<i32>,
) {
    assert!(unsafe { pg_sys::type_is_array(array_typoid) });

    let array_element_typoid = unsafe { pg_sys::get_element_type(array_typoid) };
    let tupledesc = tupledesc_for_typeoid(array_element_typoid);

    if arrays.is_empty() {
        return (
            array_default(array_typoid),
            tupledesc,
            OffsetBuffer::new(ScalarBuffer::from(vec![0; arrays.len() + 1])),
        );
    }

    let array_datums = arrays.into_iter().map(|x| x.datum()).collect::<Vec<_>>();

    let mut offsets = vec![0];
    let mut current_offset = 0;
    for datum in &array_datums {
        let len: i32 = unsafe {
            direct_function_call(pg_sys::array_length, &[datum.into_datum(), 1.into_datum()])
                .unwrap()
        };
        current_offset += len;
        offsets.push(current_offset);
    }

    let flatten_array = match array_typoid {
        FLOAT4ARRAYOID => flatten_arrays_helper::<f32>(array_typoid, array_datums),
        FLOAT8ARRAYOID => flatten_arrays_helper::<f64>(array_typoid, array_datums),
        BOOLARRAYOID => flatten_arrays_helper::<bool>(array_typoid, array_datums),
        INT2ARRAYOID => flatten_arrays_helper::<i16>(array_typoid, array_datums),
        INT4ARRAYOID => flatten_arrays_helper::<i32>(array_typoid, array_datums),
        INT8ARRAYOID => flatten_arrays_helper::<i64>(array_typoid, array_datums),
        DATEARRAYOID => flatten_arrays_helper::<Date>(array_typoid, array_datums),
        TIMESTAMPARRAYOID => flatten_arrays_helper::<Timestamp>(array_typoid, array_datums),
        TIMESTAMPTZARRAYOID => {
            flatten_arrays_helper::<TimestampWithTimeZone>(array_typoid, array_datums)
        }
        TIMEARRAYOID => flatten_arrays_helper::<Time>(array_typoid, array_datums),
        TIMETZARRAYOID => flatten_arrays_helper::<TimeWithTimeZone>(array_typoid, array_datums),
        CHARARRAYOID => flatten_arrays_helper::<i8>(array_typoid, array_datums),
        TEXTARRAYOID | VARCHARARRAYOID | BPCHARARRAYOID => {
            flatten_arrays_helper::<String>(array_typoid, array_datums)
        }
        _ => {
            let is_composite_array = unsafe { pg_sys::type_is_rowtype(array_element_typoid) };
            if is_composite_array {
                flatten_arrays_helper::<PgHeapTuple<'_, AllocatedByRust>>(
                    array_element_typoid,
                    array_datums,
                )
            } else {
                panic!("unsupported array type {}", array_typoid);
            }
        }
    };

    (
        flatten_array,
        tupledesc,
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
    )
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
) -> (pgrx::AnyArray, Option<PgTupleDesc<'static>>) {
    let tupledesc = tupledesc_for_typeoid(element_typoid);

    let array_typoid = unsafe { pg_sys::get_array_type(element_typoid) };
    let array = match array_typoid {
        FLOAT4ARRAYOID => elements_to_anyarray_helper::<f32>(elements, array_typoid),
        FLOAT8ARRAYOID => elements_to_anyarray_helper::<f64>(elements, array_typoid),
        BOOLARRAYOID => elements_to_anyarray_helper::<bool>(elements, array_typoid),
        INT2ARRAYOID => elements_to_anyarray_helper::<i16>(elements, array_typoid),
        INT4ARRAYOID => elements_to_anyarray_helper::<i32>(elements, array_typoid),
        INT8ARRAYOID => elements_to_anyarray_helper::<i64>(elements, array_typoid),
        DATEARRAYOID => elements_to_anyarray_helper::<Date>(elements, array_typoid),
        TIMESTAMPARRAYOID => elements_to_anyarray_helper::<Timestamp>(elements, array_typoid),
        TIMESTAMPTZARRAYOID => {
            elements_to_anyarray_helper::<TimestampWithTimeZone>(elements, array_typoid)
        }
        TIMEARRAYOID => elements_to_anyarray_helper::<Time>(elements, array_typoid),
        TIMETZARRAYOID => elements_to_anyarray_helper::<TimeWithTimeZone>(elements, array_typoid),
        CHARARRAYOID => elements_to_anyarray_helper::<i8>(elements, array_typoid),
        TEXTARRAYOID | VARCHARARRAYOID | BPCHARARRAYOID => {
            elements_to_anyarray_helper::<String>(elements, array_typoid)
        }
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

    (array, tupledesc)
}

fn collect_attribute_array_from_tuples(
    tuples: &[PgHeapTuple<'_, AllocatedByRust>],
    attribute_name: &str,
) -> Vec<pgrx::AnyElement> {
    let mut attribute_values = vec![];

    for record in tuples {
        let attribute_val = record.get_by_name(attribute_name).unwrap().unwrap();
        attribute_values.push(attribute_val);
    }

    attribute_values
}

fn list_array_from_primitive_data(
    name: &str,
    primitive_array: ArrayRef,
    offsets: OffsetBuffer<i32>,
) -> (Arc<Field>, ArrayRef) {
    let field = match primitive_array.data_type() {
        arrow::datatypes::DataType::Boolean => {
            Arc::new(Field::new(name, arrow::datatypes::DataType::Boolean, false))
        }
        arrow::datatypes::DataType::Int16 => {
            Arc::new(Field::new(name, arrow::datatypes::DataType::Int16, false))
        }
        arrow::datatypes::DataType::Int32 => {
            Arc::new(Field::new(name, arrow::datatypes::DataType::Int32, false))
        }
        arrow::datatypes::DataType::Int64 => {
            Arc::new(Field::new(name, arrow::datatypes::DataType::Int64, false))
        }
        arrow::datatypes::DataType::Float32 => {
            Arc::new(Field::new(name, arrow::datatypes::DataType::Float32, false))
        }
        arrow::datatypes::DataType::Float64 => {
            Arc::new(Field::new(name, arrow::datatypes::DataType::Float64, false))
        }
        arrow::datatypes::DataType::Timestamp(timeunit, timezone) => Arc::new(Field::new(
            name,
            arrow::datatypes::DataType::Timestamp(timeunit.clone(), timezone.clone()),
            false,
        )),
        arrow::datatypes::DataType::Date32 => {
            Arc::new(Field::new(name, arrow::datatypes::DataType::Date32, false))
        }
        arrow::datatypes::DataType::Time64(timeunit) => Arc::new(Field::new(
            name,
            arrow::datatypes::DataType::Time64(timeunit.clone()),
            false,
        )),
        arrow::datatypes::DataType::Interval(interval_unit) => Arc::new(Field::new(
            name,
            arrow::datatypes::DataType::Interval(interval_unit.clone()),
            false,
        )),
        arrow::datatypes::DataType::FixedSizeBinary(size) => Arc::new(Field::new(
            name,
            arrow::datatypes::DataType::FixedSizeBinary(size.clone()),
            false,
        )),
        arrow::datatypes::DataType::Utf8 => {
            Arc::new(Field::new(name, arrow::datatypes::DataType::Utf8, false))
        }
        _ => {
            panic!("unsupported array type");
        }
    };

    let list_array = ListArray::new(field.clone(), offsets, primitive_array, None);
    let list_array = make_array(list_array.into());
    let list_field = Arc::new(Field::new(
        name,
        arrow::datatypes::DataType::List(field),
        false,
    ));
    (list_field, list_array)
}

fn list_array_from_struct_data(
    struct_field: Arc<Field>,
    struct_array: ArrayRef,
    offsets: OffsetBuffer<i32>,
) -> (Arc<Field>, ArrayRef) {
    let list_array = ListArray::new(struct_field.clone(), offsets, struct_array, None);
    let list_array = make_array(list_array.into());
    let list_field = Arc::new(Field::new(
        struct_field.name().clone(),
        arrow::datatypes::DataType::List(struct_field),
        false,
    ));
    (list_field, list_array)
}

fn visit_struct_array(
    name: &str,
    tuples: pgrx::AnyArray,
    tupledesc: &PgTupleDesc,
) -> (FieldRef, ArrayRef) {
    let mut struct_fields_with_data: Vec<(Arc<Field>, ArrayRef)> = vec![];

    let tuples = unsafe {
        Vec::<PgHeapTuple<'_, AllocatedByRust>>::from_polymorphic_datum(
            tuples.datum(),
            false,
            tuples.oid(),
        )
        .unwrap()
    };

    let attributes = collect_attributes(&tupledesc);

    for attribute in attributes {
        let attribute_name = attribute.name();
        let attribute_typoid = attribute.type_oid().value();

        let attribute_is_array = unsafe { pg_sys::type_is_array(attribute_typoid) };

        let attribute_is_composite = unsafe { pg_sys::type_is_rowtype(attribute_typoid) };

        let attribute_values = collect_attribute_array_from_tuples(&tuples, attribute_name);

        let (field, array) = if attribute_is_array {
            let (attribute_values, tupledesc, offsets) =
                flatten_arrays(attribute_values, attribute_typoid);
            visit_list_array(attribute_name, attribute_values, tupledesc, offsets)
        } else if attribute_is_composite {
            let (attribute_values, tupledesc) =
                elements_to_anyarray(attribute_values, attribute_typoid);
            visit_struct_array(attribute_name, attribute_values, &tupledesc.unwrap())
        } else {
            let (attribute_values, _) = elements_to_anyarray(attribute_values, attribute_typoid);
            visit_primitive_array(attribute_name, attribute_values)
        };

        struct_fields_with_data.push((field, array));
    }

    // finalize StructArray
    let mut struct_attribute_fields = vec![];
    for (field, _) in struct_fields_with_data.iter() {
        struct_attribute_fields.push(field.clone());
    }

    let struct_field = Arc::new(Field::new(
        name,
        arrow::datatypes::DataType::Struct(Fields::from(struct_attribute_fields)),
        false,
    ));

    let struct_array = StructArray::from(struct_fields_with_data);
    let struct_array = make_array(struct_array.into());

    (struct_field, struct_array)
}

pub(crate) fn visit_list_array(
    name: &str,
    array: pgrx::AnyArray,
    tupledesc: Option<PgTupleDesc>,
    offsets: OffsetBuffer<i32>,
) -> (Arc<Field>, ArrayRef) {
    let is_array_of_composite = tupledesc.is_some();
    if is_array_of_composite {
        let tupledesc = tupledesc.unwrap();
        let (struct_field, struct_array) = visit_struct_array(name, array, &tupledesc);
        list_array_from_struct_data(struct_field, struct_array, offsets)
    } else {
        let (_, primitive_array) = visit_primitive_array(name, array);
        list_array_from_primitive_data(name, primitive_array, offsets)
    }
}

fn visit_primitive_array(name: &str, array: pgrx::AnyArray) -> (Arc<Field>, ArrayRef) {
    let array_oid = array.oid();
    match array_oid {
        FLOAT4ARRAYOID => {
            let value = unsafe {
                Vec::<f32>::from_polymorphic_datum(array.datum(), false, array_oid).unwrap()
            };
            let array = Float32Array::from(value);
            (
                Arc::new(Field::new(name, arrow::datatypes::DataType::Float32, false)),
                Arc::new(array),
            )
        }
        FLOAT8ARRAYOID => {
            let value = unsafe {
                Vec::<f64>::from_polymorphic_datum(array.datum(), false, array_oid).unwrap()
            };
            let array = Float64Array::from(value);
            (
                Arc::new(Field::new(name, arrow::datatypes::DataType::Float64, false)),
                Arc::new(array),
            )
        }
        BOOLARRAYOID => {
            let value = unsafe {
                Vec::<bool>::from_polymorphic_datum(array.datum(), false, array_oid).unwrap()
            };
            let array = BooleanArray::from(value);
            (
                Arc::new(Field::new(name, arrow::datatypes::DataType::Boolean, false)),
                Arc::new(array),
            )
        }
        INT2ARRAYOID => {
            let value = unsafe {
                Vec::<i16>::from_polymorphic_datum(array.datum(), false, array_oid).unwrap()
            };
            let array = Int16Array::from(value);
            (
                Arc::new(Field::new(name, arrow::datatypes::DataType::Int16, false)),
                Arc::new(array),
            )
        }
        INT4ARRAYOID => {
            let value = unsafe {
                Vec::<i32>::from_polymorphic_datum(array.datum(), false, array_oid).unwrap()
            };
            let array = Int32Array::from(value);
            (
                Arc::new(Field::new(name, arrow::datatypes::DataType::Int32, false)),
                Arc::new(array),
            )
        }
        INT8ARRAYOID => {
            let value = unsafe {
                Vec::<i64>::from_polymorphic_datum(array.datum(), false, array_oid).unwrap()
            };
            let array = Int64Array::from(value);
            (
                Arc::new(Field::new(name, arrow::datatypes::DataType::Int64, false)),
                Arc::new(array),
            )
        }
        DATEARRAYOID => {
            let value = unsafe {
                Vec::<Date>::from_polymorphic_datum(array.datum(), false, array_oid).unwrap()
            };
            let value = value.into_iter().map(date_to_i32).collect::<Vec<_>>();
            let array = Date32Array::from(value);
            (
                Arc::new(Field::new(name, arrow::datatypes::DataType::Date32, false)),
                Arc::new(array),
            )
        }
        TIMESTAMPARRAYOID => {
            let value = unsafe {
                Vec::<Timestamp>::from_polymorphic_datum(array.datum(), false, array_oid).unwrap()
            };
            let value = value.into_iter().map(timestamp_to_i64).collect::<Vec<_>>();

            let array = TimestampMicrosecondArray::from(value);
            (
                Arc::new(Field::new(
                    name,
                    arrow::datatypes::DataType::Timestamp(
                        arrow::datatypes::TimeUnit::Microsecond,
                        None,
                    ),
                    false,
                )),
                Arc::new(array),
            )
        }
        TIMESTAMPTZARRAYOID => {
            let value = unsafe {
                Vec::<TimestampWithTimeZone>::from_polymorphic_datum(
                    array.datum(),
                    false,
                    array_oid,
                )
                .unwrap()
            };
            let value = value
                .into_iter()
                .map(timestamptz_to_i64)
                .collect::<Vec<_>>();

            let array = TimestampMicrosecondArray::from(value).with_timezone_utc();
            (
                Arc::new(Field::new(
                    name,
                    arrow::datatypes::DataType::Timestamp(
                        arrow::datatypes::TimeUnit::Microsecond,
                        Some("+00:00".into()),
                    ),
                    false,
                )),
                Arc::new(array),
            )
        }
        TIMEARRAYOID => {
            let value = unsafe {
                Vec::<Time>::from_polymorphic_datum(array.datum(), false, array_oid).unwrap()
            };
            let value = value.into_iter().map(time_to_i64).collect::<Vec<_>>();
            let array = Time64MicrosecondArray::from(value);
            (
                Arc::new(Field::new(
                    name,
                    arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
                    false,
                )),
                Arc::new(array),
            )
        }
        TIMETZARRAYOID => {
            let value = unsafe {
                Vec::<TimeWithTimeZone>::from_polymorphic_datum(array.datum(), false, array_oid)
                    .unwrap()
            };
            let value = value.into_iter().map(timetz_to_i64).collect::<Vec<_>>();
            let array = Time64MicrosecondArray::from(value);
            (
                Arc::new(Field::new(
                    name,
                    arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
                    false,
                )),
                Arc::new(array),
            )
        }
        CHARARRAYOID => {
            let value = unsafe {
                Vec::<i8>::from_polymorphic_datum(array.datum(), false, array_oid).unwrap()
            };
            let value = value.into_iter().map(|x| vec![x as u8]).collect::<Vec<_>>();
            let array = FixedSizeBinaryArray::try_from_iter(value.iter()).unwrap();
            (
                Arc::new(Field::new(
                    name,
                    arrow::datatypes::DataType::FixedSizeBinary(1),
                    false,
                )),
                Arc::new(array),
            )
        }
        TEXTARRAYOID | VARCHARARRAYOID | BPCHARARRAYOID => {
            let value = unsafe {
                Vec::<String>::from_polymorphic_datum(array.datum(), false, array_oid).unwrap()
            };
            let array = StringArray::from(value);
            (
                Arc::new(Field::new(name, arrow::datatypes::DataType::Utf8, false)),
                Arc::new(array),
            )
        }
        _ => {
            panic!("unsupported array type {}", array_oid);
        }
    }
}
