use std::collections::HashSet;
use std::fs::File;

use parquet::{
    data_type::{
        ByteArray, ByteArrayType, DoubleType, FixedLenByteArray, FixedLenByteArrayType, FloatType,
        Int32Type, Int64Type,
    },
    file::writer::{SerializedFileWriter, SerializedRowGroupWriter},
    schema::printer,
    schema::types::{ColumnDescriptor, TypePtr},
};

use pg_sys::{
    InputFunctionCall, Oid, BOOLOID, BPCHAROID, CHAROID, DATEOID, FLOAT4ARRAYOID, FLOAT4OID,
    FLOAT8OID, INT2OID, INT4OID, INT8OID, INTERVALOID, JSONOID, NUMERICOID, TEXTOID, TIMEOID,
    TIMESTAMPOID, TIMESTAMPTZOID, TIMETZOID, UUIDOID, VARCHAROID,
};
use pgrx::prelude::*;
use pgrx::{direct_function_call, Json, PgTupleDesc, Uuid};

pgrx::pg_module_magic!();

#[pg_schema]
mod pgparquet {
    use super::*;

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

    fn parse_record_schema(tupledesc: PgTupleDesc, elem_name: &'static str) -> TypePtr {
        let mut child_fields: Vec<TypePtr> = vec![];

        for attribute_idx in 0..tupledesc.len() {
            let attribute = tupledesc.get(attribute_idx).unwrap();

            if attribute.is_dropped() {
                continue;
            }

            let attribute_name = attribute.name();
            let attribute_oid = attribute.type_oid().value();

            let is_attribute_composite = unsafe { pg_sys::type_is_rowtype(attribute_oid) };
            let is_attribute_array = unsafe { pg_sys::type_is_array(attribute_oid) };

            let child_field = if is_attribute_composite {
                let attribute_tupledesc =
                    unsafe { pg_sys::lookup_rowtype_tupdesc(attribute_oid, 0) };
                let attribute_tupledesc = unsafe { PgTupleDesc::from_pg(attribute_tupledesc) };
                parse_record_schema(
                    attribute_tupledesc,
                    // todo: do not leak
                    attribute_name.to_string().leak(),
                )
            } else if is_attribute_array {
                parse_array_schema(
                    attribute.type_oid().value(),
                    // todo: do not leak
                    attribute_name.to_string().leak(),
                )
            } else {
                parse_primitive_schema(
                    attribute.type_oid().value(),
                    // todo: do not leak
                    attribute_name.to_string().leak(),
                )
            };

            child_fields.push(child_field);
        }

        parquet::schema::types::Type::group_type_builder(elem_name)
            .with_fields(child_fields)
            .with_repetition(parquet::basic::Repetition::REQUIRED)
            .build()
            .unwrap()
            .into()
    }

    fn parse_array_schema(arraytypoid: Oid, array_name: &'static str) -> TypePtr {
        let array_element_typoid = unsafe { pg_sys::get_element_type(arraytypoid) };
        let is_array_of_composite = unsafe { pg_sys::type_is_rowtype(array_element_typoid) };
        if is_array_of_composite {
            let array_element_tupledesc =
                unsafe { pg_sys::lookup_rowtype_tupdesc(array_element_typoid, 0) };
            let array_element_tupledesc = unsafe { PgTupleDesc::from_pg(array_element_tupledesc) };
            let element_group_builder = parse_record_schema(array_element_tupledesc, array_name);

            let list_group_builder = parquet::schema::types::Type::group_type_builder(array_name)
                .with_fields(vec![element_group_builder.into()])
                .with_repetition(parquet::basic::Repetition::REPEATED)
                .with_logical_type(Some(parquet::basic::LogicalType::List))
                .build()
                .unwrap();

            return list_group_builder.into();
        }

        match arraytypoid {
            FLOAT4ARRAYOID => {
                let float4_type_builder = parquet::schema::types::Type::primitive_type_builder(
                    array_name,
                    parquet::basic::Type::FLOAT,
                )
                .with_repetition(parquet::basic::Repetition::REQUIRED)
                .build()
                .unwrap();

                let list_group_builder =
                    parquet::schema::types::Type::group_type_builder(array_name)
                        .with_fields(vec![float4_type_builder.into()])
                        .with_repetition(parquet::basic::Repetition::REPEATED)
                        .with_logical_type(Some(parquet::basic::LogicalType::List))
                        .build()
                        .unwrap();

                list_group_builder.into()
            }
            _ => {
                panic!("unsupported array type {}", arraytypoid);
            }
        }
    }

    fn parse_primitive_schema(typoid: Oid, elem_name: &'static str) -> TypePtr {
        match typoid {
            FLOAT4OID => parquet::schema::types::Type::primitive_type_builder(
                elem_name,
                parquet::basic::Type::FLOAT,
            )
            .with_repetition(parquet::basic::Repetition::REQUIRED)
            .build()
            .unwrap()
            .into(),
            FLOAT8OID => parquet::schema::types::Type::primitive_type_builder(
                elem_name,
                parquet::basic::Type::DOUBLE,
            )
            .with_repetition(parquet::basic::Repetition::REQUIRED)
            .build()
            .unwrap()
            .into(),
            INT2OID => parquet::schema::types::Type::primitive_type_builder(
                elem_name,
                parquet::basic::Type::INT32,
            )
            .with_logical_type(Some(parquet::basic::LogicalType::Integer {
                bit_width: 16,
                is_signed: true,
            }))
            .with_repetition(parquet::basic::Repetition::REQUIRED)
            .build()
            .unwrap()
            .into(),
            INT4OID => parquet::schema::types::Type::primitive_type_builder(
                elem_name,
                parquet::basic::Type::INT32,
            )
            .with_repetition(parquet::basic::Repetition::REQUIRED)
            .build()
            .unwrap()
            .into(),
            INT8OID => parquet::schema::types::Type::primitive_type_builder(
                elem_name,
                parquet::basic::Type::INT64,
            )
            .with_repetition(parquet::basic::Repetition::REQUIRED)
            .build()
            .unwrap()
            .into(),
            NUMERICOID => {
                unimplemented!("numeric type is not supported yet");
            }
            DATEOID => parquet::schema::types::Type::primitive_type_builder(
                elem_name,
                parquet::basic::Type::INT32,
            )
            .with_logical_type(Some(parquet::basic::LogicalType::Date))
            .with_repetition(parquet::basic::Repetition::REQUIRED)
            .build()
            .unwrap()
            .into(),
            TIMESTAMPOID => parquet::schema::types::Type::primitive_type_builder(
                elem_name,
                parquet::basic::Type::INT64,
            )
            .with_logical_type(Some(parquet::basic::LogicalType::Timestamp {
                is_adjusted_to_u_t_c: false,
                unit: parquet::basic::TimeUnit::MICROS(parquet::format::MicroSeconds {}),
            }))
            .with_repetition(parquet::basic::Repetition::REQUIRED)
            .build()
            .unwrap()
            .into(),
            TIMESTAMPTZOID => parquet::schema::types::Type::primitive_type_builder(
                elem_name,
                parquet::basic::Type::INT64,
            )
            .with_logical_type(Some(parquet::basic::LogicalType::Timestamp {
                is_adjusted_to_u_t_c: true,
                unit: parquet::basic::TimeUnit::MICROS(parquet::format::MicroSeconds {}),
            }))
            .with_repetition(parquet::basic::Repetition::REQUIRED)
            .build()
            .unwrap()
            .into(),
            TIMEOID => parquet::schema::types::Type::primitive_type_builder(
                elem_name,
                parquet::basic::Type::INT64,
            )
            .with_logical_type(Some(parquet::basic::LogicalType::Time {
                is_adjusted_to_u_t_c: false,
                unit: parquet::basic::TimeUnit::MICROS(parquet::format::MicroSeconds {}),
            }))
            .with_repetition(parquet::basic::Repetition::REQUIRED)
            .build()
            .unwrap()
            .into(),
            TIMETZOID => parquet::schema::types::Type::primitive_type_builder(
                elem_name,
                parquet::basic::Type::INT64,
            )
            .with_logical_type(Some(parquet::basic::LogicalType::Time {
                is_adjusted_to_u_t_c: true,
                unit: parquet::basic::TimeUnit::MICROS(parquet::format::MicroSeconds {}),
            }))
            .with_repetition(parquet::basic::Repetition::REQUIRED)
            .build()
            .unwrap()
            .into(),
            INTERVALOID => parquet::schema::types::Type::primitive_type_builder(
                elem_name,
                parquet::basic::Type::FIXED_LEN_BYTE_ARRAY,
            )
            .with_length(12)
            .with_converted_type(parquet::basic::ConvertedType::INTERVAL)
            .with_repetition(parquet::basic::Repetition::REQUIRED)
            .build()
            .unwrap()
            .into(),
            CHAROID => parquet::schema::types::Type::primitive_type_builder(
                elem_name,
                parquet::basic::Type::BYTE_ARRAY,
            )
            .with_repetition(parquet::basic::Repetition::REQUIRED)
            .build()
            .unwrap()
            .into(),
            TEXTOID | VARCHAROID | BPCHAROID => {
                parquet::schema::types::Type::primitive_type_builder(
                    elem_name,
                    parquet::basic::Type::BYTE_ARRAY,
                )
                .with_logical_type(Some(parquet::basic::LogicalType::String))
                .with_repetition(parquet::basic::Repetition::REQUIRED)
                .build()
                .unwrap()
                .into()
            }
            BOOLOID => parquet::schema::types::Type::primitive_type_builder(
                elem_name,
                parquet::basic::Type::INT32,
            )
            .with_logical_type(Some(parquet::basic::LogicalType::Integer {
                bit_width: 8,
                is_signed: true,
            }))
            .with_repetition(parquet::basic::Repetition::REQUIRED)
            .build()
            .unwrap()
            .into(),
            UUIDOID => parquet::schema::types::Type::primitive_type_builder(
                elem_name,
                parquet::basic::Type::FIXED_LEN_BYTE_ARRAY,
            )
            .with_length(16)
            .with_logical_type(Some(parquet::basic::LogicalType::Uuid))
            .with_repetition(parquet::basic::Repetition::REQUIRED)
            .build()
            .unwrap()
            .into(),
            JSONOID => parquet::schema::types::Type::primitive_type_builder(
                elem_name,
                parquet::basic::Type::BYTE_ARRAY,
            )
            .with_logical_type(Some(parquet::basic::LogicalType::Json))
            .with_repetition(parquet::basic::Repetition::REQUIRED)
            .build()
            .unwrap()
            .into(),
            _ => {
                panic!("unsupported primitive type {}", typoid)
            }
        }
    }

    fn serialize_record(
        record: PgHeapTuple<'static, AllocatedByRust>,
        row_group_writer: &mut SerializedRowGroupWriter<File>,
    ) {
        for (_, attribute) in record.attributes() {
            if attribute.is_dropped() {
                continue;
            }

            let attribute_name = attribute.name();
            let attribute_oid = attribute.type_oid().value();

            let is_attribute_composite = unsafe { pg_sys::type_is_rowtype(attribute_oid) };
            let is_attribute_array = unsafe { pg_sys::type_is_array(attribute_oid) };

            if is_attribute_composite {
                let attribute_val = record
                    .get_by_name::<PgHeapTuple<'_, AllocatedByRust>>(attribute_name)
                    .unwrap()
                    .unwrap();
                serialize_record(attribute_val, row_group_writer);
            } else if is_attribute_array {
                let attribute_val = record
                    .get_by_name::<pgrx::AnyArray>(attribute_name)
                    .unwrap()
                    .unwrap();
                serialize_array(attribute_val, &mut vec![], row_group_writer);
            } else {
                let attribute_val = record
                    .get_by_name::<pgrx::AnyElement>(attribute_name)
                    .unwrap()
                    .unwrap();
                serialize_primitive(attribute_val, row_group_writer);
            }
        }
    }

    fn anyarray_default(array_typoid: Oid) -> pgrx::AnyArray {
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

    fn flatten_anyarrays(
        arrays: Vec<pgrx::AnyElement>,
        array_typoid: Oid,
    ) -> (pgrx::AnyArray, Strides) {
        assert!(unsafe { pg_sys::type_is_array(array_typoid) });

        if arrays.is_empty() {
            return (anyarray_default(array_typoid), vec![]);
        }

        let anyarrays = arrays.iter().map(|x| x.datum()).collect::<Vec<_>>();

        let mut lengths = Vec::<usize>::new();
        for array_datum in &anyarrays {
            let a_len: i32 = unsafe {
                direct_function_call(
                    pg_sys::array_length,
                    &[array_datum.into_datum(), 1.into_datum()],
                )
                .unwrap()
            };
            lengths.push(a_len as _);
        }

        let flatten_array_datum = anyarrays
            .into_iter()
            .reduce(|a, b| unsafe {
                direct_function_call(pg_sys::array_cat, &[a.into_datum(), b.into_datum()]).unwrap()
            })
            .unwrap();

        (
            unsafe {
                pgrx::AnyArray::from_polymorphic_datum(flatten_array_datum, false, array_typoid)
                    .unwrap()
            },
            lengths,
        )
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

    fn heap_tuple_array_to_columnar_arrays(
        tuples: Vec<PgHeapTuple<'_, AllocatedByRust>>,
        tuple_typoid: Oid,
    ) -> Vec<(pgrx::AnyArray, Strides)> {
        let mut columnar_arrays_with_strides = vec![];

        let tuple_desc = unsafe { pg_sys::lookup_rowtype_tupdesc(tuple_typoid, 0) };
        let tuple_desc = unsafe { PgTupleDesc::from_pg(tuple_desc) };

        for attribute_idx in 0..tuple_desc.len() {
            let attribute = tuple_desc.get(attribute_idx).unwrap();

            if attribute.is_dropped() {
                continue;
            }

            let attribute_name = attribute.name();
            let attribute_values = collect_attribute_array_from_tuples(&tuples, attribute_name);

            let attribute_typoid = attribute.type_oid().value();
            let attribute_is_array = unsafe { pg_sys::type_is_array(attribute_typoid) };

            let attribute_array_with_strides = if attribute_is_array {
                flatten_anyarrays(attribute_values, attribute_typoid)
            } else {
                let lengths = vec![1; attribute_values.len()];

                let attribute_array = unsafe {
                    let attribute_array_typoid = pg_sys::get_array_type(attribute_typoid);
                    let attribute_array_datum = attribute_values.into_datum().unwrap();
                    pgrx::AnyArray::from_polymorphic_datum(
                        attribute_array_datum,
                        false,
                        attribute_array_typoid,
                    )
                    .unwrap()
                };

                (attribute_array, lengths)
            };

            columnar_arrays_with_strides.push(attribute_array_with_strides);
        }

        columnar_arrays_with_strides
    }

    fn serialize_array(
        array: pgrx::AnyArray,
        level_strides: &mut Vec<Strides>,
        row_group_writer: &mut SerializedRowGroupWriter<File>,
    ) {
        let array_element_typoid = unsafe { pg_sys::get_element_type(array.oid()) };
        let is_array_of_composite = unsafe { pg_sys::type_is_rowtype(array_element_typoid) };
        if is_array_of_composite {
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
                heap_tuple_array_to_columnar_arrays(tuples, array_element_typoid);

            for (columnar_attribute_array, columnar_attribute_array_strides) in
                columnar_attribute_arrays_with_strides.into_iter()
            {
                level_strides.push(columnar_attribute_array_strides);
                serialize_array(columnar_attribute_array, level_strides, row_group_writer);
                level_strides.pop();
            }

            return;
        }

        match array.oid() {
            FLOAT4ARRAYOID => {
                let value = unsafe {
                    Vec::<f32>::from_polymorphic_datum(array.datum(), false, array.oid()).unwrap()
                };

                let mut column_writer = row_group_writer.next_column().unwrap().unwrap();

                let col_desc = column_writer.typed::<FloatType>().get_descriptor();
                let def_levels = definition_vector_from_col_desc(col_desc, value.len());
                let rep_levels =
                    repetition_vector_from_col_desc(col_desc, level_strides, value.len());

                column_writer
                    .typed::<FloatType>()
                    .write_batch(&value, Some(&def_levels), Some(&rep_levels))
                    .unwrap();

                column_writer.close().unwrap();
            }
            _ => {
                panic!("unsupported array type {}", array.oid());
            }
        }
    }

    fn serialize_primitive(
        elem: pgrx::AnyElement,
        row_group_writer: &mut SerializedRowGroupWriter<File>,
    ) {
        match elem.oid() {
            FLOAT4OID => {
                let value = unsafe {
                    f32::from_polymorphic_datum(elem.datum(), false, elem.oid()).unwrap()
                };

                let mut column_writer = row_group_writer.next_column().unwrap().unwrap();

                column_writer
                    .typed::<FloatType>()
                    .write_batch(&[value], None, None)
                    .unwrap();

                column_writer.close().unwrap();
            }
            FLOAT8OID => {
                let value = unsafe {
                    f64::from_polymorphic_datum(elem.datum(), false, elem.oid()).unwrap()
                };

                let mut column_writer = row_group_writer.next_column().unwrap().unwrap();

                column_writer
                    .typed::<DoubleType>()
                    .write_batch(&[value], None, None)
                    .unwrap();

                column_writer.close().unwrap();
            }
            INT2OID => {
                let value = unsafe {
                    i16::from_polymorphic_datum(elem.datum(), false, elem.oid()).unwrap()
                };

                let mut column_writer = row_group_writer.next_column().unwrap().unwrap();

                column_writer
                    .typed::<Int32Type>()
                    .write_batch(&[value as i32], None, None)
                    .unwrap();

                column_writer.close().unwrap();
            }
            INT4OID => {
                let value = unsafe {
                    i32::from_polymorphic_datum(elem.datum(), false, elem.oid()).unwrap()
                };

                let mut column_writer = row_group_writer.next_column().unwrap().unwrap();

                column_writer
                    .typed::<Int32Type>()
                    .write_batch(&[value], None, None)
                    .unwrap();

                column_writer.close().unwrap();
            }
            INT8OID => {
                let value = unsafe {
                    i64::from_polymorphic_datum(elem.datum(), false, elem.oid()).unwrap()
                };

                let mut column_writer = row_group_writer.next_column().unwrap().unwrap();

                column_writer
                    .typed::<Int64Type>()
                    .write_batch(&[value], None, None)
                    .unwrap();

                column_writer.close().unwrap();
            }
            NUMERICOID => {
                unimplemented!("numeric type is not supported yet");
            }
            DATEOID => {
                let value = unsafe {
                    Date::from_polymorphic_datum(elem.datum(), false, elem.oid()).unwrap()
                };

                // PG epoch is (2000-01-01). Convert it to Unix epoch (1970-01-01). +10957 days
                let value: Date = unsafe {
                    direct_function_call(
                        pg_sys::date_pli,
                        &[value.into_datum(), 10957.into_datum()],
                    )
                    .unwrap()
                };

                let date_as_bytes: Vec<u8> = unsafe {
                    direct_function_call(pg_sys::date_send, &[value.into_datum()]).unwrap()
                };
                let date_as_int = i32::from_be_bytes(date_as_bytes[0..4].try_into().unwrap());

                let mut column_writer = row_group_writer.next_column().unwrap().unwrap();

                column_writer
                    .typed::<Int32Type>()
                    .write_batch(&[date_as_int], None, None)
                    .unwrap();

                column_writer.close().unwrap();
            }
            TIMESTAMPOID => {
                let value =
                    unsafe { Timestamp::from_polymorphic_datum(elem.datum(), false, elem.oid()) }
                        .unwrap();

                // PG epoch is (2000-01-01). Convert it to Unix epoch (1970-01-01). +10957 days
                let adjustment_interval = Interval::from_days(10957);
                let value: Timestamp = unsafe {
                    direct_function_call(
                        pg_sys::timestamp_pl_interval,
                        &[value.into_datum(), adjustment_interval.into_datum()],
                    )
                    .unwrap()
                };

                let timestamp_as_bytes: Vec<u8> = unsafe {
                    direct_function_call(pg_sys::time_send, &[value.into_datum()]).unwrap()
                };
                let timestamp_val =
                    i64::from_be_bytes(timestamp_as_bytes[0..8].try_into().unwrap());

                let mut column_writer = row_group_writer.next_column().unwrap().unwrap();

                column_writer
                    .typed::<Int64Type>()
                    .write_batch(&[timestamp_val], None, None)
                    .unwrap();

                column_writer.close().unwrap();
            }
            TIMESTAMPTZOID => {
                let value = unsafe {
                    TimestampWithTimeZone::from_polymorphic_datum(elem.datum(), false, elem.oid())
                }
                .unwrap();

                // PG epoch is (2000-01-01). Convert it to Unix epoch (1970-01-01). +10957 days
                let adjustment_interval = Interval::from_days(10957);
                let value: TimestampWithTimeZone = unsafe {
                    direct_function_call(
                        pg_sys::timestamptz_pl_interval,
                        &[value.into_datum(), adjustment_interval.into_datum()],
                    )
                    .unwrap()
                };

                let timestamp_as_bytes: Vec<u8> = unsafe {
                    direct_function_call(pg_sys::timestamptz_send, &[value.into_datum()]).unwrap()
                };
                let timestamp_val =
                    i64::from_be_bytes(timestamp_as_bytes[0..8].try_into().unwrap());

                let mut column_writer = row_group_writer.next_column().unwrap().unwrap();

                column_writer
                    .typed::<Int64Type>()
                    .write_batch(&[timestamp_val], None, None)
                    .unwrap();

                column_writer.close().unwrap();
            }
            TIMEOID => {
                let value =
                    unsafe { Time::from_polymorphic_datum(elem.datum(), false, elem.oid()) }
                        .unwrap();

                let time_as_bytes: Vec<u8> = unsafe {
                    direct_function_call(pg_sys::time_send, &[value.into_datum()]).unwrap()
                };
                let time_val = i64::from_be_bytes(time_as_bytes[0..8].try_into().unwrap());

                let mut column_writer = row_group_writer.next_column().unwrap().unwrap();

                column_writer
                    .typed::<Int64Type>()
                    .write_batch(&[time_val], None, None)
                    .unwrap();

                column_writer.close().unwrap();
            }
            TIMETZOID => {
                let value = unsafe {
                    TimeWithTimeZone::from_polymorphic_datum(elem.datum(), false, elem.oid())
                }
                .unwrap();

                // extract timezone as seconds
                let timezone_as_secs: AnyNumeric = unsafe {
                    direct_function_call(
                        pg_sys::extract_timetz,
                        &["timezone".into_datum(), value.into_datum()],
                    )
                }
                .unwrap();

                // adjust timezone
                let timezone_as_secs: f64 = timezone_as_secs.try_into().unwrap();
                let timezone_val = Interval::from_seconds(timezone_as_secs);
                let value: TimeWithTimeZone = unsafe {
                    direct_function_call(
                        pg_sys::timetz_pl_interval,
                        &[value.into_datum(), timezone_val.into_datum()],
                    )
                    .unwrap()
                };

                let time_as_bytes: Vec<u8> = unsafe {
                    direct_function_call(pg_sys::timetz_send, &[value.into_datum()]).unwrap()
                };
                let time_val = i64::from_be_bytes(time_as_bytes[0..8].try_into().unwrap());

                let mut column_writer = row_group_writer.next_column().unwrap().unwrap();

                column_writer
                    .typed::<Int64Type>()
                    .write_batch(&[time_val], None, None)
                    .unwrap();

                column_writer.close().unwrap();
            }
            INTERVALOID => {
                let value =
                    unsafe { Interval::from_polymorphic_datum(elem.datum(), false, elem.oid()) }
                        .unwrap();

                let interval_as_bytes: Vec<u8> = unsafe {
                    direct_function_call(pg_sys::interval_send, &[value.into_datum()]).unwrap()
                };

                // first 8 bytes: time in microsec
                // next 4 bytes: day
                // next 4 bytes: month
                let time_in_microsec_val =
                    i64::from_be_bytes(interval_as_bytes[0..8].try_into().unwrap());
                let day_val = i32::from_be_bytes(interval_as_bytes[8..12].try_into().unwrap());
                let month_val = i32::from_be_bytes(interval_as_bytes[12..16].try_into().unwrap());

                // Postgres interval has microsecond resolution, parquet only milliseconds
                // plus postgres doesn't overflow the seconds into the day field
                let ms_per_day = 1000 * 60 * 60 * 24;
                let millis_total = time_in_microsec_val / 1000;
                let days = millis_total / ms_per_day;
                let millis = millis_total % ms_per_day;
                let mut b = vec![0u8; 12];
                b[0..4].copy_from_slice(&i32::to_le_bytes(month_val));
                b[4..8].copy_from_slice(&i32::to_le_bytes(day_val + days as i32));
                b[8..12].copy_from_slice(&i32::to_le_bytes(millis as i32));
                let interval_as_bytes = FixedLenByteArray::from(b);

                let mut column_writer = row_group_writer.next_column().unwrap().unwrap();

                column_writer
                    .typed::<FixedLenByteArrayType>()
                    .write_batch(&[interval_as_bytes], None, None)
                    .unwrap();

                column_writer.close().unwrap();
            }
            CHAROID => {
                let value =
                    unsafe { i8::from_polymorphic_datum(elem.datum(), false, elem.oid()).unwrap() };
                let value: ByteArray = vec![value as u8].into();

                let mut column_writer = row_group_writer.next_column().unwrap().unwrap();

                column_writer
                    .typed::<ByteArrayType>()
                    .write_batch(&[value], None, None)
                    .unwrap();

                column_writer.close().unwrap();
            }
            TEXTOID | VARCHAROID | BPCHAROID => {
                let value = unsafe {
                    String::from_polymorphic_datum(elem.datum(), false, elem.oid()).unwrap()
                };
                let value: ByteArray = value.as_bytes().into();

                let mut column_writer = row_group_writer.next_column().unwrap().unwrap();

                column_writer
                    .typed::<ByteArrayType>()
                    .write_batch(&[value], None, None)
                    .unwrap();

                column_writer.close().unwrap();
            }
            BOOLOID => {
                let value = unsafe {
                    bool::from_polymorphic_datum(elem.datum(), false, elem.oid()).unwrap()
                };

                let mut column_writer = row_group_writer.next_column().unwrap().unwrap();

                column_writer
                    .typed::<Int32Type>()
                    .write_batch(&[value as i32], None, None)
                    .unwrap();

                column_writer.close().unwrap();
            }
            UUIDOID => {
                let value = unsafe {
                    Uuid::from_polymorphic_datum(elem.datum(), false, elem.oid()).unwrap()
                };
                let uuid_as_bytes: Vec<u8> = unsafe {
                    direct_function_call(pg_sys::uuid_send, &[value.into_datum()]).unwrap()
                };
                let value: FixedLenByteArray = uuid_as_bytes.into();

                let mut column_writer = row_group_writer.next_column().unwrap().unwrap();

                column_writer
                    .typed::<FixedLenByteArrayType>()
                    .write_batch(&[value], None, None)
                    .unwrap();

                column_writer.close().unwrap();
            }
            JSONOID => {
                let value = unsafe {
                    Json::from_polymorphic_datum(elem.datum(), false, elem.oid()).unwrap()
                };
                let json_as_bytes: Vec<u8> = unsafe {
                    direct_function_call(pg_sys::json_send, &[value.into_datum()]).unwrap()
                };
                let value: ByteArray = json_as_bytes.into();

                let mut column_writer = row_group_writer.next_column().unwrap().unwrap();

                column_writer
                    .typed::<ByteArrayType>()
                    .write_batch(&[value], None, None)
                    .unwrap();

                column_writer.close().unwrap();
            }
            _ => {
                panic!("unsupported primitive type {}", elem.oid());
            }
        };
    }

    #[pg_extern]
    fn serialize(elem: pgrx::AnyElement, file_path: &str) {
        let is_composite_type = unsafe { pg_sys::type_is_rowtype(elem.oid()) };
        if !is_composite_type {
            // PgHeapTuple is not supported yet as udf argument
            panic!("composite type is expected, got {}", elem.oid());
        }

        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(file_path)
            .unwrap();

        let attribute_tupledesc = unsafe { pg_sys::lookup_rowtype_tupdesc(elem.oid(), 0) };
        let attribute_tupledesc = unsafe { PgTupleDesc::from_pg(attribute_tupledesc) };

        let record = unsafe {
            PgHeapTuple::from_polymorphic_datum(elem.datum(), false, elem.oid()).unwrap()
        };

        let schema = parse_record_schema(attribute_tupledesc, "root");
        let mut writer = SerializedFileWriter::new(file, schema, Default::default()).unwrap();
        let mut row_group_writer = writer.next_row_group().unwrap();

        serialize_record(record, &mut row_group_writer);

        row_group_writer.close().unwrap();
        writer.close().unwrap();
    }

    #[pg_extern]
    fn schema(elem: pgrx::AnyElement) -> String {
        let is_composite_type = unsafe { pg_sys::type_is_rowtype(elem.oid()) };
        if !is_composite_type {
            // PgHeapTuple is not supported yet as udf argument
            panic!("composite type is expected, got {}", elem.oid());
        }

        let attribute_tupledesc = unsafe { pg_sys::lookup_rowtype_tupdesc(elem.oid(), 0) };
        let attribute_tupledesc = unsafe { PgTupleDesc::from_pg(attribute_tupledesc) };
        let schema = parse_record_schema(attribute_tupledesc, "root");

        let mut buf = Vec::new();
        printer::print_schema(&mut buf, &schema);
        String::from_utf8(buf).unwrap()
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
