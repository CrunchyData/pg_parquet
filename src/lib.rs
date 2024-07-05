use std::fs::File;

use parquet::{
    data_type::{
        ByteArray, ByteArrayType, DoubleType, FixedLenByteArray, FixedLenByteArrayType, FloatType,
        Int32Type, Int64Type,
    },
    file::writer::{SerializedFileWriter, SerializedRowGroupWriter},
    schema::printer,
    schema::types::TypePtr,
};

use pg_sys::{
    Oid, BOOLOID, BPCHAROID, CHAROID, DATEOID, FLOAT4ARRAYOID, FLOAT4OID, FLOAT8OID, INT2OID,
    INT4OID, INT8OID, INTERVALOID, JSONOID, NUMERICOID, TEXTOID, TIMEOID, TIMESTAMPOID,
    TIMESTAMPTZOID, TIMETZOID, UUIDOID, VARCHAROID,
};
use pgrx::prelude::*;
use pgrx::{direct_function_call, Json, PgTupleDesc, Uuid};

pgrx::pg_module_magic!();

#[pg_schema]
mod pgparquet {

    use super::*;

    fn parse_record_schema(tupledesc: PgTupleDesc, elem_name: &'static str) -> TypePtr {
        let mut child_fields: Vec<TypePtr> = vec![];

        for attribute_idx in 0..tupledesc.len() {
            let attribute = tupledesc.get(attribute_idx).unwrap();
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
                serialize_array(attribute_val, row_group_writer);
            } else {
                let attribute_val = record
                    .get_by_name::<pgrx::AnyElement>(attribute_name)
                    .unwrap()
                    .unwrap();
                serialize_primitive(attribute_val, row_group_writer);
            }
        }
    }

    fn serialize_array(
        array: pgrx::AnyArray,
        row_group_writer: &mut SerializedRowGroupWriter<File>,
    ) {
        let array_element_typoid = unsafe { pg_sys::get_element_type(array.oid()) };
        let is_array_of_composite = unsafe { pg_sys::type_is_rowtype(array_element_typoid) };
        if is_array_of_composite {
            unimplemented!("array of composite type is not supported yet");
            // let records = unsafe {
            //     Vec::<PgHeapTuple<'_, AllocatedByRust>>::from_polymorphic_datum(
            //         array.datum(),
            //         false,
            //         array.oid(),
            //     )
            //     .unwrap()
            // };

            // for record in records {
            //     serialize_record(record, row_group_writer);
            // }

            // return;
        }

        match array.oid() {
            FLOAT4ARRAYOID => {
                let value = unsafe {
                    Vec::<f32>::from_polymorphic_datum(array.datum(), false, array.oid()).unwrap()
                };

                let mut column_writer = row_group_writer.next_column().unwrap().unwrap();

                let def_levels = vec![1; value.len()];
                let mut rep_levels = vec![1; value.len()];
                rep_levels[0] = 0;
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
