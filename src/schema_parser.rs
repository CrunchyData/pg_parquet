use parquet::schema::types::TypePtr;
use pg_sys::{
    Oid, BOOLARRAYOID, BOOLOID, BPCHARARRAYOID, BPCHAROID, CHARARRAYOID, CHAROID, DATEARRAYOID,
    DATEOID, FLOAT4ARRAYOID, FLOAT4OID, FLOAT8ARRAYOID, FLOAT8OID, INT2ARRAYOID, INT2OID,
    INT4ARRAYOID, INT4OID, INT8ARRAYOID, INT8OID, INTERVALARRAYOID, INTERVALOID, JSONARRAYOID,
    JSONOID, NUMERICARRAYOID, NUMERICOID, TEXTARRAYOID, TEXTOID, TIMEARRAYOID, TIMEOID,
    TIMESTAMPARRAYOID, TIMESTAMPOID, TIMESTAMPTZARRAYOID, TIMESTAMPTZOID, TIMETZARRAYOID,
    TIMETZOID, UUIDARRAYOID, UUIDOID, VARCHARARRAYOID, VARCHAROID,
};
use pgrx::{prelude::*, PgTupleDesc};

pub(crate) fn parse_record_schema(tupledesc: PgTupleDesc, elem_name: &'static str) -> TypePtr {
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
            let attribute_tupledesc = unsafe { pg_sys::lookup_rowtype_tupdesc(attribute_oid, 0) };
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

fn parse_array_schema_internal(
    array_name: &'static str,
    parquet_type: parquet::basic::Type,
    logical_type: Option<parquet::basic::LogicalType>,
) -> TypePtr {
    let type_builder =
        parquet::schema::types::Type::primitive_type_builder(array_name, parquet_type)
            .with_repetition(parquet::basic::Repetition::REQUIRED)
            .with_logical_type(logical_type)
            .build()
            .unwrap()
            .into();

    let list_group_builder = parquet::schema::types::Type::group_type_builder(array_name)
        .with_fields(vec![type_builder])
        .with_repetition(parquet::basic::Repetition::REPEATED)
        .with_logical_type(Some(parquet::basic::LogicalType::List))
        .build()
        .unwrap();

    list_group_builder.into()
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
            parse_array_schema_internal(array_name, parquet::basic::Type::FLOAT, None)
        }
        FLOAT8ARRAYOID => {
            parse_array_schema_internal(array_name, parquet::basic::Type::DOUBLE, None)
        }
        BOOLARRAYOID => parse_array_schema_internal(
            array_name,
            parquet::basic::Type::INT32,
            Some(parquet::basic::LogicalType::Integer {
                bit_width: 8,
                is_signed: true,
            }),
        ),
        INT2ARRAYOID => parse_array_schema_internal(
            array_name,
            parquet::basic::Type::INT32,
            Some(parquet::basic::LogicalType::Integer {
                bit_width: 16,
                is_signed: true,
            }),
        ),
        INT4ARRAYOID => parse_array_schema_internal(array_name, parquet::basic::Type::INT32, None),
        INT8ARRAYOID => parse_array_schema_internal(array_name, parquet::basic::Type::INT64, None),
        NUMERICARRAYOID => {
            unimplemented!("numeric type is not supported yet");
        }
        DATEARRAYOID => parse_array_schema_internal(
            array_name,
            parquet::basic::Type::INT32,
            Some(parquet::basic::LogicalType::Date),
        ),
        TIMESTAMPARRAYOID => parse_array_schema_internal(
            array_name,
            parquet::basic::Type::INT64,
            Some(parquet::basic::LogicalType::Timestamp {
                is_adjusted_to_u_t_c: false,
                unit: parquet::basic::TimeUnit::MICROS(parquet::format::MicroSeconds {}),
            }),
        ),
        TIMESTAMPTZARRAYOID => parse_array_schema_internal(
            array_name,
            parquet::basic::Type::INT64,
            Some(parquet::basic::LogicalType::Timestamp {
                is_adjusted_to_u_t_c: true,
                unit: parquet::basic::TimeUnit::MICROS(parquet::format::MicroSeconds {}),
            }),
        ),
        TIMEARRAYOID => parse_array_schema_internal(
            array_name,
            parquet::basic::Type::INT64,
            Some(parquet::basic::LogicalType::Time {
                is_adjusted_to_u_t_c: false,
                unit: parquet::basic::TimeUnit::MICROS(parquet::format::MicroSeconds {}),
            }),
        ),
        TIMETZARRAYOID => parse_array_schema_internal(
            array_name,
            parquet::basic::Type::INT64,
            Some(parquet::basic::LogicalType::Time {
                is_adjusted_to_u_t_c: true,
                unit: parquet::basic::TimeUnit::MICROS(parquet::format::MicroSeconds {}),
            }),
        ),
        INTERVALARRAYOID => {
            let type_builder = parquet::schema::types::Type::primitive_type_builder(
                array_name,
                parquet::basic::Type::FIXED_LEN_BYTE_ARRAY,
            )
            .with_length(12)
            .with_converted_type(parquet::basic::ConvertedType::INTERVAL)
            .with_repetition(parquet::basic::Repetition::REQUIRED)
            .build()
            .unwrap()
            .into();

            let list_group_builder = parquet::schema::types::Type::group_type_builder(array_name)
                .with_fields(vec![type_builder])
                .with_repetition(parquet::basic::Repetition::REPEATED)
                .with_logical_type(Some(parquet::basic::LogicalType::List))
                .build()
                .unwrap();

            list_group_builder.into()
        }
        CHARARRAYOID => {
            parse_array_schema_internal(array_name, parquet::basic::Type::BYTE_ARRAY, None)
        }
        TEXTARRAYOID | VARCHARARRAYOID | BPCHARARRAYOID => parse_array_schema_internal(
            array_name,
            parquet::basic::Type::BYTE_ARRAY,
            Some(parquet::basic::LogicalType::String),
        ),
        UUIDARRAYOID => {
            let type_builder = parquet::schema::types::Type::primitive_type_builder(
                array_name,
                parquet::basic::Type::FIXED_LEN_BYTE_ARRAY,
            )
            .with_length(16)
            .with_logical_type(Some(parquet::basic::LogicalType::Uuid))
            .with_repetition(parquet::basic::Repetition::REQUIRED)
            .build()
            .unwrap()
            .into();

            let list_group_builder = parquet::schema::types::Type::group_type_builder(array_name)
                .with_fields(vec![type_builder])
                .with_repetition(parquet::basic::Repetition::REPEATED)
                .with_logical_type(Some(parquet::basic::LogicalType::List))
                .build()
                .unwrap();

            list_group_builder.into()
        }
        JSONARRAYOID => parse_array_schema_internal(
            array_name,
            parquet::basic::Type::BYTE_ARRAY,
            Some(parquet::basic::LogicalType::Json),
        ),
        _ => {
            panic!("unsupported array type {}", arraytypoid);
        }
    }
}

fn parse_primitive_schema_internal(
    elem_name: &'static str,
    parquet_type: parquet::basic::Type,
    logical_type: Option<parquet::basic::LogicalType>,
) -> TypePtr {
    parquet::schema::types::Type::primitive_type_builder(elem_name, parquet_type)
        .with_repetition(parquet::basic::Repetition::REQUIRED)
        .with_logical_type(logical_type)
        .build()
        .unwrap()
        .into()
}

fn parse_primitive_schema(typoid: Oid, elem_name: &'static str) -> TypePtr {
    match typoid {
        FLOAT4OID => parse_primitive_schema_internal(elem_name, parquet::basic::Type::FLOAT, None),
        FLOAT8OID => parse_primitive_schema_internal(elem_name, parquet::basic::Type::DOUBLE, None),
        BOOLOID => parse_primitive_schema_internal(
            elem_name,
            parquet::basic::Type::INT32,
            Some(parquet::basic::LogicalType::Integer {
                bit_width: 8,
                is_signed: true,
            }),
        ),
        INT2OID => parse_primitive_schema_internal(
            elem_name,
            parquet::basic::Type::INT32,
            Some(parquet::basic::LogicalType::Integer {
                bit_width: 16,
                is_signed: true,
            }),
        ),
        INT4OID => parse_primitive_schema_internal(elem_name, parquet::basic::Type::INT32, None),
        INT8OID => parse_primitive_schema_internal(elem_name, parquet::basic::Type::INT64, None),
        NUMERICOID => {
            unimplemented!("numeric type is not supported yet");
        }
        DATEOID => parse_primitive_schema_internal(
            elem_name,
            parquet::basic::Type::INT32,
            Some(parquet::basic::LogicalType::Date),
        ),
        TIMESTAMPOID => parse_primitive_schema_internal(
            elem_name,
            parquet::basic::Type::INT64,
            Some(parquet::basic::LogicalType::Timestamp {
                is_adjusted_to_u_t_c: false,
                unit: parquet::basic::TimeUnit::MICROS(parquet::format::MicroSeconds {}),
            }),
        ),
        TIMESTAMPTZOID => parse_primitive_schema_internal(
            elem_name,
            parquet::basic::Type::INT64,
            Some(parquet::basic::LogicalType::Timestamp {
                is_adjusted_to_u_t_c: true,
                unit: parquet::basic::TimeUnit::MICROS(parquet::format::MicroSeconds {}),
            }),
        ),
        TIMEOID => parse_primitive_schema_internal(
            elem_name,
            parquet::basic::Type::INT64,
            Some(parquet::basic::LogicalType::Time {
                is_adjusted_to_u_t_c: false,
                unit: parquet::basic::TimeUnit::MICROS(parquet::format::MicroSeconds {}),
            }),
        ),
        TIMETZOID => parse_primitive_schema_internal(
            elem_name,
            parquet::basic::Type::INT64,
            Some(parquet::basic::LogicalType::Time {
                is_adjusted_to_u_t_c: true,
                unit: parquet::basic::TimeUnit::MICROS(parquet::format::MicroSeconds {}),
            }),
        ),
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
        CHAROID => {
            parse_primitive_schema_internal(elem_name, parquet::basic::Type::BYTE_ARRAY, None)
        }
        TEXTOID | VARCHAROID | BPCHAROID => parse_primitive_schema_internal(
            elem_name,
            parquet::basic::Type::BYTE_ARRAY,
            Some(parquet::basic::LogicalType::String),
        ),
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
        JSONOID => parse_primitive_schema_internal(
            elem_name,
            parquet::basic::Type::BYTE_ARRAY,
            Some(parquet::basic::LogicalType::Json),
        ),
        _ => {
            panic!("unsupported primitive type {}", typoid)
        }
    }
}
