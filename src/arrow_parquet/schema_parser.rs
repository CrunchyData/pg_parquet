use std::{collections::HashMap, ops::Deref, sync::Arc};

use arrow::datatypes::{Field, Fields, Schema};
use arrow_cast::can_cast_types;
use arrow_schema::{DataType, FieldRef};
use parquet::arrow::{arrow_to_parquet_schema, PARQUET_FIELD_ID_META_KEY};
use pg_sys::{
    can_coerce_type,
    CoercionContext::{self, COERCION_EXPLICIT, COERCION_IMPLICIT},
    FormData_pg_attribute, InvalidOid, Oid, BOOLOID, BYTEAOID, CHAROID, DATEOID, FLOAT4OID,
    FLOAT8OID, INT2OID, INT4OID, INT8OID, NUMERICOID, OIDOID, TEXTOID, TIMEOID, TIMESTAMPOID,
    TIMESTAMPTZOID, TIMETZOID,
};
use pgrx::{check_for_interrupts, prelude::*, PgTupleDesc};

use crate::{
    pgrx_utils::{
        array_element_typoid, collect_attributes_for, domain_array_base_elem_typoid, is_array_type,
        is_composite_type, tuple_desc, CollectAttributesFor,
    },
    type_compat::{
        geometry::is_postgis_geometry_type,
        map::is_map_type,
        pg_arrow_type_conversions::{
            extract_precision_and_scale_from_numeric_typmod, should_write_numeric_as_text,
        },
    },
};

use super::cast_mode::CastMode;

pub(crate) fn parquet_schema_string_from_attributes(
    attributes: &[FormData_pg_attribute],
) -> String {
    let arrow_schema = parse_arrow_schema_from_attributes(attributes);
    let parquet_schema = arrow_to_parquet_schema(&arrow_schema)
        .unwrap_or_else(|e| panic!("failed to convert arrow schema to parquet schema: {}", e));

    let mut buf = Vec::new();
    parquet::schema::printer::print_schema(&mut buf, &parquet_schema.root_schema_ptr());
    String::from_utf8(buf).unwrap_or_else(|e| panic!("failed to convert schema to string: {}", e))
}

pub(crate) fn parse_arrow_schema_from_attributes(attributes: &[FormData_pg_attribute]) -> Schema {
    let mut field_id = 0;

    let mut struct_attribute_fields = vec![];

    for attribute in attributes {
        let attribute_name = attribute.name();
        let attribute_typoid = attribute.type_oid().value();
        let attribute_typmod = attribute.type_mod();

        let field = if is_composite_type(attribute_typoid) {
            let attribute_tupledesc = tuple_desc(attribute_typoid, attribute_typmod);
            parse_struct_schema(attribute_tupledesc, attribute_name, &mut field_id)
        } else if is_map_type(attribute_typoid) {
            let attribute_base_elem_typoid = domain_array_base_elem_typoid(attribute_typoid);
            parse_map_schema(
                attribute_base_elem_typoid,
                attribute_typmod,
                attribute_name,
                &mut field_id,
            )
        } else if is_array_type(attribute_typoid) {
            let attribute_element_typoid = array_element_typoid(attribute_typoid);
            parse_list_schema(
                attribute_element_typoid,
                attribute_typmod,
                attribute_name,
                &mut field_id,
            )
        } else {
            parse_primitive_schema(
                attribute_typoid,
                attribute_typmod,
                attribute_name,
                &mut field_id,
            )
        };

        struct_attribute_fields.push(field);
    }

    Schema::new(Fields::from(struct_attribute_fields))
}

fn parse_struct_schema(tupledesc: PgTupleDesc, elem_name: &str, field_id: &mut i32) -> Arc<Field> {
    check_for_interrupts!();

    let metadata = HashMap::from_iter(vec![(
        PARQUET_FIELD_ID_META_KEY.into(),
        field_id.to_string(),
    )]);

    *field_id += 1;

    let mut child_fields: Vec<Arc<Field>> = vec![];

    let attributes = collect_attributes_for(CollectAttributesFor::Struct, &tupledesc);

    for attribute in attributes {
        if attribute.is_dropped() {
            continue;
        }

        let attribute_name = attribute.name();
        let attribute_oid = attribute.type_oid().value();
        let attribute_typmod = attribute.type_mod();

        let child_field = if is_composite_type(attribute_oid) {
            let attribute_tupledesc = tuple_desc(attribute_oid, attribute_typmod);
            parse_struct_schema(attribute_tupledesc, attribute_name, field_id)
        } else if is_map_type(attribute_oid) {
            let attribute_base_elem_typoid = domain_array_base_elem_typoid(attribute_oid);
            parse_map_schema(
                attribute_base_elem_typoid,
                attribute_typmod,
                attribute_name,
                field_id,
            )
        } else if is_array_type(attribute_oid) {
            let attribute_element_typoid = array_element_typoid(attribute_oid);
            parse_list_schema(
                attribute_element_typoid,
                attribute_typmod,
                attribute_name,
                field_id,
            )
        } else {
            parse_primitive_schema(attribute_oid, attribute_typmod, attribute_name, field_id)
        };

        child_fields.push(child_field);
    }

    let nullable = true;

    Field::new(
        elem_name,
        arrow::datatypes::DataType::Struct(Fields::from(child_fields)),
        nullable,
    )
    .with_metadata(metadata)
    .into()
}

fn parse_list_schema(typoid: Oid, typmod: i32, array_name: &str, field_id: &mut i32) -> Arc<Field> {
    check_for_interrupts!();

    let list_metadata = HashMap::from_iter(vec![(
        PARQUET_FIELD_ID_META_KEY.into(),
        field_id.to_string(),
    )]);

    *field_id += 1;

    let elem_field = if is_composite_type(typoid) {
        let tupledesc = tuple_desc(typoid, typmod);
        parse_struct_schema(tupledesc, array_name, field_id)
    } else if is_map_type(typoid) {
        let base_elem_typoid = domain_array_base_elem_typoid(typoid);
        parse_map_schema(base_elem_typoid, typmod, array_name, field_id)
    } else {
        parse_primitive_schema(typoid, typmod, array_name, field_id)
    };

    let nullable = true;

    Field::new(
        array_name,
        arrow::datatypes::DataType::List(elem_field),
        nullable,
    )
    .with_metadata(list_metadata)
    .into()
}

fn parse_map_schema(typoid: Oid, typmod: i32, map_name: &str, field_id: &mut i32) -> Arc<Field> {
    let map_metadata = HashMap::from_iter(vec![(
        PARQUET_FIELD_ID_META_KEY.into(),
        field_id.to_string(),
    )]);

    *field_id += 1;

    let tupledesc = tuple_desc(typoid, typmod);

    let entries_field = parse_struct_schema(tupledesc, map_name, field_id);
    let entries_field = adjust_map_entries_field(entries_field);

    let keys_sorted = false;

    let nullable = true;

    Field::new(
        map_name,
        arrow::datatypes::DataType::Map(entries_field, keys_sorted),
        nullable,
    )
    .with_metadata(map_metadata)
    .into()
}

fn parse_primitive_schema(
    typoid: Oid,
    typmod: i32,
    elem_name: &str,
    field_id: &mut i32,
) -> Arc<Field> {
    check_for_interrupts!();

    let primitive_metadata = HashMap::<String, String>::from_iter(vec![(
        PARQUET_FIELD_ID_META_KEY.into(),
        field_id.to_string(),
    )]);

    *field_id += 1;

    let nullable = true;

    let field = match typoid {
        FLOAT4OID => Field::new(elem_name, arrow::datatypes::DataType::Float32, nullable),
        FLOAT8OID => Field::new(elem_name, arrow::datatypes::DataType::Float64, nullable),
        BOOLOID => Field::new(elem_name, arrow::datatypes::DataType::Boolean, nullable),
        INT2OID => Field::new(elem_name, arrow::datatypes::DataType::Int16, nullable),
        INT4OID => Field::new(elem_name, arrow::datatypes::DataType::Int32, nullable),
        INT8OID => Field::new(elem_name, arrow::datatypes::DataType::Int64, nullable),
        NUMERICOID => {
            let (precision, scale) = extract_precision_and_scale_from_numeric_typmod(typmod);

            if should_write_numeric_as_text(precision) {
                Field::new(elem_name, arrow::datatypes::DataType::Utf8, nullable)
            } else {
                Field::new(
                    elem_name,
                    arrow::datatypes::DataType::Decimal128(precision as _, scale as _),
                    nullable,
                )
            }
        }
        DATEOID => Field::new(elem_name, arrow::datatypes::DataType::Date32, nullable),
        TIMESTAMPOID => Field::new(
            elem_name,
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            nullable,
        ),
        TIMESTAMPTZOID => Field::new(
            elem_name,
            arrow::datatypes::DataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                Some("+00:00".into()),
            ),
            nullable,
        ),
        TIMEOID => Field::new(
            elem_name,
            arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
            nullable,
        ),
        TIMETZOID => Field::new(
            elem_name,
            arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
            nullable,
        )
        .with_metadata(HashMap::from_iter(vec![(
            "adjusted_to_utc".into(),
            "true".into(),
        )])),
        CHAROID => Field::new(elem_name, arrow::datatypes::DataType::Utf8, nullable),
        TEXTOID => Field::new(elem_name, arrow::datatypes::DataType::Utf8, nullable),
        BYTEAOID => Field::new(elem_name, arrow::datatypes::DataType::Binary, nullable),
        OIDOID => Field::new(elem_name, arrow::datatypes::DataType::UInt32, nullable),
        _ => {
            if is_postgis_geometry_type(typoid) {
                Field::new(elem_name, arrow::datatypes::DataType::Binary, nullable)
            } else {
                Field::new(elem_name, arrow::datatypes::DataType::Utf8, nullable)
            }
        }
    };

    // Combine the field metadata with the field metadata from the schema visitor
    let primitive_metadata = field
        .metadata()
        .iter()
        .chain(primitive_metadata.iter())
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    field.with_metadata(primitive_metadata).into()
}

fn adjust_map_entries_field(field: FieldRef) -> FieldRef {
    let name = field.deref().name();
    let data_type = field.deref().data_type();
    let metadata = field.deref().metadata().clone();

    let not_nullable_key_field;
    let nullable_value_field;

    match data_type {
        arrow::datatypes::DataType::Struct(fields) => {
            let key_field = fields.find("key").expect("expected key field").1;
            let value_field = fields.find("val").expect("expected val field").1;

            let key_nullable = false;

            not_nullable_key_field = Field::new(
                key_field.name(),
                key_field.data_type().clone(),
                key_nullable,
            )
            .with_metadata(key_field.metadata().clone());

            let value_nullable = true;

            nullable_value_field = Field::new(
                value_field.name(),
                value_field.data_type().clone(),
                value_nullable,
            )
            .with_metadata(value_field.metadata().clone());
        }
        _ => {
            panic!("expected struct data type for map entries")
        }
    };

    let entries_nullable = false;

    let entries_field = Field::new(
        name,
        arrow::datatypes::DataType::Struct(Fields::from(vec![
            not_nullable_key_field,
            nullable_value_field,
        ])),
        entries_nullable,
    )
    .with_metadata(metadata);

    Arc::new(entries_field)
}

// ensure_arrow_schema_match_tupledesc_schema throws an error if the arrow schema does not match the table schema.
// If the arrow schema is castable to the table schema, it returns a vector of Option<DataType> to cast to
// for each field.
pub(crate) fn ensure_arrow_schema_match_tupledesc_schema(
    arrow_schema: Arc<Schema>,
    tupledesc_schema: Arc<Schema>,
    attributes: &[FormData_pg_attribute],
    cast_mode: CastMode,
) -> Vec<Option<DataType>> {
    let mut cast_to_types = Vec::new();

    for (tupledesc_field, attribute) in tupledesc_schema.fields().iter().zip(attributes.iter()) {
        let field_name = tupledesc_field.name();

        let arrow_field = arrow_schema.column_with_name(field_name);

        if arrow_field.is_none() {
            panic!("column \"{}\" is not found in parquet file", field_name);
        }

        let (_, arrow_field) = arrow_field.unwrap();
        let arrow_field = Arc::new(arrow_field.clone());

        let from_type = arrow_field.data_type();
        let to_type = tupledesc_field.data_type();

        // no cast needed
        if from_type == to_type {
            cast_to_types.push(None);
            continue;
        }

        if let Err(coercion_error) = is_coercible(
            from_type,
            to_type,
            attribute.atttypid,
            attribute.atttypmod,
            cast_mode,
        ) {
            let type_mismatch_message = format!(
                "type mismatch for column \"{}\" between table and parquet file.\n\n\
                table has \"{}\"\n\nparquet file has \"{}\"",
                field_name, to_type, from_type
            );

            match coercion_error {
                CoercionError::NoStrictCoercionPath => ereport!(
                    pgrx::PgLogLevel::ERROR,
                    PgSqlErrorCode::ERRCODE_CANNOT_COERCE,
                    type_mismatch_message,
                    "Try COPY FROM '..' WITH (cast_mode = 'relaxed') to allow lossy casts with runtime checks."
                ),
                CoercionError::NoCoercionPath => ereport!(
                    pgrx::PgLogLevel::ERROR,
                    PgSqlErrorCode::ERRCODE_CANNOT_COERCE,
                    type_mismatch_message
                ),
                CoercionError::MapEntriesNullable => ereport!(
                    pgrx::PgLogLevel::ERROR,
                    PgSqlErrorCode::ERRCODE_CANNOT_COERCE,
                    format!("entries field in map type cannot be nullable for column \"{}\"", field_name)
                ),
            }
        }

        pgrx::debug2!(
            "column \"{}\" is being cast from \"{}\" to \"{}\"",
            field_name,
            from_type,
            to_type
        );

        cast_to_types.push(Some(to_type.clone()));
    }

    cast_to_types
}

enum CoercionError {
    NoStrictCoercionPath,
    NoCoercionPath,
    MapEntriesNullable,
}

// is_coercible first checks if "from_type" can be cast to "to_type" by arrow-cast.
// Then, it checks if the cast is meaningful at Postgres by seeing if there is
// an explicit coercion from "from_typoid" to "to_typoid".
//
// Additionaly, we need to be careful about struct rules for the cast:
// Arrow supports casting struct fields by field position instead of field name,
// which is not the intended behavior for pg_parquet. Hence, we make sure the field names
// match for structs.
fn is_coercible(
    from_type: &DataType,
    to_type: &DataType,
    to_typoid: Oid,
    to_typmod: i32,
    cast_mode: CastMode,
) -> Result<(), CoercionError> {
    match (from_type, to_type) {
        (DataType::Struct(from_fields), DataType::Struct(to_fields)) => {
            if from_fields.len() != to_fields.len() {
                return Err(CoercionError::NoCoercionPath);
            }

            let tupledesc = tuple_desc(to_typoid, to_typmod);

            let attributes = collect_attributes_for(CollectAttributesFor::Struct, &tupledesc);

            for (from_field, (to_field, to_attribute)) in from_fields
                .iter()
                .zip(to_fields.iter().zip(attributes.iter()))
            {
                if from_field.name() != to_field.name() {
                    return Err(CoercionError::NoCoercionPath);
                }

                is_coercible(
                    from_field.data_type(),
                    to_field.data_type(),
                    to_attribute.type_oid().value(),
                    to_attribute.type_mod(),
                    cast_mode,
                )?;
            }

            Ok(())
        }
        (DataType::List(from_field), DataType::List(to_field)) => {
            let element_oid = array_element_typoid(to_typoid);
            let element_typmod = to_typmod;

            is_coercible(
                from_field.data_type(),
                to_field.data_type(),
                element_oid,
                element_typmod,
                cast_mode,
            )
        }
        (DataType::Map(from_entries_field, _), DataType::Map(to_entries_field, _)) => {
            // entries field cannot be null
            if from_entries_field.is_nullable() {
                return Err(CoercionError::MapEntriesNullable);
            }

            let entries_typoid = domain_array_base_elem_typoid(to_typoid);

            is_coercible(
                from_entries_field.data_type(),
                to_entries_field.data_type(),
                entries_typoid,
                to_typmod,
                cast_mode,
            )
        }
        _ => {
            // check if arrow-cast can cast the types
            if !can_cast_types(from_type, to_type) {
                return Err(CoercionError::NoCoercionPath);
            }

            let from_typoid = pg_type_for_arrow_primitive_type(from_type);

            // pg_parquet could not recognize that arrow type
            if from_typoid == InvalidOid {
                return Err(CoercionError::NoCoercionPath);
            }

            let can_coerce_via_relaxed_mode =
                can_pg_coerce_types(from_typoid, to_typoid, COERCION_EXPLICIT);

            // check if coercion is meaningful at Postgres (it has a coercion path)
            match cast_mode {
                CastMode::Strict => {
                    let can_coerce_via_strict_mode =
                        can_pg_coerce_types(from_typoid, to_typoid, COERCION_IMPLICIT);

                    if !can_coerce_via_strict_mode && can_coerce_via_relaxed_mode {
                        Err(CoercionError::NoStrictCoercionPath)
                    } else if !can_coerce_via_strict_mode {
                        Err(CoercionError::NoCoercionPath)
                    } else {
                        Ok(())
                    }
                }
                CastMode::Relaxed => {
                    if !can_coerce_via_relaxed_mode {
                        Err(CoercionError::NoCoercionPath)
                    } else {
                        Ok(())
                    }
                }
            }
        }
    }
}

fn can_pg_coerce_types(from_typoid: Oid, to_typoid: Oid, ccontext: CoercionContext::Type) -> bool {
    let n_args = 1;
    let input_typeids = [from_typoid];
    let target_typeids = [to_typoid];

    unsafe {
        can_coerce_type(
            n_args,
            input_typeids.as_ptr(),
            target_typeids.as_ptr(),
            ccontext,
        )
    }
}

// pg_type_for_arrow_primitive_type returns Postgres type for given
// primitive arrow type. It returns InvalidOid if the arrow type is not recognized.
fn pg_type_for_arrow_primitive_type(data_type: &DataType) -> Oid {
    match data_type {
        DataType::Float32 | DataType::Float16 => FLOAT4OID,
        DataType::Float64 => FLOAT8OID,
        DataType::Int16 | DataType::UInt16 | DataType::Int8 | DataType::UInt8 => INT2OID,
        DataType::Int32 | DataType::UInt32 => INT4OID,
        DataType::Int64 | DataType::UInt64 => INT8OID,
        DataType::Decimal128(_, _) => NUMERICOID,
        DataType::Boolean => BOOLOID,
        DataType::Date32 => DATEOID,
        DataType::Time64(_) => TIMEOID,
        DataType::Timestamp(_, None) => TIMESTAMPOID,
        DataType::Timestamp(_, Some(_)) => TIMESTAMPTZOID,
        DataType::Utf8 | DataType::LargeUtf8 => TEXTOID,
        DataType::Binary | DataType::LargeBinary => BYTEAOID,
        _ => InvalidOid,
    }
}
