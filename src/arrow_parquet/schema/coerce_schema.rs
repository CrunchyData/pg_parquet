use std::sync::Arc;

use arrow_cast::can_cast_types;
use arrow_schema::{DataType, FieldRef, Schema};
use pgrx::{
    ereport,
    pg_sys::{
        can_coerce_type,
        CoercionContext::{self, COERCION_EXPLICIT},
        FormData_pg_attribute, InvalidOid, Oid, BOOLOID, BYTEAOID, DATEOID, FLOAT4OID, FLOAT8OID,
        INT2OID, INT4OID, INT8OID, JSONOID, NUMERICOID, OIDOID, TEXTOID, TIMEOID, TIMESTAMPOID,
        TIMESTAMPTZOID, UUIDOID,
    },
    PgLogLevel, PgSqlErrorCode, PgTupleDesc,
};

use crate::{
    arrow_parquet::match_by::MatchBy,
    pgrx_utils::{
        array_element_typoid, collect_attributes_for, domain_array_base_elem_type,
        is_generated_attribute, tuple_desc, CollectAttributesFor,
    },
    type_compat::pg_arrow_type_conversions::make_numeric_typmod,
};

// ensure_file_schema_match_tupledesc_schema throws an error if the file's schema does not match the table schema.
// If the file's arrow schema is castable to the table's arrow schema, it returns a vector of Option<DataType>
// to cast to for each field.
pub(crate) fn ensure_file_schema_match_tupledesc_schema(
    file_schema: Arc<Schema>,
    tupledesc_schema: Arc<Schema>,
    attributes: &[FormData_pg_attribute],
    match_by: MatchBy,
) -> Vec<Option<DataType>> {
    let mut cast_to_types = Vec::new();

    if match_by == MatchBy::Position
        && tupledesc_schema.fields().len() != file_schema.fields().len()
    {
        panic!(
            "column count mismatch between table and parquet file. \
             parquet file has {} columns, but table has {} columns",
            file_schema.fields().len(),
            tupledesc_schema.fields().len()
        );
    }

    for (tupledesc_schema_field, attribute) in
        tupledesc_schema.fields().iter().zip(attributes.iter())
    {
        let field_name = tupledesc_schema_field.name();

        let file_schema_field = match match_by {
            MatchBy::Position => file_schema.field(attribute.attnum as usize - 1),

            MatchBy::Name => {
                let file_schema_field = file_schema.column_with_name(field_name);

                if file_schema_field.is_none() {
                    panic!("column \"{}\" is not found in parquet file", field_name);
                }

                let (_, file_schema_field) = file_schema_field.unwrap();

                file_schema_field
            }
        };

        let file_schema_field = Arc::new(file_schema_field.clone());

        let from_type = file_schema_field.data_type();
        let to_type = tupledesc_schema_field.data_type();

        // no cast needed
        if from_type == to_type {
            cast_to_types.push(None);
            continue;
        }

        if !is_coercible(
            &file_schema_field,
            tupledesc_schema_field,
            attribute.atttypid,
            attribute.atttypmod,
        ) {
            panic!(
                "type mismatch for column \"{}\" between table and parquet file.\n\n\
                 table has \"{}\"\n\nparquet file has \"{}\"",
                field_name, to_type, from_type
            );
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

// is_coercible first checks if "from_type" can be cast to "to_type" by arrow-cast.
// Then, it checks if the cast is meaningful at Postgres by seeing if there is
// an explicit coercion from "from_typoid" to "to_typoid".
//
// Additionaly, we need to be careful about struct rules for the cast:
// Arrow supports casting struct fields by field position instead of field name,
// which is not the intended behavior for pg_parquet. Hence, we make sure the field names
// match for structs.
fn is_coercible(
    from_field: &FieldRef,
    to_field: &FieldRef,
    to_typoid: Oid,
    to_typmod: i32,
) -> bool {
    match (from_field.data_type(), to_field.data_type()) {
        (DataType::Struct(from_fields), DataType::Struct(to_fields)) => {
            if from_fields.len() != to_fields.len() {
                return false;
            }

            let tupledesc = tuple_desc(to_typoid, to_typmod);

            let attributes = collect_attributes_for(CollectAttributesFor::Other, &tupledesc);

            for (from_field, (to_field, to_attribute)) in from_fields
                .iter()
                .zip(to_fields.iter().zip(attributes.iter()))
            {
                if from_field.name() != to_field.name() {
                    return false;
                }

                if !is_coercible(
                    from_field,
                    to_field,
                    to_attribute.type_oid().value(),
                    to_attribute.type_mod(),
                ) {
                    return false;
                }
            }

            true
        }
        (DataType::List(from_field), DataType::List(to_field))
        | (DataType::FixedSizeList(from_field, _), DataType::List(to_field))
        | (DataType::LargeList(from_field), DataType::List(to_field)) => {
            let element_oid = array_element_typoid(to_typoid);
            let element_typmod = to_typmod;

            is_coercible(from_field, to_field, element_oid, element_typmod)
        }
        (DataType::Map(from_entries_field, _), DataType::Map(to_entries_field, _)) => {
            // entries field cannot be null
            if from_entries_field.is_nullable() {
                return false;
            }

            let (entries_typoid, entries_typmod) = domain_array_base_elem_type(to_typoid);

            is_coercible(
                from_entries_field,
                to_entries_field,
                entries_typoid,
                entries_typmod,
            )
        }
        _ => {
            // check if arrow-cast can cast the types
            if !can_cast_types(from_field.data_type(), to_field.data_type()) {
                return false;
            }

            let (from_typoid, _) = pg_type_for_arrow_primitive_field(from_field);

            // pg_parquet could not recognize that arrow type
            if from_typoid == InvalidOid {
                return false;
            }

            // check if coercion is meaningful at Postgres (it has a coercion path)
            can_pg_coerce_types(from_typoid, to_typoid, COERCION_EXPLICIT)
        }
    }
}

// pg_type_for_arrow_primitive_field returns Postgres type for given
// primitive arrow field. It returns InvalidOid if the arrow field's type is not recognized.
pub(crate) fn pg_type_for_arrow_primitive_field(field: &FieldRef) -> (Oid, i32) {
    match field.data_type() {
        DataType::Float32 | DataType::Float16 => (FLOAT4OID, -1),
        DataType::Float64 => (FLOAT8OID, -1),
        DataType::Int16 | DataType::UInt16 | DataType::Int8 | DataType::UInt8 => (INT2OID, -1),
        DataType::UInt32 => (OIDOID, -1),
        DataType::Int32 => (INT4OID, -1),
        DataType::Int64 | DataType::UInt64 => (INT8OID, -1),
        DataType::Decimal128(precision, scale) => (
            NUMERICOID,
            make_numeric_typmod(*precision as _, *scale as _),
        ),
        DataType::Boolean => (BOOLOID, -1),
        DataType::Date32 => (DATEOID, -1),
        DataType::Time64(_) => (TIMEOID, -1),
        DataType::Timestamp(_, None) => (TIMESTAMPOID, -1),
        DataType::Timestamp(_, Some(_)) => (TIMESTAMPTZOID, -1),
        DataType::Utf8 | DataType::LargeUtf8 if field.extension_type_name().is_none() => {
            (TEXTOID, -1)
        }
        DataType::Utf8 | DataType::LargeUtf8
            if field
                .try_extension_type::<arrow_schema::extension::Json>()
                .is_ok() =>
        {
            (JSONOID, -1)
        }
        DataType::Binary | DataType::LargeBinary => (BYTEAOID, -1),
        DataType::FixedSizeBinary(16)
            if field
                .try_extension_type::<arrow_schema::extension::Uuid>()
                .is_ok() =>
        {
            (UUIDOID, -1)
        }
        _ => (InvalidOid, -1),
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

pub(crate) fn error_if_copy_from_match_by_position_with_generated_columns(
    tupledesc: &PgTupleDesc,
    match_by: MatchBy,
) {
    // match_by 'name' can handle generated columns
    if let MatchBy::Name = match_by {
        return;
    }

    let attributes = collect_attributes_for(CollectAttributesFor::Other, tupledesc);

    for attribute in attributes {
        if is_generated_attribute(&attribute) {
            ereport!(
                PgLogLevel::ERROR,
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                "COPY FROM parquet with generated columns is not supported",
                "Try COPY FROM parquet WITH (match_by 'name'). \"
                 It works only if the column names match with parquet file's.",
            );
        }
    }
}
