//! Matching a file's columns to a table's, and the casts that lets a file be read at all.
//!
//! A `COPY FROM` reads a file whose schema is the file's, into a tuple whose schema is the
//! table's, and those two agree only by luck. This is where they are reconciled: the file's
//! fields are matched to the table's by position or by name, and each one is given the Arrow
//! type it has to be cast to before it can become a datum — recursively, because a struct of a
//! list of a numeric has to agree all the way down.
//!
//! A cast is only allowed when arrow-cast can perform it *and* Postgres has an explicit
//! coercion for the pair, so that a file's `int4` reads into a `numeric` column and its `text`
//! does not quietly read into a `date`. The one deliberate exception is an unbounded `numeric`,
//! which adopts the file's decimal type rather than the default `numeric(38,9)`: Postgres can
//! store digits the default would truncate.
//!
//! None of this is format-specific, which is the point of it being here. "File" below means
//! whatever the provider handed back a schema for.

use std::sync::Arc;

use arrow_cast::can_cast_types;
use arrow_schema::{extension::ExtensionType, DataType, Field, FieldRef, Fields, Schema};
use parquet_geospatial::WkbType;
use pgrx::{
    ereport,
    pg_sys::{
        can_coerce_type,
        CoercionContext::{self, COERCION_EXPLICIT},
        FormData_pg_attribute, InvalidOid, Oid, BOOLOID, BYTEAOID, DATEOID, FLOAT4OID, FLOAT8OID,
        INT2OID, INT4OID, INT8OID, JSONOID, NUMERICOID, TEXTOID, TIMEOID, TIMESTAMPOID,
        TIMESTAMPTZOID, UUIDOID,
    },
    PgLogLevel, PgSqlErrorCode, PgTupleDesc,
};

use crate::{
    match_by::MatchBy,
    pgrx_utils::{
        array_element_typoid, collect_attributes_for, domain_array_base_elem_type,
        is_generated_attribute, truncate_identifier_like_postgres, tuple_desc,
        CollectAttributesFor,
    },
    type_compat::pg_arrow_type_conversions::{
        is_unbounded_numeric_typmod, make_numeric_typmod, DEFAULT_UNBOUNDED_NUMERIC_SCALE,
        MAX_NUMERIC_PRECISION,
    },
};

// ensure_file_schema_match_tupledesc_schema throws an error if the file's schema does not match the table schema.
pub fn error_if_copy_from_match_by_position_with_generated_columns(
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
                "COPY FROM a file into generated columns is not supported",
                "Try COPY FROM ... WITH (match_by 'name'), which works when the column \
                 names match the file's.",
            );
        }
    }
}

// ensure_file_schema_match_tupledesc_schema throws an error if the file's schema does not match the table schema.
// If the file's arrow schema is castable to the table's arrow schema, it returns a vector of Option<DataType>
// to cast to for each field.
pub fn ensure_file_schema_match_tupledesc_schema(
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
            "column count mismatch between table and file. \
             the file has {} columns, but table has {} columns",
            file_schema.fields().len(),
            tupledesc_schema.fields().len()
        );
    }

    for (field_idx, (tupledesc_schema_field, attribute)) in tupledesc_schema
        .fields()
        .iter()
        .zip(attributes.iter())
        .enumerate()
    {
        let field_name = tupledesc_schema_field.name();

        let file_schema_field = match match_by {
            // the attnums of the table can have gaps, e.g. after a column is dropped,
            // so the position of the attribute is not its attnum
            MatchBy::Position => file_schema.field(field_idx),

            MatchBy::Name => {
                let file_schema_field = file_schema.column_with_name(field_name);

                if file_schema_field.is_none() {
                    panic!("column \"{field_name}\" is not found in the file");
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

        let target_type = coercion_target(
            &file_schema_field,
            tupledesc_schema_field,
            attribute.atttypid,
            attribute.atttypmod,
            match_by,
        )
        .unwrap_or_else(|| {
            panic!(
                "type mismatch for column \"{field_name}\" between table and file.\n\n\
                 table has \"{to_type}\"\n\nthe file has \"{from_type}\""
            )
        });

        pgrx::debug2!(
            "column \"{}\" is being cast from \"{}\" to \"{}\"",
            field_name,
            from_type,
            target_type
        );

        cast_to_types.push(Some(target_type));
    }

    cast_to_types
}

// overlay_file_geospatial_crs returns the tupledesc schema fields with the crs that the file
// reports for the same geometry and geography columns. The tupledesc schema only knows the crs of
// the target columns, but the read path has to restore the srid that the file was written with.
pub fn overlay_file_geospatial_crs(
    file_schema: &Schema,
    tupledesc_schema: &Schema,
    match_by: MatchBy,
) -> Fields {
    tupledesc_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(field_idx, tupledesc_schema_field)| {
            let file_schema_field = match match_by {
                // the attnums of the table can have gaps, e.g. after a column is dropped,
                // so the position of the attribute is not its attnum
                MatchBy::Position => file_schema
                    .fields()
                    .get(field_idx)
                    .map(|file_schema_field| file_schema_field.as_ref()),
                MatchBy::Name => file_schema
                    .column_with_name(tupledesc_schema_field.name())
                    .map(|(_, file_schema_field)| file_schema_field),
            };

            match file_schema_field {
                Some(file_schema_field) => {
                    with_file_geospatial_crs(tupledesc_schema_field, file_schema_field, match_by)
                }
                None => tupledesc_schema_field.clone(),
            }
        })
        .collect()
}

// with_file_geospatial_crs gives the geometry and geography fields, at any nesting level, the
// Arrow extension type that the file has for them, which is the one that carries the file's crs.
//
// The nested fields are matched the same way coercion_target matches them, so that a field takes
// the crs of the very field whose data is cast into it.
fn with_file_geospatial_crs(
    tupledesc_schema_field: &FieldRef,
    file_schema_field: &Field,
    match_by: MatchBy,
) -> FieldRef {
    if file_schema_field.extension_type_name() == Some(WkbType::NAME) {
        let Ok(wkb_type) = file_schema_field.try_extension_type::<WkbType>() else {
            return tupledesc_schema_field.clone();
        };

        let mut field = tupledesc_schema_field.as_ref().clone();

        if field.try_with_extension_type(wkb_type).is_err() {
            return tupledesc_schema_field.clone();
        }

        return Arc::new(field);
    }

    let data_type = match (
        tupledesc_schema_field.data_type(),
        file_schema_field.data_type(),
    ) {
        (DataType::List(tupledesc_item), DataType::List(file_item)) => DataType::List(
            with_file_geospatial_crs(tupledesc_item, file_item, match_by),
        ),
        (DataType::Struct(tupledesc_fields), DataType::Struct(file_fields)) => DataType::Struct(
            tupledesc_fields
                .iter()
                .enumerate()
                .map(|(field_idx, tupledesc_field)| {
                    let file_field = match match_by {
                        MatchBy::Position => file_fields.get(field_idx),
                        MatchBy::Name => file_fields.iter().find(|file_field| {
                            truncate_identifier_like_postgres(file_field.name(), false)
                                == *tupledesc_field.name()
                        }),
                    };

                    match file_field {
                        Some(file_field) => {
                            with_file_geospatial_crs(tupledesc_field, file_field, match_by)
                        }
                        None => tupledesc_field.clone(),
                    }
                })
                .collect::<Fields>(),
        ),
        (DataType::Map(tupledesc_entries, sorted), DataType::Map(file_entries, _)) => {
            // arrow-cast casts the key and the value of a map by position, never by name
            DataType::Map(
                with_file_geospatial_crs(tupledesc_entries, file_entries, MatchBy::Position),
                *sorted,
            )
        }
        _ => return tupledesc_schema_field.clone(),
    };

    Arc::new(
        tupledesc_schema_field
            .as_ref()
            .clone()
            .with_data_type(data_type),
    )
}

// coercion_target returns the type that the file's field should be cast to, or None if the
// file's field cannot be coerced to the table's type at all.
//
// It first checks if "from_type" can be cast to "to_type" by arrow-cast.
// Then, it checks if the cast is meaningful at Postgres by seeing if there is
// an explicit coercion from "from_typoid" to "to_typoid".
//
// The returned type is the table's type except for an unbounded "numeric", which adopts the
// file's decimal type to avoid losing digits that Postgres is able to store.
//
// Additionaly, we need to be careful about struct rules for the cast:
// Arrow supports casting struct fields by field position instead of field name,
// which is not the intended behavior here. Hence, struct fields are matched
// the same way as the top level columns, by position or by name.
// arrow-cast casts a struct's fields by position in two cases: when the names already
// agree pairwise, and when it cannot find every target field by name, which is
// cast_struct_to_struct's fallback. In between — every target name present but in another
// order — it reorders the fields by name instead, which would quietly contradict
// match_by 'position', so such a struct is not coercible under it.
fn arrow_cast_casts_struct_by_position(from_fields: &Fields, to_fields: &Fields) -> bool {
    let names_agree_in_order = from_fields
        .iter()
        .zip(to_fields.iter())
        .all(|(from_field, to_field)| from_field.name() == to_field.name());

    if names_agree_in_order {
        return true;
    }

    !to_fields.iter().all(|to_field| {
        from_fields
            .iter()
            .any(|from_field| from_field.name() == to_field.name())
    })
}

fn coercion_target(
    from_field: &FieldRef,
    to_field: &FieldRef,
    to_typoid: Oid,
    to_typmod: i32,
    match_by: MatchBy,
) -> Option<DataType> {
    let to_type = to_field.data_type();

    match (from_field.data_type(), to_type) {
        // a column with no type at all contains only nulls, which any type can represent
        (DataType::Null, _) => Some(to_type.clone()),
        (DataType::Struct(from_fields), DataType::Struct(to_fields)) => {
            // arrow-cast refuses a struct cast that changes the number of fields
            if from_fields.len() != to_fields.len() {
                return None;
            }

            if match_by == MatchBy::Position
                && !arrow_cast_casts_struct_by_position(from_fields, to_fields)
            {
                return None;
            }

            let tupledesc = tuple_desc(to_typoid, to_typmod);

            let attributes = collect_attributes_for(CollectAttributesFor::Other, &tupledesc);

            let mut target_fields = Vec::with_capacity(to_fields.len());

            for (field_idx, (to_field, to_attribute)) in
                to_fields.iter().zip(attributes.iter()).enumerate()
            {
                let from_field = match match_by {
                    MatchBy::Position => &from_fields[field_idx],

                    // arrow-cast reorders the struct fields by name as long as all
                    // target fields are found in the file's struct
                    // the attribute names of a composite type are truncated to the identifier
                    // length limit, so the file field names must be truncated to match them
                    MatchBy::Name => from_fields.iter().find(|from_field| {
                        truncate_identifier_like_postgres(from_field.name(), false)
                            == *to_field.name()
                    })?,
                };

                let target_type = coercion_target(
                    from_field,
                    to_field,
                    to_attribute.type_oid().value(),
                    to_attribute.type_mod(),
                    match_by,
                )?;

                target_fields.push(to_field.as_ref().clone().with_data_type(target_type));
            }

            Some(DataType::Struct(target_fields.into()))
        }
        (DataType::List(from_field), DataType::List(to_field))
        | (DataType::FixedSizeList(from_field, _), DataType::List(to_field))
        | (DataType::LargeList(from_field), DataType::List(to_field))
        | (DataType::ListView(from_field), DataType::List(to_field))
        | (DataType::LargeListView(from_field), DataType::List(to_field)) => {
            let element_oid = array_element_typoid(to_typoid);
            let element_typmod = to_typmod;

            let target_type =
                coercion_target(from_field, to_field, element_oid, element_typmod, match_by)?;

            Some(DataType::List(Arc::new(
                to_field.as_ref().clone().with_data_type(target_type),
            )))
        }
        (DataType::Map(from_entries_field, _), DataType::Map(to_entries_field, sorted)) => {
            // entries field cannot be null
            if from_entries_field.is_nullable() {
                return None;
            }

            let (entries_typoid, entries_typmod) = domain_array_base_elem_type(to_typoid);

            // arrow-cast casts the key and the value of a map by position, never by name
            let target_type = coercion_target(
                from_entries_field,
                to_entries_field,
                entries_typoid,
                entries_typmod,
                MatchBy::Position,
            )?;

            Some(DataType::Map(
                Arc::new(
                    to_entries_field
                        .as_ref()
                        .clone()
                        .with_data_type(target_type),
                ),
                *sorted,
            ))
        }
        _ => {
            // check if arrow-cast can cast the types
            if !can_cast_types(from_field.data_type(), to_type) {
                return None;
            }

            let (from_typoid, _) = pg_type_for_arrow_primitive_field(from_field);

            // no Postgres type stands in for that arrow type
            if from_typoid == InvalidOid {
                return None;
            }

            // check if coercion is meaningful at Postgres (it has a coercion path)
            if !can_pg_coerce_types(from_typoid, to_typoid, COERCION_EXPLICIT) {
                return None;
            }

            Some(
                unbounded_numeric_target(from_field.data_type(), to_typoid, to_typmod)
                    .unwrap_or_else(|| to_type.clone()),
            )
        }
    }
}

// unbounded_numeric_target returns the type to read into an unbounded "numeric" column when the
// default unbounded numeric type, "numeric(38,9)", cannot represent the file's value without
// losing digits. Postgres stores an unbounded numeric at any precision and scale, so rescaling
// it to the default ones would silently truncate the value.
fn unbounded_numeric_target(
    from_type: &DataType,
    to_typoid: Oid,
    to_typmod: i32,
) -> Option<DataType> {
    if to_typoid != NUMERICOID || !is_unbounded_numeric_typmod(to_typmod) {
        return None;
    }

    let (precision, scale) = match from_type {
        // a numeric written as text, which a numeric with a precision above Postgres' decimal
        // limit reads through the text path too. Parsing the text keeps every digit, while
        // casting it to the default decimal type would not
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => return Some(DataType::Utf8),
        // a negative scale has no Postgres representation to preserve, so such a decimal is
        // read as the default unbounded numeric type like before
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale)
            if *scale >= 0 =>
        {
            (*precision as u32, *scale as u32)
        }
        _ => return None,
    };

    // a precision wider than Postgres' decimal limit has no decimal type to read into, so the
    // value is formatted as text and parsed, which also keeps every digit
    if precision > MAX_NUMERIC_PRECISION {
        return Some(DataType::Utf8);
    }

    let fits_default_unbounded_numeric = scale <= DEFAULT_UNBOUNDED_NUMERIC_SCALE
        && precision - scale <= MAX_NUMERIC_PRECISION - DEFAULT_UNBOUNDED_NUMERIC_SCALE;

    if fits_default_unbounded_numeric {
        return None;
    }

    Some(DataType::Decimal128(precision as _, scale as _))
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

// pg_type_for_arrow_primitive_field returns the Postgres type and the typmod for given
// primitive arrow field. It returns InvalidOid if the arrow field's type is not recognized.
pub fn pg_type_for_arrow_primitive_field(field: &FieldRef) -> (Oid, i32) {
    match field.data_type() {
        // a dictionary encoded field is represented by the type of its values
        DataType::Dictionary(_, value_type) => {
            let value_field = Field::new(
                field.name(),
                value_type.as_ref().clone(),
                field.is_nullable(),
            )
            .with_metadata(field.metadata().clone());

            pg_type_for_arrow_primitive_field(&Arc::new(value_field))
        }
        DataType::Float32 | DataType::Float16 => (FLOAT4OID, -1),
        DataType::Float64 => (FLOAT8OID, -1),
        DataType::Int16 | DataType::UInt16 | DataType::Int8 | DataType::UInt8 => (INT2OID, -1),
        // "oid" is not used for UInt32 here even though the column inference maps it to a
        // wider type. This type only stands in for the file type during the cast check,
        // and "oid" cannot be cast to numeric, float or money, which would reject casts that
        // both arrow-cast and Postgres can perform.
        DataType::Int32 | DataType::UInt32 => (INT4OID, -1),
        DataType::Int64 | DataType::UInt64 => (INT8OID, -1),
        // the precision and the scale are kept in the typmod, so that a table inferred from
        // the file declares the same numeric the file has
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => {
            if *scale < 0 {
                (NUMERICOID, -1)
            } else {
                (
                    NUMERICOID,
                    make_numeric_typmod(*precision as _, *scale as _),
                )
            }
        }
        DataType::Boolean => (BOOLOID, -1),
        DataType::Date32 | DataType::Date64 => (DATEOID, -1),
        DataType::Time32(_) | DataType::Time64(_) => (TIMEOID, -1),
        DataType::Timestamp(_, None) => (TIMESTAMPOID, -1),
        DataType::Timestamp(_, Some(_)) => (TIMESTAMPTZOID, -1),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            if field
                .try_extension_type::<arrow_schema::extension::Json>()
                .is_ok()
            {
                (JSONOID, -1)
            } else {
                // an unrecognized extension type is read as its storage type
                (TEXTOID, -1)
            }
        }
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => (BYTEAOID, -1),
        DataType::FixedSizeBinary(16)
            if field
                .try_extension_type::<arrow_schema::extension::Uuid>()
                .is_ok() =>
        {
            (UUIDOID, -1)
        }
        DataType::FixedSizeBinary(_) => (BYTEAOID, -1),
        _ => (InvalidOid, -1),
    }
}
