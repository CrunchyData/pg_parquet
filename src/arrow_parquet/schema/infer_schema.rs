use std::hash::{Hash, Hasher};

use arrow_schema::{DataType, FieldRef, Fields};
use pgrx::{
    ereport,
    pg_sys::{
        makeColumnDef, makeTypeName, typenameTypeIdAndMod, AcceptInvalidationMessages, AsPgCStr,
        ColumnDef, CommandCounterIncrement, InvalidOid, Oid, INT4OID, INT8OID, NUMERICOID,
    },
    spi::{quote_identifier, quote_literal, quote_qualified_identifier},
    PgList, PgSqlErrorCode, Spi,
};

use crate::{
    arrow_parquet::uri_utils::{parquet_reader_from_uri, ParsedUriInfo},
    pgrx_utils::{
        array_typoid, collect_attributes_for, extension_exists, get_type_name_with_typemod,
        tuple_desc, type_info_from_name, CollectAttributesFor,
    },
    type_compat::pg_arrow_type_conversions::make_numeric_typmod,
};

use super::coerce_schema::pg_type_for_arrow_primitive_field;

// the schema that hosts the composite types generated for parquet structs
const STRUCT_TYPE_SCHEMA: &str = "parquet_structs";

// the advisory lock class that pg_parquet uses to serialize the creation of the
// composite types for parquet structs
const STRUCT_TYPE_LOCK_CLASS: i32 = 0x7067_7071;

// StructAttribute is a single attribute of a composite type that is generated for a parquet struct.
struct StructAttribute {
    name: String,
    typoid: Oid,
    typmod: i32,
}

impl StructAttribute {
    fn typename(&self) -> String {
        get_type_name_with_typemod(self.typoid, self.typmod)
    }
}

pub(crate) fn infer_columns_from_uri(uri_info: &ParsedUriInfo) -> PgList<ColumnDef> {
    let (parquet_reader, _) = parquet_reader_from_uri(uri_info).unwrap_or_else(|e| {
        panic!(
            "failed to create parquet reader for uri {}: {}",
            uri_info.uri, e
        )
    });

    let parquet_schema = parquet_reader.schema();

    let mut column_defs = PgList::new();

    for field in parquet_schema.fields() {
        let field_name = field.name();

        if field_name.is_empty() {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_INVALID_COLUMN_DEFINITION,
                "parquet file has a field with an empty name",
                "Create the table with explicit column definitions \
                 and then COPY FROM the parquet file.",
            );
        }

        let (typoid, typmod) = get_or_create_pg_type_from_arrow_type(field, field_name);

        let column_def =
            unsafe { makeColumnDef(field_name.as_pg_cstr(), typoid, typmod, InvalidOid) };

        column_defs.push(column_def);
    }

    column_defs
}

// get_or_create_pg_type_from_arrow_type returns the Postgres type for the given arrow field.
// If the arrow field is a struct or a map, it creates a new Postgres type in case it does not exist.
// "field_path" is the dotted path of the field in the parquet schema, which is only used to report
// a meaningful error message for the fields we cannot map to a Postgres type.
fn get_or_create_pg_type_from_arrow_type(field: &FieldRef, field_path: &str) -> (Oid, i32) {
    match field.data_type() {
        DataType::Struct(fields) => {
            let struct_attributes = get_or_create_struct_attributes(fields, field_path);

            let struct_type_name = get_parquet_struct_typename(&struct_attributes);

            let (typoid, typmod) = type_info_from_name(STRUCT_TYPE_SCHEMA, &struct_type_name);

            if typoid != InvalidOid {
                // the type name is derived from a hash of the attributes, so it is almost always
                // the type we would create. Make sure it really is before reusing it.
                ensure_struct_type_matches(&struct_type_name, typoid, typmod, &struct_attributes);

                return (typoid, typmod);
            }

            // another session might be creating the same type right now, which would make
            // one of the two fail with a unique violation on pg_type. Wait for it and
            // then look the type up again.
            lock_struct_type_creation(&struct_type_name);

            // make the type that the other session committed visible to us
            unsafe { AcceptInvalidationMessages() };

            let (typoid, typmod) = type_info_from_name(STRUCT_TYPE_SCHEMA, &struct_type_name);

            if typoid != InvalidOid {
                ensure_struct_type_matches(&struct_type_name, typoid, typmod, &struct_attributes);

                return (typoid, typmod);
            }

            create_struct_type(&struct_type_name, &struct_attributes)
        }
        DataType::List(element_field)
        | DataType::LargeList(element_field)
        | DataType::FixedSizeList(element_field, _) => {
            let (element_typoid, element_typmod) =
                get_or_create_pg_type_from_arrow_type(element_field, field_path);

            let array_typoid = array_typoid(element_typoid);

            if array_typoid == InvalidOid {
                // e.g. Postgres has no array of array type
                unsupported_arrow_type_error(
                    field_path,
                    field.data_type(),
                    &format!(
                        "Postgres has no array type for \"{}\".",
                        get_type_name_with_typemod(element_typoid, element_typmod)
                    ),
                );
            }

            (array_typoid, element_typmod)
        }
        DataType::Map(entries_field, _) => {
            let entries_fields = match entries_field.data_type() {
                DataType::Struct(entries_fields) if entries_fields.len() == 2 => entries_fields,
                _ => unsupported_arrow_type_error(
                    field_path,
                    field.data_type(),
                    "Map entries must be a struct with a key and a value field.",
                ),
            };

            if !extension_exists("crunchy_map") {
                unsupported_arrow_type_error(
                    field_path,
                    field.data_type(),
                    "Maps require the crunchy_map extension. \
                     Try \"CREATE EXTENSION crunchy_map;\" first.",
                );
            }

            // the key and the value fields are matched by position since their names
            // differ between the parquet writers e.g. "val" vs "value"
            let key_field = &entries_fields[0];
            let value_field = &entries_fields[1];

            let (key_typoid, key_typmod) =
                get_or_create_pg_type_from_arrow_type(key_field, field_path);
            let (value_typoid, value_typmod) =
                get_or_create_pg_type_from_arrow_type(value_field, field_path);

            create_map_type(
                &get_type_name_with_typemod(key_typoid, key_typmod),
                &get_type_name_with_typemod(value_typoid, value_typmod),
            )
        }
        // Postgres has no unsigned integers, so we infer the next wider type
        // that can represent all values of the parquet column. Note that "oid"
        // is not used for UInt32 since it maps 0 to NULL.
        DataType::UInt16 => (INT4OID, -1),
        DataType::UInt32 => (INT8OID, -1),
        DataType::UInt64 => (NUMERICOID, make_numeric_typmod(20, 0)),
        _ => {
            let (typoid, typmod) = pg_type_for_arrow_primitive_field(field);

            if typoid == InvalidOid {
                unsupported_arrow_type_error(
                    field_path,
                    field.data_type(),
                    "Create the table with explicit column definitions \
                     and then COPY FROM the parquet file.",
                );
            }

            (typoid, typmod)
        }
    }
}

fn unsupported_arrow_type_error(field_path: &str, data_type: &DataType, hint: &str) -> ! {
    ereport!(
        ERROR,
        PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
        format!(
            "cannot infer a Postgres type for the parquet field \"{}\" with type \"{}\"",
            field_path, data_type
        ),
        hint.to_string(),
    );
}

// get_or_create_struct_attributes returns the attributes of the composite type for a struct.
// If an attribute is a struct or a map, it creates a new Postgres type in case it does not exist.
fn get_or_create_struct_attributes(fields: &Fields, field_path: &str) -> Vec<StructAttribute> {
    let mut attributes = vec![];

    for field in fields {
        let name = field.name();

        let field_path = format!("{}.{}", field_path, name);

        let (typoid, typmod) = get_or_create_pg_type_from_arrow_type(field, &field_path);

        attributes.push(StructAttribute {
            name: name.clone(),
            typoid,
            typmod,
        });
    }

    attributes
}

// get_parquet_struct_typename returns the name of the Postgres type for the given attributes.
// The type modifiers take part in the name since e.g. "numeric(10,2)" and "numeric(5,1)"
// cannot share the same composite type.
fn get_parquet_struct_typename(attributes: &[StructAttribute]) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();

    for attribute in attributes {
        attribute.name.hash(&mut hasher);
        attribute.typename().hash(&mut hasher);
    }

    format!("struct_{}", hasher.finish())
}

// ensure_struct_type_matches throws an error if the already existing composite type
// does not consist of the given attributes.
fn ensure_struct_type_matches(
    type_name: &str,
    typoid: Oid,
    typmod: i32,
    attributes: &[StructAttribute],
) {
    let tupledesc = tuple_desc(typoid, typmod);

    let existing_attributes = collect_attributes_for(CollectAttributesFor::Other, &tupledesc);

    let matches = existing_attributes.len() == attributes.len()
        && existing_attributes.iter().zip(attributes.iter()).all(
            |(existing_attribute, attribute)| {
                existing_attribute.name() == attribute.name
                    && existing_attribute.type_oid().value() == attribute.typoid
                    && existing_attribute.type_mod() == attribute.typmod
            },
        );

    if !matches {
        ereport!(
            ERROR,
            PgSqlErrorCode::ERRCODE_DUPLICATE_OBJECT,
            format!(
                "type \"{}\" already exists with a different definition",
                quote_qualified_identifier(STRUCT_TYPE_SCHEMA, type_name)
            ),
            "Drop the type to let pg_parquet recreate it for the parquet file's schema."
                .to_string(),
        );
    }
}

// lock_struct_type_creation takes a transaction level advisory lock for the given type name.
// The lock is held until the end of the transaction since the type is not visible to the
// other sessions before it commits.
fn lock_struct_type_creation(type_name: &str) {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    type_name.hash(&mut hasher);
    let lock_key = hasher.finish() as i32;

    let lock_command = format!(
        "SELECT pg_advisory_xact_lock({}, {});",
        STRUCT_TYPE_LOCK_CLASS, lock_key
    );

    Spi::run(&lock_command).unwrap_or_else(|e| {
        panic!("failed to lock the struct type creation: {}", e);
    });
}

// create_struct_type creates a new Postgres type for the given attributes
// by executing a CREATE TYPE command.
fn create_struct_type(type_name: &str, attributes: &[StructAttribute]) -> (Oid, i32) {
    let attribute_definitions = attributes
        .iter()
        .map(|attribute| {
            format!(
                "{} {}",
                quote_identifier(attribute.name.as_str()),
                attribute.typename()
            )
        })
        .collect::<Vec<_>>()
        .join(", ");

    let create_type_command = format!(
        "CREATE TYPE {} AS ({});",
        quote_qualified_identifier(STRUCT_TYPE_SCHEMA, type_name),
        attribute_definitions
    );

    Spi::run(&create_type_command).unwrap_or_else(|e| {
        panic!("failed to create struct type: {}", e);
    });

    // increment the command counter to make the type visible
    unsafe { CommandCounterIncrement() };

    type_info_from_name(STRUCT_TYPE_SCHEMA, type_name)
}

// create_map_type creates, if it does not already exist, the crunchy_map type
// for the given key and value types.
fn create_map_type(key_typename: &str, value_typename: &str) -> (Oid, i32) {
    let create_map_command = format!(
        "SELECT crunchy_map.create({}, {});",
        quote_literal(key_typename),
        quote_literal(value_typename)
    );

    let map_typename = Spi::get_one::<&str>(create_map_command.as_str())
        .unwrap_or_else(|e| {
            panic!("failed to create map type: {}", e);
        })
        .unwrap_or_else(|| {
            panic!("failed to create map type");
        });

    let mut typoid = InvalidOid;
    let mut typmod = -1;

    let typename = unsafe { makeTypeName(map_typename.as_pg_cstr()) };

    unsafe { typenameTypeIdAndMod(std::ptr::null_mut(), typename, &mut typoid, &mut typmod) };

    (typoid, typmod)
}
