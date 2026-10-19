//! Postgres column definitions from a file's Arrow schema: `pg-arrow-convert` run backwards.
//!
//! This is what `CREATE TABLE ... FROM '<uri>'` needs, and none of it is a format's business —
//! a struct is a composite type and a 16 byte fixed binary is a uuid whichever container they
//! came out of. Reading the schema out of a file is tier 2's (`pg_arrow_base`), because that
//! needs a uri and a provider; everything from the schema onwards is here.

use std::ffi::CStr;
use std::hash::{Hash, Hasher};

use arrow_schema::{DataType, FieldRef, Fields, SchemaRef};
use pgrx::{
    ereport,
    pg_sys::{
        creating_extension as pg_creating_extension, makeColumnDef, pg_enc,
        AcceptInvalidationMessages, AsPgCStr, ColumnDef, CommandCounterIncrement,
        CurrentExtensionObject as pg_current_extension_object, GetDatabaseEncoding,
        GetDatabaseEncodingName, InvalidOid, MaxHeapAttributeNumber, Oid, INT4OID, INT8OID,
        NUMERICOID, UUIDOID,
    },
    spi::{quote_identifier, quote_literal, quote_qualified_identifier},
    PgList, PgSqlErrorCode, Spi,
};

use crate::coerce::pg_type_for_arrow_primitive_field;
use crate::pgrx_utils::{
    array_typoid, collect_attributes_for, extension_exists, get_qualified_type_name_with_typemod,
    get_type_name_with_typemod, is_composite_type, is_standalone_composite_type,
    truncate_identifier_like_postgres, tuple_desc, type_info_from_name, CollectAttributesFor,
};
use crate::type_compat::pg_arrow_type_conversions::make_numeric_typmod;

// the schema that hosts the composite types generated for a file's structs
const STRUCT_TYPE_SCHEMA: &str = "arrow_structs";

// the schema that hosts the crunchy_map types generated for a file's maps
const MAP_TYPE_SCHEMA: &str = "crunchy_map";

// the advisory lock that serializes the creation of the composite types
// for a file's structs. A single lock is used for all of them: a file can need more than one
// type and the types are created in the order the schema lists them, so per type
// locks would deadlock two sessions that read the same types in a different order.
pub const STRUCT_TYPE_LOCK_CLASS: i32 = 0x7067_7071;
pub const STRUCT_TYPE_LOCK_KEY: i32 = 0x7374_7275;

// StructAttribute is a single attribute of a composite type generated for a file's struct.
struct StructAttribute {
    name: String,
    typoid: Oid,
    typmod: i32,
}

impl StructAttribute {
    // the type name takes part in the name of the composite type, so it must not depend on
    // the search_path of the session that creates it
    fn typename(&self) -> String {
        get_qualified_type_name_with_typemod(self.typoid, self.typmod)
    }
}

// error_if_field_name_not_representable errors out if the name of a field cannot be used as an
// identifier in the database. A file stores the name as utf-8 while an identifier has to be in
// the encoding of the database, and nothing converts between the two, so a name that is not ascii
// is only safe in a utf-8 database. Elsewhere its bytes mean another string entirely, or no valid
// string at all, and the identifier truncation would cut them at a byte boundary on top of that.
// A nul byte is rejected as well: an identifier is a C string, so the name would silently lose
// everything from the nul byte on.
fn error_if_field_name_not_representable(field_name: &str) {
    if field_name.contains('\0') {
        ereport!(
            ERROR,
            PgSqlErrorCode::ERRCODE_INVALID_COLUMN_DEFINITION,
            format!(
                "field name \"{}\" contains a nul byte",
                field_name.replace('\0', "\\0")
            ),
            "Create the table with explicit column definitions \
             and then COPY FROM the file.",
        );
    }

    if field_name.is_ascii() || unsafe { GetDatabaseEncoding() } == pg_enc::PG_UTF8 as i32 {
        return;
    }

    let encoding_name = unsafe { CStr::from_ptr(GetDatabaseEncodingName()) }
        .to_str()
        .expect("database encoding name is not valid UTF-8");

    ereport!(
        ERROR,
        PgSqlErrorCode::ERRCODE_UNTRANSLATABLE_CHARACTER,
        format!(
            "field name \"{}\" cannot be used as an identifier \
             in a database with encoding \"{}\"",
            field_name, encoding_name
        ),
        "Create the table with explicit column definitions \
         and then COPY FROM the file.",
    );
}

/// One `ColumnDef` per top-level field of the schema, in the file's order. Creates the
/// composite and map types the file's nested fields need, which is why it is not a pure
/// function of the schema.
pub fn infer_columns_from_schema(file_schema: &SchemaRef) -> PgList<ColumnDef> {
    let mut column_defs = PgList::new();
    let mut column_names: Vec<String> = vec![];

    for field in file_schema.fields() {
        let field_name = field.name();

        if field_name.is_empty() {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_INVALID_COLUMN_DEFINITION,
                "file has a field with an empty name",
                "Create the table with explicit column definitions \
                 and then COPY FROM the file.",
            );
        }

        error_if_field_name_not_representable(field_name);

        // the columns are added to a CREATE TABLE command, which truncates their names
        let column_name = truncate_identifier_like_postgres(field_name, true);

        // arrow lets a schema have two fields with the same name, and two long names can also
        // end up equal once they are truncated. CREATE TABLE would reject the duplicate as if
        // the user listed the column twice, so it is reported here instead.
        if column_names.contains(&column_name) {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_DUPLICATE_COLUMN,
                format!("file has more than one field named \"{}\"", column_name),
                "Create the table with explicit column definitions \
                 and then COPY FROM the file.",
            );
        }

        let (typoid, typmod) = get_or_create_pg_type_from_arrow_type(field, field_name);

        let column_def =
            unsafe { makeColumnDef(column_name.as_pg_cstr(), typoid, typmod, InvalidOid) };

        column_defs.push(column_def);
        column_names.push(column_name);
    }

    column_defs
}

// get_or_create_pg_type_from_arrow_type returns the Postgres type for the given arrow field.
// If the arrow field is a struct or a map, it creates a new Postgres type in case it does not exist.
// "field_path" is the dotted path of the field in the file's schema, which is only used to report
// a meaningful error message for the fields we cannot map to a Postgres type.
fn get_or_create_pg_type_from_arrow_type(field: &FieldRef, field_path: &str) -> (Oid, i32) {
    match field.data_type() {
        DataType::Struct(fields) => {
            let struct_attributes = get_or_create_struct_attributes(fields, field_path);

            // CREATE TYPE would reject the composite type with Postgres' own column limit error,
            // which comes with the whole generated command as its context.
            if struct_attributes.len() > MaxHeapAttributeNumber as usize {
                ereport!(
                    ERROR,
                    PgSqlErrorCode::ERRCODE_TOO_MANY_COLUMNS,
                    format!(
                        "field \"{}\" has {} fields, more than the {} a Postgres \
                         composite type can have",
                        field_path,
                        struct_attributes.len(),
                        MaxHeapAttributeNumber
                    ),
                    "Create the table with explicit column definitions \
                     and then COPY FROM the file.",
                );
            }

            let struct_type_name = struct_typename(&struct_attributes);

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
            lock_struct_type_creation();

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
            // differ between writers e.g. "val" vs "value"
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
        // that can represent all values of the file's column. Note that "oid"
        // is not used for UInt32 since it maps 0 to NULL.
        DataType::UInt16 => (INT4OID, -1),
        DataType::UInt32 => (INT8OID, -1),
        DataType::UInt64 => (NUMERICOID, make_numeric_typmod(20, 0)),
        // a 16 byte fixed binary is loaded into a uuid column even when the file does not
        // carry the arrow.uuid extension name. Any other width falls back to "bytea"
        DataType::FixedSizeBinary(16) => (UUIDOID, -1),
        _ => {
            let (typoid, typmod) = pg_type_for_arrow_primitive_field(field);

            if typoid == InvalidOid {
                unsupported_arrow_type_error(
                    field_path,
                    field.data_type(),
                    "Create the table with explicit column definitions \
                     and then COPY FROM the file.",
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
            "cannot infer a Postgres type for the field \"{}\" with type \"{}\"",
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

        if name.is_empty() {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_INVALID_COLUMN_DEFINITION,
                format!(
                    "file has a field with an empty name in the struct \"{}\"",
                    field_path
                ),
                "Create the table with explicit column definitions \
                 and then COPY FROM the file.",
            );
        }

        error_if_field_name_not_representable(name);

        // the type is created by a CREATE TYPE command, which truncates the attribute names.
        // Truncate them here too, otherwise the type we create would never match the one we
        // look for when the same struct is seen again.
        let attribute_name = truncate_identifier_like_postgres(name, true);

        // arrow lets a struct have two fields with the same name, and two long names can also
        // end up equal once they are truncated. CREATE TYPE would reject the duplicate with
        // the generated statement in its context, so it is reported here instead.
        if attributes
            .iter()
            .any(|attribute: &StructAttribute| attribute.name == attribute_name)
        {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_DUPLICATE_COLUMN,
                format!(
                    "file has more than one field named \"{}\" in the struct \"{}\"",
                    attribute_name, field_path
                ),
                "Create the table with explicit column definitions \
                 and then COPY FROM the file.",
            );
        }

        let field_path = format!("{}.{}", field_path, name);

        let (typoid, typmod) = get_or_create_pg_type_from_arrow_type(field, &field_path);

        attributes.push(StructAttribute {
            name: attribute_name,
            typoid,
            typmod,
        });
    }

    attributes
}

// struct_typename returns the name of the Postgres type for the given attributes.
// The type modifiers take part in the name since e.g. "numeric(10,2)" and "numeric(5,1)"
// cannot share the same composite type.
fn struct_typename(attributes: &[StructAttribute]) -> String {
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
    // a relation's rowtype has the name of the relation, so it can hold the name we need. It
    // is never reused, even when its columns match: the owner of that relation could change
    // our column with ALTER TABLE or drop it together with the relation.
    if is_composite_type(typoid) && !is_standalone_composite_type(typoid) {
        ereport!(
            ERROR,
            PgSqlErrorCode::ERRCODE_DUPLICATE_OBJECT,
            format!(
                "the type name \"{}\" that the file's struct needs is the rowtype of a relation",
                quote_qualified_identifier(STRUCT_TYPE_SCHEMA, type_name)
            ),
            "Drop or rename that relation to let the type be created.".to_string(),
        );
    }

    // a non composite type, e.g. a domain or an enum, has no attributes to compare. Report it
    // as a mismatch instead of letting the tupledesc lookup fail with a less helpful message.
    let matches = is_standalone_composite_type(typoid) && {
        let tupledesc = tuple_desc(typoid, typmod);

        let existing_attributes = collect_attributes_for(CollectAttributesFor::Other, &tupledesc);

        // a dropped attribute keeps its slot in the tupledesc and the struct conversion cannot
        // fill it, so compare the raw attribute count as well to not reuse such a type.
        tupledesc.len() == attributes.len()
            && existing_attributes.len() == attributes.len()
            && existing_attributes.iter().zip(attributes.iter()).all(
                |(existing_attribute, attribute)| {
                    existing_attribute.name() == attribute.name
                        && existing_attribute.type_oid().value() == attribute.typoid
                        && existing_attribute.type_mod() == attribute.typmod
                },
            )
    };

    if !matches {
        ereport!(
            ERROR,
            PgSqlErrorCode::ERRCODE_DUPLICATE_OBJECT,
            format!(
                "type \"{}\" already exists with a different definition",
                quote_qualified_identifier(STRUCT_TYPE_SCHEMA, type_name)
            ),
            "Drop the type to let it be recreated for the file's schema.".to_string(),
        );
    }
}

// lock_struct_type_creation takes the transaction level advisory lock that serializes the
// creation of the composite types. The lock is held until the end of the transaction since
// the type is not visible to the other sessions before it commits.
// The function is schema qualified because a search_path that names pg_catalog itself puts
// pg_catalog last, which would let an earlier schema shadow it.
fn lock_struct_type_creation() {
    let lock_command = format!(
        "SELECT pg_catalog.pg_advisory_xact_lock({}, {});",
        STRUCT_TYPE_LOCK_CLASS, STRUCT_TYPE_LOCK_KEY
    );

    Spi::run(&lock_command).unwrap_or_else(|e| {
        panic!("failed to lock the struct type creation: {}", e);
    });
}

// create_type_outside_any_extension runs a type creating command with the "an extension script
// is running" state cleared. The generated types are shared by every table that reads a matching
// schema, so a type must not become a member of whichever extension happens to create it
// first: dropping that extension would then drop the columns of unrelated tables. The state is
// restored on the error path by the extension machinery itself, which resets both globals when
// the command that created the extension unwinds.
fn create_type_outside_any_extension<T>(create: impl FnOnce() -> T) -> T {
    let creating_extension = unsafe { pg_creating_extension };
    let extension_object = unsafe { pg_current_extension_object };

    if creating_extension {
        unsafe {
            pg_creating_extension = false;
            pg_current_extension_object = InvalidOid;
        }
    }

    let result = create();

    if creating_extension {
        unsafe {
            pg_creating_extension = creating_extension;
            pg_current_extension_object = extension_object;
        }
    }

    result
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

    create_type_outside_any_extension(|| {
        Spi::run(&create_type_command).unwrap_or_else(|e| {
            panic!("failed to create struct type: {}", e);
        })
    });

    // increment the command counter to make the type visible
    unsafe { CommandCounterIncrement() };

    let (typoid, typmod) = type_info_from_name(STRUCT_TYPE_SCHEMA, type_name);

    // the type can already be gone, e.g. an event trigger on CREATE TYPE can drop it. Report it
    // instead of putting an invalid type into the column definition, which Postgres reports as
    // an internal "cache lookup failed for type 0".
    if typoid == InvalidOid {
        panic!(
            "failed to find the struct type {} that was created for the field",
            quote_qualified_identifier(STRUCT_TYPE_SCHEMA, type_name)
        );
    }

    (typoid, typmod)
}

// create_map_type creates, if it does not already exist, the crunchy_map type
// for the given key and value types.
fn create_map_type(key_typename: &str, value_typename: &str) -> (Oid, i32) {
    // crunchy_map.create creates the type when it is missing, so serialize it with the
    // other sessions the same way the struct types are serialized.
    lock_struct_type_creation();

    let create_map_command = format!(
        "SELECT crunchy_map.create({}, {});",
        quote_literal(key_typename),
        quote_literal(value_typename)
    );

    let map_typename = create_type_outside_any_extension(|| {
        Spi::get_one::<String>(create_map_command.as_str())
            .unwrap_or_else(|e| {
                panic!("failed to create map type: {}", e);
            })
            .unwrap_or_else(|| {
                panic!("failed to create map type");
            })
    });

    let typoid = map_typoid_from_name(&map_typename);

    if typoid == InvalidOid {
        panic!(
            "failed to find the map type {} that was created for the field",
            map_typename
        );
    }

    // a crunchy_map type is a domain over an array of key-value pairs, which takes no typmod
    (typoid, -1)
}

// map_typoid_from_name resolves the type name that crunchy_map.create returned. The name comes
// back schema qualified, so it cannot be looked up as a single identifier. It is still resolved
// in the crunchy_map schema as a fallback in case a future version returns a bare name.
pub fn map_typoid_from_name(map_typename: &str) -> Oid {
    let lookup_command = format!(
        "SELECT coalesce(pg_catalog.to_regtype({name}), \
             pg_catalog.to_regtype({schema} || '.' || pg_catalog.quote_ident({name})))::oid;",
        name = quote_literal(map_typename),
        schema = quote_literal(MAP_TYPE_SCHEMA)
    );

    Spi::get_one::<Oid>(&lookup_command)
        .unwrap_or_else(|e| panic!("failed to look up the map type {}: {}", map_typename, e))
        .unwrap_or(InvalidOid)
}
