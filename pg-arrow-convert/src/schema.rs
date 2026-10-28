//! The Arrow schema of a Postgres tuple.
//!
//! One function matters here: [`arrow_schema_from_attributes`], which turns the attributes of a
//! `TupleDesc` into the schema every format writes the tuples as. It is the mapping rules of
//! [`crate::type_compat`] applied recursively — a composite is a struct, an array is a list, a
//! `hstore`-style domain is a map, everything unrecognized is text — and it is the same schema
//! whether the file ends up being parquet, Arrow IPC or avro.
//!
//! What is deliberately *not* here is anything a particular container wants stamped onto its
//! fields. Parquet's `field_ids` is the example: it writes a `PARQUET:field_id` into each
//! field's metadata, and it is tempting to thread a mapping through the recursion because that
//! is where the field *paths* are. It does not belong here, and it does not need to be here —
//! the paths are recoverable from the finished schema by walking it, which is what a provider
//! does with the schema it is handed. A provider that stamps its own metadata is a provider
//! that can do it without the base knowing the key.

use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use arrow::datatypes::{Field, Fields, Schema};
use arrow_schema::FieldRef;
use pgrx::pg_sys::{
    FormData_pg_attribute, Oid, BOOLOID, BYTEAOID, CHAROID, DATEOID, FLOAT4OID, FLOAT8OID, INT2OID,
    INT4OID, INT8OID, JSONBOID, JSONOID, NUMERICOID, OIDOID, TEXTOID, TIMEOID, TIMESTAMPOID,
    TIMESTAMPTZOID, TIMETZOID, UUIDOID,
};
use pgrx::{check_for_interrupts, PgTupleDesc};

use crate::pgrx_utils::{
    array_element_typoid, collect_attributes_for, domain_array_base_elem_type, is_array_type,
    is_composite_type, tuple_desc, CollectAttributesFor,
};
use crate::type_compat::geometry::{
    geography_wkb_type, geometry_wkb_type, is_postgis_geography_type, is_postgis_geometry_type,
};
use crate::type_compat::map::is_map_type;
use crate::type_compat::pg_arrow_type_conversions::{
    extract_precision_and_scale_from_numeric_typmod, should_write_numeric_as_text,
};

/// The Arrow schema of a row made of `attributes`.
///
/// Every field is nullable, including the ones Postgres declares `NOT NULL`: a constraint is a
/// property of the table a file was written from, not of the file, and a reader that trusted it
/// would be trusting whoever wrote the file. The exception is inside a map, where the key field
/// must not be nullable for the type to be a map at all.
pub fn arrow_schema_from_attributes(attributes: &[FormData_pg_attribute]) -> Schema {
    let fields = attributes
        .iter()
        .map(|attribute| {
            let field = field_from_type(
                attribute.type_oid().value(),
                attribute.type_mod(),
                attribute.name(),
            );

            check_nesting_depth(&field, attribute.name());

            field
        })
        .collect::<Vec<_>>();

    Schema::new(Fields::from(fields))
}

/// How many levels of nesting a column's type may have.
///
/// Both containers store their schema as a flatbuffer — parquet under its `ARROW:schema` key,
/// Arrow IPC in its footer — and flatbuffers refuses to *read* one nested past a fixed budget.
/// Writing is not bounded by it, so a deeper schema produces a file that no reader can ever
/// open, including the one in this process. Measured against both containers: 60 levels read
/// back, 61 do not.
pub const MAX_NESTING_LEVELS: usize = 60;

/// Errors unless a field is shallow enough to be read back.
///
/// The check is on the way *out*, before a file exists, because the alternative is a `COPY TO`
/// that reports success and leaves an unreadable file behind.
fn check_nesting_depth(field: &FieldRef, column: &str) {
    let depth = nesting_depth(field.data_type());

    if depth > MAX_NESTING_LEVELS {
        panic!(
            "column \"{column}\" nests {depth} levels deep, and a file's schema may nest at \
             most {MAX_NESTING_LEVELS}; a deeper one cannot be read back"
        );
    }
}

/// The deepest chain of nested types in a field. A primitive nests not at all, a struct of
/// primitives nests once.
fn nesting_depth(data_type: &arrow::datatypes::DataType) -> usize {
    use arrow::datatypes::DataType;

    let children: Vec<&FieldRef> = match data_type {
        DataType::Struct(fields) => fields.iter().collect(),
        DataType::Union(fields, _) => fields.iter().map(|(_, field)| field).collect(),
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _)
        | DataType::RunEndEncoded(_, field) => vec![field],
        DataType::Dictionary(_, value) => return 1 + nesting_depth(value),
        _ => return 0,
    };

    1 + children
        .into_iter()
        .map(|field| nesting_depth(field.data_type()))
        .max()
        .unwrap_or(0)
}

/// One field, by type: the four-way branch that every level of the recursion repeats.
fn field_from_type(typoid: Oid, typmod: i32, name: &str) -> FieldRef {
    if is_composite_type(typoid) {
        struct_field(tuple_desc(typoid, typmod), name)
    } else if is_map_type(typoid) {
        let (entries_typoid, entries_typmod) = domain_array_base_elem_type(typoid);
        map_field(entries_typoid, entries_typmod, name)
    } else if is_array_type(typoid) {
        list_field(array_element_typoid(typoid), typmod, name)
    } else {
        primitive_field(typoid, typmod, name)
    }
}

fn struct_field(tupledesc: PgTupleDesc, struct_name: &str) -> FieldRef {
    check_for_interrupts!();

    let attributes = collect_attributes_for(CollectAttributesFor::Other, &tupledesc);

    let child_fields = attributes
        .iter()
        .filter(|attribute| !attribute.is_dropped())
        .map(|attribute| {
            field_from_type(
                attribute.type_oid().value(),
                attribute.type_mod(),
                attribute.name(),
            )
        })
        .collect::<Vec<_>>();

    let nullable = true;

    Field::new(
        struct_name,
        arrow::datatypes::DataType::Struct(Fields::from(child_fields)),
        nullable,
    )
    .into()
}

fn list_field(typoid: Oid, typmod: i32, array_name: &str) -> FieldRef {
    check_for_interrupts!();

    // "element" is what the Arrow list encoding names it, and what every other format
    // tolerates; a name here is not addressable from SQL either way
    let element_name = "element";

    let elem_field = if is_composite_type(typoid) {
        struct_field(tuple_desc(typoid, typmod), element_name)
    } else if is_map_type(typoid) {
        let (entries_typoid, entries_typmod) = domain_array_base_elem_type(typoid);
        map_field(entries_typoid, entries_typmod, element_name)
    } else {
        primitive_field(typoid, typmod, element_name)
    };

    let nullable = true;

    Field::new(
        array_name,
        arrow::datatypes::DataType::List(elem_field),
        nullable,
    )
    .into()
}

fn map_field(typoid: Oid, typmod: i32, map_name: &str) -> FieldRef {
    let entries_field =
        adjust_map_entries_field(struct_field(tuple_desc(typoid, typmod), map_name));

    let keys_sorted = false;
    let nullable = true;

    Field::new(
        map_name,
        arrow::datatypes::DataType::Map(entries_field, keys_sorted),
        nullable,
    )
    .into()
}

fn primitive_field(typoid: Oid, typmod: i32, scalar_name: &str) -> FieldRef {
    check_for_interrupts!();

    let nullable = true;

    let field = match typoid {
        FLOAT4OID => Field::new(scalar_name, arrow::datatypes::DataType::Float32, nullable),
        FLOAT8OID => Field::new(scalar_name, arrow::datatypes::DataType::Float64, nullable),
        BOOLOID => Field::new(scalar_name, arrow::datatypes::DataType::Boolean, nullable),
        INT2OID => Field::new(scalar_name, arrow::datatypes::DataType::Int16, nullable),
        INT4OID => Field::new(scalar_name, arrow::datatypes::DataType::Int32, nullable),
        INT8OID => Field::new(scalar_name, arrow::datatypes::DataType::Int64, nullable),
        UUIDOID => Field::new(
            scalar_name,
            arrow::datatypes::DataType::FixedSizeBinary(16),
            nullable,
        )
        .with_extension_type(arrow_schema::extension::Uuid),
        NUMERICOID => {
            let (precision, scale) = extract_precision_and_scale_from_numeric_typmod(typmod);

            // a numeric with no declared precision, or one wider than a Decimal128 can hold,
            // is written as text rather than silently losing digits
            if should_write_numeric_as_text(precision) {
                Field::new(scalar_name, arrow::datatypes::DataType::Utf8, nullable)
            } else {
                Field::new(
                    scalar_name,
                    arrow::datatypes::DataType::Decimal128(precision as _, scale as _),
                    nullable,
                )
            }
        }
        DATEOID => Field::new(scalar_name, arrow::datatypes::DataType::Date32, nullable),
        TIMESTAMPOID => Field::new(
            scalar_name,
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            nullable,
        ),
        TIMESTAMPTZOID => Field::new(
            scalar_name,
            arrow::datatypes::DataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                Some("+00:00".into()),
            ),
            nullable,
        ),
        TIMEOID => Field::new(
            scalar_name,
            arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
            nullable,
        ),
        TIMETZOID => Field::new(
            scalar_name,
            arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
            nullable,
        )
        .with_metadata(HashMap::from_iter(vec![(
            "adjusted_to_utc".into(),
            "true".into(),
        )])),
        CHAROID => Field::new(scalar_name, arrow::datatypes::DataType::Utf8, nullable),
        TEXTOID => Field::new(scalar_name, arrow::datatypes::DataType::Utf8, nullable),
        JSONOID | JSONBOID => Field::new(scalar_name, arrow::datatypes::DataType::Utf8, nullable)
            .with_extension_type(arrow_schema::extension::Json::default()),
        BYTEAOID => Field::new(scalar_name, arrow::datatypes::DataType::Binary, nullable),
        OIDOID => Field::new(scalar_name, arrow::datatypes::DataType::UInt32, nullable),
        #[cfg(not(pre_pg19))]
        pgrx::pg_sys::OID8OID => {
            Field::new(scalar_name, arrow::datatypes::DataType::UInt64, nullable)
        }
        _ => {
            if is_postgis_geometry_type(typoid) {
                // the GEOMETRY logical type, whose edges are planar like postgis's geometry type
                Field::new(scalar_name, arrow::datatypes::DataType::Binary, nullable)
                    .with_extension_type(geometry_wkb_type(typmod))
            } else if is_postgis_geography_type(typoid) {
                // the GEOGRAPHY logical type, whose edges are spherical like postgis's
                // geography type
                Field::new(scalar_name, arrow::datatypes::DataType::Binary, nullable)
                    .with_extension_type(geography_wkb_type(typmod))
            } else {
                // the text fallback: a type nobody has a mapping for still round trips through
                // its own output and input functions
                Field::new(scalar_name, arrow::datatypes::DataType::Utf8, nullable)
            }
        }
    };

    field.into()
}

/// Rebuilds a map's entries field the way the Arrow map type requires: the entries struct is
/// named `key_value` and is not nullable, its key is not nullable and its value is.
fn adjust_map_entries_field(field: FieldRef) -> FieldRef {
    let arrow::datatypes::DataType::Struct(fields) = field.deref().data_type() else {
        panic!("expected struct data type for map key_value field")
    };

    let key_field = fields.find("key").expect("expected key field").1;
    let value_field = fields.find("val").expect("expected val field").1;

    let key_nullable = false;

    let not_nullable_key_field = Field::new(
        key_field.name(),
        key_field.data_type().clone(),
        key_nullable,
    )
    .with_metadata(key_field.metadata().clone());

    let value_nullable = true;

    let nullable_value_field = Field::new(
        value_field.name(),
        value_field.data_type().clone(),
        value_nullable,
    )
    .with_metadata(value_field.metadata().clone());

    let entries_nullable = false;
    let entries_name = "key_value";

    Arc::new(
        Field::new(
            entries_name,
            arrow::datatypes::DataType::Struct(Fields::from(vec![
                not_nullable_key_field,
                nullable_value_field,
            ])),
            entries_nullable,
        )
        .with_metadata(field.deref().metadata().clone()),
    )
}
