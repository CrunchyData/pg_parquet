use std::hash::{Hash, Hasher};

use arrow_schema::{DataType, FieldRef, Fields};
use pgrx::{
    pg_sys::{
        makeColumnDef, makeTypeName, typenameTypeIdAndMod, AsPgCStr, ColumnDef,
        CommandCounterIncrement, InvalidOid, Oid,
    },
    PgList, Spi,
};

use crate::{
    arrow_parquet::uri_utils::{parquet_reader_from_uri, ParsedUriInfo},
    pgrx_utils::{array_typoid, get_type_name, type_info_from_name},
};

use super::coerce_schema::pg_type_for_arrow_primitive_field;

pub(crate) fn infer_columns_from_uri(uri_info: &ParsedUriInfo) -> PgList<ColumnDef> {
    let parquet_reader = parquet_reader_from_uri(uri_info).unwrap_or_else(|e| {
        panic!(
            "failed to create parquet reader for uri {}: {}",
            uri_info.uri, e
        )
    });

    let parquet_schema = parquet_reader.schema();

    let mut column_defs = PgList::new();

    for field in parquet_schema.fields() {
        let (typoid, typmod) = get_or_create_pg_type_from_arrow_type(field);

        let field_name = field.name();

        let column_def =
            unsafe { makeColumnDef(field_name.as_pg_cstr(), typoid, typmod, InvalidOid) };

        column_defs.push(column_def);
    }

    column_defs
}

// get_or_create_pg_type_from_arrow_type returns the Postgres type for the given arrow field.
// If the arrow field is a struct or a map, it creates a new Postgres type in case it does not exist.
fn get_or_create_pg_type_from_arrow_type(field: &FieldRef) -> (Oid, i32) {
    match field.data_type() {
        DataType::Struct(fields) => {
            let struct_attribute_oids = get_or_create_struct_field_types(fields);

            let struct_attribute_names =
                fields.iter().map(|f| f.name().as_str()).collect::<Vec<_>>();

            let struct_type_name =
                get_parquet_struct_typename(&struct_attribute_oids, &struct_attribute_names);

            let struct_schema_name = "parquet_structs";

            let (typoid, typmod) = type_info_from_name(struct_schema_name, &struct_type_name);

            if typoid != InvalidOid {
                return (typoid, typmod);
            }

            create_struct_type(
                struct_schema_name,
                &struct_type_name,
                &struct_attribute_oids,
                &struct_attribute_names,
            )
        }
        DataType::List(element_field) => {
            let (element_typoid, element_typmod) =
                get_or_create_pg_type_from_arrow_type(element_field);

            (array_typoid(element_typoid), element_typmod)
        }
        DataType::Map(entries_field, _) => match entries_field.data_type() {
            DataType::Struct(fields) => {
                let key_field = fields.find("key").expect("expected key field").1;
                let (key_typoid, _) = get_or_create_pg_type_from_arrow_type(key_field);
                let key_typename = get_type_name(key_typoid);

                let value_field = fields.find("val").expect("expected val field").1;
                let (value_typoid, _) = get_or_create_pg_type_from_arrow_type(value_field);
                let value_typename = get_type_name(value_typoid);

                let create_map_command = format!(
                    "SELECT crunchy_map.create('{}', '{}');",
                    key_typename, value_typename
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

                unsafe {
                    typenameTypeIdAndMod(std::ptr::null_mut(), typename, &mut typoid, &mut typmod)
                };

                (typoid, typmod)
            }
            _ => panic!("expected struct data type for map entries"),
        },
        _ => pg_type_for_arrow_primitive_field(field),
    }
}

// get_or_create_struct_field_types returns a vector of Postgres types for the fields of a struct.
// If the field is a struct or a map, it creates a new Postgres type in case it does not exist.
fn get_or_create_struct_field_types(fields: &Fields) -> Vec<Oid> {
    let mut field_oids = vec![];

    for field in fields {
        let (typoid, _) = get_or_create_pg_type_from_arrow_type(field);

        field_oids.push(typoid);
    }

    field_oids
}

// get_parquet_struct_typename returns the name of the Postgres type for the given attribute oids and names.
fn get_parquet_struct_typename(attribute_oids: &[Oid], attribute_names: &[&str]) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();

    for (oid, name) in attribute_oids.iter().zip(attribute_names.iter()) {
        oid.hash(&mut hasher);
        name.hash(&mut hasher);
    }

    format!("struct_{}", hasher.finish())
}

// create_struct_type creates a new Postgres type for the given attribute oids and names
// by executing a CREATE TYPE command.
fn create_struct_type(
    schema_name: &str,
    type_name: &str,
    attribute_oids: &[Oid],
    attribute_names: &[&str],
) -> (Oid, i32) {
    let mut create_type_command = String::new();

    create_type_command
        .push_str(format!("CREATE TYPE {}.{} AS (", schema_name, type_name).as_str());

    for (att_idx, (oid, name)) in attribute_oids
        .iter()
        .zip(attribute_names.iter())
        .enumerate()
    {
        let field_typename = get_type_name(*oid);

        create_type_command.push_str(format!("{} {}", name, field_typename).as_str());

        if att_idx < attribute_oids.len() - 1 {
            create_type_command.push_str(", ");
        }
    }

    create_type_command.push_str(");");

    Spi::run(&create_type_command).unwrap_or_else(|e| {
        panic!("failed to create struct type: {}", e);
    });

    // increment the command counter to make the type visible
    unsafe { CommandCounterIncrement() };

    type_info_from_name(schema_name, type_name)
}
