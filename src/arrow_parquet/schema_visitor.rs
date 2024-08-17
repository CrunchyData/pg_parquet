use std::{collections::HashMap, sync::Arc};

use arrow::datatypes::{Field, Fields, Schema};
use arrow_schema::ExtensionType;
use parquet::arrow::arrow_to_parquet_schema;
use pg_sys::{
    Oid, BOOLOID, BYTEAOID, CHAROID, DATEOID, FLOAT4OID, FLOAT8OID, INT2OID, INT4OID, INT8OID,
    INTERVALOID, JSONBOID, JSONOID, NUMERICOID, OIDOID, RECORDOID, TEXTOID, TIMEOID, TIMESTAMPOID,
    TIMESTAMPTZOID, TIMETZOID, UUIDOID,
};
use pgrx::{prelude::*, PgTupleDesc};

use crate::{
    pgrx_utils::{
        array_element_typoid, collect_valid_attributes, domain_array_base_elem_typoid,
        is_array_type, is_composite_type, tuple_desc,
    },
    type_compat::{
        fallback_to_text::set_fallback_typoid,
        geometry::{is_postgis_geometry_type, set_geometry_typoid},
        map::{is_crunchy_map_type, set_crunchy_map_typoid},
        pg_arrow_type_conversions::{
            extract_precision_from_numeric_typmod, extract_scale_from_numeric_typmod,
            MAX_DECIMAL_PRECISION,
        },
    },
};

use super::arrow_utils::to_not_nullable_field;

pub(crate) fn parquet_schema_string_from_tupledesc(tupledesc: PgTupleDesc) -> String {
    let arrow_schema = parse_arrow_schema_from_tupledesc(tupledesc);
    let parquet_schema = arrow_to_parquet_schema(&arrow_schema).unwrap();

    let mut buf = Vec::new();
    parquet::schema::printer::print_schema(&mut buf, &parquet_schema.root_schema_ptr());
    String::from_utf8(buf).unwrap()
}

pub(crate) fn parse_arrow_schema_from_tupledesc(tupledesc: PgTupleDesc) -> Schema {
    debug_assert!(tupledesc.oid() == RECORDOID);

    let mut struct_attribute_fields = vec![];

    let include_generated_columns = true;
    let attributes = collect_valid_attributes(&tupledesc, include_generated_columns);

    for attribute in attributes {
        let attribute_name = attribute.name();
        let attribute_typoid = attribute.type_oid().value();
        let attribute_typmod = attribute.type_mod();

        let field = if is_composite_type(attribute_typoid) {
            let attribute_tupledesc = tuple_desc(attribute_typoid, attribute_typmod);
            visit_struct_schema(attribute_tupledesc, attribute_name)
        } else if is_crunchy_map_type(attribute_typoid) {
            set_crunchy_map_typoid(attribute_typoid);
            let attribute_base_elem_typoid = domain_array_base_elem_typoid(attribute_typoid);
            visit_map_schema(attribute_base_elem_typoid, attribute_typmod, attribute_name)
        } else if is_array_type(attribute_typoid) {
            let attribute_element_typoid = array_element_typoid(attribute_typoid);
            visit_list_schema(attribute_element_typoid, attribute_typmod, attribute_name)
        } else {
            visit_primitive_schema(attribute_typoid, attribute_typmod, attribute_name)
        };

        struct_attribute_fields.push(field);
    }

    Schema::new(Fields::from(struct_attribute_fields))
}

fn create_list_field_from_primitive_field(
    array_name: &str,
    typoid: Oid,
    typmod: i32,
) -> Arc<Field> {
    let field = visit_primitive_schema(typoid, typmod, array_name);
    let list_field = Field::new(array_name, arrow::datatypes::DataType::List(field), true);

    list_field.into()
}

fn create_list_field_from_map_field(array_name: &str, map_field: Arc<Field>) -> Arc<Field> {
    let list_field = Field::new(
        array_name,
        arrow::datatypes::DataType::List(map_field),
        true,
    );

    list_field.into()
}

fn create_list_field_from_struct_field(array_name: &str, struct_field: Arc<Field>) -> Arc<Field> {
    let list_field = Field::new(
        array_name,
        arrow::datatypes::DataType::List(struct_field),
        true,
    );

    list_field.into()
}

fn visit_struct_schema(tupledesc: PgTupleDesc, elem_name: &str) -> Arc<Field> {
    pgrx::pg_sys::check_for_interrupts!();

    let mut child_fields: Vec<Arc<Field>> = vec![];

    let include_generated_columns = true;
    let attributes = collect_valid_attributes(&tupledesc, include_generated_columns);

    for attribute in attributes {
        if attribute.is_dropped() {
            continue;
        }

        let attribute_name = attribute.name();
        let attribute_oid = attribute.type_oid().value();
        let attribute_typmod = attribute.type_mod();

        let child_field = if is_composite_type(attribute_oid) {
            let attribute_tupledesc = tuple_desc(attribute_oid, attribute_typmod);
            visit_struct_schema(attribute_tupledesc, attribute_name)
        } else if is_crunchy_map_type(attribute_oid) {
            set_crunchy_map_typoid(attribute_oid);
            let attribute_base_elem_typoid = domain_array_base_elem_typoid(attribute_oid);
            visit_map_schema(attribute_base_elem_typoid, attribute_typmod, attribute_name)
        } else if is_array_type(attribute_oid) {
            let attribute_element_typoid = array_element_typoid(attribute_oid);
            visit_list_schema(attribute_element_typoid, attribute_typmod, attribute_name)
        } else {
            visit_primitive_schema(attribute_oid, attribute_typmod, attribute_name)
        };

        child_fields.push(child_field);
    }

    let field = Field::new(
        elem_name,
        arrow::datatypes::DataType::Struct(Fields::from(child_fields)),
        true,
    );

    field.into()
}

fn visit_list_schema(typoid: Oid, typmod: i32, array_name: &str) -> Arc<Field> {
    pgrx::pg_sys::check_for_interrupts!();

    if is_composite_type(typoid) {
        let tupledesc = tuple_desc(typoid, typmod);
        let struct_field = visit_struct_schema(tupledesc, array_name);
        create_list_field_from_struct_field(array_name, struct_field)
    } else if is_crunchy_map_type(typoid) {
        set_crunchy_map_typoid(typoid);
        let base_elem_typoid = domain_array_base_elem_typoid(typoid);
        let map_field = visit_map_schema(base_elem_typoid, typmod, array_name);
        create_list_field_from_map_field(array_name, map_field)
    } else {
        create_list_field_from_primitive_field(array_name, typoid, typmod)
    }
}

fn visit_map_schema(typoid: Oid, typmod: i32, map_name: &str) -> Arc<Field> {
    let tupledesc = tuple_desc(typoid, typmod);
    let struct_field = visit_struct_schema(tupledesc, map_name);
    let struct_field = to_not_nullable_field(struct_field);

    Field::new(
        map_name,
        arrow::datatypes::DataType::Map(struct_field, false),
        true,
    )
    .into()
}

fn visit_primitive_schema(typoid: Oid, typmod: i32, elem_name: &str) -> Arc<Field> {
    pgrx::pg_sys::check_for_interrupts!();

    match typoid {
        FLOAT4OID => Field::new(elem_name, arrow::datatypes::DataType::Float32, true).into(),
        FLOAT8OID => Field::new(elem_name, arrow::datatypes::DataType::Float64, true).into(),
        BOOLOID => Field::new(elem_name, arrow::datatypes::DataType::Boolean, true).into(),
        INT2OID => Field::new(elem_name, arrow::datatypes::DataType::Int16, true).into(),
        INT4OID => Field::new(elem_name, arrow::datatypes::DataType::Int32, true).into(),
        INT8OID => Field::new(elem_name, arrow::datatypes::DataType::Int64, true).into(),
        NUMERICOID => {
            let precision = extract_precision_from_numeric_typmod(typmod);
            let scale = extract_scale_from_numeric_typmod(typmod);

            if precision > MAX_DECIMAL_PRECISION {
                set_fallback_typoid(typoid);
                Field::new(elem_name, arrow::datatypes::DataType::Utf8, true).into()
            } else {
                Field::new(
                    elem_name,
                    arrow::datatypes::DataType::Decimal128(precision as _, scale as _),
                    true,
                )
                .into()
            }
        }
        DATEOID => Field::new(elem_name, arrow::datatypes::DataType::Date32, true).into(),
        TIMESTAMPOID => Field::new(
            elem_name,
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            true,
        )
        .into(),
        TIMESTAMPTZOID => Field::new(
            elem_name,
            arrow::datatypes::DataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                Some("+00:00".into()),
            ),
            true,
        )
        .into(),
        TIMEOID => Field::new(
            elem_name,
            arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
            true,
        )
        .into(),
        TIMETZOID => Field::new(
            elem_name,
            arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
            true,
        )
        .with_metadata(HashMap::from_iter(vec![(
            "adjusted_to_utc".into(),
            "true".into(),
        )]))
        .into(),
        INTERVALOID => Field::new(
            elem_name,
            arrow::datatypes::DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano),
            true,
        )
        .into(),
        UUIDOID => Field::new(
            elem_name,
            arrow::datatypes::DataType::FixedSizeBinary(16),
            true,
        )
        .with_extension_type(ExtensionType::Uuid)
        .into(),
        JSONOID | JSONBOID => Field::new(elem_name, arrow::datatypes::DataType::Utf8, true)
            .with_extension_type(ExtensionType::Json)
            .into(),
        CHAROID => Field::new(elem_name, arrow::datatypes::DataType::Utf8, true).into(),
        TEXTOID => Field::new(elem_name, arrow::datatypes::DataType::Utf8, true).into(),
        BYTEAOID => Field::new(elem_name, arrow::datatypes::DataType::Binary, true).into(),
        OIDOID => Field::new(elem_name, arrow::datatypes::DataType::UInt32, true).into(),
        _ => {
            if is_postgis_geometry_type(typoid) {
                set_geometry_typoid(typoid);
                Field::new(elem_name, arrow::datatypes::DataType::Binary, true).into()
            } else {
                set_fallback_typoid(typoid);
                Field::new(elem_name, arrow::datatypes::DataType::Utf8, true).into()
            }
        }
    }
}
