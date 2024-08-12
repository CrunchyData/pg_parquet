use std::sync::Arc;

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
        array_element_typoid, collect_valid_attributes, is_array_type, is_composite_type,
        tuple_desc,
    },
    type_compat::{set_fallback_typoid, DECIMAL_PRECISION, DECIMAL_SCALE},
};

pub(crate) fn parquet_schema_string_from_tupledesc(tupledesc: PgTupleDesc) -> String {
    let arrow_schema = parse_arrow_schema_from_tupledesc(tupledesc);
    let parquet_schema = arrow_to_parquet_schema(&arrow_schema).unwrap();

    let mut buf = Vec::new();
    parquet::schema::printer::print_schema(&mut buf, &parquet_schema.root_schema_ptr());
    String::from_utf8(buf).unwrap()
}

pub(crate) fn parse_arrow_schema_from_tupledesc(tupledesc: PgTupleDesc) -> Schema {
    assert!(tupledesc.oid() == RECORDOID);

    let mut struct_attribute_fields = vec![];

    let include_generated_columns = true;
    let attributes = collect_valid_attributes(&tupledesc, include_generated_columns);

    for attribute in attributes {
        let attribute_name = attribute.name();
        let attribute_typoid = attribute.type_oid().value();
        let attribute_typmod = attribute.type_mod();

        let is_composite = is_composite_type(attribute_typoid);
        let is_array = is_array_type(attribute_typoid);

        let field = if is_composite {
            let attribute_tupledesc = tuple_desc(attribute_typoid, attribute_typmod);
            visit_struct_schema(attribute_tupledesc, attribute_name)
        } else if is_array {
            let attribute_element_typoid = array_element_typoid(attribute_typoid);
            visit_list_schema(attribute_element_typoid, attribute_typmod, attribute_name)
        } else {
            visit_primitive_schema(attribute_typoid, attribute_name)
        };

        struct_attribute_fields.push(field);
    }

    Schema::new(Fields::from(struct_attribute_fields))
}

fn create_list_field_from_primitive_field(array_name: &str, typoid: Oid) -> Arc<Field> {
    let field = visit_primitive_schema(typoid, array_name);
    let list_field = Field::new(array_name, arrow::datatypes::DataType::List(field), true);

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
        } else if is_array_type(attribute_oid) {
            let attribute_element_typoid = array_element_typoid(attribute_oid);
            visit_list_schema(attribute_element_typoid, attribute_typmod, attribute_name)
        } else {
            visit_primitive_schema(attribute_oid, attribute_name)
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
    } else {
        create_list_field_from_primitive_field(array_name, typoid)
    }
}

fn visit_primitive_schema(typoid: Oid, elem_name: &str) -> Arc<Field> {
    pgrx::pg_sys::check_for_interrupts!();

    match typoid {
        FLOAT4OID => Field::new(elem_name, arrow::datatypes::DataType::Float32, true).into(),
        FLOAT8OID => Field::new(elem_name, arrow::datatypes::DataType::Float64, true).into(),
        BOOLOID => Field::new(elem_name, arrow::datatypes::DataType::Boolean, true).into(),
        INT2OID => Field::new(elem_name, arrow::datatypes::DataType::Int16, true).into(),
        INT4OID => Field::new(elem_name, arrow::datatypes::DataType::Int32, true).into(),
        INT8OID => Field::new(elem_name, arrow::datatypes::DataType::Int64, true).into(),
        NUMERICOID => Field::new(
            elem_name,
            arrow::datatypes::DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
            true,
        )
        .into(),
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
            set_fallback_typoid(typoid);
            Field::new(elem_name, arrow::datatypes::DataType::Utf8, true).into()
        }
    }
}
