use std::sync::Arc;

use arrow::datatypes::{Field, Fields, Schema};
use parquet::{arrow::arrow_to_parquet_schema, schema::types::SchemaDescriptor};
use pg_sys::{
    Oid, BOOLOID, DATEOID, FLOAT4OID, FLOAT8OID, INT2OID, INT4OID, INT8OID, RECORDOID, TEXTOID,
    TIMEOID, TIMESTAMPOID, TIMESTAMPTZOID, TIMETZOID, VARCHAROID,
};
use pgrx::{prelude::*, PgTupleDesc};

use crate::pgrx_utils::{
    array_element_typoid, collect_valid_attributes, is_array_type, is_composite_type, tuple_desc,
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

    let attributes = collect_valid_attributes(&tupledesc);

    for attribute in attributes {
        let attribute_name = attribute.name();
        let attribute_typoid = attribute.type_oid().value();
        let attribute_typmod = attribute.type_mod();

        let is_composite = is_composite_type(attribute_typoid);
        let is_array = is_array_type(attribute_typoid);

        let field = if is_composite {
            let attribute_tupledesc = tuple_desc(attribute_typoid, attribute_typmod);
            visit_struct_schema(attribute_tupledesc, attribute_name.to_string().leak())
        } else if is_array {
            let attribute_element_typoid = array_element_typoid(attribute_typoid);
            visit_list_schema(
                attribute_element_typoid,
                attribute_typmod,
                attribute_name.to_string().leak(),
            )
        } else {
            visit_primitive_schema(attribute_typoid, attribute_name.to_string().leak())
        };

        struct_attribute_fields.push(field);
    }

    Schema::new(Fields::from(struct_attribute_fields))
}

pub(crate) fn parse_parquet_schema_from_tupledesc(tupledesc: PgTupleDesc) -> SchemaDescriptor {
    let arrow_schema = parse_arrow_schema_from_tupledesc(tupledesc);
    arrow_to_parquet_schema(&arrow_schema).unwrap()
}

fn create_list_field_from_primitive_field(array_name: &str, typoid: Oid) -> Arc<Field> {
    let field = match typoid {
        FLOAT4OID => Field::new(array_name, arrow::datatypes::DataType::Float32, true),
        FLOAT8OID => Field::new(array_name, arrow::datatypes::DataType::Float64, true),
        BOOLOID => Field::new(array_name, arrow::datatypes::DataType::Int8, true),
        INT2OID => Field::new(array_name, arrow::datatypes::DataType::Int16, true),
        INT4OID => Field::new(array_name, arrow::datatypes::DataType::Int32, true),
        INT8OID => Field::new(array_name, arrow::datatypes::DataType::Int64, true),
        DATEOID => Field::new(array_name, arrow::datatypes::DataType::Date32, true),
        TIMESTAMPOID => Field::new(
            array_name,
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            true,
        ),
        TIMESTAMPTZOID => Field::new(
            array_name,
            arrow::datatypes::DataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                Some("+00:00".into()),
            ),
            true,
        ),
        TIMEOID => Field::new(
            array_name,
            arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
            true,
        ),
        TIMETZOID => Field::new(
            array_name,
            arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
            true,
        ),
        TEXTOID | VARCHAROID => Field::new(array_name, arrow::datatypes::DataType::Utf8, true),
        _ => {
            panic!("unsupported array type {}", typoid);
        }
    };

    let list_field = Field::new(
        array_name,
        arrow::datatypes::DataType::List(field.into()),
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

fn visit_struct_schema(tupledesc: PgTupleDesc, elem_name: &'static str) -> Arc<Field> {
    pgrx::pg_sys::check_for_interrupts!();

    let mut child_fields: Vec<Arc<Field>> = vec![];

    let attributes = collect_valid_attributes(&tupledesc);

    for attribute in attributes {
        if attribute.is_dropped() {
            continue;
        }

        let attribute_name = attribute.name();
        let attribute_oid = attribute.type_oid().value();
        let attribute_typmod = attribute.type_mod();

        let child_field = if is_composite_type(attribute_oid) {
            let attribute_tupledesc = tuple_desc(attribute_oid, attribute_typmod);
            visit_struct_schema(
                attribute_tupledesc,
                // todo: do not leak
                attribute_name.to_string().leak(),
            )
        } else if is_array_type(attribute_oid) {
            let attribute_element_typoid = array_element_typoid(attribute_oid);
            visit_list_schema(
                attribute_element_typoid,
                attribute_typmod,
                // todo: do not leak
                attribute_name.to_string().leak(),
            )
        } else {
            visit_primitive_schema(
                attribute_oid,
                // todo: do not leak
                attribute_name.to_string().leak(),
            )
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

fn visit_list_schema(typoid: Oid, typmod: i32, array_name: &'static str) -> Arc<Field> {
    pgrx::pg_sys::check_for_interrupts!();

    if is_composite_type(typoid) {
        let tupledesc = tuple_desc(typoid, typmod);
        let struct_field = visit_struct_schema(tupledesc, array_name);
        create_list_field_from_struct_field(array_name, struct_field)
    } else {
        create_list_field_from_primitive_field(array_name, typoid)
    }
}

fn visit_primitive_schema(typoid: Oid, elem_name: &'static str) -> Arc<Field> {
    pgrx::pg_sys::check_for_interrupts!();

    match typoid {
        FLOAT4OID => Field::new(elem_name, arrow::datatypes::DataType::Float32, true).into(),
        FLOAT8OID => Field::new(elem_name, arrow::datatypes::DataType::Float64, true).into(),
        BOOLOID => Field::new(elem_name, arrow::datatypes::DataType::Int8, true).into(),
        INT2OID => Field::new(elem_name, arrow::datatypes::DataType::Int16, true).into(),
        INT4OID => Field::new(elem_name, arrow::datatypes::DataType::Int32, true).into(),
        INT8OID => Field::new(elem_name, arrow::datatypes::DataType::Int64, true).into(),
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
        TEXTOID | VARCHAROID => {
            Field::new(elem_name, arrow::datatypes::DataType::Utf8, true).into()
        }
        _ => {
            panic!("unsupported primitive type {}", typoid)
        }
    }
}
