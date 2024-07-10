use std::sync::Arc;

use arrow::datatypes::{Field, Fields, Schema};
use parquet::{arrow::arrow_to_parquet_schema, schema::types::SchemaDescriptor};
use pg_sys::{
    Oid, BOOLARRAYOID, BOOLOID, BPCHARARRAYOID, BPCHAROID, CHARARRAYOID, CHAROID, DATEARRAYOID,
    DATEOID, FLOAT4ARRAYOID, FLOAT4OID, FLOAT8ARRAYOID, FLOAT8OID, INT2ARRAYOID, INT2OID,
    INT4ARRAYOID, INT4OID, INT8ARRAYOID, INT8OID, TEXTARRAYOID, TEXTOID, TIMEARRAYOID, TIMEOID,
    TIMESTAMPARRAYOID, TIMESTAMPOID, TIMESTAMPTZARRAYOID, TIMESTAMPTZOID, TIMETZARRAYOID,
    TIMETZOID, VARCHARARRAYOID, VARCHAROID,
};
use pgrx::{prelude::*, PgTupleDesc};

use crate::pgrx_utils::{collect_attributes, tupledesc_for_tuples, tupledesc_for_typeoid};

pub(crate) fn schema_string_for_tuples(tuples: Vec<PgHeapTuple<'_, AllocatedByRust>>) -> String {
    let array_oid = tuples.composite_type_oid().unwrap();

    let (_, tupledesc) = tupledesc_for_tuples(tuples);

    let arrow_schema = parse_schema(array_oid, tupledesc, "root");
    let parquet_schema = to_parquet_schema(&arrow_schema);

    let mut buf = Vec::new();
    parquet::schema::printer::print_schema(&mut buf, &parquet_schema.root_schema_ptr());
    String::from_utf8(buf).unwrap()
}

pub(crate) fn parse_schema(
    arraytypoid: Oid,
    tupledesc: PgTupleDesc,
    array_name: &'static str,
) -> Schema {
    let list_field = visit_list_schema(arraytypoid, Some(tupledesc), array_name);
    Schema::new(vec![list_field])
}

pub(crate) fn to_parquet_schema(arrow_schema: &Schema) -> SchemaDescriptor {
    arrow_to_parquet_schema(arrow_schema).unwrap()
}

fn list_field_from_primitive_field(array_name: &str, arraytypoid: Oid) -> Arc<Field> {
    let field = match arraytypoid {
        FLOAT4ARRAYOID => Field::new(array_name, arrow::datatypes::DataType::Float32, false),
        FLOAT8ARRAYOID => Field::new(array_name, arrow::datatypes::DataType::Float64, false),
        BOOLARRAYOID => Field::new(array_name, arrow::datatypes::DataType::Int8, false),
        INT2ARRAYOID => Field::new(array_name, arrow::datatypes::DataType::Int16, false),
        INT4ARRAYOID => Field::new(array_name, arrow::datatypes::DataType::Int32, false),
        INT8ARRAYOID => Field::new(array_name, arrow::datatypes::DataType::Int64, false),
        DATEARRAYOID => Field::new(array_name, arrow::datatypes::DataType::Date32, false),
        TIMESTAMPARRAYOID => Field::new(
            array_name,
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            false,
        ),
        TIMESTAMPTZARRAYOID => Field::new(
            array_name,
            arrow::datatypes::DataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                Some("+00:00".into()),
            ),
            false,
        ),
        TIMEARRAYOID => Field::new(
            array_name,
            arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
            false,
        ),
        TIMETZARRAYOID => Field::new(
            array_name,
            arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
            false,
        ),

        CHARARRAYOID => Field::new(
            array_name,
            arrow::datatypes::DataType::FixedSizeBinary(1),
            false,
        ),
        TEXTARRAYOID | VARCHARARRAYOID | BPCHARARRAYOID => {
            Field::new(array_name, arrow::datatypes::DataType::Utf8, false)
        }
        _ => {
            panic!("unsupported array type {}", arraytypoid);
        }
    };

    let list_field = Field::new(
        array_name,
        arrow::datatypes::DataType::List(field.into()),
        false,
    );

    list_field.into()
}

fn list_field_from_struct_field(array_name: &str, struct_field: Arc<Field>) -> Arc<Field> {
    let list_field = Field::new(
        array_name,
        arrow::datatypes::DataType::List(struct_field),
        false,
    );

    list_field.into()
}

fn visit_struct_schema<'a>(
    attributes: Vec<&'a pg_sys::FormData_pg_attribute>,
    elem_name: &'static str,
) -> Arc<Field> {
    let mut child_fields: Vec<Arc<Field>> = vec![];

    for attribute in attributes {
        if attribute.is_dropped() {
            continue;
        }

        let attribute_name = attribute.name();
        let attribute_oid = attribute.type_oid().value();

        let is_attribute_composite = unsafe { pg_sys::type_is_rowtype(attribute_oid) };
        let is_attribute_array = unsafe { pg_sys::type_is_array(attribute_oid) };

        let child_field = if is_attribute_composite {
            let attribute_tupledesc = tupledesc_for_typeoid(attribute_oid).unwrap();
            let attribute_attributes = collect_attributes(&attribute_tupledesc);
            visit_struct_schema(
                attribute_attributes,
                // todo: do not leak
                attribute_name.to_string().leak(),
            )
        } else if is_attribute_array {
            let attribute_element_typoid = unsafe { pg_sys::get_element_type(attribute_oid) };
            let attribute_tupledesc = tupledesc_for_typeoid(attribute_element_typoid);
            visit_list_schema(
                attribute.type_oid().value(),
                attribute_tupledesc,
                // todo: do not leak
                attribute_name.to_string().leak(),
            )
        } else {
            visit_primitive_schema(
                attribute.type_oid().value(),
                // todo: do not leak
                attribute_name.to_string().leak(),
            )
        };

        child_fields.push(child_field);
    }

    Field::new(
        elem_name,
        arrow::datatypes::DataType::Struct(Fields::from(child_fields)),
        false,
    )
    .into()
}

fn visit_list_schema(
    arraytypoid: Oid,
    tupledesc: Option<PgTupleDesc>,
    array_name: &'static str,
) -> Arc<Field> {
    let is_array_of_composite = tupledesc.is_some();
    if is_array_of_composite {
        let tupledesc = tupledesc.unwrap();
        let array_element_attributes = collect_attributes(&tupledesc);
        let struct_field = visit_struct_schema(array_element_attributes, array_name);
        return list_field_from_struct_field(array_name, struct_field);
    } else {
        list_field_from_primitive_field(array_name, arraytypoid)
    }
}

fn visit_primitive_schema(typoid: Oid, elem_name: &'static str) -> Arc<Field> {
    match typoid {
        FLOAT4OID => Field::new(elem_name, arrow::datatypes::DataType::Float32, false).into(),
        FLOAT8OID => Field::new(elem_name, arrow::datatypes::DataType::Float64, false).into(),
        BOOLOID => Field::new(elem_name, arrow::datatypes::DataType::Int8, false).into(),
        INT2OID => Field::new(elem_name, arrow::datatypes::DataType::Int16, false).into(),
        INT4OID => Field::new(elem_name, arrow::datatypes::DataType::Int32, false).into(),
        INT8OID => Field::new(elem_name, arrow::datatypes::DataType::Int64, false).into(),
        DATEOID => Field::new(elem_name, arrow::datatypes::DataType::Date32, false).into(),
        TIMESTAMPOID => Field::new(
            elem_name,
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            false,
        )
        .into(),
        TIMESTAMPTZOID => Field::new(
            elem_name,
            arrow::datatypes::DataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                Some("+00:00".into()),
            ),
            false,
        )
        .into(),
        TIMEOID => Field::new(
            elem_name,
            arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
            false,
        )
        .into(),
        TIMETZOID => Field::new(
            elem_name,
            arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
            false,
        )
        .into(),
        CHAROID => Field::new(
            elem_name,
            arrow::datatypes::DataType::FixedSizeBinary(1),
            false,
        )
        .into(),
        TEXTOID | VARCHAROID | BPCHAROID => {
            Field::new(elem_name, arrow::datatypes::DataType::Utf8, false).into()
        }
        _ => {
            panic!("unsupported primitive type {}", typoid)
        }
    }
}
