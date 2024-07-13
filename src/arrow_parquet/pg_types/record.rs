use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, StructArray},
    buffer::{BooleanBuffer, NullBuffer},
    datatypes::{DataType, Field, FieldRef, Fields},
};
use pgrx::{
    heap_tuple::PgHeapTuple,
    pg_sys::{
        InvalidOid, Oid, BOOLARRAYOID, BOOLOID, DATEARRAYOID, DATEOID, FLOAT4ARRAYOID, FLOAT4OID,
        FLOAT8ARRAYOID, FLOAT8OID, INT2ARRAYOID, INT2OID, INT4ARRAYOID, INT4OID, INT8ARRAYOID,
        INT8OID, TEXTARRAYOID, TEXTOID, TIMEARRAYOID, TIMEOID, TIMESTAMPARRAYOID, TIMESTAMPOID,
        TIMESTAMPTZARRAYOID, TIMESTAMPTZOID, TIMETZARRAYOID, TIMETZOID, VARCHARARRAYOID,
        VARCHAROID,
    },
    AllocatedByRust, Date, FromDatum, IntoDatum, Time, TimeWithTimeZone, Timestamp,
    TimestampWithTimeZone,
};

use crate::{
    arrow_parquet::{
        pg_to_arrow::PgTypeToArrowArray,
        utils::{array_offsets, create_arrow_list_array, create_arrow_null_list_array},
    },
    pgrx_utils::{array_element_typoid, is_array_type, is_composite_type},
};

use crate::pgrx_utils::{collect_attributes, tuple_desc};

// PgHeapTuple
impl PgTypeToArrowArray<PgHeapTuple<'_, AllocatedByRust>>
    for Vec<Option<PgHeapTuple<'_, AllocatedByRust>>>
{
    fn as_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let mut struct_attribute_arrays = vec![];
        let mut struct_attribute_fields = vec![];

        let tupledesc = tuple_desc(typoid, typmod);

        let attributes = collect_attributes(&tupledesc);

        for attribute in attributes {
            let attribute_name = attribute.name();
            let attribute_typoid = attribute.type_oid().value();
            let attribute_typmod = attribute.type_mod();

            let (field, array) = collect_attribute_array_from_tuples(
                &self,
                attribute_name,
                attribute_typoid,
                attribute_typmod,
            );

            struct_attribute_fields.push(field);
            struct_attribute_arrays.push(array);
        }

        let struct_field = Arc::new(Field::new(
            name,
            DataType::Struct(Fields::from(struct_attribute_fields.clone())),
            true,
        ));

        // determines which structs in the array are null
        let is_null_buffer =
            BooleanBuffer::collect_bool(self.len(), |idx| self.get(idx).unwrap().is_some());
        let struct_null_buffer = NullBuffer::new(is_null_buffer);

        let struct_array = StructArray::new(
            Fields::from(struct_attribute_fields),
            struct_attribute_arrays,
            Some(struct_null_buffer),
        );
        let struct_array = make_array(struct_array.into());

        (struct_field, struct_array)
    }
}

// PgHeapTuple[]
impl PgTypeToArrowArray<Vec<Option<PgHeapTuple<'_, AllocatedByRust>>>>
    for Vec<Option<Vec<Option<PgHeapTuple<'_, AllocatedByRust>>>>>
{
    fn as_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, all_nulls) = array_offsets(&self);

        let len = self.len();

        let array = self.into_iter().flatten().flatten().collect::<Vec<_>>();
        let (field, primitive_array) = array.as_arrow_array(name, typoid, typmod);

        if all_nulls {
            return create_arrow_null_list_array(name, &field, len);
        }

        create_arrow_list_array(name, field, primitive_array, offsets)
    }
}

fn collect_attribute_array_from_tuples(
    tuples: &[Option<PgHeapTuple<'_, AllocatedByRust>>],
    attribute_name: &str,
    attribute_typoid: Oid,
    attribute_typmod: i32,
) -> (FieldRef, ArrayRef) {
    let attribute_element_typoid = if is_array_type(attribute_typoid) {
        array_element_typoid(attribute_typoid)
    } else {
        InvalidOid
    };

    match attribute_typoid {
        FLOAT4OID => {
            let attribute_array =
                collect_attribute_array_from_tuples_helper::<f32>(tuples, attribute_name);
            attribute_array.as_arrow_array(attribute_name, attribute_typoid, attribute_typmod)
        }
        FLOAT4ARRAYOID => {
            let attribute_array = collect_attribute_array_from_tuples_helper::<Vec<Option<f32>>>(
                tuples,
                attribute_name,
            );
            attribute_array.as_arrow_array(
                attribute_name,
                attribute_element_typoid,
                attribute_typmod,
            )
        }
        FLOAT8OID => {
            let attribute_array =
                collect_attribute_array_from_tuples_helper::<f64>(tuples, attribute_name);
            attribute_array.as_arrow_array(attribute_name, attribute_typoid, attribute_typmod)
        }
        FLOAT8ARRAYOID => {
            let attribute_array = collect_attribute_array_from_tuples_helper::<Vec<Option<f64>>>(
                tuples,
                attribute_name,
            );
            attribute_array.as_arrow_array(
                attribute_name,
                attribute_element_typoid,
                attribute_typmod,
            )
        }
        INT2OID => {
            let attribute_array =
                collect_attribute_array_from_tuples_helper::<i16>(tuples, attribute_name);
            attribute_array.as_arrow_array(attribute_name, attribute_typoid, attribute_typmod)
        }
        INT2ARRAYOID => {
            let attribute_array = collect_attribute_array_from_tuples_helper::<Vec<Option<i16>>>(
                tuples,
                attribute_name,
            );
            attribute_array.as_arrow_array(
                attribute_name,
                attribute_element_typoid,
                attribute_typmod,
            )
        }
        INT4OID => {
            let attribute_array =
                collect_attribute_array_from_tuples_helper::<i32>(tuples, attribute_name);
            attribute_array.as_arrow_array(attribute_name, attribute_typoid, attribute_typmod)
        }
        INT4ARRAYOID => {
            let attribute_array = collect_attribute_array_from_tuples_helper::<Vec<Option<i32>>>(
                tuples,
                attribute_name,
            );
            attribute_array.as_arrow_array(
                attribute_name,
                attribute_element_typoid,
                attribute_typmod,
            )
        }
        INT8OID => {
            let attribute_array =
                collect_attribute_array_from_tuples_helper::<i64>(tuples, attribute_name);
            attribute_array.as_arrow_array(attribute_name, attribute_typoid, attribute_typmod)
        }
        INT8ARRAYOID => {
            let attribute_array = collect_attribute_array_from_tuples_helper::<Vec<Option<i64>>>(
                tuples,
                attribute_name,
            );
            attribute_array.as_arrow_array(
                attribute_name,
                attribute_element_typoid,
                attribute_typmod,
            )
        }
        BOOLOID => {
            let attribute_array =
                collect_attribute_array_from_tuples_helper::<bool>(tuples, attribute_name);
            attribute_array.as_arrow_array(attribute_name, attribute_typoid, attribute_typmod)
        }
        BOOLARRAYOID => {
            let attribute_array = collect_attribute_array_from_tuples_helper::<Vec<Option<bool>>>(
                tuples,
                attribute_name,
            );
            attribute_array.as_arrow_array(
                attribute_name,
                attribute_element_typoid,
                attribute_typmod,
            )
        }
        DATEOID => {
            let attribute_array =
                collect_attribute_array_from_tuples_helper::<Date>(tuples, attribute_name);
            attribute_array.as_arrow_array(attribute_name, attribute_typoid, attribute_typmod)
        }
        DATEARRAYOID => {
            let attribute_array = collect_attribute_array_from_tuples_helper::<Vec<Option<Date>>>(
                tuples,
                attribute_name,
            );
            attribute_array.as_arrow_array(
                attribute_name,
                attribute_element_typoid,
                attribute_typmod,
            )
        }
        TIMEOID => {
            let attribute_array =
                collect_attribute_array_from_tuples_helper::<Time>(tuples, attribute_name);
            attribute_array.as_arrow_array(attribute_name, attribute_typoid, attribute_typmod)
        }
        TIMEARRAYOID => {
            let attribute_array = collect_attribute_array_from_tuples_helper::<Vec<Option<Time>>>(
                tuples,
                attribute_name,
            );
            attribute_array.as_arrow_array(
                attribute_name,
                attribute_element_typoid,
                attribute_typmod,
            )
        }
        TIMETZOID => {
            let attribute_array = collect_attribute_array_from_tuples_helper::<TimeWithTimeZone>(
                tuples,
                attribute_name,
            );
            attribute_array.as_arrow_array(attribute_name, attribute_typoid, attribute_typmod)
        }
        TIMETZARRAYOID => {
            let attribute_array = collect_attribute_array_from_tuples_helper::<
                Vec<Option<TimeWithTimeZone>>,
            >(tuples, attribute_name);
            attribute_array.as_arrow_array(
                attribute_name,
                attribute_element_typoid,
                attribute_typmod,
            )
        }
        TIMESTAMPOID => {
            let attribute_array =
                collect_attribute_array_from_tuples_helper::<Timestamp>(tuples, attribute_name);
            attribute_array.as_arrow_array(attribute_name, attribute_typoid, attribute_typmod)
        }
        TIMESTAMPARRAYOID => {
            let attribute_array = collect_attribute_array_from_tuples_helper::<
                Vec<Option<Timestamp>>,
            >(tuples, attribute_name);
            attribute_array.as_arrow_array(
                attribute_name,
                attribute_element_typoid,
                attribute_typmod,
            )
        }
        TIMESTAMPTZOID => {
            let attribute_array = collect_attribute_array_from_tuples_helper::<TimestampWithTimeZone>(
                tuples,
                attribute_name,
            );
            attribute_array.as_arrow_array(attribute_name, attribute_typoid, attribute_typmod)
        }
        TIMESTAMPTZARRAYOID => {
            let attribute_array = collect_attribute_array_from_tuples_helper::<
                Vec<Option<TimestampWithTimeZone>>,
            >(tuples, attribute_name);
            attribute_array.as_arrow_array(
                attribute_name,
                attribute_element_typoid,
                attribute_typmod,
            )
        }
        TEXTOID | VARCHAROID => {
            let attribute_array =
                collect_attribute_array_from_tuples_helper::<String>(tuples, attribute_name);
            attribute_array.as_arrow_array(attribute_name, attribute_typoid, attribute_typmod)
        }
        TEXTARRAYOID | VARCHARARRAYOID => {
            let attribute_array = collect_attribute_array_from_tuples_helper::<Vec<Option<String>>>(
                tuples,
                attribute_name,
            );
            attribute_array.as_arrow_array(
                attribute_name,
                attribute_element_typoid,
                attribute_typmod,
            )
        }
        _ => {
            if is_composite_type(attribute_typoid) {
                let attribute_array = collect_attribute_array_from_tuples_helper::<
                    PgHeapTuple<AllocatedByRust>,
                >(tuples, attribute_name);

                attribute_array.as_arrow_array(attribute_name, attribute_typoid, attribute_typmod)
            } else if is_composite_type(attribute_element_typoid) {
                let attribute_array = collect_attribute_array_from_tuples_helper::<
                    Vec<Option<PgHeapTuple<AllocatedByRust>>>,
                >(tuples, attribute_name);

                attribute_array.as_arrow_array(
                    attribute_name,
                    attribute_element_typoid,
                    attribute_typmod,
                )
            } else {
                panic!("unsupported type {}", attribute_typoid);
            }
        }
    }
}

fn collect_attribute_array_from_tuples_helper<T>(
    tuples: &[Option<PgHeapTuple<'_, AllocatedByRust>>],
    attribute_name: &str,
) -> impl PgTypeToArrowArray<T>
where
    T: IntoDatum + FromDatum + 'static,
    Vec<Option<T>>: PgTypeToArrowArray<T>,
{
    let mut attribute_values = vec![];

    for record in tuples {
        if let Some(record) = record {
            let attribute_val: Option<T> = record.get_by_name(attribute_name).unwrap();
            attribute_values.push(attribute_val);
        } else {
            attribute_values.push(None);
        }
    }

    attribute_values
}
