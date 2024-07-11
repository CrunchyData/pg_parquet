use std::sync::Arc;

use arrow::{
    array::{make_array, ArrayRef, Date32Array, ListArray, StructArray},
    buffer::{OffsetBuffer, ScalarBuffer},
    datatypes::{Field, FieldRef, Fields},
};
use pgrx::{
    heap_tuple::PgHeapTuple,
    pg_sys::{
        self, BOOLARRAYOID, BOOLOID, BPCHARARRAYOID, BPCHAROID, CHARARRAYOID, CHAROID,
        DATEARRAYOID, DATEOID, FLOAT4ARRAYOID, FLOAT4OID, FLOAT8ARRAYOID, FLOAT8OID, INT2ARRAYOID,
        INT2OID, INT4ARRAYOID, INT4OID, INT8ARRAYOID, INT8OID, TEXTARRAYOID, TEXTOID, TIMEARRAYOID,
        TIMEOID, TIMESTAMPARRAYOID, TIMESTAMPOID, TIMESTAMPTZARRAYOID, TIMESTAMPTZOID,
        TIMETZARRAYOID, TIMETZOID, VARCHARARRAYOID, VARCHAROID,
    },
    AllocatedByRust, Date, FromDatum, IntoDatum, Time, TimeWithTimeZone, Timestamp,
    TimestampWithTimeZone,
};

use crate::{
    conversion::{date_to_i32, time_to_i64, timestamp_to_i64, timestamptz_to_i64, timetz_to_i64},
    pgrx_utils::{collect_attributes, tupledesc_for_tuples},
};

pub(crate) trait PgTypeToArrowArray<T: IntoDatum + FromDatum> {
    fn as_arrow_array(self, name: &str) -> (FieldRef, ArrayRef);
}

// Date
impl PgTypeToArrowArray<Date> for Vec<Date> {
    fn as_arrow_array(self, name: &str) -> (FieldRef, ArrayRef) {
        let date_array = self
            .into_iter()
            .map(|date| date_to_i32(date))
            .collect::<Vec<i32>>();

        let field = Field::new(name, arrow::datatypes::DataType::Date32, false);
        let array = Date32Array::from(date_array);
        (Arc::new(field), Arc::new(array))
    }
}

fn array_offsets<T>(arrays: &Vec<Vec<T>>) -> OffsetBuffer<i32> {
    let mut offsets = vec![0];
    let mut current_offset = 0;
    for array in arrays {
        let len = array.len() as i32;
        current_offset += len;
        offsets.push(current_offset);
    }
    OffsetBuffer::new(ScalarBuffer::from(offsets))
}

impl PgTypeToArrowArray<Vec<Date>> for Vec<Vec<Date>> {
    fn as_arrow_array(self, name: &str) -> (FieldRef, ArrayRef) {
        let offsets = array_offsets(&self);
        let array = self
            .into_iter()
            .flatten()
            .map(|date| date_to_i32(date))
            .collect::<Vec<_>>();
        let array = Date32Array::from(array);
        let field = Field::new(name, arrow::datatypes::DataType::Date32, false);
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        let list_array = ListArray::new(field.clone(), offsets, primitive_array, None);
        let list_array = make_array(list_array.into());
        let list_field = Arc::new(Field::new(
            name,
            arrow::datatypes::DataType::List(field),
            false,
        ));
        (list_field, list_array)
    }
}

// Timestamp
impl PgTypeToArrowArray<Timestamp> for Vec<Timestamp> {
    fn as_arrow_array(self, name: &str) -> (FieldRef, ArrayRef) {
        let timestamp_array = self
            .into_iter()
            .map(|timestamp| timestamp_to_i64(timestamp))
            .collect::<Vec<i64>>();

        let field = Field::new(
            name,
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            false,
        );

        let array = arrow::array::TimestampMicrosecondArray::from(timestamp_array);
        (Arc::new(field), Arc::new(array))
    }
}

impl PgTypeToArrowArray<Vec<Timestamp>> for Vec<Vec<Timestamp>> {
    fn as_arrow_array(self, name: &str) -> (FieldRef, ArrayRef) {
        let offsets = array_offsets(&self);
        let array = self
            .into_iter()
            .flatten()
            .map(|timestamp| timestamp_to_i64(timestamp))
            .collect::<Vec<_>>();
        let array = arrow::array::TimestampMicrosecondArray::from(array);
        let field = Field::new(
            name,
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            false,
        );
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        let list_array = ListArray::new(field.clone(), offsets, primitive_array, None);
        let list_array = make_array(list_array.into());
        let list_field = Arc::new(Field::new(
            name,
            arrow::datatypes::DataType::List(field),
            false,
        ));
        (list_field, list_array)
    }
}

// TimestampTz
impl PgTypeToArrowArray<TimestampWithTimeZone> for Vec<TimestampWithTimeZone> {
    fn as_arrow_array(self, name: &str) -> (FieldRef, ArrayRef) {
        let timestamptz_array = self
            .into_iter()
            .map(|timestamptz| timestamptz_to_i64(timestamptz))
            .collect::<Vec<i64>>();

        let field = Field::new(
            name,
            arrow::datatypes::DataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                Some("+00:00".into()),
            ),
            false,
        );

        let array =
            arrow::array::TimestampMicrosecondArray::from(timestamptz_array).with_timezone_utc();
        (Arc::new(field), Arc::new(array))
    }
}

impl PgTypeToArrowArray<Vec<TimestampWithTimeZone>> for Vec<Vec<TimestampWithTimeZone>> {
    fn as_arrow_array(self, name: &str) -> (FieldRef, ArrayRef) {
        let offsets = array_offsets(&self);
        let array = self
            .into_iter()
            .flatten()
            .map(|timestamptz| timestamptz_to_i64(timestamptz))
            .collect::<Vec<_>>();
        let array = arrow::array::TimestampMicrosecondArray::from(array).with_timezone_utc();
        let field = Field::new(
            name,
            arrow::datatypes::DataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                Some("+00:00".into()),
            ),
            false,
        );
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        let list_array = ListArray::new(field.clone(), offsets, primitive_array, None);
        let list_array = make_array(list_array.into());
        let list_field = Arc::new(Field::new(
            name,
            arrow::datatypes::DataType::List(field),
            false,
        ));
        (list_field, list_array)
    }
}

// Time
impl PgTypeToArrowArray<Time> for Vec<Time> {
    fn as_arrow_array(self, name: &str) -> (FieldRef, ArrayRef) {
        let time_array = self
            .into_iter()
            .map(|time| time_to_i64(time))
            .collect::<Vec<i64>>();

        let field = Field::new(
            name,
            arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
            false,
        );

        let array = arrow::array::Time64MicrosecondArray::from(time_array);
        (Arc::new(field), Arc::new(array))
    }
}

impl PgTypeToArrowArray<Vec<Time>> for Vec<Vec<Time>> {
    fn as_arrow_array(self, name: &str) -> (FieldRef, ArrayRef) {
        let offsets = array_offsets(&self);
        let array = self
            .into_iter()
            .flatten()
            .map(|time| time_to_i64(time))
            .collect::<Vec<_>>();
        let array = arrow::array::Time64MicrosecondArray::from(array);
        let field = Field::new(
            name,
            arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
            false,
        );
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        let list_array = ListArray::new(field.clone(), offsets, primitive_array, None);
        let list_array = make_array(list_array.into());
        let list_field = Arc::new(Field::new(
            name,
            arrow::datatypes::DataType::List(field),
            false,
        ));
        (list_field, list_array)
    }
}

// TimeTz
impl PgTypeToArrowArray<TimeWithTimeZone> for Vec<TimeWithTimeZone> {
    fn as_arrow_array(self, name: &str) -> (FieldRef, ArrayRef) {
        let timetz_array = self
            .into_iter()
            .map(|timetz| timetz_to_i64(timetz))
            .collect::<Vec<i64>>();

        let field = Field::new(
            name,
            arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
            false,
        );

        let array = arrow::array::Time64MicrosecondArray::from(timetz_array);
        (Arc::new(field), Arc::new(array))
    }
}

impl PgTypeToArrowArray<Vec<TimeWithTimeZone>> for Vec<Vec<TimeWithTimeZone>> {
    fn as_arrow_array(self, name: &str) -> (FieldRef, ArrayRef) {
        let offsets = array_offsets(&self);
        let array = self
            .into_iter()
            .flatten()
            .map(|timetz| timetz_to_i64(timetz))
            .collect::<Vec<_>>();
        let array = arrow::array::Time64MicrosecondArray::from(array);
        let field = Field::new(
            name,
            arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
            false,
        );
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        let list_array = ListArray::new(field.clone(), offsets, primitive_array, None);
        let list_array = make_array(list_array.into());
        let list_field = Arc::new(Field::new(
            name,
            arrow::datatypes::DataType::List(field),
            false,
        ));
        (list_field, list_array)
    }
}

// Char
impl PgTypeToArrowArray<char> for Vec<char> {
    fn as_arrow_array(self, name: &str) -> (FieldRef, ArrayRef) {
        let array = self.into_iter().map(|c| c as i8).collect::<Vec<_>>();
        let field = Field::new(name, arrow::datatypes::DataType::Int8, false);
        let array = arrow::array::Int8Array::from(array);
        (Arc::new(field), Arc::new(array))
    }
}

impl PgTypeToArrowArray<Vec<char>> for Vec<Vec<char>> {
    fn as_arrow_array(self, name: &str) -> (FieldRef, ArrayRef) {
        let offsets = array_offsets(&self);
        let array = self
            .into_iter()
            .flatten()
            .map(|c| c as i8)
            .collect::<Vec<_>>();
        let array = arrow::array::Int8Array::from(array);
        let field = Field::new(name, arrow::datatypes::DataType::Int8, false);
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        let list_array = ListArray::new(field.clone(), offsets, primitive_array, None);
        let list_array = make_array(list_array.into());
        let list_field = Arc::new(Field::new(
            name,
            arrow::datatypes::DataType::List(field),
            false,
        ));
        (list_field, list_array)
    }
}

// Text, Varchar, Bpchar
impl PgTypeToArrowArray<String> for Vec<String> {
    fn as_arrow_array(self, name: &str) -> (FieldRef, ArrayRef) {
        let field = Field::new(name, arrow::datatypes::DataType::Utf8, false);
        let array = arrow::array::StringArray::from(self);
        (Arc::new(field), Arc::new(array))
    }
}

impl PgTypeToArrowArray<Vec<String>> for Vec<Vec<String>> {
    fn as_arrow_array(self, name: &str) -> (FieldRef, ArrayRef) {
        let offsets = array_offsets(&self);
        let array = self.into_iter().flatten().collect::<Vec<_>>();
        let array = arrow::array::StringArray::from(array);
        let field = Field::new(name, arrow::datatypes::DataType::Utf8, false);
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        let list_array = ListArray::new(field.clone(), offsets, primitive_array, None);
        let list_array = make_array(list_array.into());
        let list_field = Arc::new(Field::new(
            name,
            arrow::datatypes::DataType::List(field),
            false,
        ));
        (list_field, list_array)
    }
}

impl PgTypeToArrowArray<bool> for Vec<bool> {
    fn as_arrow_array(self, name: &str) -> (FieldRef, ArrayRef) {
        let array = self.into_iter().map(|b| b as i8).collect::<Vec<_>>();
        let field = Field::new(name, arrow::datatypes::DataType::Int8, false);
        let array = arrow::array::Int8Array::from(array);
        (Arc::new(field), Arc::new(array))
    }
}

impl PgTypeToArrowArray<Vec<bool>> for Vec<Vec<bool>> {
    fn as_arrow_array(self, name: &str) -> (FieldRef, ArrayRef) {
        let offsets = array_offsets(&self);
        let array = self
            .into_iter()
            .flatten()
            .map(|v| v as i8)
            .collect::<Vec<_>>();
        let array = arrow::array::Int8Array::from(array);
        let field = Field::new(name, arrow::datatypes::DataType::Int8, false);
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        let list_array = ListArray::new(field.clone(), offsets, primitive_array, None);
        let list_array = make_array(list_array.into());
        let list_field = Arc::new(Field::new(
            name,
            arrow::datatypes::DataType::List(field),
            false,
        ));
        (list_field, list_array)
    }
}

// Int16
impl PgTypeToArrowArray<i16> for Vec<i16> {
    fn as_arrow_array(self, name: &str) -> (FieldRef, ArrayRef) {
        let field = Field::new(name, arrow::datatypes::DataType::Int16, false);
        let array = arrow::array::Int16Array::from(self);
        (Arc::new(field), Arc::new(array))
    }
}

impl PgTypeToArrowArray<Vec<i16>> for Vec<Vec<i16>> {
    fn as_arrow_array(self, name: &str) -> (FieldRef, ArrayRef) {
        let offsets = array_offsets(&self);
        let array = self.into_iter().flatten().collect::<Vec<_>>();
        let array = arrow::array::Int16Array::from(array);
        let field = Field::new(name, arrow::datatypes::DataType::Int16, false);
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        let list_array = ListArray::new(field.clone(), offsets, primitive_array, None);
        let list_array = make_array(list_array.into());
        let list_field = Arc::new(Field::new(
            name,
            arrow::datatypes::DataType::List(field),
            false,
        ));
        (list_field, list_array)
    }
}

// Int32
impl PgTypeToArrowArray<i32> for Vec<i32> {
    fn as_arrow_array(self, name: &str) -> (FieldRef, ArrayRef) {
        let field = Field::new(name, arrow::datatypes::DataType::Int32, false);
        let array = arrow::array::Int32Array::from(self);
        (Arc::new(field), Arc::new(array))
    }
}

impl PgTypeToArrowArray<Vec<i32>> for Vec<Vec<i32>> {
    fn as_arrow_array(self, name: &str) -> (FieldRef, ArrayRef) {
        let offsets = array_offsets(&self);
        let array = self.into_iter().flatten().collect::<Vec<_>>();
        let array = arrow::array::Int32Array::from(array);
        let field = Field::new(name, arrow::datatypes::DataType::Int32, false);
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        let list_array = ListArray::new(field.clone(), offsets, primitive_array, None);
        let list_array = make_array(list_array.into());
        let list_field = Arc::new(Field::new(
            name,
            arrow::datatypes::DataType::List(field),
            false,
        ));
        (list_field, list_array)
    }
}

// Int64
impl PgTypeToArrowArray<i64> for Vec<i64> {
    fn as_arrow_array(self, name: &str) -> (FieldRef, ArrayRef) {
        let field = Field::new(name, arrow::datatypes::DataType::Int64, false);
        let array = arrow::array::Int64Array::from(self);
        (Arc::new(field), Arc::new(array))
    }
}

impl PgTypeToArrowArray<Vec<i64>> for Vec<Vec<i64>> {
    fn as_arrow_array(self, name: &str) -> (FieldRef, ArrayRef) {
        let offsets = array_offsets(&self);
        let array = self.into_iter().flatten().collect::<Vec<_>>();
        let array = arrow::array::Int64Array::from(array);
        let field = Field::new(name, arrow::datatypes::DataType::Int64, false);
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        let list_array = ListArray::new(field.clone(), offsets, primitive_array, None);
        let list_array = make_array(list_array.into());
        let list_field = Arc::new(Field::new(
            name,
            arrow::datatypes::DataType::List(field),
            false,
        ));
        (list_field, list_array)
    }
}

// Float32
impl PgTypeToArrowArray<f32> for Vec<f32> {
    fn as_arrow_array(self, name: &str) -> (FieldRef, ArrayRef) {
        let field = Field::new(name, arrow::datatypes::DataType::Float32, false);
        let array = arrow::array::Float32Array::from(self);
        (Arc::new(field), Arc::new(array))
    }
}

impl PgTypeToArrowArray<Vec<f32>> for Vec<Vec<f32>> {
    fn as_arrow_array(self, name: &str) -> (FieldRef, ArrayRef) {
        let offsets = array_offsets(&self);
        let array = self.into_iter().flatten().collect::<Vec<_>>();
        let array = arrow::array::Float32Array::from(array);
        let field = Field::new(name, arrow::datatypes::DataType::Float32, false);
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        let list_array = ListArray::new(field.clone(), offsets, primitive_array, None);
        let list_array = make_array(list_array.into());
        let list_field = Arc::new(Field::new(
            name,
            arrow::datatypes::DataType::List(field),
            false,
        ));
        (list_field, list_array)
    }
}

// Float64
impl PgTypeToArrowArray<f64> for Vec<f64> {
    fn as_arrow_array(self, name: &str) -> (FieldRef, ArrayRef) {
        let field = Field::new(name, arrow::datatypes::DataType::Float64, false);
        let array = arrow::array::Float64Array::from(self);
        (Arc::new(field), Arc::new(array))
    }
}

impl PgTypeToArrowArray<Vec<f64>> for Vec<Vec<f64>> {
    fn as_arrow_array(self, name: &str) -> (FieldRef, ArrayRef) {
        let offsets = array_offsets(&self);
        let array = self.into_iter().flatten().collect::<Vec<_>>();
        let array = arrow::array::Float64Array::from(array);
        let field = Field::new(name, arrow::datatypes::DataType::Float64, false);
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        let list_array = ListArray::new(field.clone(), offsets, primitive_array, None);
        let list_array = make_array(list_array.into());
        let list_field = Arc::new(Field::new(
            name,
            arrow::datatypes::DataType::List(field),
            false,
        ));
        (list_field, list_array)
    }
}

// PgHeapTuple
impl PgTypeToArrowArray<PgHeapTuple<'_, AllocatedByRust>>
    for Vec<PgHeapTuple<'_, AllocatedByRust>>
{
    fn as_arrow_array(self, name: &str) -> (FieldRef, ArrayRef) {
        let mut struct_fields_with_data: Vec<(Arc<Field>, ArrayRef)> = vec![];

        let (tuples, tupledesc) = tupledesc_for_tuples(self);

        let attributes = collect_attributes(&tupledesc);

        for attribute in attributes {
            let attribute_name = attribute.name();
            let attribute_typoid = attribute.type_oid().value();

            let attribute_array_with_field = match attribute_typoid {
                FLOAT4OID => {
                    let attribute_array =
                        collect_attribute_array_from_tuples::<f32>(&tuples, attribute_name);
                    attribute_array.as_arrow_array(attribute_name)
                }
                FLOAT4ARRAYOID => {
                    let attribute_array =
                        collect_attribute_array_from_tuples::<Vec<f32>>(&tuples, attribute_name);
                    attribute_array.as_arrow_array(attribute_name)
                }
                FLOAT8OID => {
                    let attribute_array =
                        collect_attribute_array_from_tuples::<f64>(&tuples, attribute_name);
                    attribute_array.as_arrow_array(attribute_name)
                }
                FLOAT8ARRAYOID => {
                    let attribute_array =
                        collect_attribute_array_from_tuples::<Vec<f64>>(&tuples, attribute_name);
                    attribute_array.as_arrow_array(attribute_name)
                }
                INT2OID => {
                    let attribute_array =
                        collect_attribute_array_from_tuples::<i16>(&tuples, attribute_name);
                    attribute_array.as_arrow_array(attribute_name)
                }
                INT2ARRAYOID => {
                    let attribute_array =
                        collect_attribute_array_from_tuples::<Vec<i16>>(&tuples, attribute_name);
                    attribute_array.as_arrow_array(attribute_name)
                }
                INT4OID => {
                    let attribute_array =
                        collect_attribute_array_from_tuples::<i32>(&tuples, attribute_name);
                    attribute_array.as_arrow_array(attribute_name)
                }
                INT4ARRAYOID => {
                    let attribute_array =
                        collect_attribute_array_from_tuples::<Vec<i32>>(&tuples, attribute_name);
                    attribute_array.as_arrow_array(attribute_name)
                }
                INT8OID => {
                    let attribute_array =
                        collect_attribute_array_from_tuples::<i64>(&tuples, attribute_name);
                    attribute_array.as_arrow_array(attribute_name)
                }
                INT8ARRAYOID => {
                    let attribute_array =
                        collect_attribute_array_from_tuples::<Vec<i64>>(&tuples, attribute_name);
                    attribute_array.as_arrow_array(attribute_name)
                }
                BOOLOID => {
                    let attribute_array =
                        collect_attribute_array_from_tuples::<bool>(&tuples, attribute_name);
                    attribute_array.as_arrow_array(attribute_name)
                }
                BOOLARRAYOID => {
                    let attribute_array =
                        collect_attribute_array_from_tuples::<Vec<bool>>(&tuples, attribute_name);
                    attribute_array.as_arrow_array(attribute_name)
                }
                DATEOID => {
                    let attribute_array =
                        collect_attribute_array_from_tuples::<Date>(&tuples, attribute_name);
                    attribute_array.as_arrow_array(attribute_name)
                }
                DATEARRAYOID => {
                    let attribute_array =
                        collect_attribute_array_from_tuples::<Vec<Date>>(&tuples, attribute_name);
                    attribute_array.as_arrow_array(attribute_name)
                }
                TIMEOID => {
                    let attribute_array =
                        collect_attribute_array_from_tuples::<Time>(&tuples, attribute_name);
                    attribute_array.as_arrow_array(attribute_name)
                }
                TIMEARRAYOID => {
                    let attribute_array =
                        collect_attribute_array_from_tuples::<Vec<Time>>(&tuples, attribute_name);
                    attribute_array.as_arrow_array(attribute_name)
                }
                TIMETZOID => {
                    let attribute_array = collect_attribute_array_from_tuples::<TimeWithTimeZone>(
                        &tuples,
                        attribute_name,
                    );
                    attribute_array.as_arrow_array(attribute_name)
                }
                TIMETZARRAYOID => {
                    let attribute_array = collect_attribute_array_from_tuples::<
                        Vec<TimeWithTimeZone>,
                    >(&tuples, attribute_name);
                    attribute_array.as_arrow_array(attribute_name)
                }
                TIMESTAMPOID => {
                    let attribute_array =
                        collect_attribute_array_from_tuples::<Timestamp>(&tuples, attribute_name);
                    attribute_array.as_arrow_array(attribute_name)
                }
                TIMESTAMPARRAYOID => {
                    let attribute_array = collect_attribute_array_from_tuples::<Vec<Timestamp>>(
                        &tuples,
                        attribute_name,
                    );
                    attribute_array.as_arrow_array(attribute_name)
                }
                TIMESTAMPTZOID => {
                    let attribute_array = collect_attribute_array_from_tuples::<
                        TimestampWithTimeZone,
                    >(&tuples, attribute_name);
                    attribute_array.as_arrow_array(attribute_name)
                }
                TIMESTAMPTZARRAYOID => {
                    let attribute_array = collect_attribute_array_from_tuples::<
                        Vec<TimestampWithTimeZone>,
                    >(&tuples, attribute_name);
                    attribute_array.as_arrow_array(attribute_name)
                }
                CHAROID => {
                    let attribute_array =
                        collect_attribute_array_from_tuples::<char>(&tuples, attribute_name);
                    attribute_array.as_arrow_array(attribute_name)
                }
                CHARARRAYOID => {
                    let attribute_array =
                        collect_attribute_array_from_tuples::<Vec<char>>(&tuples, attribute_name);
                    attribute_array.as_arrow_array(attribute_name)
                }
                TEXTOID | VARCHAROID | BPCHAROID => {
                    let attribute_array =
                        collect_attribute_array_from_tuples::<String>(&tuples, attribute_name);
                    attribute_array.as_arrow_array(attribute_name)
                }
                TEXTARRAYOID | VARCHARARRAYOID | BPCHARARRAYOID => {
                    let attribute_array =
                        collect_attribute_array_from_tuples::<Vec<String>>(&tuples, attribute_name);
                    attribute_array.as_arrow_array(attribute_name)
                }
                _ => {
                    let attribute_is_composite = unsafe { pg_sys::type_is_rowtype(attribute_typoid) };
                    if attribute_is_composite {
                        let attribute_array = collect_attribute_array_from_tuples::<
                            PgHeapTuple::<AllocatedByRust>
                        >(&tuples, attribute_name);
                        return attribute_array.as_arrow_array(attribute_name);
                    } 

                    let attribute_element_typoid =
                        unsafe { pg_sys::get_element_type(attribute_typoid) };
                    let is_attribute_composite_array =
                        unsafe { pg_sys::type_is_rowtype(attribute_element_typoid) };
                    if is_attribute_composite_array {
                        let attribute_array = collect_attribute_array_from_tuples::<
                            Vec<PgHeapTuple<AllocatedByRust>>,
                        >(&tuples, attribute_name);
                        attribute_array.as_arrow_array(attribute_name)
                    } else {
                        panic!("Unsupported type");
                    }
                }
            };

            struct_fields_with_data.push(attribute_array_with_field);
        }

        // finalize StructArray
        let mut struct_attribute_fields = vec![];
        for (field, _) in struct_fields_with_data.iter() {
            struct_attribute_fields.push(field.clone());
        }

        let struct_field = Arc::new(Field::new(
            name,
            arrow::datatypes::DataType::Struct(Fields::from(struct_attribute_fields)),
            false,
        ));

        let struct_array = StructArray::from(struct_fields_with_data);
        let struct_array = make_array(struct_array.into());

        (struct_field, struct_array)
    }
}

impl PgTypeToArrowArray<Vec<PgHeapTuple<'_, AllocatedByRust>>>
    for Vec<Vec<PgHeapTuple<'_, AllocatedByRust>>>
{
    fn as_arrow_array(self, name: &str) -> (FieldRef, ArrayRef) {
        let offsets = array_offsets(&self);
        let array = self.into_iter().flatten().collect::<Vec<_>>();
        let (field, primitive_array) = array.as_arrow_array(name);

        let list_array = ListArray::new(field.clone(), offsets, primitive_array, None);
        let list_array = make_array(list_array.into());
        let list_field = Arc::new(Field::new(
            name,
            arrow::datatypes::DataType::List(field),
            false,
        ));
        (list_field, list_array)
    }
}

pub(crate) fn collect_attribute_array_from_tuples<T>(
    tuples: &[PgHeapTuple<'_, AllocatedByRust>],
    attribute_name: &str,
) -> impl PgTypeToArrowArray<T>
where
    T: IntoDatum + FromDatum + 'static,
    Vec<T>: PgTypeToArrowArray<T>,
{
    let mut attribute_values = vec![];

    for record in tuples {
        let attribute_val: T = record.get_by_name(attribute_name).unwrap().unwrap();
        attribute_values.push(attribute_val);
    }

    attribute_values
}
