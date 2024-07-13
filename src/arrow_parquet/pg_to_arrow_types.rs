use std::sync::Arc;

use arrow::{
    array::{
        make_array, ArrayRef, Date32Array, Float32Array, Float64Array, Int16Array, Int32Array,
        Int64Array, Int8Array, ListArray, StringArray, StructArray, Time64MicrosecondArray,
        TimestampMicrosecondArray,
    },
    buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer},
    datatypes::{DataType, Field, FieldRef, Fields, TimeUnit},
};
use pgrx::{
    heap_tuple::PgHeapTuple,
    pg_sys::{
        self, InvalidOid, Oid, BOOLARRAYOID, BOOLOID, DATEARRAYOID, DATEOID, FLOAT4ARRAYOID,
        FLOAT4OID, FLOAT8ARRAYOID, FLOAT8OID, INT2ARRAYOID, INT2OID, INT4ARRAYOID, INT4OID,
        INT8ARRAYOID, INT8OID, RECORDARRAYOID, RECORDOID, TEXTARRAYOID, TEXTOID, TIMEARRAYOID,
        TIMEOID, TIMESTAMPARRAYOID, TIMESTAMPOID, TIMESTAMPTZARRAYOID, TIMESTAMPTZOID,
        TIMETZARRAYOID, TIMETZOID, VARCHARARRAYOID, VARCHAROID,
    },
    AllocatedByRust, Date, FromDatum, IntoDatum, Time, TimeWithTimeZone, Timestamp,
    TimestampWithTimeZone,
};

use crate::{
    conversion::{date_to_i32, time_to_i64, timestamp_to_i64, timestamptz_to_i64, timetz_to_i64},
    pgrx_utils::{collect_attributes, tuple_desc},
};

fn create_arrow_null_list_array(name: &str, field: &Field, len: usize) -> (FieldRef, ArrayRef) {
    let list_array = ListArray::new_null(field.clone().into(), len);
    let list_array = make_array(list_array.into());
    let list_field = Arc::new(Field::new(name, DataType::List(field.clone().into()), true));
    (list_field, list_array)
}

pub(crate) trait PgTypeToArrowArray<T: IntoDatum + FromDatum> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef);
}

// Date
impl PgTypeToArrowArray<Date> for Vec<Option<Date>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let date_array = self
            .into_iter()
            .map(|date| date.and_then(date_to_i32))
            .collect::<Vec<_>>();

        let field = Field::new(name, DataType::Date32, true);
        let array = Date32Array::from(date_array);
        (Arc::new(field), Arc::new(array))
    }
}

impl PgTypeToArrowArray<Vec<Option<Date>>> for Vec<Option<Vec<Option<Date>>>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, all_nulls) = array_offsets(&self);

        let field = Field::new(name, DataType::Date32, true);

        if all_nulls {
            return create_arrow_null_list_array(name, &field, offsets.len() - 1);
        }

        let array = self
            .into_iter()
            .flatten()
            .flatten()
            .map(|date| date.and_then(date_to_i32))
            .collect::<Vec<_>>();

        let array = Date32Array::from(array);
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        let list_array = ListArray::new(field.clone(), offsets, primitive_array, None);
        let list_array = make_array(list_array.into());
        let list_field = Arc::new(Field::new(name, DataType::List(field), true));
        (list_field, list_array)
    }
}

// Timestamp
impl PgTypeToArrowArray<Timestamp> for Vec<Option<Timestamp>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let timestamp_array = self
            .into_iter()
            .map(|timstamp| timstamp.and_then(timestamp_to_i64))
            .collect::<Vec<_>>();

        let field = Field::new(name, DataType::Timestamp(TimeUnit::Microsecond, None), true);

        let array = TimestampMicrosecondArray::from(timestamp_array);
        (Arc::new(field), Arc::new(array))
    }
}

impl PgTypeToArrowArray<Vec<Option<Timestamp>>> for Vec<Option<Vec<Option<Timestamp>>>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, all_nulls) = array_offsets(&self);

        let field = Field::new(name, DataType::Timestamp(TimeUnit::Microsecond, None), true);

        if all_nulls {
            return create_arrow_null_list_array(name, &field, offsets.len() - 1);
        }

        let array = self
            .into_iter()
            .flatten()
            .flatten()
            .map(|timestamp| timestamp.and_then(timestamp_to_i64))
            .collect::<Vec<_>>();
        let array = TimestampMicrosecondArray::from(array);
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        let list_array = ListArray::new(field.clone(), offsets, primitive_array, None);
        let list_array = make_array(list_array.into());
        let list_field = Arc::new(Field::new(name, DataType::List(field), true));
        (list_field, list_array)
    }
}

// TimestampTz
impl PgTypeToArrowArray<TimestampWithTimeZone> for Vec<Option<TimestampWithTimeZone>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let timestamptz_array = self
            .into_iter()
            .map(|timestamptz| timestamptz.and_then(timestamptz_to_i64))
            .collect::<Vec<_>>();

        let field = Field::new(
            name,
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
            true,
        );

        let array = TimestampMicrosecondArray::from(timestamptz_array).with_timezone_utc();
        (Arc::new(field), Arc::new(array))
    }
}

impl PgTypeToArrowArray<Vec<Option<TimestampWithTimeZone>>>
    for Vec<Option<Vec<Option<TimestampWithTimeZone>>>>
{
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, all_nulls) = array_offsets(&self);

        let field = Field::new(
            name,
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
            true,
        );

        if all_nulls {
            return create_arrow_null_list_array(name, &field, offsets.len() - 1);
        }

        let array = self
            .into_iter()
            .flatten()
            .flatten()
            .map(|timestamptz| timestamptz.and_then(timestamptz_to_i64))
            .collect::<Vec<_>>();
        let array = TimestampMicrosecondArray::from(array).with_timezone_utc();
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        let list_array = ListArray::new(field.clone(), offsets, primitive_array, None);
        let list_array = make_array(list_array.into());
        let list_field = Arc::new(Field::new(name, DataType::List(field), true));
        (list_field, list_array)
    }
}

// Time
impl PgTypeToArrowArray<Time> for Vec<Option<Time>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let time_array = self
            .into_iter()
            .map(|time| time.and_then(time_to_i64))
            .collect::<Vec<_>>();

        let field = Field::new(name, DataType::Time64(TimeUnit::Microsecond), true);

        let array = Time64MicrosecondArray::from(time_array);
        (Arc::new(field), Arc::new(array))
    }
}

impl PgTypeToArrowArray<Vec<Option<Time>>> for Vec<Option<Vec<Option<Time>>>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, all_nulls) = array_offsets(&self);

        let field = Field::new(name, DataType::Time64(TimeUnit::Microsecond), true);

        if all_nulls {
            return create_arrow_null_list_array(name, &field, offsets.len() - 1);
        }

        let array = self
            .into_iter()
            .flatten()
            .flatten()
            .map(|time| time.and_then(time_to_i64))
            .collect::<Vec<_>>();
        let array = Time64MicrosecondArray::from(array);
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        let list_array = ListArray::new(field.clone(), offsets, primitive_array, None);
        let list_array = make_array(list_array.into());
        let list_field = Arc::new(Field::new(name, DataType::List(field), true));
        (list_field, list_array)
    }
}

// TimeTz
impl PgTypeToArrowArray<TimeWithTimeZone> for Vec<Option<TimeWithTimeZone>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let timetz_array = self
            .into_iter()
            .map(|timetz| timetz.and_then(timetz_to_i64))
            .collect::<Vec<_>>();

        let field = Field::new(name, DataType::Time64(TimeUnit::Microsecond), true);

        let array = Time64MicrosecondArray::from(timetz_array);
        (Arc::new(field), Arc::new(array))
    }
}

impl PgTypeToArrowArray<Vec<Option<TimeWithTimeZone>>>
    for Vec<Option<Vec<Option<TimeWithTimeZone>>>>
{
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, all_nulls) = array_offsets(&self);

        let field = Field::new(name, DataType::Time64(TimeUnit::Microsecond), true);

        if all_nulls {
            return create_arrow_null_list_array(name, &field, offsets.len() - 1);
        }

        let array = self
            .into_iter()
            .flatten()
            .flatten()
            .map(|timetz| timetz.and_then(timetz_to_i64))
            .collect::<Vec<_>>();
        let array = Time64MicrosecondArray::from(array);
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        let list_array = ListArray::new(field.clone(), offsets, primitive_array, None);
        let list_array = make_array(list_array.into());
        let list_field = Arc::new(Field::new(name, DataType::List(field), true));
        (list_field, list_array)
    }
}

// Text, Varchar
impl PgTypeToArrowArray<String> for Vec<Option<String>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let field = Field::new(name, DataType::Utf8, true);
        let array = StringArray::from(self);
        (Arc::new(field), Arc::new(array))
    }
}

impl PgTypeToArrowArray<Vec<Option<String>>> for Vec<Option<Vec<Option<String>>>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, all_nulls) = array_offsets(&self);

        let field = Field::new(name, DataType::Utf8, true);

        if all_nulls {
            return create_arrow_null_list_array(name, &field, offsets.len() - 1);
        }

        let array = self.into_iter().flatten().flatten().collect::<Vec<_>>();
        let array = StringArray::from(array);
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        let list_array = ListArray::new(field.clone(), offsets, primitive_array, None);
        let list_array = make_array(list_array.into());
        let list_field = Arc::new(Field::new(name, DataType::List(field), true));
        (list_field, list_array)
    }
}

impl PgTypeToArrowArray<bool> for Vec<Option<bool>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let array = self
            .into_iter()
            .map(|v| v.and_then(|v| Some(v as i8)))
            .collect::<Vec<_>>();
        let field = Field::new(name, DataType::Int8, true);
        let array = Int8Array::from(array);
        (Arc::new(field), Arc::new(array))
    }
}

impl PgTypeToArrowArray<Vec<Option<bool>>> for Vec<Option<Vec<Option<bool>>>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, all_nulls) = array_offsets(&self);

        let field = Field::new(name, DataType::Int8, true);

        if all_nulls {
            return create_arrow_null_list_array(name, &field, offsets.len() - 1);
        }

        let array = self
            .into_iter()
            .flatten()
            .flatten()
            .map(|v| v.and_then(|v| Some(v as i8)))
            .collect::<Vec<_>>();
        let array = Int8Array::from(array);
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        let list_array = ListArray::new(field.clone(), offsets, primitive_array, None);
        let list_array = make_array(list_array.into());
        let list_field = Arc::new(Field::new(name, DataType::List(field), true));
        (list_field, list_array)
    }
}

// Int16
impl PgTypeToArrowArray<i16> for Vec<Option<i16>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let field = Field::new(name, DataType::Int16, true);
        let array = Int16Array::from(self);
        (Arc::new(field), Arc::new(array))
    }
}

impl PgTypeToArrowArray<Vec<Option<i16>>> for Vec<Option<Vec<Option<i16>>>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, all_nulls) = array_offsets(&self);

        let field = Field::new(name, DataType::Int16, true);

        if all_nulls {
            return create_arrow_null_list_array(name, &field, offsets.len() - 1);
        }

        let array = self.into_iter().flatten().flatten().collect::<Vec<_>>();
        let array = Int16Array::from(array);
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        let list_array = ListArray::new(field.clone(), offsets, primitive_array, None);
        let list_array = make_array(list_array.into());
        let list_field = Arc::new(Field::new(name, DataType::List(field), true));
        (list_field, list_array)
    }
}

// Int32
impl PgTypeToArrowArray<i32> for Vec<Option<i32>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let field = Field::new(name, DataType::Int32, true);
        let array = Int32Array::from(self);
        (Arc::new(field), Arc::new(array))
    }
}

impl PgTypeToArrowArray<Vec<Option<i32>>> for Vec<Option<Vec<Option<i32>>>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, all_nulls) = array_offsets(&self);

        let field = Field::new(name, DataType::Int32, true);

        if all_nulls {
            return create_arrow_null_list_array(name, &field, offsets.len() - 1);
        }

        let array = self.into_iter().flatten().flatten().collect::<Vec<_>>();
        let array = Int32Array::from(array);
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        let list_array = ListArray::new(field.clone(), offsets, primitive_array, None);
        let list_array = make_array(list_array.into());
        let list_field = Arc::new(Field::new(name, DataType::List(field), true));
        (list_field, list_array)
    }
}

// Int64
impl PgTypeToArrowArray<i64> for Vec<Option<i64>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let field = Field::new(name, DataType::Int64, true);
        let array = Int64Array::from(self);
        (Arc::new(field), Arc::new(array))
    }
}

impl PgTypeToArrowArray<Vec<Option<i64>>> for Vec<Option<Vec<Option<i64>>>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, all_nulls) = array_offsets(&self);

        let field = Field::new(name, DataType::Int64, true);

        if all_nulls {
            return create_arrow_null_list_array(name, &field, offsets.len() - 1);
        }

        let array = self.into_iter().flatten().flatten().collect::<Vec<_>>();
        let array = Int64Array::from(array);
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        let list_array = ListArray::new(field.clone(), offsets, primitive_array, None);
        let list_array = make_array(list_array.into());
        let list_field = Arc::new(Field::new(name, DataType::List(field), true));
        (list_field, list_array)
    }
}

// Float32
impl PgTypeToArrowArray<f32> for Vec<Option<f32>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let field = Field::new(name, DataType::Float32, true);
        let array = Float32Array::from(self);
        (Arc::new(field), Arc::new(array))
    }
}

impl PgTypeToArrowArray<Vec<Option<f32>>> for Vec<Option<Vec<Option<f32>>>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, all_nulls) = array_offsets(&self);

        let field = Field::new(name, DataType::Float32, true);

        if all_nulls {
            return create_arrow_null_list_array(name, &field, offsets.len() - 1);
        }

        let array = self.into_iter().flatten().flatten().collect::<Vec<_>>();
        let array = Float32Array::from(array);
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        let list_array = ListArray::new(field.clone(), offsets, primitive_array, None);
        let list_array = make_array(list_array.into());
        let list_field = Arc::new(Field::new(name, DataType::List(field), true));
        (list_field, list_array)
    }
}

// Float64
impl PgTypeToArrowArray<f64> for Vec<Option<f64>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let field = Field::new(name, DataType::Float64, true);
        let array = Float64Array::from(self);
        (Arc::new(field), Arc::new(array))
    }
}

impl PgTypeToArrowArray<Vec<Option<f64>>> for Vec<Option<Vec<Option<f64>>>> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, all_nulls) = array_offsets(&self);

        let field = Field::new(name, DataType::Float64, true);

        if all_nulls {
            return create_arrow_null_list_array(name, &field, offsets.len() - 1);
        }

        let array = self.into_iter().flatten().flatten().collect::<Vec<_>>();
        let array = Float64Array::from(array);
        let (field, primitive_array) = (Arc::new(field), Arc::new(array));

        let list_array = ListArray::new(field.clone(), offsets, primitive_array, None);
        let list_array = make_array(list_array.into());
        let list_field = Arc::new(Field::new(name, DataType::List(field), true));
        (list_field, list_array)
    }
}

// PgHeapTuple
impl PgTypeToArrowArray<PgHeapTuple<'_, AllocatedByRust>>
    for Vec<Option<PgHeapTuple<'_, AllocatedByRust>>>
{
    fn as_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let mut struct_fields_with_data: Vec<(Arc<Field>, ArrayRef)> = vec![];

        let tupledesc = tuple_desc(typoid, typmod);

        let attributes = collect_attributes(&tupledesc);

        for attribute in attributes {
            let attribute_name = attribute.name();
            let attribute_typoid = attribute.type_oid().value();
            let attribute_typmod = attribute.type_mod();

            let is_attribute_array = unsafe { pg_sys::type_is_array(attribute_typoid) };
            let attribute_element_typoid =
                if is_attribute_array || attribute_typoid == RECORDARRAYOID {
                    if attribute_typoid == RECORDARRAYOID {
                        RECORDOID
                    } else {
                        unsafe { pg_sys::get_element_type(attribute_typoid) }
                    }
                } else {
                    InvalidOid
                };

            let attribute_array_with_field = match attribute_typoid {
                FLOAT4OID => {
                    let attribute_array =
                        collect_attribute_array_from_tuples::<f32>(&self, attribute_name);
                    attribute_array.as_arrow_array(
                        attribute_name,
                        attribute_typoid,
                        attribute_typmod,
                    )
                }
                FLOAT4ARRAYOID => {
                    let attribute_array = collect_attribute_array_from_tuples::<Vec<Option<f32>>>(
                        &self,
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
                        collect_attribute_array_from_tuples::<f64>(&self, attribute_name);
                    attribute_array.as_arrow_array(
                        attribute_name,
                        attribute_typoid,
                        attribute_typmod,
                    )
                }
                FLOAT8ARRAYOID => {
                    let attribute_array = collect_attribute_array_from_tuples::<Vec<Option<f64>>>(
                        &self,
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
                        collect_attribute_array_from_tuples::<i16>(&self, attribute_name);
                    attribute_array.as_arrow_array(
                        attribute_name,
                        attribute_typoid,
                        attribute_typmod,
                    )
                }
                INT2ARRAYOID => {
                    let attribute_array = collect_attribute_array_from_tuples::<Vec<Option<i16>>>(
                        &self,
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
                        collect_attribute_array_from_tuples::<i32>(&self, attribute_name);
                    attribute_array.as_arrow_array(
                        attribute_name,
                        attribute_typoid,
                        attribute_typmod,
                    )
                }
                INT4ARRAYOID => {
                    let attribute_array = collect_attribute_array_from_tuples::<Vec<Option<i32>>>(
                        &self,
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
                        collect_attribute_array_from_tuples::<i64>(&self, attribute_name);
                    attribute_array.as_arrow_array(
                        attribute_name,
                        attribute_typoid,
                        attribute_typmod,
                    )
                }
                INT8ARRAYOID => {
                    let attribute_array = collect_attribute_array_from_tuples::<Vec<Option<i64>>>(
                        &self,
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
                        collect_attribute_array_from_tuples::<bool>(&self, attribute_name);
                    attribute_array.as_arrow_array(
                        attribute_name,
                        attribute_typoid,
                        attribute_typmod,
                    )
                }
                BOOLARRAYOID => {
                    let attribute_array = collect_attribute_array_from_tuples::<Vec<Option<bool>>>(
                        &self,
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
                        collect_attribute_array_from_tuples::<Date>(&self, attribute_name);
                    attribute_array.as_arrow_array(
                        attribute_name,
                        attribute_typoid,
                        attribute_typmod,
                    )
                }
                DATEARRAYOID => {
                    let attribute_array = collect_attribute_array_from_tuples::<Vec<Option<Date>>>(
                        &self,
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
                        collect_attribute_array_from_tuples::<Time>(&self, attribute_name);
                    attribute_array.as_arrow_array(
                        attribute_name,
                        attribute_typoid,
                        attribute_typmod,
                    )
                }
                TIMEARRAYOID => {
                    let attribute_array = collect_attribute_array_from_tuples::<Vec<Option<Time>>>(
                        &self,
                        attribute_name,
                    );
                    attribute_array.as_arrow_array(
                        attribute_name,
                        attribute_element_typoid,
                        attribute_typmod,
                    )
                }
                TIMETZOID => {
                    let attribute_array = collect_attribute_array_from_tuples::<TimeWithTimeZone>(
                        &self,
                        attribute_name,
                    );
                    attribute_array.as_arrow_array(
                        attribute_name,
                        attribute_typoid,
                        attribute_typmod,
                    )
                }
                TIMETZARRAYOID => {
                    let attribute_array = collect_attribute_array_from_tuples::<
                        Vec<Option<TimeWithTimeZone>>,
                    >(&self, attribute_name);
                    attribute_array.as_arrow_array(
                        attribute_name,
                        attribute_element_typoid,
                        attribute_typmod,
                    )
                }
                TIMESTAMPOID => {
                    let attribute_array =
                        collect_attribute_array_from_tuples::<Timestamp>(&self, attribute_name);
                    attribute_array.as_arrow_array(
                        attribute_name,
                        attribute_typoid,
                        attribute_typmod,
                    )
                }
                TIMESTAMPARRAYOID => {
                    let attribute_array = collect_attribute_array_from_tuples::<
                        Vec<Option<Timestamp>>,
                    >(&self, attribute_name);
                    attribute_array.as_arrow_array(
                        attribute_name,
                        attribute_element_typoid,
                        attribute_typmod,
                    )
                }
                TIMESTAMPTZOID => {
                    let attribute_array = collect_attribute_array_from_tuples::<
                        TimestampWithTimeZone,
                    >(&self, attribute_name);
                    attribute_array.as_arrow_array(
                        attribute_name,
                        attribute_typoid,
                        attribute_typmod,
                    )
                }
                TIMESTAMPTZARRAYOID => {
                    let attribute_array = collect_attribute_array_from_tuples::<
                        Vec<Option<TimestampWithTimeZone>>,
                    >(&self, attribute_name);
                    attribute_array.as_arrow_array(
                        attribute_name,
                        attribute_element_typoid,
                        attribute_typmod,
                    )
                }
                TEXTOID | VARCHAROID => {
                    let attribute_array =
                        collect_attribute_array_from_tuples::<String>(&self, attribute_name);
                    attribute_array.as_arrow_array(
                        attribute_name,
                        attribute_typoid,
                        attribute_typmod,
                    )
                }
                TEXTARRAYOID | VARCHARARRAYOID => {
                    let attribute_array = collect_attribute_array_from_tuples::<Vec<Option<String>>>(
                        &self,
                        attribute_name,
                    );
                    attribute_array.as_arrow_array(
                        attribute_name,
                        attribute_element_typoid,
                        attribute_typmod,
                    )
                }
                _ => {
                    let attribute_is_composite =
                        unsafe { pg_sys::type_is_rowtype(attribute_typoid) };
                    let is_attribute_composite_array =
                        unsafe { pg_sys::type_is_rowtype(attribute_element_typoid) };

                    if attribute_is_composite {
                        let attribute_array = collect_attribute_array_from_tuples::<
                            PgHeapTuple<AllocatedByRust>,
                        >(&self, attribute_name);
                        attribute_array.as_arrow_array(
                            attribute_name,
                            attribute_typoid,
                            attribute_typmod,
                        )
                    } else if is_attribute_composite_array {
                        let attribute_array = collect_attribute_array_from_tuples::<
                            Vec<Option<PgHeapTuple<AllocatedByRust>>>,
                        >(&self, attribute_name);
                        attribute_array.as_arrow_array(
                            attribute_name,
                            attribute_element_typoid,
                            attribute_typmod,
                        )
                    } else {
                        panic!("Unsupported type");
                    }
                }
            };

            struct_fields_with_data.push(attribute_array_with_field);
        }

        // finalize StructArray
        let mut struct_attribute_datas = vec![];
        let mut struct_attribute_fields = vec![];
        for (field, data) in struct_fields_with_data {
            struct_attribute_fields.push(field);
            struct_attribute_datas.push(data);
        }

        let struct_field = Arc::new(Field::new(
            name,
            DataType::Struct(Fields::from(struct_attribute_fields.clone())),
            true,
        ));

        let is_null_buffer =
            BooleanBuffer::collect_bool(self.len(), |idx| self.get(idx).unwrap().is_some());
        let struct_null_buffer = NullBuffer::new(is_null_buffer);
        let struct_array = StructArray::new(
            Fields::from(struct_attribute_fields),
            struct_attribute_datas,
            Some(struct_null_buffer),
        );
        let struct_array = make_array(struct_array.into());

        (struct_field, struct_array)
    }
}

impl PgTypeToArrowArray<Vec<Option<PgHeapTuple<'_, AllocatedByRust>>>>
    for Vec<Option<Vec<Option<PgHeapTuple<'_, AllocatedByRust>>>>>
{
    fn as_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef) {
        let (offsets, all_nulls) = array_offsets(&self);

        let array = self.into_iter().flatten().flatten().collect::<Vec<_>>();
        let (field, primitive_array) = array.as_arrow_array(name, typoid, typmod);

        if all_nulls {
            return create_arrow_null_list_array(name, &field, offsets.len() - 1);
        }

        let list_array = ListArray::new(field.clone(), offsets, primitive_array, None);
        let list_array = make_array(list_array.into());
        let list_field = Arc::new(Field::new(name, DataType::List(field), true));
        (list_field, list_array)
    }
}

pub(crate) fn collect_attribute_array_from_tuples<T>(
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

fn array_offsets<T>(arrays: &Vec<Option<Vec<Option<T>>>>) -> (OffsetBuffer<i32>, bool) {
    let mut has_some = false;
    let mut offsets = vec![0];
    let mut current_offset = 0;
    for array in arrays {
        if let Some(array) = array {
            let len = array.len() as i32;
            current_offset += len;
            offsets.push(current_offset);
            has_some = true;
        } else {
            offsets.push(0);
        }
    }

    let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets));
    let all_nulls = !arrays.is_empty() && !has_some;

    (offsets, all_nulls)
}
