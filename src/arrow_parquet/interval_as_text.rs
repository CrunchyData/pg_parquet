use std::sync::Arc;

use arrow::array::{make_array, Array, ArrayRef, FixedSizeBinaryArray, RecordBatch, StringBuilder};
use arrow_schema::{DataType, FieldRef, Fields, Schema, SchemaRef};
use parquet::{
    basic::{ConvertedType, Type as PhysicalType},
    file::metadata::{FileMetaData, ParquetMetaData, ParquetMetaDataBuilder},
    schema::types::{SchemaDescriptor, Type as ParquetType, TypePtr},
};

// A parquet INTERVAL column is a FIXED_LEN_BYTE_ARRAY(12) of three little endian 4 byte
// integers: the months, the days and the milliseconds of it. parquet-rs reads it as
// Interval(DayTime), which drops the months, so the file is read with the INTERVAL annotation
// cleared, which hands the 12 bytes back as they are, and they are decoded here into the text
// of an interval, which is the form a value reaches an interval column in anyway.

// the length of a parquet INTERVAL value
const INTERVAL_LENGTH: i32 = 12;

// metadata_with_interval_as_bytes returns the metadata of the file with the INTERVAL
// annotation cleared from its columns, which makes the reader hand their bytes back
// untouched. It returns None if the file has no INTERVAL column.
pub(crate) fn metadata_with_interval_as_bytes(
    metadata: &ParquetMetaData,
) -> Option<Arc<ParquetMetaData>> {
    let file_metadata = metadata.file_metadata();

    let root_type =
        parquet_type_with_interval_as_bytes(&file_metadata.schema_descr().root_schema_ptr())?;

    let file_metadata = FileMetaData::new(
        file_metadata.version(),
        file_metadata.num_rows(),
        file_metadata.created_by().map(ToString::to_string),
        file_metadata.key_value_metadata().cloned(),
        Arc::new(SchemaDescriptor::new(root_type)),
        file_metadata.column_orders().cloned(),
    );

    // only the schema of the file changes, its row groups and its page index are the ones
    // that were read from the file
    let mut builder = metadata.clone().into_builder();
    let row_groups = builder.take_row_groups();
    let page_index = builder.take_page_index();

    Some(Arc::new(
        ParquetMetaDataBuilder::new(file_metadata)
            .set_row_groups(row_groups)
            .set_page_index(page_index)
            .build(),
    ))
}

fn parquet_type_with_interval_as_bytes(parquet_type: &TypePtr) -> Option<TypePtr> {
    let basic_info = parquet_type.get_basic_info();
    let id = basic_info.has_id().then(|| basic_info.id());

    let new_type = match parquet_type.as_ref() {
        ParquetType::PrimitiveType {
            physical_type,
            type_length,
            ..
        } => {
            if *physical_type != PhysicalType::FIXED_LEN_BYTE_ARRAY
                || basic_info.converted_type() != ConvertedType::INTERVAL
                || *type_length != INTERVAL_LENGTH
            {
                return None;
            }

            ParquetType::primitive_type_builder(basic_info.name(), *physical_type)
                .with_repetition(basic_info.repetition())
                .with_length(*type_length)
                .with_id(id)
                .build()
        }
        ParquetType::GroupType { fields, .. } => {
            let mut new_fields = Vec::with_capacity(fields.len());
            let mut found_interval = false;

            for field in fields {
                match parquet_type_with_interval_as_bytes(field) {
                    Some(new_field) => {
                        found_interval = true;
                        new_fields.push(new_field);
                    }
                    None => new_fields.push(field.clone()),
                }
            }

            if !found_interval {
                return None;
            }

            let mut builder = ParquetType::group_type_builder(basic_info.name())
                .with_fields(new_fields)
                .with_logical_type(basic_info.logical_type_ref().cloned())
                .with_converted_type(basic_info.converted_type())
                .with_id(id);

            // the root of a schema has no repetition
            if basic_info.has_repetition() {
                builder = builder.with_repetition(basic_info.repetition());
            }

            builder.build()
        }
    };

    // a type that cannot be rebuilt leaves the file to be read as it was read before
    Some(Arc::new(new_type.ok()?))
}

// schema_with_interval_as_text returns the schema that the file is read as: the schema of the
// reader, which has the 12 bytes of an interval as fixed size binary, with the fields that
// interval_schema, the schema the file had before its INTERVAL annotation was cleared, has as
// an interval replaced by text fields. It returns None if there is no such field.
pub(crate) fn schema_with_interval_as_text(
    schema: &Schema,
    interval_schema: &Schema,
) -> Option<SchemaRef> {
    let fields = fields_with_interval_as_text(schema.fields(), interval_schema.fields())?;

    Some(Arc::new(Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    )))
}

fn fields_with_interval_as_text(fields: &Fields, interval_fields: &Fields) -> Option<Fields> {
    let mut new_fields = Vec::with_capacity(fields.len());
    let mut found_interval = false;

    for (field, interval_field) in fields.iter().zip(interval_fields) {
        match field_with_interval_as_text(field, interval_field) {
            Some(new_field) => {
                found_interval = true;
                new_fields.push(new_field);
            }
            None => new_fields.push(field.clone()),
        }
    }

    // the two schemas describe the same file, so they only differ in the types of their fields
    (found_interval && new_fields.len() == fields.len()).then(|| new_fields.into())
}

fn field_with_interval_as_text(field: &FieldRef, interval_field: &FieldRef) -> Option<FieldRef> {
    let data_type = match (field.data_type(), interval_field.data_type()) {
        (DataType::FixedSizeBinary(INTERVAL_LENGTH), DataType::Interval(_)) => DataType::Utf8,
        (DataType::List(field), DataType::List(interval_field)) => {
            DataType::List(field_with_interval_as_text(field, interval_field)?)
        }
        (DataType::Struct(fields), DataType::Struct(interval_fields)) => {
            DataType::Struct(fields_with_interval_as_text(fields, interval_fields)?)
        }
        _ => return None,
    };

    Some(Arc::new(field.as_ref().clone().with_data_type(data_type)))
}

// record_batch_with_interval_as_text returns the record batch with the columns that the
// schema has as text, and that the reader handed back as the 12 bytes of an interval,
// decoded into that text. The batch is returned as it is if it has no such column.
pub(crate) fn record_batch_with_interval_as_text(
    record_batch: RecordBatch,
    schema: &SchemaRef,
) -> RecordBatch {
    debug_assert_eq!(record_batch.num_columns(), schema.fields().len());

    let mut new_columns = Vec::with_capacity(record_batch.num_columns());
    let mut found_interval = false;

    for (column, field) in record_batch.columns().iter().zip(schema.fields()) {
        match array_with_interval_as_text(column, field) {
            Some(new_column) => {
                found_interval = true;
                new_columns.push(new_column);
            }
            None => new_columns.push(column.clone()),
        }
    }

    if !found_interval {
        return record_batch;
    }

    RecordBatch::try_new(schema.clone(), new_columns)
        .unwrap_or_else(|e| panic!("failed to decode the interval columns of the file: {e}"))
}

fn array_with_interval_as_text(array: &ArrayRef, field: &FieldRef) -> Option<ArrayRef> {
    if let (DataType::FixedSizeBinary(INTERVAL_LENGTH), DataType::Utf8) =
        (array.data_type(), field.data_type())
    {
        return Some(interval_text_array(array));
    }

    // a field of a container type is at the same position in the array as it is in the type
    let child_fields = match field.data_type() {
        DataType::List(field) => vec![field.clone()],
        DataType::Struct(fields) => fields.iter().cloned().collect(),
        _ => return None,
    };

    let array_data = array.to_data();

    let mut new_child_data = Vec::with_capacity(child_fields.len());
    let mut found_interval = false;

    for (child_data, child_field) in array_data.child_data().iter().zip(&child_fields) {
        let child = make_array(child_data.clone());

        match array_with_interval_as_text(&child, child_field) {
            Some(new_child) => {
                found_interval = true;
                new_child_data.push(new_child.to_data());
            }
            None => new_child_data.push(child_data.clone()),
        }
    }

    if !found_interval {
        return None;
    }

    // a decoded child keeps its length, its place and its nulls, so the offsets and the nulls
    // of the array stay valid under its new type
    let new_array_data = array_data
        .into_builder()
        .data_type(field.data_type().clone())
        .child_data(new_child_data)
        .build()
        .unwrap_or_else(|e| panic!("failed to decode the interval columns of the file: {e}"));

    Some(make_array(new_array_data))
}

fn interval_text_array(array: &ArrayRef) -> ArrayRef {
    let intervals = array
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("expected a fixed size binary array for an interval column");

    let mut texts = StringBuilder::with_capacity(intervals.len(), intervals.len() * 32);

    for interval in intervals {
        match interval {
            Some(interval) => texts.append_value(interval_text(interval)),
            None => texts.append_null(),
        }
    }

    Arc::new(texts.finish())
}

// interval_text formats the 12 bytes of a parquet interval as the text of a PostgreSQL
// interval. The parts are read as signed, since that is how a writer stores a negative
// interval, and they are the parts that PostgreSQL stores itself, except that its
// sub-second part is microseconds.
fn interval_text(interval: &[u8]) -> String {
    debug_assert_eq!(interval.len(), INTERVAL_LENGTH as usize);

    let part = |start: usize| {
        i32::from_le_bytes(
            interval[start..start + 4]
                .try_into()
                .expect("expected 4 bytes for a part of an interval"),
        )
    };

    let (months, days, millis) = (part(0), part(4), part(8));

    let sign = if millis < 0 { "-" } else { "" };
    let millis = (millis as i64).unsigned_abs();

    format!(
        "{months} mons {days} days {sign}{}.{:03} secs",
        millis / 1000,
        millis % 1000
    )
}
