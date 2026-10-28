//! The schema a file is *read* with, which is not always the schema it declares.
//!
//! Two cases where upstream's reader, given the file's own schema, either panics or returns a
//! wrong value, and both are decided by the parquet file rather than by anything Postgres does
//! — which is why the correction lives in this crate:
//!
//! - a dictionary encoded column whose value type parquet's dictionary decoder cannot build
//!   (boolean, decimal), read as its value type instead;
//! - an `INT96` timestamp, whose resolution nothing in the file states, read as microseconds
//!   rather than as parquet-rs's default of nanoseconds.
//!
//! Both return `None` when there is nothing to correct, so a file with neither is read with its
//! own schema and pays nothing.

use std::sync::Arc;

use arrow::datatypes::{Fields, Schema, SchemaRef, TimeUnit};
use arrow_schema::{DataType, FieldRef};
use parquet::arrow::ARROW_SCHEMA_META_KEY;
use parquet::basic::Type as PhysicalType;
use parquet::file::metadata::FileMetaData;
use parquet::schema::types::Type as ParquetType;

/// The schema to read a file with, or `None` to read it with its own.
pub fn schema_hint(schema: &Schema, file_metadata: &FileMetaData) -> Option<SchemaRef> {
    let without_dictionaries = schema_without_dictionaries(schema);
    let base = without_dictionaries.as_deref().unwrap_or(schema);

    schema_with_int96_as_micros(base, file_metadata).or(without_dictionaries)
}

/// The schema with its dictionary encoded fields replaced by their value types, or `None` if it
/// has none. Parquet's dictionary decoder cannot build a dictionary of every value type — of
/// boolean or of decimal, for instance — and panics rather than refusing, so those columns are
/// read as their value types.
fn schema_without_dictionaries(schema: &Schema) -> Option<SchemaRef> {
    let fields = fields_without_dictionaries(schema.fields())?;

    Some(Arc::new(Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    )))
}

fn fields_without_dictionaries(fields: &Fields) -> Option<Fields> {
    let mut new_fields = Vec::with_capacity(fields.len());
    let mut found_dictionary = false;

    for field in fields {
        match field_without_dictionary(field) {
            Some(new_field) => {
                found_dictionary = true;
                new_fields.push(new_field);
            }
            None => new_fields.push(field.clone()),
        }
    }

    found_dictionary.then(|| new_fields.into())
}

fn field_without_dictionary(field: &FieldRef) -> Option<FieldRef> {
    let data_type = match field.data_type() {
        DataType::Dictionary(_, value_type) => value_type.as_ref().clone(),
        DataType::List(field) => DataType::List(field_without_dictionary(field)?),
        DataType::LargeList(field) => DataType::LargeList(field_without_dictionary(field)?),
        DataType::ListView(field) => DataType::ListView(field_without_dictionary(field)?),
        DataType::LargeListView(field) => DataType::LargeListView(field_without_dictionary(field)?),
        DataType::FixedSizeList(field, size) => {
            DataType::FixedSizeList(field_without_dictionary(field)?, *size)
        }
        DataType::Struct(fields) => DataType::Struct(fields_without_dictionaries(fields)?),
        DataType::Map(entries_field, sorted) => {
            DataType::Map(field_without_dictionary(entries_field)?, *sorted)
        }
        _ => return None,
    };

    Some(Arc::new(field.as_ref().clone().with_data_type(data_type)))
}

/// The schema with every `INT96` timestamp read as microseconds, or `None` if the file has
/// none to correct.
///
/// An `INT96` column has no logical type to tell parquet-rs its resolution, so parquet-rs
/// defaults it to nanoseconds, and `Int96::to_nanos` wraps around for anything outside
/// 1677..2262. Hive, Impala and Spark write their timestamps as `INT96` and record no Arrow
/// schema that would hint the resolution, so the wrapped value would be read as a real
/// timestamp.
fn schema_with_int96_as_micros(schema: &Schema, file_metadata: &FileMetaData) -> Option<SchemaRef> {
    // a file that records an Arrow schema declares the resolution of its timestamps, down to
    // an INT96 column - pyarrow records seconds, milliseconds or microseconds for one - and
    // parquet-rs applies what it finds, so there is no default to correct
    if arrow_schema_recorded(file_metadata) {
        return None;
    }

    let fields =
        fields_with_int96_as_micros(schema.fields(), file_metadata.schema_descr().root_schema())?;

    Some(Arc::new(Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    )))
}

fn arrow_schema_recorded(file_metadata: &FileMetaData) -> bool {
    file_metadata
        .key_value_metadata()
        .iter()
        .flat_map(|key_values| key_values.iter())
        .any(|key_value| key_value.key == ARROW_SCHEMA_META_KEY)
}

fn fields_with_int96_as_micros(fields: &Fields, parquet_type: &ParquetType) -> Option<Fields> {
    // the arrow schema is derived from the parquet schema, so the fields are in the same order
    let parquet_fields = parquet_type.get_fields();

    if parquet_fields.len() != fields.len() {
        return None;
    }

    let mut new_fields = Vec::with_capacity(fields.len());
    let mut found_int96 = false;

    for (field, parquet_field) in fields.iter().zip(parquet_fields.iter()) {
        match field_with_int96_as_micros(field, parquet_field) {
            Some(new_field) => {
                found_int96 = true;
                new_fields.push(new_field);
            }
            None => new_fields.push(field.clone()),
        }
    }

    found_int96.then(|| new_fields.into())
}

fn field_with_int96_as_micros(field: &FieldRef, parquet_type: &ParquetType) -> Option<FieldRef> {
    let data_type = match field.data_type() {
        DataType::Timestamp(TimeUnit::Nanosecond, None) => {
            if !parquet_type.is_primitive()
                || parquet_type.get_physical_type() != PhysicalType::INT96
            {
                return None;
            }

            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        DataType::Struct(fields) => {
            DataType::Struct(fields_with_int96_as_micros(fields, parquet_type)?)
        }
        // the large, view and fixed size list variants are not derived from a parquet schema,
        // they only come from a recorded arrow schema, which returns above
        DataType::List(field) => DataType::List(field_with_int96_as_micros(
            field,
            parquet_list_element(parquet_type)?,
        )?),
        DataType::Map(entries_field, sorted) => DataType::Map(
            field_with_int96_as_micros(entries_field, parquet_type.get_fields().first()?)?,
            *sorted,
        ),
        _ => return None,
    };

    Some(Arc::new(field.as_ref().clone().with_data_type(data_type)))
}

/// A list is three levels - group(List) { repeated group list { element } } - unless the
/// repeated level is the element itself, which is the legacy two level encoding.
fn parquet_list_element(parquet_type: &ParquetType) -> Option<&ParquetType> {
    let repeated_type = parquet_type.get_fields().first()?;

    if repeated_type.is_primitive() || repeated_type.get_fields().len() != 1 {
        return Some(repeated_type.as_ref());
    }

    repeated_type
        .get_fields()
        .first()
        .map(|field| field.as_ref())
}
