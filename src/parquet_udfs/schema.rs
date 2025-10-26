use std::sync::Arc;

use crate::arrow_parquet::uri_utils::{
    ensure_access_privilege_to_uri, parquet_schema_from_uri, uri_as_string, ParsedUriInfo,
};

use ::parquet::basic::{ConvertedType, LogicalType, Repetition};
use ::parquet::schema::types::{ColumnDescPtr, ColumnDescriptor, ColumnPath, Type, TypePtr};
use pgrx::{iter::TableIterator, name, pg_extern, pg_schema};

#[pg_schema]
mod parquet {
    use super::*;

    #[pg_extern]
    #[allow(clippy::type_complexity)]
    fn schema(
        uri: String,
    ) -> TableIterator<
        'static,
        (
            name!(uri, String),
            name!(name, String),
            name!(type_name, Option<String>),
            name!(type_length, Option<String>),
            name!(repetition_type, Option<String>),
            name!(num_children, Option<i32>),
            name!(converted_type, Option<String>),
            name!(scale, Option<i32>),
            name!(precision, Option<i32>),
            name!(field_id, Option<i32>),
            name!(logical_type, Option<String>),
        ),
    > {
        let uri_info = ParsedUriInfo::try_from(uri.as_str()).unwrap_or_else(|e| {
            panic!("{}", e.to_string());
        });

        ensure_access_privilege_to_uri(&uri_info, true);
        let parquet_schema = parquet_schema_from_uri(&uri_info);

        let mut rows = vec![];

        let root_type = parquet_schema.root_schema_ptr();

        let mut column_descriptors = vec![];

        let max_rep_level = 0;
        let max_def_level = 0;
        let mut path_so_far = vec![];

        collect_all_column_descriptors(
            &root_type,
            max_rep_level,
            max_def_level,
            &mut column_descriptors,
            &mut path_so_far,
        );

        for column_descriptor in column_descriptors {
            let column_type = column_descriptor.self_type();

            let row = get_column_info(&uri_info, Some(&column_descriptor), column_type);

            rows.push(row);
        }

        TableIterator::new(rows)
    }
}

#[allow(clippy::type_complexity)]
fn get_column_info(
    uri_info: &ParsedUriInfo,
    column_descriptor: Option<&Arc<ColumnDescriptor>>,
    column_type: &Type,
) -> (
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i32>,
    Option<String>,
    Option<i32>,
    Option<i32>,
    Option<i32>,
    Option<String>,
) {
    let type_length = column_descriptor.and_then(|cd| {
        if !column_type.is_primitive() {
            return None;
        }

        let type_length = cd.type_length();

        if type_length == -1 {
            None
        } else {
            Some(type_length.to_string())
        }
    });

    let name = column_type.name().to_string();

    let type_name = if column_type.is_primitive() {
        Some(column_type.get_physical_type().to_string())
    } else {
        None
    };

    let scale = if column_type.is_primitive() {
        let scale = column_type.get_scale();

        if scale == -1 {
            None
        } else {
            Some(scale)
        }
    } else {
        None
    };

    let precision = if column_type.is_primitive() {
        let precision = column_type.get_precision();

        if precision == -1 {
            None
        } else {
            Some(precision)
        }
    } else {
        None
    };

    let column_info = column_type.get_basic_info();

    let field_id = if column_info.has_id() {
        Some(column_info.id())
    } else {
        None
    };

    let repetition_type = if column_info.has_repetition() {
        Some(column_info.repetition().to_string())
    } else {
        None
    };

    let converted_type = if column_info.converted_type() == ConvertedType::NONE {
        None
    } else {
        Some(column_info.converted_type().to_string())
    };

    let logical_type = column_info.logical_type().as_ref().map(logical_type_to_str);

    let num_children = if !column_type.is_primitive() {
        Some(column_type.get_fields().len() as i32)
    } else {
        None
    };

    (
        uri_as_string(&uri_info.uri),
        name,
        type_name,
        type_length,
        repetition_type,
        num_children,
        converted_type,
        scale,
        precision,
        field_id,
        logical_type,
    )
}

// collect_all_column_descriptors recursively collects all column descriptors from the given type.
fn collect_all_column_descriptors<'a>(
    tp: &'a TypePtr,
    mut max_rep_level: i16,
    mut max_def_level: i16,
    all_columns: &mut Vec<ColumnDescPtr>,
    path_so_far: &mut Vec<&'a str>,
) {
    path_so_far.push(tp.name());

    if tp.get_basic_info().has_repetition() {
        match tp.get_basic_info().repetition() {
            Repetition::OPTIONAL => {
                max_def_level += 1;
            }
            Repetition::REPEATED => {
                max_def_level += 1;
                max_rep_level += 1;
            }
            _ => {}
        }
    }

    let mut path: Vec<String> = vec![];
    path.extend(path_so_far.iter().copied().map(String::from));
    all_columns.push(Arc::new(ColumnDescriptor::new(
        tp.clone(),
        max_def_level,
        max_rep_level,
        ColumnPath::new(path),
    )));

    if let Type::GroupType { fields, .. } = tp.as_ref() {
        for f in fields {
            collect_all_column_descriptors(
                f,
                max_rep_level,
                max_def_level,
                all_columns,
                path_so_far,
            );
            path_so_far.pop();
        }
    }
}

fn logical_type_to_str(logical_type: &LogicalType) -> String {
    match logical_type {
        LogicalType::String => "STRING",
        LogicalType::Map => "MAP",
        LogicalType::List => "LIST",
        LogicalType::Enum => "ENUM",
        LogicalType::Decimal { .. } => "DECIMAL",
        LogicalType::Date => "DATE",
        LogicalType::Time { .. } => "TIME",
        LogicalType::Timestamp { .. } => "TIMESTAMP",
        LogicalType::Integer { .. } => "INTEGER",
        LogicalType::Json => "JSON",
        LogicalType::Bson => "BSON",
        LogicalType::Uuid => "UUID",
        LogicalType::Float16 => "FLOAT16",
        LogicalType::Variant { .. } => "VARIANT",
        LogicalType::Geometry { .. } => "GEOMETRY",
        LogicalType::Geography { .. } => "GEOGRAPHY",
        _ => "UNKNOWN",
    }
    .into()
}
