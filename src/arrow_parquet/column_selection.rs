use std::{collections::HashMap, fmt::Display, str::FromStr};

use arrow_schema::Schema;
use parquet::{arrow::ArrowSchemaConverter, schema::types::ColumnPath};

/// Implements parsing for the options in COPY .. TO statements that select the columns
/// a writer property applies to, like bloom_filter and dictionary
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ColumnSelection {
    None,
    All,
    Columns(HashMap<String, bool>),
}

pub(crate) const DEFAULT_BLOOM_FILTER: ColumnSelection = ColumnSelection::None;
pub(crate) const DEFAULT_DICTIONARY: ColumnSelection = ColumnSelection::All;

impl FromStr for ColumnSelection {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "none" => Ok(ColumnSelection::None),
            "all" => Ok(ColumnSelection::All),
            columns => Ok(ColumnSelection::Columns(
                serde_json::from_str(columns).map_err(|_| {
                    "invalid column selection. Allowed values are: all, none, or a JSON \
                     object with column names as keys and booleans as values."
                        .to_string()
                })?,
            )),
        }
    }
}

impl Display for ColumnSelection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ColumnSelection::None => write!(f, "none"),
            ColumnSelection::All => write!(f, "all"),
            ColumnSelection::Columns(columns) => {
                write!(f, "{}", serde_json::to_string(columns).unwrap())
            }
        }
    }
}

impl ColumnSelection {
    /// Returns whether the selection enables the given leaf column, or None when the
    /// selection does not mention the column, in which case the default of the writer
    /// property applies. A selected column is a top level column, which is a leaf column
    /// itself for a primitive type but expands to many leaf columns for a nested type.
    pub(crate) fn enabled_for(&self, leaf_column_path: &ColumnPath) -> Option<bool> {
        match self {
            ColumnSelection::None => Some(false),
            ColumnSelection::All => Some(true),
            ColumnSelection::Columns(columns) => {
                let top_level_column_name = &leaf_column_path.parts()[0];

                columns.get(top_level_column_name).copied()
            }
        }
    }

    /// Validates that every selected column exists as a top level column in the provided
    /// Arrow schema
    pub(crate) fn validate_against_schema(
        &self,
        arrow_schema: &Schema,
        option_name: &str,
    ) -> Result<(), String> {
        let columns = match self {
            ColumnSelection::None | ColumnSelection::All => return Ok(()),
            ColumnSelection::Columns(columns) => columns,
        };

        for column_name in columns.keys() {
            if arrow_schema.column_with_name(column_name).is_none() {
                return Err(format!(
                    "column \"{}\" in \"{}\" does not exist.\nAvailable columns: {:?}",
                    column_name,
                    option_name,
                    arrow_schema
                        .fields()
                        .iter()
                        .map(|field| field.name())
                        .collect::<Vec<_>>()
                ));
            }
        }

        Ok(())
    }
}

/// Returns the path of every leaf column in the parquet schema that the Arrow schema is
/// written as. Writer properties that are set per column are keyed by those paths.
pub(crate) fn leaf_column_paths(arrow_schema: &Schema) -> Vec<ColumnPath> {
    let schema_descriptor = ArrowSchemaConverter::new()
        .convert(arrow_schema)
        .unwrap_or_else(|e| panic!("failed to convert the schema to a parquet schema: {e}"));

    schema_descriptor
        .columns()
        .iter()
        .map(|column_descriptor| column_descriptor.path().clone())
        .collect()
}
