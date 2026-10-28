//! The options that name the columns a writer property applies to: `bloom_filter` and
//! `dictionary`.

use std::collections::HashMap;
use std::fmt::Display;
use std::str::FromStr;

use arrow_schema::Schema;
use format_registry::Result;
use parquet::arrow::ArrowSchemaConverter;
use parquet::schema::types::ColumnPath;

/// `all`, `none`, or a JSON object of column names to booleans.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnSelection {
    None,
    All,
    Columns(HashMap<String, bool>),
}

pub const DEFAULT_BLOOM_FILTER: ColumnSelection = ColumnSelection::None;
pub const DEFAULT_DICTIONARY: ColumnSelection = ColumnSelection::All;

impl FromStr for ColumnSelection {
    type Err = String;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "none" => Ok(Self::None),
            "all" => Ok(Self::All),
            columns => Ok(Self::Columns(serde_json::from_str(columns).map_err(
                |_| {
                    "invalid column selection. Allowed values are: all, none, or a JSON \
                     object with column names as keys and booleans as values."
                        .to_string()
                },
            )?)),
        }
    }
}

impl Display for ColumnSelection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, "none"),
            Self::All => write!(f, "all"),
            Self::Columns(columns) => write!(
                f,
                "{}",
                serde_json::to_string(columns).map_err(|_| std::fmt::Error)?
            ),
        }
    }
}

impl ColumnSelection {
    /// Whether this selection enables a leaf column, or `None` when it does not mention it, in
    /// which case the writer property's own default applies. A selection names *top level*
    /// columns, which for a primitive is the leaf itself and for a nested type is many leaves.
    pub fn enabled_for(&self, leaf_column_path: &ColumnPath) -> Option<bool> {
        match self {
            Self::None => Some(false),
            Self::All => Some(true),
            Self::Columns(columns) => columns.get(&leaf_column_path.parts()[0]).copied(),
        }
    }

    /// Checks that every named column is a top level column of the schema.
    pub fn validate_against_schema(&self, arrow_schema: &Schema, option_name: &str) -> Result<()> {
        let columns = match self {
            Self::None | Self::All => return Ok(()),
            Self::Columns(columns) => columns,
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

/// The path of every leaf column of the parquet schema an Arrow schema is written as. The
/// per-column writer properties are keyed by those paths.
pub fn leaf_column_paths(arrow_schema: &Schema) -> Result<Vec<ColumnPath>> {
    let descriptor = ArrowSchemaConverter::new()
        .convert(arrow_schema)
        .map_err(|e| format!("failed to convert the schema to a parquet schema: {e}"))?;

    Ok(descriptor
        .columns()
        .iter()
        .map(|column| column.path().clone())
        .collect())
}
