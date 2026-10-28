//! The `field_ids` option: Iceberg's field ids, written as parquet field ids.
//!
//! Two halves. The mapping is what a statement asked for — `none`, `auto`, or a JSON tree of
//! ids keyed by field name. [`stamp_field_ids`] is what turns that into a schema, by walking
//! the finished Arrow schema and putting a `PARQUET:field_id` into each field's metadata.
//!
//! The walk lives here rather than in `pg_arrow_base` because `PARQUET:field_id` is a parquet
//! metadata key: tier 2 builds a schema from a tuple descriptor and knows no format's keys. It
//! runs *after* that schema is built, which is the one difference from `pg_parquet` 0.6, where
//! the ids were assigned by the schema builder as it recursed. The assignment order is the same
//! — a pre-order walk, a container's own id before its children's — so a file written through
//! either path gets the same ids.

use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::str::FromStr;

use arrow_schema::{DataType, Field, FieldRef, Fields, Schema};
use format_registry::Result;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use serde::{Deserialize, Serialize};

/// The key a nested field's *own* id is under, since its name is taken by its children.
const ROOT_FIELD_ID: &str = "__root_field_id";

#[derive(Debug, Clone, Default, PartialEq)]
pub enum FieldIds {
    #[default]
    None,
    Auto,
    Explicit(FieldIdMapping),
}

impl FromStr for FieldIds {
    type Err = String;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "none" => Ok(Self::None),
            "auto" => Ok(Self::Auto),
            field_ids => Ok(Self::Explicit(
                serde_json::from_str(field_ids)
                    .map_err(|_| "invalid JSON string for field_ids".to_string())?,
            )),
        }
    }
}

impl Display for FieldIds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, "none"),
            Self::Auto => write!(f, "auto"),
            Self::Explicit(mapping) => write!(
                f,
                "{}",
                serde_json::to_string(mapping).map_err(|_| std::fmt::Error)?
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
enum FieldIdMappingItem {
    FieldId(i32),
    FieldIdMapping(FieldIdMapping),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldIdMapping {
    #[serde(flatten)]
    fields: HashMap<String, FieldIdMappingItem>,
}

impl FieldIdMapping {
    /// The id this mapping gives a field path, if it gives it one.
    fn field_id(&self, field_path: &[String]) -> Option<i32> {
        let (field_name, nested_path) = field_path.split_first()?;

        match self.fields.get(field_name) {
            // an id is the id of the field it names, and of no field nested in it, which would
            // hand the same id to the whole subtree of a struct, a list or a map
            Some(FieldIdMappingItem::FieldId(field_id)) => {
                nested_path.is_empty().then_some(*field_id)
            }
            Some(FieldIdMappingItem::FieldIdMapping(mapping)) => mapping.field_id(nested_path),
            None => None,
        }
    }

    /// Checks that every name in the mapping is a field of the schema, so that a typo is an
    /// error rather than an id that silently goes nowhere.
    fn validate_against_schema(&self, arrow_schema: &Schema) -> Result<()> {
        for (field_name, item) in &self.fields {
            // the root field's id is not a field of the schema
            if field_name == ROOT_FIELD_ID {
                continue;
            }

            let Some((_, arrow_field)) = arrow_schema.fields().find(field_name) else {
                return Err(format!(
                    "Field '{}' in the mapping does not exist in the Arrow schema.\nAvailable fields: {:?}",
                    field_name,
                    arrow_schema
                        .fields()
                        .iter()
                        .map(|field| field.name())
                        .collect::<Vec<_>>()
                ));
            };

            let FieldIdMappingItem::FieldIdMapping(mapping) = item else {
                // a bare id names one field, so a nested one, whose own id is the
                // `__root_field_id` key of a mapping, would leave its children without ids
                if matches!(
                    arrow_field.data_type(),
                    DataType::Struct(_)
                        | DataType::List(_)
                        | DataType::LargeList(_)
                        | DataType::Map(_, _)
                ) {
                    return Err(format!(
                        "Field '{field_name}' in the mapping is a nested field, so it needs a \
                         mapping with a \"{ROOT_FIELD_ID}\" key rather than a field id."
                    ));
                }

                continue;
            };

            // a nested mapping has to line up with a nested type: its keys are the children's
            // names, plus `__root_field_id` for the container itself
            match arrow_field.data_type() {
                DataType::Struct(subfields) => {
                    mapping.validate_against_schema(&Schema::new(subfields.clone()))?;
                }
                DataType::List(element) | DataType::LargeList(element) => {
                    mapping.validate_against_schema(&Schema::new(vec![element.clone()]))?;
                }
                DataType::Map(entries, _) => match entries.data_type() {
                    DataType::Struct(entry_fields) => {
                        mapping.validate_against_schema(&Schema::new(entry_fields.clone()))?;
                    }
                    other => {
                        return Err(format!(
                            "Map entry field should be a struct, but got '{other:?}' for field '{field_name}'"
                        ));
                    }
                },
                other => {
                    return Err(format!(
                        "Field '{field_name}' in the mapping is of type '{other:?}', which has \
                         no nested fields to map. It needs a field id."
                    ));
                }
            }
        }

        Ok(())
    }
}

/// Puts the ids a statement asked for into a schema's field metadata.
///
/// A `none` is the schema unchanged, which is also the only path a format that is not parquet
/// ever takes.
pub fn stamp_field_ids(schema: &Schema, field_ids: &FieldIds) -> Result<Schema> {
    if matches!(field_ids, FieldIds::None) {
        return Ok(schema.clone());
    }

    if let FieldIds::Explicit(mapping) = field_ids {
        mapping.validate_against_schema(schema)?;
    }

    let mut stamper = Stamper {
        field_ids,
        next_auto: 0,
        assigned: HashSet::new(),
    };

    let mut path = Vec::new();
    let fields: Fields = schema
        .fields()
        .iter()
        .map(|field| stamper.field(field, &mut path))
        .collect::<Result<Vec<_>>>()?
        .into();

    Ok(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

/// The walk's state: where it is, and which ids it has handed out.
struct Stamper<'ids> {
    field_ids: &'ids FieldIds,
    next_auto: i32,
    assigned: HashSet<i32>,
}

impl Stamper<'_> {
    /// The id for the field at `path`, if it gets one. An `auto` run numbers the fields in the
    /// order they are walked; an explicit mapping is looked up by path and may say nothing.
    fn id_at(&mut self, path: &[String]) -> Result<Option<i32>> {
        let field_id = match self.field_ids {
            FieldIds::None => None,
            FieldIds::Explicit(mapping) => mapping.field_id(path),
            FieldIds::Auto => {
                let field_id = self.next_auto;
                self.next_auto += 1;
                Some(field_id)
            }
        };

        if let Some(field_id) = field_id {
            if !self.assigned.insert(field_id) {
                return Err(format!("duplicate field id {field_id} in \"field_ids\""));
            }
        }

        Ok(field_id)
    }

    /// A nested field's own id, which lives under `__root_field_id` so that the rest of the
    /// mapping at that level can name its children.
    fn root_id_at(&mut self, path: &mut Vec<String>) -> Result<Option<i32>> {
        path.push(ROOT_FIELD_ID.to_string());
        let field_id = self.id_at(path);
        path.pop();

        field_id
    }

    fn field(&mut self, field: &FieldRef, path: &mut Vec<String>) -> Result<FieldRef> {
        path.push(field.name().clone());

        let stamped = match field.data_type() {
            DataType::Struct(children) => {
                let field_id = self.root_id_at(path)?;
                let children = self.children(children, path)?;

                with_id(
                    field
                        .as_ref()
                        .clone()
                        .with_data_type(DataType::Struct(children)),
                    field_id,
                )
            }
            DataType::List(element) => {
                let field_id = self.root_id_at(path)?;
                let element = self.field(element, path)?;

                with_id(
                    field
                        .as_ref()
                        .clone()
                        .with_data_type(DataType::List(element)),
                    field_id,
                )
            }
            DataType::LargeList(element) => {
                let field_id = self.root_id_at(path)?;
                let element = self.field(element, path)?;

                with_id(
                    field
                        .as_ref()
                        .clone()
                        .with_data_type(DataType::LargeList(element)),
                    field_id,
                )
            }
            DataType::Map(entries, keys_sorted) => {
                let field_id = self.root_id_at(path)?;

                // the entries struct is not a level of the mapping: its key and val sit
                // directly under the map's name, and it carries the map's own id
                let DataType::Struct(entry_fields) = entries.data_type() else {
                    return Err(format!(
                        "the entries of map field \"{}\" are not a struct",
                        field.name()
                    ));
                };

                let entry_fields = self.children(entry_fields, path)?;
                let entries = with_id(
                    entries
                        .as_ref()
                        .clone()
                        .with_data_type(DataType::Struct(entry_fields)),
                    field_id,
                );

                with_id(
                    field
                        .as_ref()
                        .clone()
                        .with_data_type(DataType::Map(entries, *keys_sorted)),
                    field_id,
                )
            }
            // everything else is a leaf: its id is the one at its own path
            _ => with_id(field.as_ref().clone(), self.id_at(path)?),
        };

        path.pop();

        Ok(stamped)
    }

    fn children(&mut self, children: &Fields, path: &mut Vec<String>) -> Result<Fields> {
        Ok(children
            .iter()
            .map(|child| self.field(child, path))
            .collect::<Result<Vec<_>>>()?
            .into())
    }
}

/// Appends the id to a field's metadata, leaving the rest of it alone.
fn with_id(field: Field, field_id: Option<i32>) -> FieldRef {
    let Some(field_id) = field_id else {
        return field.into();
    };

    let mut metadata = field.metadata().clone();
    metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), field_id.to_string());

    field.with_metadata(metadata).into()
}
