use std::{collections::HashMap, fmt::Display, str::FromStr};

use arrow_schema::{DataType, Field, FieldRef, Schema};
use serde::{Deserialize, Serialize};

/// The key that gives a nested field its own id in an explicit field id mapping. It shares a
/// namespace with the names of the fields nested in it, so a nested field cannot have that name.
pub(crate) const ROOT_FIELD_ID_KEY: &str = "__root_field_id";

#[derive(Debug, Clone, Default)]
pub(crate) enum FieldIds {
    #[default]
    None,
    Auto,
    Explicit(FieldIdMapping),
}

/// Implements parsing for the field_ids option in COPY .. TO statements
impl FromStr for FieldIds {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "none" => Ok(FieldIds::None),
            "auto" => Ok(FieldIds::Auto),
            field_ids => Ok(FieldIds::Explicit(field_id_mapping_from_json_string(
                field_ids,
            )?)),
        }
    }
}

impl Display for FieldIds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FieldIds::None => write!(f, "none"),
            FieldIds::Auto => write!(f, "auto"),
            FieldIds::Explicit(field_id_mapping) => {
                write!(f, "{}", field_id_mapping_to_json_string(field_id_mapping))
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
enum FieldIdMappingItem {
    FieldId(i32),
    FieldIdMapping(FieldIdMapping),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct FieldIdMapping {
    #[serde(flatten)]
    fields: HashMap<String, FieldIdMappingItem>,
}

impl FieldIdMapping {
    /// Returns the field ID, if any, from `FieldIdMapping` for the given field path.
    pub(crate) fn field_id(&self, field_path: &[String]) -> Option<i32> {
        let (field_name, nested_path) = field_path.split_first()?;

        match self.fields.get(field_name) {
            // an id is the id of the field it names, and of no field nested in it, which would
            // hand the same id to the whole subtree of a struct, a list or a map
            Some(FieldIdMappingItem::FieldId(field_id)) => {
                nested_path.is_empty().then_some(*field_id)
            }
            Some(FieldIdMappingItem::FieldIdMapping(field_id_mapping)) => {
                field_id_mapping.field_id(nested_path)
            }
            None => None,
        }
    }

    /// Validates that every field name in the `FieldIdMapping` exists in the provided Arrow schema
    fn validate_against_schema(&self, arrow_schema: &Schema) -> Result<(), String> {
        // Build a map from field name to &Field for quick lookups
        let mut arrow_field_map = HashMap::new();
        for field in arrow_schema.fields() {
            arrow_field_map.insert(field.name().clone(), field);
        }

        // Check every field name in the JSON mapping
        for (field_name, mapping_item) in &self.fields {
            if field_name == ROOT_FIELD_ID_KEY {
                // Skip the root field, as it doesn't exist in the Arrow schema
                continue;
            }

            // Ensure the field exists in the Arrow schema
            let arrow_field = match arrow_field_map.get(field_name) {
                Some(f) => f,
                None => {
                    return Err(format!(
                    "Field '{}' in the mapping does not exist in the Arrow schema.\nAvailable fields: {:?}",
                    field_name,
                    arrow_schema
                        .fields()
                        .iter()
                        .map(|f| f.name())
                        .collect::<Vec<_>>()
                ));
                }
            };

            match mapping_item {
                // If the JSON item is an integer field ID, we're done, unless the field is a
                // nested one, which is given its own id by the "__root_field_id" key of a mapping
                FieldIdMappingItem::FieldId(_id) => {
                    if matches!(
                        arrow_field.data_type(),
                        DataType::Struct(_) | DataType::List(_) | DataType::Map(_, _)
                    ) {
                        return Err(format!(
                            "Field '{field_name}' in the mapping is a nested field, so it needs a \
                             mapping with a \"{ROOT_FIELD_ID_KEY}\" key rather than a field id."
                        ));
                    }
                }

                // If the JSON item is a nested mapping, we need to validate it
                FieldIdMappingItem::FieldIdMapping(mapping) => match arrow_field.data_type() {
                    DataType::Struct(subfields) => {
                        // We expect the JSON keys to include something like:
                        //   "__root_field_id": <int>,
                        //   "field_name": <int or nested mapping>

                        let subschema = Schema::new(subfields.clone());
                        mapping.validate_against_schema(&subschema)?;
                    }
                    DataType::List(element_field) => {
                        // We expect the JSON keys to include something like:
                        //   "__root_field_id": <int>,
                        //   "element": <int or nested mapping>
                        //

                        let element_schema = Schema::new(vec![element_field.clone()]);
                        mapping.validate_against_schema(&element_schema)?;
                    }
                    DataType::Map(entry_field, _) => {
                        // We expect the JSON keys to include something like:
                        //   "__root_field_id": <int>,
                        //   "key": <int or nested mapping>
                        //   "val": <int or nested mapping>

                        match entry_field.data_type() {
                            DataType::Struct(entry_fields) => {
                                let entry_schema = Schema::new(entry_fields.clone());
                                mapping.validate_against_schema(&entry_schema)?;
                            }
                            other_type => {
                                return Err(format!(
                                "Map entry field should be a struct, but got '{other_type:?}' for field '{field_name}'"
                            ));
                            }
                        };
                    }
                    other_type => {
                        return Err(format!(
                            "Field '{field_name}' in the mapping is of type '{other_type:?}', \
                             which has no nested fields to map. It needs a field id."
                        ));
                    }
                },
            }
        }

        Ok(())
    }
}

pub(crate) fn field_id_mapping_from_json_string(
    json_string: &str,
) -> Result<FieldIdMapping, String> {
    serde_json::from_str(json_string).map_err(|e| format!("invalid JSON string for field_ids: {e}"))
}

fn nested_fields(field: &Field) -> Vec<&FieldRef> {
    match field.data_type() {
        DataType::Struct(fields) => fields.iter().collect(),
        DataType::List(nested_field) | DataType::Map(nested_field, _) => vec![nested_field],
        _ => vec![],
    }
}

/// Rejects a nested field named after the `ROOT_FIELD_ID_KEY` key, whose mapping would be
/// ambiguous: it shares its path with the id of the field it is nested in.
fn validate_nested_field_names(field: &Field) -> Result<(), String> {
    for nested_field in nested_fields(field) {
        if nested_field.name() == ROOT_FIELD_ID_KEY {
            return Err(format!(
                "Field '{}' in the Arrow schema has a nested field named \
                 \"{ROOT_FIELD_ID_KEY}\", which cannot be given a field id since \"field_ids\" \
                 uses that key for the id of the field it is nested in.",
                field.name()
            ));
        }

        validate_nested_field_names(nested_field)?;
    }

    Ok(())
}

fn field_id_mapping_to_json_string(field_id_mapping: &FieldIdMapping) -> String {
    serde_json::to_string(field_id_mapping).unwrap()
}

/// Validate that every field name in the `FieldIdMapping` exists in the provided Arrow schema
/// when the `FieldIds` are explicitly specified.
pub(crate) fn validate_field_ids(
    field_ids: &FieldIds,
    arrow_schema: &Schema,
) -> Result<(), String> {
    match field_ids {
        FieldIds::None => Ok(()),
        FieldIds::Auto => Ok(()),
        FieldIds::Explicit(field_id_mapping) => {
            for field in arrow_schema.fields() {
                validate_nested_field_names(field)?;
            }

            field_id_mapping.validate_against_schema(arrow_schema)
        }
    }
}
