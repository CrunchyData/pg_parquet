use std::{collections::HashMap, fmt::Display, str::FromStr};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default)]
pub(crate) enum FieldIds {
    #[default]
    None,
    Auto,
    Explicit(FieldIdMapping),
}

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
    pub(crate) fn field_id(&self, field_path: &[String]) -> Option<i32> {
        if field_path.is_empty() {
            panic!("Field path is empty");
        }

        let field_name = &field_path[0];

        match self.fields.get(field_name) {
            Some(FieldIdMappingItem::FieldId(field_id)) => Some(*field_id),
            Some(FieldIdMappingItem::FieldIdMapping(field_id_mapping)) => {
                field_id_mapping.field_id(&field_path[1..])
            }
            None => None,
        }
    }
}

pub(crate) fn field_id_mapping_from_json_string(
    json_string: &str,
) -> Result<FieldIdMapping, String> {
    serde_json::from_str(json_string).map_err(|_| "invalid JSON string for field_ids".into())
}

fn field_id_mapping_to_json_string(field_id_mapping: &FieldIdMapping) -> String {
    serde_json::to_string(field_id_mapping).unwrap()
}
