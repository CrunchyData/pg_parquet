use std::str::FromStr;

use arrow_schema::Fields;

use crate::pgrx_utils::truncate_identifier_like_postgres;

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) enum MatchBy {
    #[default]
    Position,
    Name,
}

impl FromStr for MatchBy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "position" => Ok(MatchBy::Position),
            "name" => Ok(MatchBy::Name),
            _ => Err(format!("unrecognized match_by method: {s}")),
        }
    }
}

// column_index_by_name is the position of the field that match_by 'name' matches the attribute of
// the given name with. A field of the file is matched by its name truncated the way PostgreSQL
// truncates an identifier, since the attribute name it is held against went through the same
// truncation and a name longer than the identifier limit could never be matched otherwise.
// Truncating a name that is short enough leaves it as it is, so every other field is matched by
// its own name, and the nested fields of a struct are matched this way already.
pub(crate) fn column_index_by_name(fields: &Fields, attribute_name: &str) -> Option<usize> {
    let mut matches = fields
        .iter()
        .enumerate()
        .filter(|(_, field)| {
            truncate_identifier_like_postgres(field.name(), false) == attribute_name
        })
        .map(|(field_idx, _)| field_idx);

    let column_idx = matches.next()?;

    // arrow lets a schema have two fields with the same name, and two long names can also end up
    // equal once they are truncated, so which of them the attribute is read from would be a matter
    // of the order the file happens to hold them in
    if matches.next().is_some() {
        panic!(
            "parquet file has more than one field named \"{attribute_name}\". \
             Try COPY FROM parquet WITH (match_by 'position')."
        );
    }

    Some(column_idx)
}
