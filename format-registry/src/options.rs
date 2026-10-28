//! The format-specific half of a `COPY ... WITH (...)` list.
//!
//! Postgres parses `WITH (...)` into a `List *` of `DefElem`, and `COPY` rejects an option it
//! does not recognize. Anything the base does not recognize either is assumed to belong to the
//! format and arrives here as a name and a value, both already flattened to text. A provider
//! validates names itself, because only it knows that `compression 'zstd'` is meaningful and
//! `row_group_size` is not.

use std::collections::BTreeMap;

use crate::Result;

/// The name of the file being written, which the base sets on a writer's options.
///
/// A provider is given no uri, deliberately — but a format whose *defaults* depend on the file
/// name needs that much of it, and parquet's do: `orders.parquet.zst` means zstd. So the base
/// sets this one, and a provider that has no use for it ignores it.
///
/// `__` is reserved for options the base sets this way. A statement cannot write one: an option
/// a `COPY` names is offered to `validate_option` first, and no provider recognizes a reserved
/// name.
pub const FILE_NAME_OPTION: &str = "__file_name";

/// The size the base is splitting the statement's files at, set on a writer's options when
/// `file_size_bytes` is in force and absent otherwise.
///
/// Also a thing a provider has no business knowing, with the same exception: a format whose own
/// buffering decides where a file may end needs to know how large that file is meant to be.
/// Parquet's does — a file ends at a row group boundary — so a row group larger than the target
/// would make every file larger than it.
pub const FILE_SIZE_BYTES_OPTION: &str = "__file_size_bytes";

/// Format-specific options, in the order Postgres saw them.
///
/// Ordered, not a hash map, because the error message for an unrecognized option should name
/// the first one the user wrote rather than an arbitrary one.
#[derive(Debug, Clone, Default)]
pub struct FormatOptions {
    entries: Vec<(String, String)>,
}

impl FormatOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, name: impl Into<String>, value: impl Into<String>) {
        self.entries.push((name.into(), value.into()));
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.entries
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
    }

    /// The last value given for an option. Last, not first, because `COPY` itself lets a
    /// repeated option win on the right.
    pub fn get(&self, name: &str) -> Option<&str> {
        self.entries
            .iter()
            .rev()
            .find(|(option, _)| option.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    /// An option parsed as one of a fixed set of spellings, case-insensitively. The error names
    /// what was allowed, since a provider's own list is the only place that knows.
    pub fn get_enum(&self, name: &str, allowed: &[&str]) -> Result<Option<String>> {
        let Some(value) = self.get(name) else {
            return Ok(None);
        };

        allowed
            .iter()
            .find(|candidate| candidate.eq_ignore_ascii_case(value))
            .map(|candidate| Some(candidate.to_string()))
            .ok_or_else(|| {
                format!(
                    "\"{value}\" is not a recognized value for {name}; expected one of {}",
                    allowed.join(", ")
                )
            })
    }

    pub fn get_bool(&self, name: &str) -> Result<Option<bool>> {
        let Some(value) = self.get(name) else {
            return Ok(None);
        };

        // the spellings Postgres' own parse_bool accepts, so that a format option behaves like
        // a core COPY option
        match value.to_ascii_lowercase().as_str() {
            "t" | "true" | "y" | "yes" | "on" | "1" => Ok(Some(true)),
            "f" | "false" | "n" | "no" | "off" | "0" => Ok(Some(false)),
            _ => Err(format!("{name} requires a Boolean value, not \"{value}\"")),
        }
    }

    pub fn get_usize(&self, name: &str) -> Result<Option<usize>> {
        let Some(value) = self.get(name) else {
            return Ok(None);
        };

        value
            .parse::<usize>()
            .map(Some)
            .map_err(|_| format!("{name} requires a non-negative integer, not \"{value}\""))
    }

    /// The options as a map, for a provider that would rather not care about order.
    pub fn to_map(&self) -> BTreeMap<String, String> {
        self.entries
            .iter()
            .map(|(name, value)| (name.to_ascii_lowercase(), value.clone()))
            .collect()
    }
}

impl<N: Into<String>, V: Into<String>> FromIterator<(N, V)> for FormatOptions {
    fn from_iter<I: IntoIterator<Item = (N, V)>>(iter: I) -> Self {
        Self {
            entries: iter
                .into_iter()
                .map(|(name, value)| (name.into(), value.into()))
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_repeated_option_wins_on_the_right() {
        let options =
            FormatOptions::from_iter([("compression", "snappy"), ("compression", "zstd")]);

        assert_eq!(options.get("compression"), Some("zstd"));
        assert_eq!(options.get("COMPRESSION"), Some("zstd"));
        assert_eq!(options.get("codec"), None);
    }

    #[test]
    fn an_unrecognized_enum_value_names_what_was_allowed() {
        let options = FormatOptions::from_iter([("compression", "brotlli")]);

        let error = options
            .get_enum("compression", &["zstd", "lz4"])
            .expect_err("a misspelled value is rejected");

        assert!(error.contains("brotlli"), "{error}");
        assert!(error.contains("zstd, lz4"), "{error}");
    }

    #[test]
    fn booleans_accept_what_copy_accepts() {
        for spelling in ["on", "TRUE", "y", "1"] {
            let options = FormatOptions::from_iter([("preserve_order", spelling)]);
            assert_eq!(options.get_bool("preserve_order").unwrap(), Some(true));
        }

        let options = FormatOptions::from_iter([("preserve_order", "maybe")]);
        assert!(options.get_bool("preserve_order").is_err());
    }
}
