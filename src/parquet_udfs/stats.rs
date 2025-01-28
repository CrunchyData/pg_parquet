use std::{collections::HashMap, ffi::CStr, fmt::Write};

use ::parquet::{
    basic::{ConvertedType, LogicalType},
    file::statistics::Statistics,
    schema::types::ColumnDescriptor,
};
use pgrx::{
    iter::TableIterator,
    name, pg_extern, pg_schema,
    pg_sys::{getTypeOutputInfo, InvalidOid, OidOutputFunctionCall},
    IntoDatum,
};

use crate::{
    arrow_parquet::uri_utils::{
        ensure_access_privilege_to_uri, parquet_metadata_from_uri, parse_uri,
    },
    type_compat::pg_arrow_type_conversions::{
        i128_to_numeric, i32_to_date, i64_to_time, i64_to_timestamp, i64_to_timestamptz,
        i64_to_timetz, make_numeric_typmod,
    },
};

#[pg_schema]
mod parquet {
    use super::*;

    #[pg_extern]
    #[allow(clippy::type_complexity)]
    fn column_stats(
        uri: String,
    ) -> TableIterator<
        'static,
        (
            name!(field_id, i32),
            name!(stats_min, Option<String>),
            name!(stats_max, Option<String>),
            name!(stats_null_count, Option<i64>),
            name!(stats_distinct_count, Option<i64>),
        ),
    > {
        let uri = parse_uri(&uri);

        ensure_access_privilege_to_uri(&uri, true);
        let parquet_metadata = parquet_metadata_from_uri(&uri);

        let mut column_stats = HashMap::new();
        let mut column_descriptors = HashMap::new();

        for row_group in parquet_metadata.row_groups().iter() {
            for column in row_group.columns().iter() {
                if !column
                    .column_descr_ptr()
                    .self_type()
                    .get_basic_info()
                    .has_id()
                {
                    continue;
                }

                let field_id = column.column_descr_ptr().self_type().get_basic_info().id();

                column_descriptors
                    .entry(field_id)
                    .or_insert(column.column_descr());

                // column statistics exist for each leaf column per row group
                column_stats
                    .entry(field_id)
                    .or_insert_with(Vec::new)
                    .push(column.statistics());
            }
        }

        let mut stats_rows = Vec::new();

        for (field_id, stats) in column_stats.iter_mut() {
            let column_descriptor = column_descriptors
                .get(field_id)
                .expect("column descriptor not found");

            stats_rows.push((
                *field_id,
                stats_min_value_aggregated_by_row_groups(stats, column_descriptor),
                stats_max_value_aggregated_by_row_groups(stats, column_descriptor),
                stats_null_count_aggregated_by_row_groups(stats),
                stats_distinct_count_aggregated_by_row_groups(stats),
            ));
        }

        TableIterator::new(stats_rows)
    }
}

fn stats_null_count_aggregated_by_row_groups(stats: &[Option<&Statistics>]) -> Option<i64> {
    let mut null_count_sum = None;

    for stat in stats.iter().flatten() {
        if let Some(null_count) = stat.null_count_opt() {
            null_count_sum = match null_count_sum {
                Some(sum) => Some(sum + null_count as i64),
                None => Some(null_count as i64),
            };
        }
    }

    null_count_sum
}

fn stats_distinct_count_aggregated_by_row_groups(stats: &[Option<&Statistics>]) -> Option<i64> {
    let mut distinct_count_sum = None;

    for stat in stats.iter().flatten() {
        if let Some(distinct_count) = stat.distinct_count_opt() {
            distinct_count_sum = match distinct_count_sum {
                Some(sum) => Some(sum + distinct_count as i64),
                None => Some(distinct_count as i64),
            };
        }
    }

    distinct_count_sum
}

fn stats_min_value_aggregated_by_row_groups(
    row_group_stats: &[Option<&Statistics>],
    column_descriptor: &ColumnDescriptor,
) -> Option<String> {
    let mut min_value = None;

    for stat in row_group_stats.iter().flatten() {
        if let Some(current_min_value) = min_value {
            min_value = Some(stats_min_of_two(current_min_value, stat));
        } else {
            min_value = Some(*stat);
        }
    }

    min_value.and_then(|v| stats_min_value_to_pg_str(v, column_descriptor))
}

fn stats_max_value_aggregated_by_row_groups(
    row_group_stats: &[Option<&Statistics>],
    column_descriptor: &ColumnDescriptor,
) -> Option<String> {
    let mut max_value = None;

    for stat in row_group_stats.iter().flatten() {
        if let Some(current_max_value) = max_value {
            max_value = Some(stats_max_of_two(current_max_value, stat));
        } else {
            max_value = Some(*stat);
        }
    }

    max_value.and_then(|v| stats_max_value_to_pg_str(v, column_descriptor))
}

pub(crate) fn stats_min_value_to_pg_str(
    statistics: &Statistics,
    column_descriptor: &ColumnDescriptor,
) -> Option<String> {
    let logical_type = column_descriptor.logical_type();

    let converted_type = column_descriptor.converted_type();

    let is_string = matches!(logical_type, Some(LogicalType::String))
        || matches!(converted_type, ConvertedType::UTF8);

    let is_date = matches!(logical_type, Some(LogicalType::Date))
        || matches!(converted_type, ConvertedType::DATE);

    let is_timestamp = matches!(
        logical_type,
        Some(LogicalType::Timestamp {
            is_adjusted_to_u_t_c,
            ..
        }) if !is_adjusted_to_u_t_c
    );

    let is_timestamptz = matches!(
        logical_type,
        Some(LogicalType::Timestamp {
            is_adjusted_to_u_t_c,
            ..
        }) if is_adjusted_to_u_t_c
    );

    let is_time = matches!(
        logical_type,
        Some(LogicalType::Time {
            is_adjusted_to_u_t_c,
            ..
        }) if !is_adjusted_to_u_t_c
    );

    let is_timetz = matches!(
        logical_type,
        Some(LogicalType::Time {
            is_adjusted_to_u_t_c,
            ..
        }) if is_adjusted_to_u_t_c
    );

    let is_numeric = matches!(logical_type, Some(LogicalType::Decimal { .. }))
        || matches!(converted_type, ConvertedType::DECIMAL);

    match statistics {
        Statistics::Boolean(statistics) => statistics.min_opt().map(|v| v.to_string()),
        Statistics::Int32(statistics) => statistics.min_opt().map(|v| {
            if is_date {
                pg_format(i32_to_date(*v))
            } else if is_numeric {
                pg_format_numeric(*v as i128, column_descriptor)
            } else {
                v.to_string()
            }
        }),
        Statistics::Int64(statistics) => statistics.min_opt().map(|v| {
            if is_timestamp {
                pg_format(i64_to_timestamp(*v))
            } else if is_timestamptz {
                pg_format(i64_to_timestamptz(*v, "UTC"))
            } else if is_numeric {
                pg_format_numeric(*v as i128, column_descriptor)
            } else if is_time {
                pg_format(i64_to_time(*v))
            } else if is_timetz {
                pg_format(i64_to_timetz(*v))
            } else {
                v.to_string()
            }
        }),
        Statistics::Int96(statistics) => statistics.min_opt().map(|v| v.to_string()),
        Statistics::Float(statistics) => statistics.min_opt().map(|v| v.to_string()),
        Statistics::Double(statistics) => statistics.min_opt().map(|v| v.to_string()),
        Statistics::ByteArray(statistics) => statistics.min_opt().map(|v| {
            if is_string {
                v.as_utf8()
                    .unwrap_or_else(|e| panic!("cannot convert stats to utf8 {e}"))
                    .to_string()
            } else {
                hex_encode(v.data())
            }
        }),
        Statistics::FixedLenByteArray(statistics) => statistics.min_opt().map(|v| {
            if is_string {
                v.as_utf8()
                    .unwrap_or_else(|e| panic!("cannot convert stats to utf8 {e}"))
                    .to_string()
            } else if is_numeric {
                let mut numeric_bytes: [u8; 16] = [0; 16];

                let offset = numeric_bytes.len() - v.data().len();
                numeric_bytes[offset..].copy_from_slice(v.data());

                let numeric = i128::from_be_bytes(numeric_bytes);

                pg_format_numeric(numeric, column_descriptor)
            } else {
                hex_encode(v.data())
            }
        }),
    }
}

pub(crate) fn stats_max_value_to_pg_str(
    statistics: &Statistics,
    column_descriptor: &ColumnDescriptor,
) -> Option<String> {
    let logical_type = column_descriptor.logical_type();

    let converted_type = column_descriptor.converted_type();

    let is_string = matches!(logical_type, Some(LogicalType::String))
        || matches!(converted_type, ConvertedType::UTF8);

    let is_date = matches!(logical_type, Some(LogicalType::Date))
        || matches!(converted_type, ConvertedType::DATE);

    let is_timestamp = matches!(
        logical_type,
        Some(LogicalType::Timestamp {
            is_adjusted_to_u_t_c,
            ..
        }) if !is_adjusted_to_u_t_c
    );

    let is_timestamptz = matches!(
        logical_type,
        Some(LogicalType::Timestamp {
            is_adjusted_to_u_t_c,
            ..
        }) if is_adjusted_to_u_t_c
    );

    let is_time = matches!(
        logical_type,
        Some(LogicalType::Time {
            is_adjusted_to_u_t_c,
            ..
        }) if !is_adjusted_to_u_t_c
    );

    let is_timetz = matches!(
        logical_type,
        Some(LogicalType::Time {
            is_adjusted_to_u_t_c,
            ..
        }) if is_adjusted_to_u_t_c
    );

    let is_numeric = matches!(logical_type, Some(LogicalType::Decimal { .. }))
        || matches!(converted_type, ConvertedType::DECIMAL);

    match statistics {
        Statistics::Boolean(statistics) => statistics.max_opt().map(|v| v.to_string()),
        Statistics::Int32(statistics) => statistics.max_opt().map(|v| {
            if is_date {
                pg_format(i32_to_date(*v))
            } else if is_numeric {
                pg_format_numeric(*v as i128, column_descriptor)
            } else {
                v.to_string()
            }
        }),
        Statistics::Int64(statistics) => statistics.max_opt().map(|v| {
            if is_timestamp {
                pg_format(i64_to_timestamp(*v))
            } else if is_timestamptz {
                pg_format(i64_to_timestamptz(*v, "UTC"))
            } else if is_numeric {
                pg_format_numeric(*v as i128, column_descriptor)
            } else if is_time {
                pg_format(i64_to_time(*v))
            } else if is_timetz {
                pg_format(i64_to_timetz(*v))
            } else {
                v.to_string()
            }
        }),
        Statistics::Int96(statistics) => statistics.max_opt().map(|v| v.to_string()),
        Statistics::Float(statistics) => statistics.max_opt().map(|v| v.to_string()),
        Statistics::Double(statistics) => statistics.max_opt().map(|v| v.to_string()),
        Statistics::ByteArray(statistics) => statistics.max_opt().map(|v| {
            if is_string {
                v.as_utf8()
                    .unwrap_or_else(|e| panic!("cannot convert stats to utf8 {e}"))
                    .to_string()
            } else {
                hex_encode(v.data())
            }
        }),
        Statistics::FixedLenByteArray(statistics) => statistics.max_opt().map(|v| {
            if is_string {
                v.as_utf8()
                    .unwrap_or_else(|e| panic!("cannot convert stats to utf8 {e}"))
                    .to_string()
            } else if is_numeric {
                let mut numeric_bytes: [u8; 16] = [0; 16];

                let offset = numeric_bytes.len() - v.data().len();
                numeric_bytes[offset..].copy_from_slice(v.data());

                let numeric = i128::from_be_bytes(numeric_bytes);

                pg_format_numeric(numeric, column_descriptor)
            } else {
                hex_encode(v.data())
            }
        }),
    }
}

macro_rules! stats_max_helper {
    ($stats_a:ident, $stats_b:ident, $a_max_val:ident, $b_max_val:ident) => {
        match ($a_max_val.max_opt(), $b_max_val.max_opt()) {
            (Some(a), Some(b)) => {
                if *a > *b {
                    $stats_a
                } else {
                    $stats_b
                }
            }
            (Some(_), None) => $stats_a,
            (None, Some(_)) => $stats_b,
            (None, None) => $stats_a,
        }
    };
}

macro_rules! stats_min_helper {
    ($stats_a:ident, $stats_b:ident, $a_min_val:ident, $b_min_val:ident) => {
        match ($a_min_val.min_opt(), $b_min_val.min_opt()) {
            (Some(a), Some(b)) => {
                if *a < *b {
                    $stats_a
                } else {
                    $stats_b
                }
            }
            (Some(_), None) => $stats_a,
            (None, Some(_)) => $stats_b,
            (None, None) => $stats_a,
        }
    };
}

fn stats_max_of_two<'b, 'a: 'b>(
    stats_a: &'a Statistics,
    stats_b: &'b Statistics,
) -> &'b Statistics {
    match (stats_a, stats_b) {
        (Statistics::Boolean(a_max), Statistics::Boolean(b_max)) => {
            stats_max_helper!(stats_a, stats_b, a_max, b_max)
        }
        (Statistics::Int32(a_max), Statistics::Int32(b_max)) => {
            stats_max_helper!(stats_a, stats_b, a_max, b_max)
        }
        (Statistics::Int64(a_max), Statistics::Int64(b_max)) => {
            stats_max_helper!(stats_a, stats_b, a_max, b_max)
        }
        (Statistics::Int96(a_max), Statistics::Int96(b_max)) => {
            stats_max_helper!(stats_a, stats_b, a_max, b_max)
        }
        (Statistics::Float(a_max), Statistics::Float(b_max)) => {
            stats_max_helper!(stats_a, stats_b, a_max, b_max)
        }
        (Statistics::Double(a_max), Statistics::Double(b_max)) => {
            stats_max_helper!(stats_a, stats_b, a_max, b_max)
        }
        (Statistics::ByteArray(a_max), Statistics::ByteArray(b_max)) => {
            stats_max_helper!(stats_a, stats_b, a_max, b_max)
        }
        (Statistics::FixedLenByteArray(a_max), Statistics::FixedLenByteArray(b_max)) => {
            stats_max_helper!(stats_a, stats_b, a_max, b_max)
        }
        _ => panic!("unexpected statistics comparison"),
    }
}

fn stats_min_of_two<'b, 'a: 'b>(
    stats_a: &'a Statistics,
    stats_b: &'b Statistics,
) -> &'b Statistics {
    match (stats_a, stats_b) {
        (Statistics::Boolean(a_min), Statistics::Boolean(b_min)) => {
            stats_min_helper!(stats_a, stats_b, a_min, b_min)
        }
        (Statistics::Int32(a_min), Statistics::Int32(b_min)) => {
            stats_min_helper!(stats_a, stats_b, a_min, b_min)
        }
        (Statistics::Int64(a_min), Statistics::Int64(b_min)) => {
            stats_min_helper!(stats_a, stats_b, a_min, b_min)
        }
        (Statistics::Int96(a_min), Statistics::Int96(b_min)) => {
            stats_min_helper!(stats_a, stats_b, a_min, b_min)
        }
        (Statistics::Float(a_min), Statistics::Float(b_min)) => {
            stats_min_helper!(stats_a, stats_b, a_min, b_min)
        }
        (Statistics::Double(a_min), Statistics::Double(b_min)) => {
            stats_min_helper!(stats_a, stats_b, a_min, b_min)
        }
        (Statistics::ByteArray(a_min), Statistics::ByteArray(b_min)) => {
            stats_min_helper!(stats_a, stats_b, a_min, b_min)
        }
        (Statistics::FixedLenByteArray(a_min), Statistics::FixedLenByteArray(b_min)) => {
            stats_min_helper!(stats_a, stats_b, a_min, b_min)
        }
        _ => panic!("unexpected statistics comparison"),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().fold("\\x".into(), |mut output, b| {
        let _ = write!(output, "{b:02X}");
        output
    })
}

fn pg_format<T: IntoDatum>(val: T) -> String {
    let mut typoutput_func = InvalidOid;
    let mut varlena = false;
    unsafe {
        getTypeOutputInfo(T::type_oid(), &mut typoutput_func, &mut varlena);

        let output = OidOutputFunctionCall(
            typoutput_func,
            val.into_datum().expect("invalid stats datum"),
        );

        CStr::from_ptr(output)
            .to_str()
            .expect("invalid stats string")
            .to_string()
    }
}

fn pg_format_numeric(numeric: i128, column_descriptor: &ColumnDescriptor) -> String {
    let precision = column_descriptor.type_precision();

    let scale = column_descriptor.type_scale();

    let typmod = make_numeric_typmod(precision, scale);

    pg_format(i128_to_numeric(
        numeric,
        precision as u32,
        scale as u32,
        typmod,
    ))
}
