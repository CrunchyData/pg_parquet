use crate::arrow_parquet::uri_utils::parquet_metadata_from_uri;

use parquet::file::statistics::Statistics;
use pgrx::{iter::TableIterator, name, pg_extern, pg_schema};

#[pg_schema]
mod pgparquet {
    use super::*;

    #[pg_extern]
    #[allow(clippy::type_complexity)]
    fn metadata(
        uri: String,
    ) -> TableIterator<
        'static,
        (
            name!(filename, String),
            name!(row_group_id, i64),
            name!(row_group_num_rows, i64),
            name!(row_group_num_columns, i64),
            name!(row_group_bytes, i64),
            name!(column_id, i64),
            name!(file_offset, i64),
            name!(num_values, i64),
            name!(path_in_schema, String),
            name!(type_name, String),
            name!(stats_null_count, Option<i64>),
            name!(stats_distinct_count, Option<i64>),
            name!(stats_min, Option<String>),
            name!(stats_max, Option<String>),
            name!(compression, String),
            name!(encodings, String),
            name!(index_page_offset, Option<i64>),
            name!(dictionary_page_offset, Option<i64>),
            name!(data_page_offset, i64),
            name!(total_compressed_size, i64),
            name!(total_uncompressed_size, i64),
        ),
    > {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let parquet_metadata = runtime.block_on(parquet_metadata_from_uri(&uri));

        let mut rows = vec![];

        for (row_group_id, row_group) in parquet_metadata.row_groups().iter().enumerate() {
            let row_group_num_rows = row_group.num_rows();
            let row_group_num_columns = row_group.num_columns() as i64;
            let row_group_bytes = row_group.total_byte_size();

            for (column_id, column) in row_group.columns().iter().enumerate() {
                let file_offset = column.file_offset();

                let num_values = column.num_values();

                let path_in_schema = column.column_path().string();

                let type_name = column.column_type().to_string();

                let mut stats_min = None;
                let mut stats_max = None;
                let mut stats_null_count = None;
                let mut stats_distinct_count = None;

                if let Some(statistics) = column.statistics() {
                    if statistics.has_min_max_set() {
                        stats_min = Some(stats_min_value_to_str(statistics));
                        stats_max = Some(stats_max_value_to_str(statistics));
                    }

                    if statistics.has_nulls() {
                        stats_null_count = Some(statistics.null_count() as i64);
                    }

                    stats_distinct_count = statistics.distinct_count().map(|v| v as i64);
                }

                let compression = column.compression().to_string();

                let encodings = column
                    .encodings()
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join(",");

                let index_page_offset = column.index_page_offset();

                let dictionary_page_offset = column.dictionary_page_offset();

                let data_page_offset = column.data_page_offset();

                let total_compressed_size = column.compressed_size();

                let total_uncompressed_size = column.uncompressed_size();

                let row = (
                    uri.clone(),
                    row_group_id as i64,
                    row_group_num_rows,
                    row_group_num_columns,
                    row_group_bytes,
                    column_id as i64,
                    file_offset,
                    num_values,
                    path_in_schema,
                    type_name,
                    stats_null_count,
                    stats_distinct_count,
                    stats_min,
                    stats_max,
                    compression,
                    encodings,
                    index_page_offset,
                    dictionary_page_offset,
                    data_page_offset,
                    total_compressed_size,
                    total_uncompressed_size,
                );

                rows.push(row);
            }
        }

        TableIterator::new(rows)
    }

    #[pg_extern]
    fn file_metadata(
        uri: String,
    ) -> TableIterator<
        'static,
        (
            name!(filename, String),
            name!(created_by, Option<String>),
            name!(num_rows, i64),
            name!(num_row_groups, i64),
            name!(format_version, String),
        ),
    > {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let parquet_metadata = runtime.block_on(parquet_metadata_from_uri(&uri));

        let filename = uri;

        let created_by = parquet_metadata
            .file_metadata()
            .created_by()
            .map(|c| c.to_string());

        let num_rows = parquet_metadata.file_metadata().num_rows();

        let num_row_groups = parquet_metadata.num_row_groups() as i64;

        let format_version = parquet_metadata.file_metadata().version().to_string();

        let row = (
            filename,
            created_by,
            num_rows,
            num_row_groups,
            format_version,
        );

        TableIterator::new(vec![row])
    }

    #[pg_extern]
    fn kv_metadata(
        uri: String,
    ) -> TableIterator<
        'static,
        (
            name!(filename, String),
            name!(key, Vec<u8>),
            name!(value, Option<Vec<u8>>),
        ),
    > {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let parquet_metadata = runtime.block_on(parquet_metadata_from_uri(&uri));
        let kv_metadata = parquet_metadata.file_metadata().key_value_metadata();

        if kv_metadata.is_none() {
            return TableIterator::new(vec![]);
        }

        let kv_metadata = kv_metadata.unwrap();

        let mut rows = vec![];

        for kv in kv_metadata {
            let key = kv.key.as_bytes().to_owned();
            let value = kv.value.as_ref().map(|v| v.as_bytes().to_owned());

            let row = (uri.clone(), key, value);

            rows.push(row);
        }

        TableIterator::new(rows)
    }
}

fn stats_min_value_to_str(statistics: &Statistics) -> String {
    match statistics {
        Statistics::Boolean(statistics) => statistics.min().to_string(),
        Statistics::Int32(statistics) => statistics.min().to_string(),
        Statistics::Int64(statistics) => statistics.min().to_string(),
        Statistics::Int96(statistics) => statistics.min().to_string(),
        Statistics::Float(statistics) => statistics.min().to_string(),
        Statistics::Double(statistics) => statistics.min().to_string(),
        Statistics::ByteArray(statistics) => match statistics.min().as_utf8() {
            Ok(v) => v.to_string(),
            Err(_) => statistics.min().to_string(),
        },
        Statistics::FixedLenByteArray(statistics) => statistics.min().to_string(),
    }
}

fn stats_max_value_to_str(statistics: &Statistics) -> String {
    match statistics {
        Statistics::Boolean(statistics) => statistics.max().to_string(),
        Statistics::Int32(statistics) => statistics.max().to_string(),
        Statistics::Int64(statistics) => statistics.max().to_string(),
        Statistics::Int96(statistics) => statistics.max().to_string(),
        Statistics::Float(statistics) => statistics.max().to_string(),
        Statistics::Double(statistics) => statistics.max().to_string(),
        Statistics::ByteArray(statistics) => match statistics.max().as_utf8() {
            Ok(v) => v.to_string(),
            Err(_) => statistics.max().to_string(),
        },
        Statistics::FixedLenByteArray(statistics) => statistics.max().to_string(),
    }
}
