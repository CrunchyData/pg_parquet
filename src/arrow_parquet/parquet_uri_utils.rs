use std::{str::FromStr, sync::Arc};

use arrow::datatypes::SchemaRef;
use object_store::aws::AmazonS3Builder;
use object_store::{local::LocalFileSystem, path::Path, ObjectStore};
use parquet::{
    arrow::{
        async_reader::{ParquetObjectReader, ParquetRecordBatchStream},
        AsyncArrowWriter, ParquetRecordBatchStreamBuilder,
    },
    file::properties::WriterProperties,
};

use crate::parquet_copy_hook::copy_utils::DEFAULT_ROW_GROUP_SIZE;

use super::parquet_object_writer::ParquetObjectWriter;

enum UriFormat {
    File,
    S3,
}

impl FromStr for UriFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with("file://") {
            return Ok(UriFormat::File);
        } else if s.starts_with("s3://") {
            return Ok(UriFormat::S3);
        }

        Err(format!(
            "unsupported uri {}. Only URIs with prefix file:// or s3:// are supported.",
            s
        ))
    }
}

fn parse_bucket_and_key(uri: &str) -> (String, String) {
    let uri = uri.strip_prefix("s3://").unwrap();

    let mut parts = uri.splitn(2, '/');
    let bucket = parts.next().expect("bucket not found in uri");
    let key = parts.next().expect("key not found in uri");

    (bucket.to_string(), key.to_string())
}

pub(crate) async fn parquet_reader_from_uri(
    uri: &str,
) -> ParquetRecordBatchStream<ParquetObjectReader> {
    let uri_format = UriFormat::from_str(&uri).unwrap_or_else(|e| panic!("{}", e));

    let parquet_object_reader = match uri_format {
        UriFormat::File => {
            let uri = uri.strip_prefix("file://").unwrap();
            let storage_container = Arc::new(LocalFileSystem::new());
            let location = Path::from_filesystem_path(uri).unwrap();
            let meta = storage_container.head(&location).await.unwrap();
            ParquetObjectReader::new(storage_container, meta)
        }
        UriFormat::S3 => {
            let (bucket, key) = parse_bucket_and_key(&uri);
            let storage_container = Arc::new(
                AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .build()
                    .unwrap(),
            );
            let location = Path::from(key);
            let meta = storage_container.head(&location).await.unwrap();
            ParquetObjectReader::new(storage_container, meta)
        }
    };

    let builder = ParquetRecordBatchStreamBuilder::new(parquet_object_reader)
        .await
        .unwrap();
    pgrx::debug2!("Converted arrow schema is: {}", builder.schema());

    builder
        .with_batch_size(DEFAULT_ROW_GROUP_SIZE as usize)
        .build()
        .unwrap()
}

pub(crate) async fn parquet_writer_from_uri(
    uri: &str,
    arrow_schema: SchemaRef,
    writer_props: WriterProperties,
) -> AsyncArrowWriter<ParquetObjectWriter> {
    let uri_format = UriFormat::from_str(&uri).unwrap_or_else(|e| panic!("{}", e));

    let parquet_object_writer = match uri_format {
        UriFormat::File => {
            let uri = uri.strip_prefix("file://").unwrap();

            // create if not exists
            std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(uri)
                .unwrap();

            let storage_container = Arc::new(LocalFileSystem::new());
            let location = Path::from_filesystem_path(uri).unwrap();
            ParquetObjectWriter::new(storage_container, location)
        }
        UriFormat::S3 => {
            let (bucket, key) = parse_bucket_and_key(&uri);
            let storage_container = Arc::new(
                AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .build()
                    .unwrap(),
            );
            let location = Path::from(key);
            ParquetObjectWriter::new(storage_container, location)
        }
    };

    AsyncArrowWriter::try_new(parquet_object_writer, arrow_schema, Some(writer_props)).unwrap()
}
