use std::{str::FromStr, sync::Arc};

use arrow::datatypes::SchemaRef;
use object_store::aws::AmazonS3Builder;
use object_store::{local::LocalFileSystem, path::Path, ObjectStore};
use parquet::arrow::arrow_to_parquet_schema;
use parquet::arrow::async_writer::ParquetObjectWriter;
use parquet::file::metadata::ParquetMetaData;
use parquet::schema::types::SchemaDescriptor;
use parquet::{
    arrow::{
        async_reader::{ParquetObjectReader, ParquetRecordBatchStream},
        AsyncArrowWriter, ParquetRecordBatchStreamBuilder,
    },
    file::properties::WriterProperties,
};

use crate::parquet_copy_hook::copy_utils::DEFAULT_ROW_GROUP_SIZE;

#[derive(Debug, PartialEq)]
enum UriFormat {
    File,
    S3,
}

impl FromStr for UriFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if !s.contains("://") {
            return Ok(UriFormat::File);
        } else if s.starts_with("s3://") {
            return Ok(UriFormat::S3);
        }

        Err(format!(
            "unsupported uri {}. Only local files and URIs with s3:// prefix are supported.",
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

async fn object_store_with_location(uri: &str) -> (Arc<dyn ObjectStore>, Path) {
    let uri_format = UriFormat::from_str(uri).unwrap_or_else(|e| panic!("{}", e));

    match uri_format {
        UriFormat::File => {
            let storage_container = Arc::new(LocalFileSystem::new());
            let location = Path::from_filesystem_path(uri).unwrap();
            (storage_container, location)
        }
        UriFormat::S3 => {
            let (bucket, key) = parse_bucket_and_key(uri);
            let storage_container = Arc::new(
                AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .build()
                    .unwrap(),
            );
            let location = Path::from(key);
            (storage_container, location)
        }
    }
}

pub(crate) async fn parquet_schema_from_uri(uri: &str) -> SchemaDescriptor {
    let parquet_reader = parquet_reader_from_uri(uri).await;

    let arrow_schema = parquet_reader.schema();

    arrow_to_parquet_schema(arrow_schema).unwrap()
}

pub(crate) async fn parquet_metadata_from_uri(uri: &str) -> Arc<ParquetMetaData> {
    let (parquet_object_store, location) = object_store_with_location(uri).await;

    let object_store_meta = parquet_object_store.head(&location).await.unwrap();

    let parquet_object_reader = ParquetObjectReader::new(parquet_object_store, object_store_meta);

    let builder = ParquetRecordBatchStreamBuilder::new(parquet_object_reader)
        .await
        .unwrap();

    builder.metadata().to_owned()
}

pub(crate) async fn parquet_reader_from_uri(
    uri: &str,
) -> ParquetRecordBatchStream<ParquetObjectReader> {
    let (parquet_object_store, location) = object_store_with_location(uri).await;

    let object_store_meta = parquet_object_store.head(&location).await.unwrap();

    let parquet_object_reader = ParquetObjectReader::new(parquet_object_store, object_store_meta);

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
    let uri_format = UriFormat::from_str(uri).unwrap_or_else(|e| panic!("{}", e));

    if uri_format == UriFormat::File {
        // we overwrite the local file if it exists
        std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(uri)
            .unwrap();
    }

    let (parquet_object_store, location) = object_store_with_location(uri).await;

    let parquet_object_writer = ParquetObjectWriter::new(parquet_object_store, location);

    AsyncArrowWriter::try_new(parquet_object_writer, arrow_schema, Some(writer_props)).unwrap()
}
