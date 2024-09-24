use std::{str::FromStr, sync::Arc};

use arrow::datatypes::SchemaRef;
use aws_config::environment::{
    EnvironmentVariableCredentialsProvider, EnvironmentVariableRegionProvider,
};
use aws_config::meta::credentials::CredentialsProviderChain;
use aws_config::meta::region::RegionProviderChain;
use aws_config::profile::{ProfileFileCredentialsProvider, ProfileFileRegionProvider};
use aws_credential_types::provider::ProvideCredentials;
use object_store::aws::{AmazonS3, AmazonS3Builder};
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
use pgrx::pg_sys::AsPgCStr;

use crate::arrow_parquet::parquet_writer::DEFAULT_ROW_GROUP_SIZE;

const PARQUET_OBJECT_STORE_READ_ROLE: &str = "parquet_object_store_read";
const PARQUET_OBJECT_STORE_WRITE_ROLE: &str = "parquet_object_store_write";

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
    let uri = uri.strip_prefix("s3://").expect("unexpected s3 uri");

    let mut parts = uri.splitn(2, '/');
    let bucket = parts.next().expect("bucket not found in uri");
    let key = parts.next().expect("key not found in uri");

    (bucket.to_string(), key.to_string())
}

async fn object_store_with_location(uri: &str, read_only: bool) -> (Arc<dyn ObjectStore>, Path) {
    let uri_format = UriFormat::from_str(uri).unwrap_or_else(|e| panic!("{}", e));

    match uri_format {
        UriFormat::File => {
            if !read_only {
                // create or overwrite the local file
                std::fs::OpenOptions::new()
                    .write(true)
                    .truncate(true)
                    .create(true)
                    .open(uri)
                    .unwrap_or_else(|e| panic!("{}", e));
            }

            let storage_container = Arc::new(LocalFileSystem::new());

            let location = Path::from_filesystem_path(uri).unwrap_or_else(|e| panic!("{}", e));

            (storage_container, location)
        }
        UriFormat::S3 => {
            ensure_object_store_access(read_only);

            let (bucket_name, key) = parse_bucket_and_key(uri);

            let storage_container = Arc::new(get_s3_object_store(&bucket_name).await);

            let location = Path::from(key);

            (storage_container, location)
        }
    }
}

pub(crate) async fn parquet_schema_from_uri(uri: &str) -> SchemaDescriptor {
    let parquet_reader = parquet_reader_from_uri(uri).await;

    let arrow_schema = parquet_reader.schema();

    arrow_to_parquet_schema(arrow_schema).unwrap_or_else(|e| panic!("{}", e))
}

pub(crate) async fn parquet_metadata_from_uri(uri: &str) -> Arc<ParquetMetaData> {
    let read_only = true;
    let (parquet_object_store, location) = object_store_with_location(uri, read_only).await;

    let object_store_meta = parquet_object_store
        .head(&location)
        .await
        .unwrap_or_else(|e| panic!("failed to get object store metadata for uri {}: {}", uri, e));

    let parquet_object_reader = ParquetObjectReader::new(parquet_object_store, object_store_meta);

    let builder = ParquetRecordBatchStreamBuilder::new(parquet_object_reader)
        .await
        .unwrap_or_else(|e| panic!("{}", e));

    builder.metadata().to_owned()
}

pub(crate) async fn parquet_reader_from_uri(
    uri: &str,
) -> ParquetRecordBatchStream<ParquetObjectReader> {
    let read_only = true;
    let (parquet_object_store, location) = object_store_with_location(uri, read_only).await;

    let object_store_meta = parquet_object_store
        .head(&location)
        .await
        .unwrap_or_else(|e| panic!("failed to get object store metadata for uri {}: {}", uri, e));

    let parquet_object_reader = ParquetObjectReader::new(parquet_object_store, object_store_meta);

    let builder = ParquetRecordBatchStreamBuilder::new(parquet_object_reader)
        .await
        .unwrap_or_else(|e| panic!("{}", e));

    pgrx::debug2!("Converted arrow schema is: {}", builder.schema());

    builder
        .with_batch_size(DEFAULT_ROW_GROUP_SIZE as usize)
        .build()
        .unwrap_or_else(|e| panic!("{}", e))
}

pub(crate) async fn parquet_writer_from_uri(
    uri: &str,
    arrow_schema: SchemaRef,
    writer_props: WriterProperties,
) -> AsyncArrowWriter<ParquetObjectWriter> {
    let read_only = false;
    let (parquet_object_store, location) = object_store_with_location(uri, read_only).await;

    let parquet_object_writer = ParquetObjectWriter::new(parquet_object_store, location);

    AsyncArrowWriter::try_new(parquet_object_writer, arrow_schema, Some(writer_props))
        .unwrap_or_else(|e| panic!("failed to create parquet writer for uri {}: {}", uri, e))
}

pub async fn get_s3_object_store(bucket_name: &str) -> AmazonS3 {
    // try loading the .env file
    dotenvy::from_path("/tmp/.env").ok();

    let mut aws_s3_builder = AmazonS3Builder::new().with_bucket_name(bucket_name);

    let is_test_running = std::env::var("PG_PARQUET_TEST").is_ok();

    if is_test_running {
        // use minio for testing
        aws_s3_builder = aws_s3_builder.with_endpoint("http://localhost:9000");
        aws_s3_builder = aws_s3_builder.with_allow_http(true);
    }

    let aws_profile_name = std::env::var("AWS_PROFILE").unwrap_or("default".to_string());

    // load the region from the environment variables or the profile file
    let region_provider = RegionProviderChain::first_try(EnvironmentVariableRegionProvider::new())
        .or_else(
            ProfileFileRegionProvider::builder()
                .profile_name(aws_profile_name.clone())
                .build(),
        );

    let region = region_provider.region().await;

    if let Some(region) = region {
        aws_s3_builder = aws_s3_builder.with_region(region.to_string());
    }

    // load the credentials from the environment variables or the profile file
    let credential_provider = CredentialsProviderChain::first_try(
        "Environment",
        EnvironmentVariableCredentialsProvider::new(),
    )
    .or_else(
        "Profile",
        ProfileFileCredentialsProvider::builder()
            .profile_name(aws_profile_name)
            .build(),
    );

    if let Ok(credentials) = credential_provider.provide_credentials().await {
        aws_s3_builder = aws_s3_builder.with_access_key_id(credentials.access_key_id());

        aws_s3_builder = aws_s3_builder.with_secret_access_key(credentials.secret_access_key());

        if let Some(token) = credentials.session_token() {
            aws_s3_builder = aws_s3_builder.with_token(token);
        }
    }

    aws_s3_builder.build().unwrap_or_else(|e| panic!("{}", e))
}

fn ensure_object_store_access(read_only: bool) {
    if unsafe { pgrx::pg_sys::superuser() } {
        return;
    }

    let user_id = unsafe { pgrx::pg_sys::GetUserId() };

    let required_role_name = if read_only {
        PARQUET_OBJECT_STORE_READ_ROLE
    } else {
        PARQUET_OBJECT_STORE_WRITE_ROLE
    };

    let required_role_id =
        unsafe { pgrx::pg_sys::get_role_oid(required_role_name.to_string().as_pg_cstr(), false) };

    let operation_str = if read_only { "read" } else { "write" };

    if !unsafe { pgrx::pg_sys::has_privs_of_role(user_id, required_role_id) } {
        panic!(
            "current user does not have the role, named {}, to {} the bucket",
            required_role_name, operation_str
        );
    }
}
