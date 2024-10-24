use std::{
    panic,
    sync::{Arc, LazyLock},
};

use arrow::datatypes::SchemaRef;
use aws_config::{
    environment::{EnvironmentVariableCredentialsProvider, EnvironmentVariableRegionProvider},
    meta::{credentials::CredentialsProviderChain, region::RegionProviderChain},
    profile::{ProfileFileCredentialsProvider, ProfileFileRegionProvider},
};
use aws_credential_types::provider::ProvideCredentials;
use object_store::{
    aws::{AmazonS3, AmazonS3Builder},
    azure::{MicrosoftAzure, MicrosoftAzureBuilder},
    local::LocalFileSystem,
    path::Path,
    ObjectStore, ObjectStoreScheme,
};
use parquet::{
    arrow::{
        arrow_to_parquet_schema,
        async_reader::{ParquetObjectReader, ParquetRecordBatchStream},
        async_writer::ParquetObjectWriter,
        AsyncArrowWriter, ParquetRecordBatchStreamBuilder,
    },
    file::{metadata::ParquetMetaData, properties::WriterProperties},
    schema::types::SchemaDescriptor,
};
use pgrx::{
    ereport,
    pg_sys::{get_role_oid, has_privs_of_role, superuser, AsPgCStr, GetUserId},
};
use tokio::runtime::Runtime;
use url::Url;

use crate::arrow_parquet::parquet_writer::DEFAULT_ROW_GROUP_SIZE;

const PARQUET_OBJECT_STORE_READ_ROLE: &str = "parquet_object_store_read";
const PARQUET_OBJECT_STORE_WRITE_ROLE: &str = "parquet_object_store_write";

// PG_BACKEND_TOKIO_RUNTIME creates a tokio runtime that uses the current thread
// to run the tokio reactor. This uses the same thread that is running the Postgres backend.
pub(crate) static PG_BACKEND_TOKIO_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap_or_else(|e| panic!("failed to create tokio runtime: {}", e))
});

fn parse_azure_blob_container(uri: &Url) -> Option<String> {
    let host = uri.host_str()?;

    // https://{account}.blob.core.windows.net/{container}/key
    if host.ends_with("blob.core.windows.net") {
        let path_segments: Vec<&str> = uri.path_segments()?.collect();

        if !path_segments.is_empty() {
            return Some(path_segments[0].to_string());
        } else {
            return None;
        }
    }
    // http://localhost:10000/{account}/{container}/key
    else if is_testing() && uri.scheme() == "http" {
        let path_segments: Vec<&str> = uri.path_segments()?.collect();

        if path_segments.len() >= 2 {
            return Some(path_segments[1].to_string());
        } else {
            return None;
        }
    }

    None
}

fn parse_s3_bucket(uri: &Url) -> Option<String> {
    let host = uri.host_str()?;

    // s3(a)://{bucket}/key
    if uri.scheme() == "s3" || uri.scheme() == "s3a" {
        return Some(host.to_string());
    }
    // https://s3.amazonaws.com/{bucket}/key
    else if host == "s3.amazonaws.com" {
        let path_segments: Vec<&str> = uri.path_segments()?.collect();
        if !path_segments.is_empty() {
            return Some(path_segments[0].to_string()); // Bucket name is the first part of the path
        } else {
            return None;
        }
    }
    // https://{bucket}.s3.amazonaws.com/key
    else if host.ends_with("s3.amazonaws.com") {
        let bucket_name = host.split('.').next()?;
        return Some(bucket_name.to_string());
    }

    None
}

fn object_store_with_location(uri: &Url, copy_from: bool) -> (Arc<dyn ObjectStore>, Path) {
    let (scheme, path) =
        ObjectStoreScheme::parse(uri).unwrap_or_else(|_| panic!("unsupported uri {}", uri));

    match scheme {
        ObjectStoreScheme::AmazonS3 => {
            ensure_object_store_access_privilege(copy_from);

            let bucket_name = parse_s3_bucket(uri).unwrap_or_else(|| {
                panic!("failed to parse bucket name from uri: {}", uri);
            });

            let storage_container = PG_BACKEND_TOKIO_RUNTIME
                .block_on(async { Arc::new(get_s3_object_store(&bucket_name).await) });

            (storage_container, path)
        }
        ObjectStoreScheme::MicrosoftAzure => {
            ensure_object_store_access_privilege(copy_from);

            let container_name = parse_azure_blob_container(uri).unwrap_or_else(|| {
                panic!("failed to parse container name from uri: {}", uri);
            });

            let storage_container = PG_BACKEND_TOKIO_RUNTIME
                .block_on(async { Arc::new(get_azure_object_store(&container_name).await) });

            (storage_container, path)
        }
        ObjectStoreScheme::Http if is_testing() => {
            ensure_object_store_access_privilege(copy_from);

            let container_name = parse_azure_blob_container(uri).unwrap_or_else(|| {
                panic!("failed to parse container name from uri: {}", uri);
            });

            let storage_container = PG_BACKEND_TOKIO_RUNTIME
                .block_on(async { Arc::new(get_azure_object_store(&container_name).await) });

            (storage_container, path)
        }
        ObjectStoreScheme::Local => {
            ensure_local_file_access_privilege(copy_from);

            let uri = uri_as_string(uri);

            if !copy_from {
                // create or overwrite the local file
                std::fs::OpenOptions::new()
                    .write(true)
                    .truncate(true)
                    .create(true)
                    .open(&uri)
                    .unwrap_or_else(|e| panic!("{}", e));
            }

            let storage_container = Arc::new(LocalFileSystem::new());

            let path = Path::from_filesystem_path(&uri).unwrap_or_else(|e| panic!("{}", e));

            (storage_container, path)
        }
        _ => {
            panic!("unsupported uri {}", uri);
        }
    }
}

async fn get_s3_object_store(bucket_name: &str) -> AmazonS3 {
    let mut aws_s3_builder = AmazonS3Builder::new().with_bucket_name(bucket_name);

    if is_testing() {
        // use minio for testing
        aws_s3_builder = aws_s3_builder.with_endpoint("http://localhost:9000");
        aws_s3_builder = aws_s3_builder.with_allow_http(true);
    }

    let aws_profile_name = std::env::var("AWS_PROFILE").unwrap_or("default".to_string());

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

async fn get_azure_object_store(container_name: &str) -> MicrosoftAzure {
    let mut azure_builder = MicrosoftAzureBuilder::from_env().with_container_name(container_name);

    if is_testing() {
        // use azurite for testing
        azure_builder = azure_builder.with_use_emulator(true);
    }

    azure_builder.build().unwrap_or_else(|e| panic!("{}", e))
}

fn is_testing() -> bool {
    std::env::var("PG_PARQUET_TEST").is_ok()
}

pub(crate) fn parse_uri(uri: &str) -> Url {
    if !uri.contains("://") {
        // local file
        return Url::from_file_path(uri)
            .unwrap_or_else(|_| panic!("not a valid file path: {}", uri));
    }

    let uri = Url::parse(uri).unwrap_or_else(|e| panic!("{}", e));

    let (scheme, _) =
        ObjectStoreScheme::parse(&uri).unwrap_or_else(|_| panic!("unsupported uri {}", uri));

    if scheme == ObjectStoreScheme::AmazonS3 {
        parse_s3_bucket(&uri)
            .unwrap_or_else(|| panic!("failed to parse bucket name from s3 uri {}", uri));
    } else if scheme == ObjectStoreScheme::MicrosoftAzure || scheme == ObjectStoreScheme::Http {
        parse_azure_blob_container(&uri).unwrap_or_else(|| {
            panic!(
                "failed to parse container name from azure blob storage uri {}",
                uri
            )
        });
    } else {
        panic!(
            "unsupported uri {}. Only Azure and S3 uris are supported.",
            uri
        );
    };

    uri
}

pub(crate) fn uri_as_string(uri: &Url) -> String {
    if uri.scheme() == "file" {
        // removes file:// prefix from the local path uri
        return uri
            .to_file_path()
            .unwrap_or_else(|_| panic!("invalid local path: {}", uri))
            .to_string_lossy()
            .to_string();
    }

    uri.to_string()
}

pub(crate) fn parquet_schema_from_uri(uri: &Url) -> SchemaDescriptor {
    let parquet_reader = parquet_reader_from_uri(uri);

    let arrow_schema = parquet_reader.schema();

    arrow_to_parquet_schema(arrow_schema).unwrap_or_else(|e| panic!("{}", e))
}

pub(crate) fn parquet_metadata_from_uri(uri: &Url) -> Arc<ParquetMetaData> {
    let copy_from = true;
    let (parquet_object_store, location) = object_store_with_location(uri, copy_from);

    PG_BACKEND_TOKIO_RUNTIME.block_on(async {
        let object_store_meta = parquet_object_store
            .head(&location)
            .await
            .unwrap_or_else(|e| {
                panic!("failed to get object store metadata for uri {}: {}", uri, e)
            });

        let parquet_object_reader =
            ParquetObjectReader::new(parquet_object_store, object_store_meta);

        let builder = ParquetRecordBatchStreamBuilder::new(parquet_object_reader)
            .await
            .unwrap_or_else(|e| panic!("{}", e));

        builder.metadata().to_owned()
    })
}

pub(crate) fn parquet_reader_from_uri(uri: &Url) -> ParquetRecordBatchStream<ParquetObjectReader> {
    let copy_from = true;
    let (parquet_object_store, location) = object_store_with_location(uri, copy_from);

    PG_BACKEND_TOKIO_RUNTIME.block_on(async {
        let object_store_meta = parquet_object_store
            .head(&location)
            .await
            .unwrap_or_else(|e| {
                panic!("failed to get object store metadata for uri {}: {}", uri, e)
            });

        let parquet_object_reader =
            ParquetObjectReader::new(parquet_object_store, object_store_meta);

        let builder = ParquetRecordBatchStreamBuilder::new(parquet_object_reader)
            .await
            .unwrap_or_else(|e| panic!("{}", e));

        pgrx::debug2!("Converted arrow schema is: {}", builder.schema());

        builder
            .with_batch_size(DEFAULT_ROW_GROUP_SIZE as usize)
            .build()
            .unwrap_or_else(|e| panic!("{}", e))
    })
}

pub(crate) fn parquet_writer_from_uri(
    uri: &Url,
    arrow_schema: SchemaRef,
    writer_props: WriterProperties,
) -> AsyncArrowWriter<ParquetObjectWriter> {
    let copy_from = false;
    let (parquet_object_store, location) = object_store_with_location(uri, copy_from);

    let parquet_object_writer = ParquetObjectWriter::new(parquet_object_store, location);

    AsyncArrowWriter::try_new(parquet_object_writer, arrow_schema, Some(writer_props))
        .unwrap_or_else(|e| panic!("failed to create parquet writer for uri {}: {}", uri, e))
}

fn ensure_object_store_access_privilege(copy_from: bool) {
    if unsafe { superuser() } {
        return;
    }

    let user_id = unsafe { GetUserId() };

    let required_role_name = if copy_from {
        PARQUET_OBJECT_STORE_READ_ROLE
    } else {
        PARQUET_OBJECT_STORE_WRITE_ROLE
    };

    let required_role_id =
        unsafe { get_role_oid(required_role_name.to_string().as_pg_cstr(), false) };

    let operation_str = if copy_from { "from" } else { "to" };

    if !unsafe { has_privs_of_role(user_id, required_role_id) } {
        ereport!(
            pgrx::PgLogLevel::ERROR,
            pgrx::PgSqlErrorCode::ERRCODE_INSUFFICIENT_PRIVILEGE,
            format!("permission denied to COPY {} a remote uri", operation_str),
            format!(
                "Only roles with privileges of the \"{}\" role may COPY {} a remote uri.",
                required_role_name, operation_str
            ),
        );
    }
}

fn ensure_local_file_access_privilege(copy_from: bool) {
    if unsafe { superuser() } {
        return;
    }

    let user_id = unsafe { GetUserId() };

    let required_role_name = if copy_from {
        "pg_read_server_files"
    } else {
        "pg_write_server_files"
    };

    let required_role_id =
        unsafe { get_role_oid(required_role_name.to_string().as_pg_cstr(), false) };

    let operation_str = if copy_from { "from" } else { "to" };

    if !unsafe { has_privs_of_role(user_id, required_role_id) } {
        ereport!(
            pgrx::PgLogLevel::ERROR,
            pgrx::PgSqlErrorCode::ERRCODE_INSUFFICIENT_PRIVILEGE,
            format!("permission denied to COPY {} a file", operation_str),
            format!(
                "Only roles with privileges of the \"{}\" role may COPY {} a file.",
                required_role_name, operation_str
            ),
        );
    }
}
