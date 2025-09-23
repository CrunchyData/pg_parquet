use std::{ffi::CStr, fs::File, os::fd::FromRawFd, panic, sync::Arc};

use arrow::datatypes::SchemaRef;
use object_store::{path::Path, ObjectStoreScheme};
use parquet::{
    arrow::{
        async_reader::{ParquetObjectReader, ParquetRecordBatchStream},
        async_writer::ParquetObjectWriter,
        ArrowSchemaConverter, AsyncArrowWriter, ParquetRecordBatchStreamBuilder,
    },
    file::{metadata::ParquetMetaData, properties::WriterProperties},
    schema::types::SchemaDescriptor,
};
use pgrx::{
    ereport,
    ffi::c_char,
    pg_sys::{
        get_role_oid, has_privs_of_role, palloc0, superuser, AsPgCStr, ClosePipeStream, DataDir,
        FileClose, FilePathName, GetUserId, InvalidOid, OpenPipeStream, OpenTemporaryFile,
        TempTablespacePath, MAXPGPATH, PG_BINARY_R, PG_BINARY_W,
    },
};
use url::Url;

use crate::{
    object_store::{
        aws::parse_s3_bucket, azure::parse_azure_blob_container, gcs::parse_gcs_bucket,
        http::parse_http_base_uri, object_store_cache::get_or_create_object_store,
    },
    PG_BACKEND_TOKIO_RUNTIME,
};

const PARQUET_OBJECT_STORE_READ_ROLE: &str = "parquet_object_store_read";
const PARQUET_OBJECT_STORE_WRITE_ROLE: &str = "parquet_object_store_write";

// ParsedUriInfo is a struct that holds the parsed uri information.
#[derive(Debug)]
pub(crate) struct ParsedUriInfo {
    pub(crate) uri: Url,
    pub(crate) bucket: Option<String>,
    pub(crate) path: Path,
    pub(crate) scheme: ObjectStoreScheme,
    // tmp_fd is used as intermediate file for copying data to/from stdin/out or program pipes
    pub(crate) tmp_fd: Option<i32>,
    // pipe_file is used to hold the pipe file descriptor for copying data to/from a program
    // call open_program_pipe to open the pipe to a program
    pub(crate) is_program: bool,
    pub(crate) pipe_file: *mut libc::FILE,
}

impl ParsedUriInfo {
    pub(crate) fn for_std_inout() -> Self {
        Self::with_tmp_file()
    }

    pub(crate) fn for_program() -> Self {
        let mut uri_info = Self::with_tmp_file();
        uri_info.is_program = true;
        uri_info
    }

    pub(crate) fn open_program_pipe(&mut self, program: &str, copy_from: bool) -> File {
        let pipe_mode = if copy_from { PG_BINARY_R } else { PG_BINARY_W };

        let pipe_file = unsafe { OpenPipeStream(program.as_pg_cstr(), pipe_mode.as_ptr()) };

        if pipe_file.is_null() {
            panic!("Failed to open pipe stream for program: {}", program);
        }

        self.pipe_file = pipe_file as _;

        let fd = unsafe { libc::fileno(self.pipe_file) };

        if fd < 0 {
            panic!("Failed to get file descriptor for pipe stream: {}", program);
        }

        unsafe { File::from_raw_fd(fd) }
    }

    fn with_tmp_file() -> Self {
        // open temp postgres file, which is removed after transaction ends
        let tmp_path_fd = unsafe { OpenTemporaryFile(false) };

        let tmp_path = unsafe {
            let data_dir = CStr::from_ptr(DataDir).to_str().expect("invalid base dir");

            let tmp_tblspace_path: *const c_char = palloc0(MAXPGPATH as _) as _;
            TempTablespacePath(tmp_tblspace_path as _, InvalidOid);
            let tmp_tblspace_path = CStr::from_ptr(tmp_tblspace_path)
                .to_str()
                .expect("invalid temp tablespace path");

            let tmp_file_path = FilePathName(tmp_path_fd);
            let tmp_file_path = CStr::from_ptr(tmp_file_path)
                .to_str()
                .expect("invalid temp path");

            let tmp_path = std::path::Path::new(data_dir)
                .join(tmp_tblspace_path)
                .join(tmp_file_path);

            tmp_path.to_str().expect("invalid tmp path").to_string()
        };

        let mut parsed_uri = Self::try_from(tmp_path.as_str()).unwrap_or_else(|e| panic!("{}", e));

        parsed_uri.tmp_fd = Some(tmp_path_fd);

        parsed_uri
    }

    fn is_std_inout(&self) -> bool {
        self.tmp_fd.is_some() && !self.is_program
    }

    fn try_parse_uri(uri: &str) -> Result<Url, String> {
        if !uri.contains("://") {
            // local file
            Url::from_file_path(uri).map_err(|_| format!("not a valid file path: {uri}"))
        } else {
            Url::parse(uri).map_err(|e| e.to_string())
        }
    }

    fn try_parse_scheme(uri: &Url) -> Result<(ObjectStoreScheme, Path), String> {
        ObjectStoreScheme::parse(uri).map_err(|_| {
            format!(
                "unrecognized uri {uri}. pg_parquet supports local paths, https://, s3://, az:// or gs:// schemes."
            )
        })
    }

    fn try_parse_bucket(scheme: &ObjectStoreScheme, uri: &Url) -> Result<Option<String>, String> {
        match scheme {
            ObjectStoreScheme::AmazonS3 => parse_s3_bucket(uri)
                .ok_or(format!("unsupported s3 uri {uri}"))
                .map(Some),
            ObjectStoreScheme::MicrosoftAzure => parse_azure_blob_container(uri)
                .ok_or(format!("unsupported azure blob storage uri: {uri}"))
                .map(Some),
            ObjectStoreScheme::Http => parse_http_base_uri(uri).
                ok_or(format!("unsupported http storage uri: {uri}"))
                .map(Some),
            ObjectStoreScheme::GoogleCloudStorage => parse_gcs_bucket(uri)
                .ok_or(format!("unsupported gcs uri {uri}"))
                .map(Some),
            ObjectStoreScheme::Local => Ok(None),
            _ => Err(format!("unsupported scheme {} in uri {}. pg_parquet supports local paths, https://, s3://, az:// or gs:// schemes.",
                            uri.scheme(), uri))
        }
    }
}

impl TryFrom<&str> for ParsedUriInfo {
    type Error = String;

    fn try_from(uri: &str) -> Result<Self, Self::Error> {
        let uri = Self::try_parse_uri(uri)?;

        let (scheme, path) = Self::try_parse_scheme(&uri)?;

        let bucket = Self::try_parse_bucket(&scheme, &uri)?;

        Ok(ParsedUriInfo {
            uri: uri.clone(),
            bucket,
            path,
            scheme,
            tmp_fd: None,
            is_program: false,
            pipe_file: std::ptr::null_mut(),
        })
    }
}

impl Drop for ParsedUriInfo {
    fn drop(&mut self) {
        if let Some(tmp_fd) = self.tmp_fd {
            // close temp file, postgres api will remove it on close
            unsafe { FileClose(tmp_fd) };
        }

        if !self.pipe_file.is_null() {
            // close pipe file, postgres api will remove it on close
            unsafe { ClosePipeStream(self.pipe_file as _) };
        }
    }
}

pub(crate) fn uri_as_string(uri: &Url) -> String {
    if uri.scheme() == "file" {
        // removes file:// prefix from the local path uri
        return uri
            .to_file_path()
            .unwrap_or_else(|_| panic!("invalid local path: {uri}"))
            .to_string_lossy()
            .to_string();
    }

    uri.to_string()
}

pub(crate) fn object_store_base_uri(uri: &Url) -> String {
    if uri.scheme() == "file" {
        // root path for local file
        return "/".to_string();
    }

    let scheme = uri.scheme();

    let host = uri.host_str().expect("missing host in uri");

    let port = uri.port().map(|p| format!(":{}", p)).unwrap_or_default();

    format!("{}://{}{}", scheme, host, port)
}

pub(crate) fn parquet_schema_from_uri(uri_info: &ParsedUriInfo) -> SchemaDescriptor {
    let parquet_reader = parquet_reader_from_uri(uri_info).unwrap_or_else(|e| {
        panic!(
            "failed to create parquet reader for uri {}: {}",
            uri_info.uri, e
        )
    });

    let arrow_schema = parquet_reader.schema();

    ArrowSchemaConverter::new()
        .convert(arrow_schema)
        .unwrap_or_else(|e| panic!("{}", e))
}

pub(crate) fn parquet_metadata_from_uri(uri_info: &ParsedUriInfo) -> Arc<ParquetMetaData> {
    let copy_from = true;
    let (parquet_object_store, location) = get_or_create_object_store(uri_info, copy_from);

    PG_BACKEND_TOKIO_RUNTIME.block_on(async {
        let object_store_meta = parquet_object_store
            .head(&location)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "failed to get object store metadata for uri {}: {}",
                    uri_info.uri, e
                )
            });

        let parquet_object_reader = ParquetObjectReader::new(parquet_object_store, location)
            .with_file_size(object_store_meta.size);

        let builder = ParquetRecordBatchStreamBuilder::new(parquet_object_reader)
            .await
            .unwrap_or_else(|e| panic!("{}", e));

        builder.metadata().to_owned()
    })
}

// default # of records per batch during arrow-parquet conversions (RecordBatch api)
pub(crate) const RECORD_BATCH_SIZE: i64 = 1024;

pub(crate) fn parquet_reader_from_uri(
    uri_info: &ParsedUriInfo,
) -> Result<ParquetRecordBatchStream<ParquetObjectReader>, String> {
    let copy_from = true;
    let (parquet_object_store, location) = get_or_create_object_store(uri_info, copy_from);

    PG_BACKEND_TOKIO_RUNTIME.block_on(async {
        let object_store_meta = parquet_object_store.head(&location).await.map_err(|e| {
            format!(
                "failed to get object store metadata for uri {}: {}",
                uri_info.uri, e
            )
        })?;

        let parquet_object_reader = ParquetObjectReader::new(parquet_object_store, location)
            .with_file_size(object_store_meta.size);

        let builder = ParquetRecordBatchStreamBuilder::new(parquet_object_reader)
            .await
            .unwrap_or_else(|e| panic!("{}", e));

        pgrx::debug2!("Converted arrow schema is: {}", builder.schema());

        let batch_size = calculate_reader_batch_size(builder.metadata());

        Ok(builder
            .with_batch_size(batch_size)
            .build()
            .unwrap_or_else(|e| panic!("{}", e)))
    })
}

fn calculate_reader_batch_size(metadata: &Arc<ParquetMetaData>) -> usize {
    const MAX_ARROW_ARRAY_SIZE: i64 = i32::MAX as _;

    for row_group in metadata.row_groups() {
        for column in row_group.columns() {
            // try our best to get the size of the column
            let column_size = column
                .unencoded_byte_array_data_bytes()
                .unwrap_or(column.uncompressed_size());

            if column_size > MAX_ARROW_ARRAY_SIZE {
                // to prevent decoding large arrays into memory, process one row at a time
                return 1;
            }
        }
    }

    // default batch size
    RECORD_BATCH_SIZE as _
}

pub(crate) fn parquet_writer_from_uri(
    uri_info: &ParsedUriInfo,
    arrow_schema: SchemaRef,
    writer_props: WriterProperties,
) -> AsyncArrowWriter<ParquetObjectWriter> {
    let copy_from = false;
    let (parquet_object_store, location) = get_or_create_object_store(uri_info, copy_from);

    let parquet_object_writer = ParquetObjectWriter::new(parquet_object_store, location);

    AsyncArrowWriter::try_new(parquet_object_writer, arrow_schema, Some(writer_props))
        .unwrap_or_else(|e| {
            panic!(
                "failed to create parquet writer for uri {}: {}",
                uri_info.uri, e
            )
        })
}

pub(crate) fn ensure_access_privilege_to_uri(uri_info: &ParsedUriInfo, copy_from: bool) {
    if unsafe { superuser() } {
        return;
    }

    // permission check is not needed for stdin/out
    if uri_info.is_std_inout() {
        return;
    }

    let user_id = unsafe { GetUserId() };
    let is_file = uri_info.uri.scheme() == "file";

    let required_role_name = if uri_info.is_program {
        "pg_execute_server_program"
    } else if is_file {
        if copy_from {
            "pg_read_server_files"
        } else {
            "pg_write_server_files"
        }
    } else {
        // object_store
        if copy_from {
            PARQUET_OBJECT_STORE_READ_ROLE
        } else {
            PARQUET_OBJECT_STORE_WRITE_ROLE
        }
    };

    let required_role_id =
        unsafe { get_role_oid(required_role_name.to_string().as_pg_cstr(), false) };

    let operation_str = if copy_from { "from" } else { "to" };
    let object_type = if uri_info.is_program {
        "program"
    } else if is_file {
        "file"
    } else {
        "remote uri"
    };

    if !unsafe { has_privs_of_role(user_id, required_role_id) } {
        ereport!(
            pgrx::PgLogLevel::ERROR,
            pgrx::PgSqlErrorCode::ERRCODE_INSUFFICIENT_PRIVILEGE,
            format!(
                "permission denied to COPY {} a {}",
                operation_str, object_type
            ),
            format!(
                "Only roles with privileges of the \"{}\" role may COPY {} a {}.",
                required_role_name, operation_str, object_type
            ),
        );
    }
}
