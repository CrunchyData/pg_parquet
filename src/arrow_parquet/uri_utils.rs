use std::{ffi::CStr, fs::File, ops::Range, os::fd::FromRawFd, panic, sync::Arc};

use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use futures::{future::BoxFuture, FutureExt, StreamExt, TryFutureExt};
use object_store::{path::Path, ObjectStore, ObjectStoreExt, ObjectStoreScheme};
use parquet::{
    arrow::{
        arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions},
        async_reader::{AsyncFileReader, ParquetRecordBatchStream},
        async_writer::AsyncFileWriter,
        ArrowSchemaConverter, AsyncArrowWriter, ParquetRecordBatchStreamBuilder,
    },
    errors::{ParquetError, Result as ParquetResult},
    file::{
        metadata::{ParquetMetaData, ParquetMetaDataReader},
        properties::WriterProperties,
    },
    schema::types::SchemaDescriptor,
};
use pgrx::{
    ereport,
    pg_sys::{
        get_role_oid, has_privs_of_role, superuser, AsPgCStr, ClosePipeStream, DataDir, FileClose,
        FilePathName, GetUserId, OpenPipeStream, OpenTemporaryFile, PG_BINARY_R, PG_BINARY_W,
    },
};
use url::Url;

use crate::{
    arrow_parquet::{
        parquet_shared_upload::shared_upload_writer,
        parquet_single_upload::single_upload_writer,
        schema::parse_schema::{schema_with_int96_as_micros, schema_without_dictionaries},
    },
    block_on,
    object_store::{
        aws::parse_s3_bucket, azure::parse_azure_blob_container, gcs::parse_gcs_bucket,
        http::parse_http_base_uri, object_store_cache::get_or_create_object_store,
        object_store_error_message,
    },
};

// the four bytes that every parquet file begins and ends with
const PARQUET_MAGIC: &[u8] = b"PAR1";

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

        // the pipe stream owns fd and ClosePipeStream closes it in drop, so the caller gets a
        // descriptor of its own rather than one that postgres closes a second time
        let dup_fd = unsafe { libc::dup(fd) };

        if dup_fd < 0 {
            panic!(
                "Failed to duplicate file descriptor for program: {}",
                program
            );
        }

        unsafe { File::from_raw_fd(dup_fd) }
    }

    fn with_tmp_file() -> Self {
        // open temp postgres file, which is removed after transaction ends
        let tmp_path_fd = unsafe { OpenTemporaryFile(false) };

        let tmp_path = unsafe {
            let data_dir = CStr::from_ptr(DataDir).to_str().expect("invalid base dir");

            // FilePathName already returns the path of the temporary file relative to the
            // data directory, e.g. "base/pgsql_tmp/pgsql_tmp<pid>.<n>", so the data directory
            // is the only prefix we need. Prepending the temp tablespace path on top of it
            // would point us to a file that postgres does not know about and never removes.
            let tmp_file_path = FilePathName(tmp_path_fd);
            let tmp_file_path = CStr::from_ptr(tmp_file_path)
                .to_str()
                .expect("invalid temp path");

            let tmp_path = std::path::Path::new(data_dir).join(tmp_file_path);

            tmp_path.to_str().expect("invalid tmp path").to_string()
        };

        let mut parsed_uri = Self::try_from(tmp_path.as_str()).unwrap_or_else(|e| panic!("{}", e));

        parsed_uri.tmp_fd = Some(tmp_path_fd);

        parsed_uri
    }

    pub(crate) fn is_std_inout(&self) -> bool {
        self.tmp_fd.is_some() && !self.is_program
    }

    fn try_parse_uri(uri: &str) -> Result<Url, String> {
        if !uri.contains("://") {
            // local file. The built uri is parsed again so that the ".." segments of the path
            // are collapsed the way parsing collapses them for any other scheme, since the
            // object store rejects a path that still holds one.
            let file_uri =
                Url::from_file_path(uri).map_err(|_| format!("not a valid file path: {uri}"))?;

            Url::parse(file_uri.as_str()).map_err(|e| e.to_string())
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
    let (parquet_reader, _) = parquet_reader_from_uri(uri_info).unwrap_or_else(|e| {
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
    let (_, reader_metadata, _) = open_parquet_file(uri_info).unwrap_or_else(|e| panic!("{e}"));

    reader_metadata.metadata().to_owned()
}

// default # of records per batch during arrow-parquet conversions (RecordBatch api)
pub(crate) const RECORD_BATCH_SIZE: i64 = 1024;

// open_parquet_file reads everything that the readers of one file share: a handle to the
// file's bytes, the metadata to read it with, and the batch size that its columns call for.
// None of it differs between the row groups of the file, which is what lets a parallel COPY
// FROM worker read all of its row groups without going back to the footer.
fn open_parquet_file(
    uri_info: &ParsedUriInfo,
) -> Result<(ParquetObjectStoreReader, ArrowReaderMetadata, usize), String> {
    let copy_from = true;

    // the object store is created before the runtime is entered, since creating one blocks on
    // the runtime itself, e.g. to resolve the credentials an object store uri is reached with
    let (parquet_object_store, location) = get_or_create_object_store(uri_info, copy_from);

    block_on(async move {
        let object_store_meta = match parquet_object_store.head(&location).await {
            Ok(object_store_meta) => object_store_meta,
            Err(e) => {
                let mut message = format!(
                    "failed to get object store metadata for uri {}: {}",
                    uri_info.uri,
                    object_store_error_message(e)
                );

                if let Some(hint) = folder_hint(&parquet_object_store, &location, uri_info).await {
                    message.push_str(". ");
                    message.push_str(&hint);
                }

                return Err(message);
            }
        };

        let mut object_reader =
            ParquetObjectStoreReader::new(parquet_object_store, location, object_store_meta.size);

        let reader_metadata = parquet_reader_metadata(&mut object_reader, &uri_info.uri).await;

        pgrx::debug2!("Converted arrow schema is: {}", reader_metadata.schema());

        let batch_size = calculate_reader_batch_size(reader_metadata.metadata());

        Ok((object_reader, reader_metadata, batch_size))
    })
}

// folder_hint describes a uri that holds no object of its own while objects are stored under
// it, which is what a folder that a COPY TO with file_size_bytes wrote looks like. Such a
// uri is read with a pattern that names the files in it rather than by itself, which an
// object store reports as nothing but a missing object.
async fn folder_hint(
    parquet_object_store: &Arc<dyn ObjectStore>,
    location: &Path,
    uri_info: &ParsedUriInfo,
) -> Option<String> {
    // a store that cannot list, e.g. an http one, tells nothing about what is under the uri
    parquet_object_store
        .list(Some(location))
        .next()
        .await?
        .ok()?;

    let uri = uri_as_string(&uri_info.uri);
    let uri = uri.trim_end_matches('/');

    Some(format!(
        "\"{uri}\" holds files rather than being one, so it is read with a pattern \
         that names them, e.g. \"{uri}/*.parquet\"."
    ))
}

// not_parquet_hint describes an object that the footer could not be read of because the object
// is not a parquet file at all, rather than being a parquet file that is damaged. A pattern
// matching everything under a folder reaches the files a job writes beside its parquet ones,
// which is what this is usually about.
async fn not_parquet_hint(object_reader: &ParquetObjectStoreReader) -> Option<String> {
    if object_reader.ends_with_parquet_magic().await? {
        return None;
    }

    Some(format!(
        "The last bytes read of the object are not the \"{}\" marker that closes every parquet \
         file, so what was read is not a parquet file at all rather than a damaged one. A \
         pattern that ends in \"*.parquet\" leaves out the files a job writes beside its \
         parquet ones, e.g. the \".crc\" and \"_committed\" files of a folder written by Spark.",
        String::from_utf8_lossy(PARQUET_MAGIC)
    ))
}

// parquet_reader_metadata reads the file's footer and settles which schema to read the file
// with: the file's own schema is not always the one, since the Parquet dictionary decoder
// cannot build a dictionary of all value types, and an INT96 column needs its resolution
// hinted.
async fn parquet_reader_metadata(
    object_reader: &mut ParquetObjectStoreReader,
    uri: &Url,
) -> ArrowReaderMetadata {
    let load_result =
        ArrowReaderMetadata::load_async(object_reader, ArrowReaderOptions::new()).await;

    let reader_metadata = match load_result {
        Ok(reader_metadata) => reader_metadata,
        Err(e) => {
            // the uri is worth naming since a pattern reads many files with one command
            let mut message = format!("failed to read the footer of uri {uri}: {e}");

            if let Some(hint) = not_parquet_hint(object_reader).await {
                message.push_str(". ");
                message.push_str(&hint);
            }

            panic!("{}", message);
        }
    };

    let schema_hint = {
        let file_schema = reader_metadata.schema();
        let file_metadata = reader_metadata.metadata().file_metadata();

        let without_dictionaries = schema_without_dictionaries(file_schema);
        let hint_base = without_dictionaries.as_deref().unwrap_or(file_schema);

        schema_with_int96_as_micros(hint_base, file_metadata).or(without_dictionaries)
    };

    let Some(schema) = schema_hint else {
        return reader_metadata;
    };

    let options = ArrowReaderOptions::new().with_schema(schema);

    match ArrowReaderMetadata::try_new(reader_metadata.metadata().clone(), options) {
        Ok(hinted_metadata) => hinted_metadata,
        // keep the file's own schema if it rejects the hinted one
        Err(e) => {
            pgrx::debug2!("Schema hint is rejected: {}", e);

            reader_metadata
        }
    }
}

// parquet_reader_from_uri creates a reader over the whole file. It also returns the number of
// the rows that the file has since the row count is only reachable from the reader's builder.
pub(crate) fn parquet_reader_from_uri(
    uri_info: &ParsedUriInfo,
) -> Result<(ParquetRecordBatchStream<ParquetObjectStoreReader>, i64), String> {
    let (object_reader, reader_metadata, batch_size) = open_parquet_file(uri_info)?;

    let total_rows = reader_metadata.metadata().file_metadata().num_rows();

    let reader = ParquetRecordBatchStreamBuilder::new_with_metadata(object_reader, reader_metadata)
        .with_batch_size(batch_size)
        .build()
        .unwrap_or_else(|e| panic!("{}", e));

    Ok((reader, total_rows))
}

// ParquetRowGroupOpener opens one file and then hands out a reader per row group of it. The
// row groups of one file are what the parallel COPY FROM workers split between themselves,
// and a worker owns many of them, so the footer is read and parsed once per worker instead of
// once per row group. Over an object store that is the difference between one request and one
// request per row group.
pub(crate) struct ParquetRowGroupOpener {
    uri: String,
    object_reader: ParquetObjectStoreReader,
    reader_metadata: ArrowReaderMetadata,
    batch_size: usize,
}

impl ParquetRowGroupOpener {
    pub(crate) fn try_new(uri_info: &ParsedUriInfo) -> Result<Self, String> {
        let (object_reader, reader_metadata, batch_size) = open_parquet_file(uri_info)?;

        Ok(Self {
            uri: uri_info.uri.to_string(),
            object_reader,
            reader_metadata,
            batch_size,
        })
    }

    // row_groups is how many row groups the file holds and how many rows they hold together
    pub(crate) fn row_groups(&self) -> (u32, i64) {
        let metadata = self.reader_metadata.metadata();

        (
            metadata.num_row_groups() as u32,
            metadata.file_metadata().num_rows(),
        )
    }

    // reader_for_row_group creates a reader restricted to a single row group of the file. It
    // reads nothing itself, since what a reader needs to know about the file was already read
    // when the file was opened. The returned row count is the row count of that row group.
    pub(crate) fn reader_for_row_group(
        &self,
        row_group: usize,
    ) -> Result<(ParquetRecordBatchStream<ParquetObjectStoreReader>, i64), String> {
        let total_rows = self
            .reader_metadata
            .metadata()
            .row_groups()
            .get(row_group)
            .ok_or_else(|| format!("row group {} does not exist in uri {}", row_group, self.uri))?
            .num_rows();

        let reader = ParquetRecordBatchStreamBuilder::new_with_metadata(
            self.object_reader.clone(),
            self.reader_metadata.clone(),
        )
        .with_row_groups(vec![row_group])
        .with_batch_size(self.batch_size)
        .build()
        .unwrap_or_else(|e| panic!("{}", e));

        Ok((reader, total_rows))
    }
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
) -> AsyncArrowWriter<Box<dyn AsyncFileWriter>> {
    let parquet_object_writer = parquet_sink_from_uri(uri_info);

    AsyncArrowWriter::try_new(parquet_object_writer, arrow_schema, Some(writer_props))
        .unwrap_or_else(|e| {
            panic!(
                "failed to create parquet writer for uri {}: {}",
                uri_info.uri, e
            )
        })
}

// parquet_sink_from_uri is what the parquet writer of the given uri writes its bytes into. A
// range of a parallel COPY TO writes into the destination's own multipart upload, which the
// participant that claimed the range shares with the other ranges it claims, so that the
// bytes of the copy are uploaded once and by the participant that wrote them. Everything
// else writes the destination on its own, in which case the write is registered for cleanup:
// a copy that is interrupted in the middle of one has an upload to give up.
fn parquet_sink_from_uri(uri_info: &ParsedUriInfo) -> Box<dyn AsyncFileWriter> {
    if let Some(shared_upload_writer) = shared_upload_writer(&uri_as_string(&uri_info.uri)) {
        return shared_upload_writer;
    }

    let copy_from = false;
    let (parquet_object_store, location) = get_or_create_object_store(uri_info, copy_from);

    single_upload_writer(parquet_object_store, location)
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

// A minimal AsyncFileReader over an ObjectStore, mirroring parquet's examples/object_store.rs.
// parquet deprecated ParquetObjectReader in 59.2.0 and asks downstreams to implement the trait
// themselves instead, see https://github.com/apache/arrow-rs/issues/10308.
#[derive(Clone, Debug)]
pub(crate) struct ParquetObjectStoreReader {
    object_store: Arc<dyn ObjectStore>,
    location: Path,
    file_size: u64,
}

impl ParquetObjectStoreReader {
    pub(crate) fn new(object_store: Arc<dyn ObjectStore>, location: Path, file_size: u64) -> Self {
        Self {
            object_store,
            location,
            file_size,
        }
    }

    // ends_with_parquet_magic reports whether the object closes with the marker that every
    // parquet file closes with, which tells an object that is not a parquet file from one that
    // is damaged. None is a tail that could not be read at all, which tells neither.
    async fn ends_with_parquet_magic(&self) -> Option<bool> {
        // an object shorter than the marker cannot end in it, and the range that would be
        // asked for reaches before the start of the object
        let Some(tail_start) = self.file_size.checked_sub(PARQUET_MAGIC.len() as u64) else {
            return Some(false);
        };

        let tail = self
            .object_store
            .get_range(&self.location, tail_start..self.file_size)
            .await
            .ok()?;

        Some(tail == PARQUET_MAGIC)
    }
}

impl AsyncFileReader for ParquetObjectStoreReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        self.object_store
            .get_range(&self.location, range)
            .map_err(ParquetError::from)
            .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
        async move {
            self.object_store
                .get_ranges(&self.location, &ranges)
                .await
                .map_err(ParquetError::from)
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        let file_size = self.file_size;

        async move {
            let metadata = ParquetMetaDataReader::new()
                .with_arrow_reader_options(options)
                .load_and_finish(self, file_size)
                .await?;

            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}
