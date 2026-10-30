use std::{ffi::CStr, fs::File, ops::Range, os::fd::FromRawFd, panic, sync::Arc};

use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use futures::{future::BoxFuture, FutureExt, StreamExt};
use object_store::{
    coalesce_ranges, path::Path, GetOptions, ObjectStore, ObjectStoreExt, ObjectStoreScheme,
    OBJECT_STORE_COALESCE_DEFAULT,
};
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

    // ensure_names_an_object refuses a uri that names the store but no object in it, e.g.
    // "s3://bucket", which a bucket name with an empty path appended to it leaves behind. The
    // object store takes the name of the object it is given, and an empty one asks it to read
    // or write the store itself: a read is answered with the zero bytes a bucket is long, which
    // reads as a file that is too short to hold a footer, and a write is refused only once
    // every row has been written and the upload is being finished, in the words of the store
    // and once per store at that. The uri is refused before any of that instead.
    //
    // A folder destination is not reached here, since the files it holds are named after it,
    // and neither is a pattern, which names the objects it matches rather than a single one.
    pub(crate) fn ensure_names_an_object(&self) -> Result<(), String> {
        if !self.path.as_ref().is_empty() {
            return Ok(());
        }

        let (names_what, example) = match self.scheme {
            ObjectStoreScheme::AmazonS3 => ("the bucket", "s3://bucket/file.parquet"),
            ObjectStoreScheme::MicrosoftAzure => ("the container", "az://container/file.parquet"),
            ObjectStoreScheme::GoogleCloudStorage => ("the bucket", "gs://bucket/file.parquet"),
            ObjectStoreScheme::Http => ("the endpoint", "https://endpoint/file.parquet"),
            _ => ("the root", "/tmp/file.parquet"),
        };

        Err(format!(
            "uri {} names no object. It names {names_what} and nothing in it, while an object \
             is named by the path that follows: write the name of the object as well, e.g. \
             {example}.",
            self.uri
        ))
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
            Self::try_reject_removed_whitespace(uri)?;

            Url::parse(uri).map_err(|e| e.to_string())
        }
    }

    // a uri is parsed as the url standard asks it to be parsed, which removes every tab,
    // carriage return and newline in it and trims the whitespace around it. The name of the
    // object a copy writes is taken from the parsed uri, so a uri holding one of those
    // characters names an object other than the one it was written as: a copy into
    // "s3://bucket/cl<tab>ash.parquet" writes "s3://bucket/clash.parquet", over whatever that
    // object holds, and reports the rows it wrote. Such a uri is refused instead. A local path
    // is not parsed this way, since its characters are escaped into the uri it is turned into,
    // and keeps the name it is given, a trailing space included.
    fn try_reject_removed_whitespace(uri: &str) -> Result<(), String> {
        let uri_as_parsed = uri
            .chars()
            .filter(|c| !matches!(c, '\t' | '\n' | '\r'))
            .collect::<String>();

        // the url standard trims the C0 controls and the spaces around a uri rather than
        // removing them, so the uri is trimmed the same way before it is compared
        let uri_as_parsed = uri_as_parsed.trim_matches(|c: char| c <= ' ');

        if uri_as_parsed == uri {
            return Ok(());
        }

        Err(format!(
            "unsupported whitespace in uri {uri_as_parsed}. A tab, a newline, a carriage return \
             and the whitespace around a uri are removed from it before the name of the object \
             is read out of it, so write the uri without them."
        ))
    }

    // the credentials a uri can carry before its host, "s3://key:secret@bucket/a.parquet",
    // never reach an object store: the store is built from the scheme, the host and the port
    // alone, and its credentials come from the environment, so such a uri is signed with the
    // environment's credentials no matter what it carries. What the credentials in it do reach
    // is every message that names the uri, the server log included, so a uri holding them is
    // refused rather than copied into a log file.
    fn try_reject_credentials(uri: &Url) -> Result<(), String> {
        if uri.username().is_empty() && uri.password().is_none() {
            return Ok(());
        }

        let uri_without_credentials = format!("{}{}", object_store_base_uri(uri), uri.path());

        Err(format!(
            "unsupported credentials in uri {uri_without_credentials}. pg_parquet reads the \
             credentials of an object store from its environment rather than from a uri, so \
             write the uri without them."
        ))
    }

    fn try_parse_scheme(uri: &Url) -> Result<(ObjectStoreScheme, Path), String> {
        if let Ok(scheme_and_path) = ObjectStoreScheme::parse(uri) {
            return Ok(scheme_and_path);
        }

        // the parse fails for a scheme the object store does not know, for a uri that names no
        // host where its scheme needs one, and for a path it refuses, e.g. one with an empty
        // "//" segment or with a separator escaped into a segment, and the error that tells
        // them apart is of a type the object store does not export. Parsing the same uri
        // without its path, and then with a host in place of the one it is missing, tells them
        // apart instead.
        let mut uri_without_path = uri.clone();
        uri_without_path.set_path("");

        if ObjectStoreScheme::parse(&uri_without_path).is_ok() {
            // the path rather than the scheme is what is wrong with the uri, which is reported
            // as such so that the user is not sent looking for a scheme that is already there
            return Err(match Path::from_url_path(uri.path()) {
                Err(path_error) => format!("unsupported path in uri {uri}: {path_error}"),
                // the path the object store refused is not the one the uri carries, as for an
                // https uri whose bucket is the first segment of its path
                Ok(_) => format!("unsupported path in uri {uri}"),
            });
        }

        if uri.host_str().is_none() {
            let mut uri_with_a_host = uri_without_path.clone();

            if uri_with_a_host.set_host(Some("bucket")).is_ok()
                && ObjectStoreScheme::parse(&uri_with_a_host).is_ok()
            {
                // the scheme needs a host and the uri has none, which is what a bucket name
                // that is read from an empty variable leaves behind
                return Err(format!(
                    "no bucket in uri {uri}. pg_parquet reads the bucket or container of an object store from the host of its uri, e.g. s3://bucket/file.parquet."
                ));
            }
        }

        Err(format!(
            "unrecognized uri {uri}. pg_parquet supports local paths, https://, s3://, az:// or gs:// schemes."
        ))
    }

    // the host of an s3, azure or gcs uri is the bucket or the container that holds the object
    // rather than an endpoint, so a port written after it is read by nothing: the endpoint
    // comes from the environment, and the copy goes to the store configured there rather than
    // to the one at the port. A uri carrying a port is refused instead, so that a copy whose
    // uri appears to say where it goes does not go somewhere else. An http(s) uri keeps its
    // port, since there the host is the endpoint and the port is part of it.
    fn try_reject_port(scheme: &ObjectStoreScheme, uri: &Url) -> Result<(), String> {
        let Some(port) = uri.port() else {
            return Ok(());
        };

        let endpoint_setting = match scheme {
            ObjectStoreScheme::AmazonS3 => "the AWS_ENDPOINT_URL environment variable",
            ObjectStoreScheme::MicrosoftAzure => "the AZURE_STORAGE_ENDPOINT environment variable",
            ObjectStoreScheme::GoogleCloudStorage => {
                "the \"gcs_base_url\" of the service account key"
            }
            _ => return Ok(()),
        };

        Err(format!(
            "unsupported port {port} in uri {uri}. The host of this uri is the bucket or the \
             container that holds the object rather than an endpoint, so the port is read by \
             nothing: set the endpoint with {endpoint_setting} and write the uri without a port."
        ))
    }

    // the name of an object is the path of its uri, and a "#" or a "?" written in that name ends
    // the path: what follows is the fragment or the query of the uri, which names nothing in an
    // object store and is sent to none. A uri carrying one names a shorter object than it appears
    // to, which is read or written in place of the object the uri was meant to name, so it is
    // refused instead. Both characters are part of the name of an object once they are escaped,
    // which is what the refusal asks for. A local path never reaches this: building its uri
    // escapes both of them.
    fn try_reject_uri_beyond_the_path(uri: &Url, path: &Path) -> Result<(), String> {
        let (marker, part, escape) = if let Some(index) = uri.as_str().find('?') {
            match uri.as_str()[..index].find('#') {
                // a "?" written after the "#" is part of the fragment rather than a query
                Some(_) => ('#', "fragment", "%23"),
                None => ('?', "query", "%3F"),
            }
        } else if uri.fragment().is_some() {
            ('#', "fragment", "%23")
        } else {
            return Ok(());
        };

        // a query is where a uri carries what is meant for the store rather than for the object,
        // which pg_parquet sends to none, and saying so keeps the escape from being read as the
        // way to write one
        let reaches_the_store = match marker {
            '?' => {
                " A query that is meant for the store, as the version id or the signature of \
                    a presigned uri is, is sent to it by nothing."
            }
            _ => "",
        };

        // every one of both characters is escaped, since the ones that follow the first are
        // already part of the query or the fragment and are meant as part of the name as well
        let escaped_uri = uri.as_str().replace('?', "%3F").replace('#', "%23");

        Err(format!(
            "unsupported \"{marker}\" in uri {uri}. Everything from the \"{marker}\" is the \
             {part} of the uri rather than part of the name of the object, so this uri names the \
             object \"{path}\" rather than the one it appears to name. Write a \"{marker}\" that \
             is part of the name of an object as \"{escape}\", e.g. {escaped_uri}.{reaches_the_store}"
        ))
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

        Self::try_reject_credentials(&uri)?;

        let (scheme, path) = Self::try_parse_scheme(&uri)?;

        Self::try_reject_port(&scheme, &uri)?;

        Self::try_reject_uri_beyond_the_path(&uri, &path)?;

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
    uri_info.ensure_names_an_object()?;

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

                let hint = match pattern_hint(uri_info) {
                    Some(hint) => Some(hint),
                    None => folder_hint(&parquet_object_store, &location, uri_info).await,
                };

                if let Some(hint) = hint {
                    message.push_str(". ");
                    message.push_str(&hint);
                }

                return Err(message);
            }
        };

        // the version the store reports for the object is what the reads of the file ask for,
        // so that they all read the object this metadata describes
        let mut object_reader = ParquetObjectStoreReader::new(
            parquet_object_store,
            location,
            object_store_meta.size,
            object_store_meta.e_tag,
        );

        let reader_metadata = parquet_reader_metadata(&mut object_reader, &uri_info.uri).await;

        pgrx::debug2!("Converted arrow schema is: {}", reader_metadata.schema());

        let batch_size = calculate_reader_batch_size(reader_metadata.metadata());

        Ok((object_reader, reader_metadata, batch_size))
    })
}

// pattern_hint describes a uri that names no object because it is a pattern. A COPY FROM and
// parquet.list resolve the "*" and "**" of one, so a pattern is what a user who listed a folder
// has at hand, while a read of a single file, which is what parquet.schema and the metadata
// functions are, asks the store for the name it was given: the stars are sent as the key, and
// the store reports it as an object that is simply missing, percent escaped at that. An object
// whose own name holds a "*" is read before this is reached, since the uri is read as it is
// written first.
fn pattern_hint(uri_info: &ParsedUriInfo) -> Option<String> {
    if !uri_info.path.as_ref().contains('*') {
        return None;
    }

    let uri = uri_as_string(&uri_info.uri);

    Some(format!(
        "\"{uri}\" holds a \"*\", which a read of a single file does not resolve into the names \
         it matches. Name one of the files instead, e.g. one of those that \
         parquet.list('{uri}') lists."
    ))
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

    // uri is the file the opener was opened on, which is what names the file in the errors of
    // a worker that read a row group of it
    pub(crate) fn uri(&self) -> &str {
        &self.uri
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
    // the version of the object that every read of this reader asks for, which is the version
    // its size and its footer were read from. See read_range.
    e_tag: Option<String>,
}

impl ParquetObjectStoreReader {
    pub(crate) fn new(
        object_store: Arc<dyn ObjectStore>,
        location: Path,
        file_size: u64,
        e_tag: Option<String>,
    ) -> Self {
        Self {
            object_store,
            location,
            file_size,
            e_tag,
        }
    }

    // read_range reads a range of the object as it was when the reader was opened. An object
    // store knows nothing of a read that spans requests, and reading a parquet file takes many
    // of them: the footer says where the row groups are, and the row groups are read with
    // requests of their own. An object that is overwritten in between is otherwise read as one
    // file made of two, the footer of the one it was and the bytes of the one it became, which
    // is a wrong read that reports nothing when the two happen to be laid out alike. So the
    // read asks for the version the footer came from. A store that reports no version for the
    // object cannot be asked for one, which leaves the read as unconditional as it was.
    async fn read_range(&self, range: Range<u64>) -> ParquetResult<Bytes> {
        let options = GetOptions {
            range: Some(range.into()),
            if_match: self.e_tag.clone(),
            ..Default::default()
        };

        let read = async {
            self.object_store
                .get_opts(&self.location, options)
                .await?
                .bytes()
                .await
        };

        read.await.map_err(|e| self.read_error(e))
    }

    // read_error describes an error of a read. A read that is refused because the version it
    // asks for is gone is described as the overwrite it is, since the store says that a
    // precondition failed without saying whose precondition it was or what it was for.
    fn read_error(&self, error: object_store::Error) -> ParquetError {
        if matches!(error, object_store::Error::Precondition { .. }) {
            return ParquetError::General(format!(
                "the object \"{}\" was overwritten while it was being read, so the rest of the \
                 read would have been the bytes of the object as it is now, which the footer \
                 that was read first does not describe. Read it again to read it as it is now. \
                 The read of the version it was asked for: {}",
                self.location,
                object_store_error_message(error)
            ));
        }

        ParquetError::from(error)
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

        // the tail is read of the version the footer was read of, so that an object which was
        // overwritten is not described by what took its place: a read of a version that is
        // gone fails, which tells nothing, and nothing is what is said of it then
        let tail = self.read_range(tail_start..self.file_size).await.ok()?;

        Some(tail == PARQUET_MAGIC)
    }

    // ensure_range_is_in_the_object refuses a read of bytes the object does not have. The footer
    // of a parquet file says where in the file the rows it describes are, and an object that is
    // shorter than those places is not the file its footer describes, which is what a file cut
    // short after its footer was written and two files written into one object both leave behind.
    // Such a read is answered by nothing useful: a store answers a range that begins in the
    // object and ends after it with the bytes it does hold, which the reader slices the asked for
    // range out of and panics over ("range start must not be greater than end: 742858 <= 2925",
    // measured on minio, fake-gcs-server and a webdav), and a local store answers it with a
    // message that reports the same number as both what it expected and what it found. The read
    // is refused with what is wrong with the object instead.
    fn ensure_range_is_in_the_object(&self, range: &Range<u64>) -> ParquetResult<()> {
        if range.end <= self.file_size {
            return Ok(());
        }

        Err(ParquetError::General(format!(
            "the object \"{}\" is {} bytes long, and the footer read from it places a part of \
             the file it describes at the bytes {}..{} of it, which are beyond its end. The \
             object is not the file that footer describes: a file that was cut short after its \
             footer was written reads this way, as do two parquet files that were written into \
             one object. Write the file again.",
            self.location, self.file_size, range.start, range.end
        )))
    }
}

impl AsyncFileReader for ParquetObjectStoreReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        if let Err(e) = self.ensure_range_is_in_the_object(&range) {
            return std::future::ready(Err(e)).boxed();
        }

        async move { self.read_range(range).await }.boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
        async move {
            for range in &ranges {
                self.ensure_range_is_in_the_object(range)?;
            }

            // the ranges are merged and read the way ObjectStore::get_ranges reads them, which
            // is what this would call if a read of a range could be conditional there
            coalesce_ranges(
                &ranges,
                |range| self.read_range(range),
                OBJECT_STORE_COALESCE_DEFAULT,
            )
            .await
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
