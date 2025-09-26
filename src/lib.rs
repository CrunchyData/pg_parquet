use pgrx::pg_sys::AsPgCStr;
use std::ffi::CStr;
use std::ffi::CString;
use std::sync::LazyLock;

use parquet_copy_hook::hook::{init_parquet_copy_hook, ENABLE_PARQUET_COPY_HOOK};
use parquet_copy_hook::pg_compat::MarkGUCPrefixReserved;
use pgrx::{prelude::*, GucContext, GucFlags, GucRegistry, GucSetting};
use tokio::runtime::Runtime;

// AWS Configuration GUCs
pub(crate) static AWS_ACCESS_KEY_ID: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
pub(crate) static AWS_SECRET_ACCESS_KEY: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
pub(crate) static AWS_SESSION_TOKEN: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
pub(crate) static AWS_ENDPOINT_URL: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
pub(crate) static AWS_REGION: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

pub(crate) static GOOGLE_SERVICE_ACCOUNT_KEY: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
pub(crate) static GOOGLE_SERVICE_ACCOUNT_PATH: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

mod arrow_parquet;
mod object_store;
mod parquet_copy_hook;
mod parquet_udfs;
#[cfg(any(test, feature = "pg_test"))]
mod pgrx_tests;
mod pgrx_utils;
mod type_compat;

// re-export external api
#[allow(unused_imports)]
pub use crate::arrow_parquet::compression::PgParquetCompression;
#[allow(unused_imports)]
pub use crate::parquet_copy_hook::copy_to_split_dest_receiver::create_copy_to_parquet_split_dest_receiver;

pgrx::pg_module_magic!();

extension_sql_file!("../sql/bootstrap.sql", name = "role_setup", bootstrap);

// PG_BACKEND_TOKIO_RUNTIME creates a tokio runtime that uses the current thread
// to run the tokio reactor. This uses the same thread that is running the Postgres backend.
pub(crate) static PG_BACKEND_TOKIO_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap_or_else(|e| panic!("failed to create tokio runtime: {e}"))
});

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    if !unsafe { pgrx::pg_sys::process_shared_preload_libraries_in_progress } {
        panic!("pg_parquet must be loaded via shared_preload_libraries. Add 'pg_parquet' to shared_preload_libraries in postgresql.conf and restart Postgres.");
    }

    unsafe {
        GucRegistry::define_bool_guc(
            CStr::from_ptr("pg_parquet.enable_copy_hooks".as_pg_cstr()),
            CStr::from_ptr("Enable parquet copy hooks".as_pg_cstr()),
            CStr::from_ptr("Enable parquet copy hooks".as_pg_cstr()),
            &ENABLE_PARQUET_COPY_HOOK,
            GucContext::Userset,
            GucFlags::default(),
        );

        // AWS Configuration GUCs
        GucRegistry::define_string_guc(
            CStr::from_ptr("pg_parquet.aws_access_key_id".as_pg_cstr()),
            CStr::from_ptr("AWS Access Key ID for S3 authentication".as_pg_cstr()),
            CStr::from_ptr(
                "AWS Access Key ID used for authenticating with S3-compatible storage".as_pg_cstr(),
            ),
            &AWS_ACCESS_KEY_ID,
            GucContext::Userset,
            GucFlags::default(),
        );

        GucRegistry::define_string_guc(
            CStr::from_ptr("pg_parquet.aws_secret_access_key".as_pg_cstr()),
            CStr::from_ptr("AWS Secret Access Key for S3 authentication".as_pg_cstr()),
            CStr::from_ptr(
                "AWS Secret Access Key used for authenticating with S3-compatible storage"
                    .as_pg_cstr(),
            ),
            &AWS_SECRET_ACCESS_KEY,
            GucContext::Userset,
            GucFlags::default(),
        );

        GucRegistry::define_string_guc(
            CStr::from_ptr("pg_parquet.aws_session_token".as_pg_cstr()),
            CStr::from_ptr("AWS Session Token for S3 authentication".as_pg_cstr()),
            CStr::from_ptr(
                "AWS Session Token used for temporary credentials with S3-compatible storage"
                    .as_pg_cstr(),
            ),
            &AWS_SESSION_TOKEN,
            GucContext::Userset,
            GucFlags::default(),
        );

        GucRegistry::define_string_guc(
            CStr::from_ptr("pg_parquet.aws_endpoint_url".as_pg_cstr()),
            CStr::from_ptr("AWS S3 Endpoint URL".as_pg_cstr()),
            CStr::from_ptr("Custom endpoint URL for S3-compatible storage services".as_pg_cstr()),
            &AWS_ENDPOINT_URL,
            GucContext::Userset,
            GucFlags::default(),
        );

        GucRegistry::define_string_guc(
            CStr::from_ptr("pg_parquet.aws_region".as_pg_cstr()),
            CStr::from_ptr("AWS Region for S3 operations".as_pg_cstr()),
            CStr::from_ptr("AWS region for S3 bucket operations".as_pg_cstr()),
            &AWS_REGION,
            GucContext::Userset,
            GucFlags::default(),
        );

        GucRegistry::define_string_guc(
            CStr::from_ptr("pg_parquet.google_service_account_key".as_pg_cstr()),
            CStr::from_ptr("Google Service Account Key JSON".as_pg_cstr()),
            CStr::from_ptr("Google Cloud service account key used for authentication".as_pg_cstr()),
            &GOOGLE_SERVICE_ACCOUNT_KEY,
            GucContext::Userset,
            GucFlags::default(),
        );

        GucRegistry::define_string_guc(
            CStr::from_ptr("pg_parquet.google_service_account_path".as_pg_cstr()),
            CStr::from_ptr("Google Service Account Key Path".as_pg_cstr()),
            CStr::from_ptr("Path to Google Cloud service account key file".as_pg_cstr()),
            &GOOGLE_SERVICE_ACCOUNT_PATH,
            GucContext::Userset,
            GucFlags::default(),
        );
    };

    MarkGUCPrefixReserved("pg_parquet");

    init_parquet_copy_hook();
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![
            "shared_preload_libraries = 'pgaudit,pg_parquet'",
            "pgaudit.log = 'write'",
        ]
    }
}
