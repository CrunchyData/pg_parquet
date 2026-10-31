use std::ffi::CStr;
use std::future::Future;
use std::sync::LazyLock;
use std::time::Duration;

use arrow_parquet::geospatial_stats::init_geospatial_stats_accumulator_factory;
use object_store::profile::{GCS_CREDENTIAL_DIRECTORY, PROFILE_NAME};
use parquet_copy_hook::hook::{init_parquet_copy_hook, ENABLE_PARQUET_COPY_HOOK};
use parquet_copy_hook::parallel_copy_to::{MAX_PARALLEL_WORKERS, MAX_PARALLEL_WORKERS_LIMIT};
use parquet_copy_hook::pg_compat::MarkGUCPrefixReserved;
use pgrx::memcxt::PgMemoryContexts;
use pgrx::pg_sys::AsPgCStr;
use pgrx::{prelude::*, GucContext, GucFlags, GucRegistry};
use tokio::runtime::Runtime;

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

extension_sql_file!(
    "../sql/profiles.sql",
    name = "profile_setup",
    requires = ["role_setup"]
);

// PG_BACKEND_TOKIO_RUNTIME creates a tokio runtime that uses the current thread
// to run the tokio reactor. This uses the same thread that is running the Postgres backend.
pub(crate) static PG_BACKEND_TOKIO_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap_or_else(|e| panic!("failed to create tokio runtime: {e}"))
});

// how long the backend waits on a future before it looks at the signals it was sent
const INTERRUPT_CHECK_INTERVAL: Duration = Duration::from_millis(100);

// block_on waits for a future on the backend's runtime, which is how every request
// pg_parquet makes to an object store is waited for. The wait is interruptible: a cancel or
// a termination the backend was sent ends it, rather than the request's own timeout and
// retries, which an object store spends minutes of its own on by default and which a
// Ctrl-C cannot shorten at all.
pub(crate) fn block_on<F: Future>(future: F) -> F::Output {
    let output = PG_BACKEND_TOKIO_RUNTIME.block_on(async move {
        let mut future = std::pin::pin!(future);

        loop {
            match tokio::time::timeout(INTERRUPT_CHECK_INTERVAL, &mut future).await {
                Ok(output) => return Some(output),
                // the future is dropped as this block returns, which is what gives up the
                // request it is in the middle of
                Err(_) if backend_was_interrupted() => return None,
                Err(_) => {}
            }
        }
    });

    match output {
        Some(output) => output,
        // reported outside the runtime: Postgres reports an error by jumping out of the
        // frames above it, and the frames of the runtime must not be among them
        None => {
            unsafe { pg_sys::ProcessInterrupts() };

            unreachable!("the backend was interrupted without reporting it");
        }
    }
}

// backend_was_interrupted tells whether the backend was sent a cancel or a termination that
// ProcessInterrupts is in a position to report. A signal that is held off is left to the
// interrupt check that comes after the wait, which is what the hold off is there for.
fn backend_was_interrupted() -> bool {
    unsafe {
        (pg_sys::QueryCancelPending != 0 || pg_sys::ProcDiePending != 0)
            && pg_sys::InterruptHoldoffCount == 0
            && pg_sys::QueryCancelHoldoffCount == 0
            && pg_sys::CritSectionCount == 0
    }
}

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    if !unsafe { pgrx::pg_sys::process_shared_preload_libraries_in_progress } {
        panic!("pg_parquet must be loaded via shared_preload_libraries. Add 'pg_parquet' to shared_preload_libraries in postgresql.conf and restart Postgres.");
    }

    unsafe {
        PgMemoryContexts::TopMemoryContext.switch_to(|_| {
            GucRegistry::define_bool_guc(
                CStr::from_ptr("pg_parquet.enable_copy_hooks".as_pg_cstr()),
                CStr::from_ptr("Enable parquet copy hooks".as_pg_cstr()),
                CStr::from_ptr("Enable parquet copy hooks".as_pg_cstr()),
                &ENABLE_PARQUET_COPY_HOOK,
                GucContext::Userset,
                GucFlags::default(),
            );

            GucRegistry::define_string_guc(
                CStr::from_ptr("pg_parquet.profile_name".as_pg_cstr()),
                CStr::from_ptr(
                    "Name of the profile the object store credentials are read from".as_pg_cstr(),
                ),
                CStr::from_ptr(
                    "A profile names a set of credentials in the config files of the cloud \
                     sdks, which the session makes its object store requests with instead of \
                     the credentials of the server's environment. Any role may set this, and \
                     parquet.user_profile_mapping is what says which profiles a role may \
                     name. Unset reads the environment of the server."
                        .as_pg_cstr(),
                ),
                &PROFILE_NAME,
                GucContext::Userset,
                GucFlags::default(),
            );

            GucRegistry::define_string_guc(
                CStr::from_ptr("pg_parquet.gcs_credential_directory".as_pg_cstr()),
                CStr::from_ptr(
                    "Directory the service account key of a gcs profile is read from".as_pg_cstr(),
                ),
                CStr::from_ptr(
                    "A gcs profile is read from <directory>/<profile>.json, since gcs has no \
                     config file with a section per profile as s3 and azure have. Only a \
                     superuser sets this, which is what keeps a session to the keys that \
                     were put there."
                        .as_pg_cstr(),
                ),
                &GCS_CREDENTIAL_DIRECTORY,
                GucContext::Suset,
                GucFlags::default(),
            );

            GucRegistry::define_int_guc(
                CStr::from_ptr("pg_parquet.max_parallel_workers".as_pg_cstr()),
                CStr::from_ptr(
                    "Maximum number of parallel workers for a COPY TO or COPY FROM parquet"
                        .as_pg_cstr(),
                ),
                CStr::from_ptr(
                    "A COPY TO has the workers write the rows of the table into parts next \
                     to the destination, which are merged into it, and a COPY FROM has them \
                     decode the row groups of the file the leader inserts. The request is \
                     capped to the server's max_parallel_workers. 0 disables both."
                        .as_pg_cstr(),
                ),
                &MAX_PARALLEL_WORKERS,
                0,
                MAX_PARALLEL_WORKERS_LIMIT,
                GucContext::Userset,
                GucFlags::default(),
            );
        });
    };

    MarkGUCPrefixReserved("pg_parquet");

    init_parquet_copy_hook();

    init_geospatial_stats_accumulator_factory();
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
