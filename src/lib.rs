use std::ffi::{CStr, CString};

use arrow_parquet::geospatial_stats::init_geospatial_stats_accumulator_factory;
use format_registry::abi::{
    PgArrowRegistryApiV1, PGARROW_OK, PG_ARROW_REGISTRY_MAGIC, PG_ARROW_REGISTRY_RENDEZVOUS_KEY,
    PG_ARROW_REGISTRY_VERSION,
};
use parquet_copy_hook::hook::{init_parquet_copy_hook, ENABLE_PARQUET_COPY_HOOK};
use parquet_copy_hook::pg_compat::MarkGUCPrefixReserved;
use pgrx::memcxt::PgMemoryContexts;
use pgrx::pg_sys::{find_rendezvous_variable, load_file, AsPgCStr};
use pgrx::{prelude::*, GucContext, GucFlags, GucRegistry};

mod arrow_parquet;
mod descriptor;
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

/// Resolves `pg_arrow_base` and hands it the parquet descriptor.
///
/// Everything here reports with `ereport` rather than a return value, because it runs in
/// `_PG_init` where there is nobody to return to: either the library finishes loading with the
/// provider registered, or startup fails saying why.
///
/// The base loads a provider on demand — the first session to name `format 'parquet'` or to
/// infer it from a `.parquet` uri `load_file`s `$libdir/pg_parquet` — so this also runs when
/// the base pulled us in, and then the rendezvous slot is already filled.
fn register_provider() {
    // a mismatch would register this provider under a name the format does not answer to, and
    // the symptom would be a statement that cannot find a format that is plainly installed
    if !descriptor::describes_the_same_format() {
        error!("the pg_parquet descriptor does not match the format it describes");
    }

    let key = CString::new(PG_ARROW_REGISTRY_RENDEZVOUS_KEY)
        .expect("the rendezvous key has no interior nul");

    let api = unsafe {
        let slot = find_rendezvous_variable(key.as_ptr());

        let mut pointer = *slot;

        // null means the base is not loaded in this backend yet, which is the normal case:
        // pg_parquet is preloaded and the base is not
        if pointer.is_null() {
            load_file("$libdir/pg_arrow_base".as_pg_cstr(), false);
            pointer = *slot;
        }

        if pointer.is_null() {
            ereport!(
                PgLogLevel::ERROR,
                PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
                "pg_arrow_base is not available",
                "pg_parquet is a format provider for pg_arrow_base. Install it with \
                 CREATE EXTENSION pg_arrow_base CASCADE."
            );

            unreachable!("ereport at ERROR does not return");
        }

        &*(pointer as *const PgArrowRegistryApiV1)
    };

    // checked on both sides: an ABI skew between two loaded libraries is a crash, not a link
    // error
    if api.magic != PG_ARROW_REGISTRY_MAGIC || api.version != PG_ARROW_REGISTRY_VERSION {
        error!(
            "pg_arrow_base published magic 0x{:08x} version {}, pg_parquet speaks 0x{:08x} \
             version {}",
            api.magic, api.version, PG_ARROW_REGISTRY_MAGIC, PG_ARROW_REGISTRY_VERSION
        );
    }

    let mut message = std::ptr::null_mut();

    let code = unsafe { (api.register_provider)(&descriptor::PROVIDER, &mut message) };

    if code != PGARROW_OK {
        let message = if message.is_null() {
            "no reason given".to_string()
        } else {
            unsafe { std::ffi::CStr::from_ptr(message) }
                .to_string_lossy()
                .into_owned()
        };

        error!("could not register the parquet format provider: {message}");
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
            )
        });
    };

    MarkGUCPrefixReserved("pg_parquet");

    // before the hook below, so that pg_arrow_base's own ProcessUtility hook ends up *under*
    // this one: a `COPY ... 'x.parquet'` is still served by the code it was served by before
    // this extension became a provider, and the base's path is reached only by a statement that
    // names a format the old hook does not know
    register_provider();

    init_parquet_copy_hook();

    // object-store-core traces nowhere unless a host says where. Ours is ereport(DEBUG2),
    // which is where the "credentials for <bucket> expired" message used to be written.
    object_store_core::runtime::set_trace_sink(|message| {
        pgrx::ereport!(
            PgLogLevel::DEBUG2,
            PgSqlErrorCode::ERRCODE_WARNING,
            message.to_string()
        );
    });

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
            // for as long as this extension still carries its own COPY hook: loading the base
            // installs that hook too, and two of them on one `COPY` is one too many. Off here
            // means the statements in this suite are served by exactly the code that served
            // them before pg_parquet became a provider. It goes when the old hook does.
            "arrow.enable_copy_hooks = off",
            "pgaudit.log = 'write'",
        ]
    }
}
