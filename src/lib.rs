use std::ffi::CString;

use format_registry::abi::{
    PgArrowRegistryApiV1, PGARROW_OK, PG_ARROW_REGISTRY_MAGIC, PG_ARROW_REGISTRY_RENDEZVOUS_KEY,
    PG_ARROW_REGISTRY_VERSION,
};
use pgrx::pg_sys::{find_rendezvous_variable, load_file, AsPgCStr};
use pgrx::prelude::*;

mod descriptor;
mod legacy_gucs;
mod parquet_udfs;
mod pg_compat;
#[cfg(any(test, feature = "pg_test"))]
mod pgrx_tests;
mod store;

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

/// Registering the format is all there is to do.
///
/// Everything that used to be here — the `COPY` hook, the GUC that turned it off, the trace
/// sink for the object store — belongs to a tier that owns it now. `COPY ... WITH (format
/// 'parquet')` is served by `pg_arrow_base`'s hook over the provider registered below, and the
/// object store is `pg_object_store`'s, including the tracing. What is left of this extension
/// is the format itself (`parquet-format/`), the descriptor that publishes it, and the UDFs
/// that answer parquet-only questions about a file.
///
/// `shared_preload_libraries` is no longer required either: a provider is loaded on demand by
/// the base the first time a session names its format.
///
/// The one thing still kept here that is not the format is the pair of 0.6.0 GUCs, which name
/// settings the base owns now. They are defined before the base is resolved because one of
/// them decides whether the base installs a hook at all. See `legacy_gucs`.
#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    legacy_gucs::define_and_forward();

    register_provider();
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // the COPY hook is the base's, and it has to be installed at load time, so the base is
        // what this suite preloads. pg_parquet itself is loaded on demand, the same way it is
        // in a cluster that never touched shared_preload_libraries.
        vec![
            "shared_preload_libraries = 'pgaudit,pg_arrow_base'",
            "pgaudit.log = 'write'",
        ]
    }
}
