//! The Arrow IPC formats as a Postgres extension.
//!
//! ```text
//! tier 3  pg_arrow               <- this
//! tier 2  pg_arrow_base          the COPY hook, the format registry, pg<->arrow mapping
//! tier 1  pg_object_store        uris, credentials, s3|azure|gcs|http|file, one client cache
//! ```
//!
//! This extension is deliberately almost nothing: a `static` descriptor, an `_PG_init` that
//! registers it, and the marshalling in between. It ships **no SQL at all** — no functions, no
//! schema, no types. `COPY ... WITH (format 'arrow')`, `arrow.schema()`, `arrow.file_info()`
//! and the rest already exist in `pg_arrow_base` and are dispatched on the registry, so adding
//! a format adds no surface. That is the whole point of the tiering: `pg_avro` will be the same
//! shape, and the SQL a user writes will not change.
//!
//! The format itself is in `arrow-ipc-format`, which has no pgrx dependency and is tested
//! without a Postgres anywhere; see [`descriptor`] for why that split is not just convenience.
//!
//! ## Loading
//!
//! A format provider is of no use until its library is loaded into a backend, and nobody wants
//! to put four of them in `shared_preload_libraries`. So the base loads one on demand: the
//! first time a session names `format 'arrow'`, or infers it from a `.arrow` uri, it
//! `load_file`s `$libdir/pg_arrow`, which runs the `_PG_init` below. A session that never
//! mentions arrow never loads this.
//!
//! The other direction works too. If this library is loaded first — preloaded, or because
//! another extension pulled it in — `_PG_init` finds the registry slot empty and `load_file`s
//! the base. Either order ends with one registry holding this provider.

use std::ffi::CString;

use format_registry::abi::{
    PgArrowRegistryApiV1, PGARROW_OK, PG_ARROW_REGISTRY_MAGIC, PG_ARROW_REGISTRY_RENDEZVOUS_KEY,
    PG_ARROW_REGISTRY_VERSION,
};
use pgrx::pg_sys::{find_rendezvous_variable, load_file, AsPgCStr};
use pgrx::prelude::*;

mod descriptor;

pgrx::pg_module_magic!();

/// Resolves tier 2 and hands it the descriptor.
///
/// Everything here reports with `ereport` rather than a return value, because it runs in
/// `_PG_init` where there is nobody to return to: either the library finishes loading with the
/// provider registered, or the statement that caused the load fails saying why.
fn register() {
    // a mismatch would register this provider under a name the format does not answer to, and
    // the symptom would be a statement that cannot find a format that is plainly installed
    if !descriptor::describes_the_same_format() {
        error!("the pg_arrow descriptor does not match the format it describes");
    }

    let key = CString::new(PG_ARROW_REGISTRY_RENDEZVOUS_KEY)
        .expect("the rendezvous key has no interior nul");

    let api = unsafe {
        let slot = find_rendezvous_variable(key.as_ptr());

        let mut pointer = *slot;

        // null means the base is not loaded in this backend yet, which is the normal case when
        // this library was preloaded and the base was not
        if pointer.is_null() {
            load_file("$libdir/pg_arrow_base".as_pg_cstr(), false);
            pointer = *slot;
        }

        if pointer.is_null() {
            ereport!(
                PgLogLevel::ERROR,
                PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
                "pg_arrow_base is not available",
                "pg_arrow is a format provider for pg_arrow_base and does nothing without it. \
                 Install it with CREATE EXTENSION pg_arrow_base CASCADE."
            );

            unreachable!("ereport at ERROR does not return");
        }

        &*(pointer as *const PgArrowRegistryApiV1)
    };

    // checked on both sides: an ABI skew between two loaded libraries is a crash, not a link
    // error
    if api.magic != PG_ARROW_REGISTRY_MAGIC || api.version != PG_ARROW_REGISTRY_VERSION {
        error!(
            "pg_arrow_base published magic 0x{:08x} version {}, pg_arrow speaks 0x{:08x} \
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

        error!("could not register the arrow format provider: {message}");
    }
}

#[pg_guard]
#[allow(non_snake_case)]
pub extern "C-unwind" fn _PG_init() {
    register();
}

#[cfg(any(test, feature = "pg_test"))]
mod pgrx_tests;

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    /// The test cluster needs the other two tiers, which is also what makes a missing
    /// prerequisite fail loudly at startup rather than confusingly per test; see
    /// `src/pgrx_tests.rs` for how to install them.
    ///
    /// Preloading is not how this extension is meant to be used — the base loads a provider on
    /// demand — but it is the only way to have the `ProcessUtility` hook installed, since the
    /// base installs it in `_PG_init` and only when the GUC is already on.
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![
            "shared_preload_libraries = 'pg_object_store, pg_arrow_base'",
            "arrow.enable_copy_hooks = on",
        ]
    }
}
