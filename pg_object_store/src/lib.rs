//! `pg_object_store` is the Postgres half of object store access: the GUCs, the
//! `object_store_read` / `object_store_write` roles and the privilege check over them, the SQL
//! functions, and the C vtable that other extensions resolve so that a cluster has one
//! credential cache and one tokio runtime no matter how many extensions read object storage.
//!
//! Everything about actually reaching a store — uri parsing, credential chains, the clients,
//! the cache — is in the `object-store-core` crate, which does not depend on pgrx. See its
//! module docs for why the line is drawn there.
//!
//! Unlike `pg_parquet`, this extension does not insist on `shared_preload_libraries`. It
//! installs no hooks, so it is correct whether it is preloaded or dlopened on demand by a
//! dependent. Preloading it is still what you want in production: the vtable then exists
//! before anything asks for it, and the first query of a session does not pay for the load.

use std::ffi::CStr;

use pgrx::memcxt::PgMemoryContexts;
use pgrx::pg_sys::AsPgCStr;
use pgrx::{ereport, PgLogLevel, PgSqlErrorCode};
use pgrx::{prelude::*, GucContext, GucFlags, GucRegistry, GucSetting};

mod multipart;
mod pg_compat;
pub mod privilege;
pub mod udfs;
pub mod vtable;
mod writer;

#[cfg(any(test, feature = "pg_test"))]
mod pgrx_tests;

pgrx::pg_module_magic!();

extension_sql_file!("../sql/bootstrap.sql", name = "role_setup", bootstrap);

/// Whether an object store client, once built, is reused for the rest of the session. Building
/// one resolves credentials, so leaving this on is what keeps a per-statement round trip to
/// the metadata service or to STS off the hot path. Turn it off when credentials can be
/// revoked out from under a long-lived session and you would rather pay to re-resolve.
pub static ENABLE_STORE_CACHE: GucSetting<bool> = GucSetting::<bool>::new(true);

/// Pushes the current value of the GUCs into the core crate, which has no way to read them.
/// Called at the top of every entry point rather than from an assign hook, because an assign
/// hook fires on `SET` while this also has to be right after a `RESET`, a function-local
/// `SET`, or a transaction that rolled a `SET` back.
pub fn sync_core_settings() {
    object_store_core::cache::set_cache_enabled(ENABLE_STORE_CACHE.get());
}

/// Where `object-store-core` sends its trace messages while this extension is loaded.
fn trace_to_ereport(message: &str) {
    ereport!(
        PgLogLevel::DEBUG2,
        PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
        message.to_string()
    );
}

/// What `object-store-core` does while it waits on a store: whatever a cancellation, a
/// statement timeout or a shutdown asks of the backend. Raises, and so does not return, when
/// one of those is pending and may be acted on.
fn stop_if_interrupted() {
    pgrx::check_for_interrupts!();
}

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    // the GUC name and description outlive _PG_init, so they are allocated where Postgres
    // will not free them under us
    unsafe {
        PgMemoryContexts::TopMemoryContext.switch_to(|_| {
            GucRegistry::define_bool_guc(
                CStr::from_ptr("object_store.enable_store_cache".as_pg_cstr()),
                CStr::from_ptr(
                    "Reuse object store clients for the rest of the session".as_pg_cstr(),
                ),
                CStr::from_ptr(
                    "Building a client resolves credentials, which can mean a round trip to a \
                     metadata service or to STS. Clients are cached per (scheme, bucket) and \
                     rebuilt when their credentials expire. Turning this off re-resolves on \
                     every access."
                        .as_pg_cstr(),
                ),
                &ENABLE_STORE_CACHE,
                GucContext::Userset,
                GucFlags::default(),
            );
        });
    }

    pg_compat::MarkGUCPrefixReserved("object_store");

    object_store_core::runtime::set_trace_sink(trace_to_ereport);
    object_store_core::runtime::set_interrupt_sink(stop_if_interrupted);

    vtable::publish();
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
