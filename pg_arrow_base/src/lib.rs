//! The substrate every Arrow-backed file format extension is written against.
//!
//! ```text
//! tier 3  FORMAT PROVIDERS       pg_parquet | pg_arrow | pg_avro | pg_geoarrow
//! tier 2  pg_arrow_base          <- this: the COPY hook, the registry, pg<->arrow mapping
//! tier 1  pg_object_store        uris, credentials, s3|azure|gcs|http|file, one client cache
//! ```
//!
//! Tier 2 owns everything that is the same for every format and everything that touches
//! Postgres: the `ProcessUtility` hook, the `COPY` execution path, the tuple-to-`RecordBatch`
//! mapping, the `arrow` schema and the `arrow.*` GUCs. It reaches an object store by calling tier
//! 1's vtable rather than linking it, so that a cluster with four format extensions installed
//! still has one credential cache, one tokio runtime and one set of connection pools.
//!
//! A format is a provider: it implements `format_registry::provider::FormatProvider`, or fills
//! in the equivalent C descriptor and registers it over the vtable this extension publishes.
//! Either way a provider sees bytes and Arrow and nothing else — no uri, no credential, no
//! `pg_sys`. That is what makes adding `pg_avro` later a tier-3-only change.
//!
//! The hook is installed only when `arrow.enable_copy_hooks` is on, which it is by default now
//! that both directions are served: a `COPY` the extension does not recognize is passed on
//! untouched, so the only statements it changes are the ones that name a uri in a format a
//! loaded provider registered. See [`copy_hook`].

use std::ffi::CString;

use pgrx::{prelude::*, GucContext, GucFlags, GucRegistry, GucSetting};

mod batch_reader;
mod batch_writer;
mod copy_from;
mod copy_hook;
mod copy_plan;
mod copy_progress;
mod copy_stmt;
mod copy_to;
mod copy_to_dest_receiver;
mod create_table;
mod endpoint;
mod io;
mod object_store;
mod parallel_copy_from;
mod parallel_copy_to;
mod pg_compat;
mod providers;
mod registry_vtable;
mod split;
mod udfs;

pgrx::pg_module_magic!();

pgrx::extension_sql_file!("../sql/bootstrap.sql", name = "schema_setup", bootstrap);

/// Whether a `COPY` naming a uri and a registered format is served by this extension.
///
/// On by default. Off means the `ProcessUtility` hook is not installed at all, so a `COPY`
/// means exactly what it means without the extension — which is what a cluster sets while it
/// works out whether a statement's behaviour changed.
///
/// Note that it is read at `_PG_init` for the install and again per statement for the
/// dispatch: turning it off disables the hook for the rest of the session, but turning it on
/// in a backend that loaded the library with it off installs nothing. A cluster that wants it
/// preloads the library.
pub static ENABLE_COPY_HOOKS: GucSetting<bool> = GucSetting::<bool>::new(true);

/// The format to assume when a statement does not name one and the uri's extension does not
/// imply one. Empty means no assumption, which is the default: guessing wrong is worse than
/// asking, because a wrong guess reads a whole file before failing to parse it.
pub static DEFAULT_FORMAT: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);

/// `arrow.default_format`, lowercased, or `None` when the session made no assumption.
pub(crate) fn default_format() -> Option<String> {
    let format = DEFAULT_FORMAT.get()?.to_string_lossy().to_ascii_lowercase();

    (!format.is_empty()).then_some(format)
}

#[pg_guard]
#[allow(non_snake_case)]
pub extern "C-unwind" fn _PG_init() {
    // the GUC machinery keeps the strings it is given for the life of the process, so they
    // cannot come from a context that is reset at the end of this statement
    unsafe {
        pgrx::PgMemoryContexts::TopMemoryContext.switch_to(|_| {
            GucRegistry::define_bool_guc(
                c"arrow.enable_copy_hooks",
                c"whether COPY to or from a uri in a registered format is served by pg_arrow_base",
                c"Both directions are served. Off means no ProcessUtility hook is installed at \
                  all. Must be on when the library is loaded, so a cluster that wants it \
                  preloads the library.",
                &ENABLE_COPY_HOOKS,
                GucContext::Userset,
                GucFlags::default(),
            );

            GucRegistry::define_string_guc(
                c"arrow.default_format",
                c"the format to assume when a statement names none and the uri does not imply one",
                c"Empty means make no assumption and report that the format could not be \
                  determined.",
                &DEFAULT_FORMAT,
                GucContext::Userset,
                GucFlags::default(),
            );

            GucRegistry::define_int_guc(
                c"arrow.max_parallel_workers",
                c"how many parallel workers one COPY TO a folder destination may ask for",
                c"A COPY TO with file_size_bytes writes a folder of whole files, which several \
                  processes can fill at once: each claims ctid ranges of the table and writes \
                  the rows of a range into files of its own. The request is capped to the \
                  server's max_parallel_workers. 0, the default, keeps every statement single \
                  process.",
                &parallel_copy_to::MAX_PARALLEL_WORKERS,
                0,
                parallel_copy_to::MAX_PARALLEL_WORKERS_LIMIT,
                GucContext::Userset,
                GucFlags::default(),
            );
        });
    }

    // without this, `arrow.enabl_copy_hooks = on` is silently accepted as a placeholder and
    // does nothing, which is the most expensive kind of typo
    pg_compat::MarkGUCPrefixReserved("arrow");

    // before any provider can look for us; a provider that loads first and finds the slot
    // empty is expected to load_file this library, which runs this
    registry_vtable::publish();

    if ENABLE_COPY_HOOKS.get() {
        copy_hook::install();
    }

    // every backend of a test cluster, not just the one running the tests: see `pgrx_tests`
    #[cfg(feature = "pg_test")]
    pgrx_tests::lines::register();
}

#[cfg(any(test, feature = "pg_test"))]
mod pgrx_tests;

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // the hook is installed from `_PG_init` and nowhere else, so a test that drives a
        // whole `COPY` needs the library loaded at startup with the GUC already on. Preloading
        // is also the configuration a production cluster uses, which makes this the honest
        // thing to test against rather than a backend that loaded us mid-session.
        vec![
            "shared_preload_libraries = 'pg_arrow_base'",
            "arrow.enable_copy_hooks = on",
        ]
    }
}
