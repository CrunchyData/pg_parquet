//! The two GUCs 0.6.0 had, kept working now that the settings they named belong to the base.
//!
//! `pg_parquet.enable_copy_hooks` and `pg_parquet.max_parallel_workers` configured a `COPY`
//! hook this extension no longer owns: both moved to `arrow.*` with the hook itself. Dropping
//! them would not have been an error a user sees, which is the problem — a dotted name nothing
//! defines is accepted as a placeholder, so `SET pg_parquet.enable_copy_hooks = off` would
//! still succeed and the hook would still be installed. So they are defined here, and what
//! they are set to is forwarded to their `arrow.*` equivalent once, at load.
//!
//! Only a *non-default* value is forwarded. A cluster that upgraded without touching these has
//! nothing to carry over, and one that set `arrow.max_parallel_workers` directly is not
//! overruled by a `pg_parquet.max_parallel_workers` it never set.

use pgrx::pg_sys::{AsPgCStr, GucContext::PGC_SUSET, GucSource::PGC_S_SESSION, SetConfigOption};
use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting, PgMemoryContexts};

/// Deprecated alias for `arrow.enable_copy_hooks`, with the default it had in 0.6.0.
static ENABLE_COPY_HOOKS: GucSetting<bool> = GucSetting::<bool>::new(true);

/// Deprecated alias for `arrow.max_parallel_workers`, with the default and ceiling it had.
static MAX_PARALLEL_WORKERS: GucSetting<i32> = GucSetting::<i32>::new(0);
const MAX_PARALLEL_WORKERS_LIMIT: i32 = 64;

/// Defines both names and forwards them.
///
/// Called before the base is resolved, because `arrow.enable_copy_hooks` decides whether the
/// base installs a hook at all and the base reads it in its own `_PG_init`. A 0.6.0 cluster
/// has this library in `shared_preload_libraries` — 0.6.0's `_PG_init` refused to load any
/// other way — so this runs at startup, before anything has loaded the base.
pub(crate) fn define_and_forward() {
    // the GUC machinery keeps the strings it is given for the life of the process
    unsafe {
        PgMemoryContexts::TopMemoryContext.switch_to(|_| {
            GucRegistry::define_bool_guc(
                c"pg_parquet.enable_copy_hooks",
                c"deprecated: use arrow.enable_copy_hooks",
                c"Kept for compatibility with pg_parquet 0.6.0. Off here turns \
                  arrow.enable_copy_hooks off for the whole cluster, which disables the hook \
                  for every format, not only parquet.",
                &ENABLE_COPY_HOOKS,
                GucContext::Userset,
                GucFlags::default(),
            );

            GucRegistry::define_int_guc(
                c"pg_parquet.max_parallel_workers",
                c"deprecated: use arrow.max_parallel_workers",
                c"Kept for compatibility with pg_parquet 0.6.0. A value above 0 here is the \
                  cluster's starting arrow.max_parallel_workers, for every format.",
                &MAX_PARALLEL_WORKERS,
                0,
                MAX_PARALLEL_WORKERS_LIMIT,
                GucContext::Userset,
                GucFlags::default(),
            );
        });
    }

    // without this, `pg_parquet.enabl_copy_hooks = on` is silently accepted as a placeholder
    // and does nothing, which is the most expensive kind of typo
    crate::pg_compat::MarkGUCPrefixReserved("pg_parquet");

    // `arrow.*` is a placeholder at this point, since the base is not loaded yet: guc.c keeps
    // the assignment and applies it when the base defines the real setting. PGC_S_SESSION
    // rather than an override source, so a later `SET arrow....` still wins.
    if !ENABLE_COPY_HOOKS.get() {
        forward("arrow.enable_copy_hooks", "off");
    }

    let workers = MAX_PARALLEL_WORKERS.get();

    if workers > 0 {
        forward("arrow.max_parallel_workers", &workers.to_string());
    }
}

fn forward(name: &str, value: &str) {
    unsafe {
        SetConfigOption(
            name.as_pg_cstr(),
            value.as_pg_cstr(),
            PGC_SUSET,
            PGC_S_SESSION,
        )
    };
}
