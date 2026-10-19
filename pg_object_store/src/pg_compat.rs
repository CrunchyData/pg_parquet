#![allow(non_snake_case)]

use pgrx::pg_sys::AsPgCStr;

/// Postgres 15 renamed `EmitWarningsOnPlaceholders` to `MarkGUCPrefixReserved`. Both turn a
/// typo under our prefix into a warning instead of a silently accepted placeholder, which is
/// the only defence against `object_store.enable_stor_cache = off` doing nothing at all.
pub(crate) fn MarkGUCPrefixReserved(guc_prefix: &str) {
    #[cfg(pre_pg15)]
    unsafe {
        pgrx::pg_sys::EmitWarningsOnPlaceholders(guc_prefix.as_pg_cstr())
    }

    #[cfg(not(pre_pg15))]
    unsafe {
        pgrx::pg_sys::MarkGUCPrefixReserved(guc_prefix.as_pg_cstr())
    }
}
