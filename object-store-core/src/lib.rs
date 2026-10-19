//! Postgres-free object store access layer.
//!
//! This crate owns everything about *reaching* an object store: parsing a uri into a
//! (scheme, bucket, path) triple, resolving credentials from the environment and from the
//! cloud vendors' own config files, building the `object_store` client, and caching the
//! clients per (scheme, bucket) until their credentials expire.
//!
//! It deliberately does not depend on `pgrx`. Everything that needs Postgres — the GUCs, the
//! `object_store_read`/`object_store_write` roles, the privilege check, `ereport`, the SQL
//! functions and the C vtable other extensions resolve — lives in the `pg_object_store`
//! extension crate on top of this one. The split is what lets a second extension vendor this
//! crate without also inheriting a second copy of the GUC and role *definitions*, which
//! Postgres only permits one of per cluster.
//!
//! Fallibility is the other half of the split. The code this was extracted from ran inside a
//! Postgres backend and could `panic!` its way out of any problem, because pgrx turns a panic
//! into an `ereport(ERROR)` and the transaction aborts. A library cannot assume that, so every
//! entry point here returns `Result<_, String>` and the caller decides how loud to be.

pub mod cache;
pub mod client_options;
pub mod error;
pub mod runtime;
pub mod stores;
pub mod uri;

pub use cache::{get_or_create_multipart_store, get_or_create_object_store, ObjectStoreCache};
pub use error::object_store_error_message;
pub use runtime::{block_on, runtime};
pub use uri::{base_uri, uri_as_string, ParsedUri};

/// The result of anything that can fail in this crate. The error is a message meant to be
/// handed to a user as-is: the caller is expected to be the one that knows how to report it.
pub type Result<T> = std::result::Result<T, String>;
