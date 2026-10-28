//! What a file format has to be able to do, and nothing about Postgres or about credentials.
//!
//! This crate is the contract between tier 2 and tier 3 of the split:
//!
//! ```text
//! tier 3  FORMAT PROVIDERS       pg_parquet | pg_arrow | pg_avro | pg_geoarrow
//!                                  ^ implement FormatProvider, register at _PG_init
//! tier 2  pg_arrow_base          the COPY hook, this registry, pg<->arrow mapping
//!                                  ^ owns the GUCs, the `arrow` schema, all I/O
//! tier 1  pg_object_store        uris, credentials, s3|azure|gcs|http|file
//! ```
//!
//! It has no pgrx dependency, deliberately: a provider that linked pgrx to speak to the
//! registry would be a provider that could reach Postgres internals, and the whole point of
//! the boundary is that it cannot. A provider sees bytes and Arrow, and reports failures as
//! `Result<_, String>`.
//!
//! ## Who does the I/O
//!
//! The base does. A provider is handed the bytes of a file and hands back the bytes of a file;
//! it never sees a uri, a credential or an object store client. That is what keeps the
//! placement rule — *providers never touch Postgres internals, pg_arrow_base never touches a
//! credential* — decidable rather than aspirational, and it is why adding `pg_avro` later costs
//! nothing in tier 1 or tier 2.
//!
//! The baseline of that is whole-buffer: a read materializes the file and a write buffers the
//! output before upload. That is correct but not how `pg_parquet` reads a 40 GB parquet file,
//! which issues coalesced ranged reads against the footer and then per-row-group. So v2 of the
//! provider ABI inverts the direction at both ends without giving a provider a uri: it hands
//! one a [`provider::ByteSource`] to pull ranges from and a [`provider::ByteSink`] to push
//! bytes into, both implemented by the base over tier 1. A provider that implements neither is
//! handed whole files as before; see `MIGRATION.md`.
//!
//! ## Two ways to be a provider
//!
//! - **Statically linked.** Implement [`provider::FormatProvider`] and call
//!   [`registry::register`]. Only possible for a provider built into the same `.so` as the
//!   base, which in practice means a test provider.
//! - **A separate extension.** Fill in an [`abi::PgArrowProviderV1`] and register it over the
//!   base's rendezvous vtable at `_PG_init`. The base wraps it back into a
//!   `dyn FormatProvider` with [`abi::CProvider`], so everything above the registry is written
//!   once. This is what a real provider does, because Rust has no stable ABI and a pgrx
//!   extension is a `cdylib`.

pub mod abi;
pub mod options;
pub mod panic_guard;
pub mod provider;
pub mod registry;

/// Nothing in this crate panics and nothing in it knows how loud a failure should be, so every
/// fallible entry point says so in its return type. The host turns this into an `ereport`.
pub type Result<T> = std::result::Result<T, String>;
