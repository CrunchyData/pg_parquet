//! Apache Parquet as a `pg_arrow_base` provider.
//!
//! ```text
//! tier 3  pg_parquet             <- this, plus the C descriptor in ../src/descriptor.rs
//! tier 2  pg_arrow_base          the COPY hook, the format registry, pg<->arrow mapping
//! tier 1  pg_object_store        uris, credentials, s3|azure|gcs|http|file, one client cache
//! ```
//!
//! ## Why this is a separate crate from the extension
//!
//! Nothing here depends on pgrx, which means `cargo test -p parquet-format` runs the whole of
//! it with no Postgres anywhere: a round trip is a pure function from `RecordBatch`es to bytes
//! and back. It is also the honest shape of the layering — this crate is what the provider
//! *is*, and the extension is how Postgres reaches it.
//!
//! What is *not* here is everything parquet shares with every other Arrow format: the type
//! mapping, the COPY machinery, the uris and the credentials. Those were the bulk of
//! `pg_parquet` and they are tier 2's and tier 1's now.
//!
//! ## What parquet brings that a container format does not
//!
//! The reason provider ABI v2 exists at all:
//!
//! - **Ranged reads.** A parquet reader fetches the footer, decides which row groups it needs,
//!   and fetches those. Reading a 40 GB file through a whole-buffer interface is not slow, it
//!   is impossible. [`source`] turns tier 2's [`format_registry::provider::ByteSource`] into a
//!   `ChunkReader`, so the upstream reader does its own fetching.
//! - **Splits.** Row groups are independently readable, which is what lets one worker of a
//!   parallel `COPY FROM` take a range of them.
//! - **Streaming writes.** The footer is written last, but every page before it can go straight
//!   into an upload. [`writer`] writes into tier 2's `ByteSink` as it goes.
//! - **Options worth having.** Compression and its level, row group sizing, the writer version,
//!   per-column bloom filters and dictionaries, and Iceberg field ids.
//!
//! Field ids are the one option that is about the *schema* rather than about the encoding, and
//! they are stamped here rather than in tier 2 ([`field_ids`]): `PARQUET:field_id` is a parquet
//! metadata key, so tier 2's schema builder has no business knowing it exists.

pub mod column_selection;
pub mod field_ids;
pub mod options;
pub mod provider;
pub mod source;
pub mod writer;

#[cfg(test)]
mod tests;

pub use provider::{Parquet, FILE_EXTENSIONS, FORMAT_NAME, OPTIONS, SPLIT_LABEL};
