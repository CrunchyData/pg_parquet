//! The Arrow IPC formats — file (Feather v2) and stream — as a `pg_arrow_base` provider.
//!
//! ```text
//! tier 3  pg_arrow               <- this, plus the C descriptor in ../pg_arrow
//! tier 2  pg_arrow_base          the COPY hook, the format registry, pg<->arrow mapping
//! tier 1  pg_object_store        uris, credentials, s3|azure|gcs|http|file, one client cache
//! ```
//!
//! ## Why this is a separate crate from the extension
//!
//! Nothing here depends on pgrx, which means `cargo test -p arrow-ipc-format` runs the whole of
//! it with no Postgres anywhere: a round trip through either variant is a pure function from
//! `RecordBatch`es to bytes and back. The extension crate next door is then only the C
//! descriptor and `_PG_init`, which is the part that genuinely needs a backend to test.
//!
//! It is also the honest shape of the layering. This crate is what the provider *is*; the
//! extension is how Postgres reaches it. A second consumer — a test harness, another extension
//! that wants IPC in-process — can depend on this one and never load a `.so`.
//!
//! ## What the format can and cannot say about itself
//!
//! Arrow IPC is a container around Arrow, which means it is the cheapest possible format to be
//! a provider for: no type mapping, no encoding decisions, no statistics. It also means it
//! records less about itself than parquet does, and `arrow.file_info()` reports NULL rather
//! than inventing a number:
//!
//! - **Row count**: not in the footer. The file footer lists the offset and length of each
//!   record batch block, not how many rows are in one, so a row count means decoding every
//!   block. `num_rows` is NULL.
//! - **Compression**: per-buffer, recorded in each record batch's own metadata rather than once
//!   for the file. `compression` is NULL. A file may even mix codecs across batches, which is
//!   why there is no single honest answer to report.
//! - **Splits**: the file variant knows, because the footer lists the blocks. The stream
//!   variant does not: a stream has no footer, and counting its messages means reading it.
//!
//! The same asymmetry decides what can be read in pieces. A file is read through the base's
//! byte source — the footer, then only the blocks that are wanted — so a range of its record
//! batches is a read of its own, which is what gives one worker of a parallel `COPY FROM` its
//! share. A stream is one forward pass by construction and is read whole.
//!
//! The two variants are distinguished by the `ARROW1` magic a file starts with, on read, and by
//! the `variant` option on write. Not by the uri's extension, even though `.arrows` by
//! convention means the stream: a provider never sees the uri, by design.

mod provider;
mod source;
mod writer;

#[cfg(test)]
mod tests;

pub use provider::{ArrowIpc, Variant, FILE_EXTENSIONS, FORMAT_NAME, OPTIONS, SPLIT_LABEL};
pub use writer::{IpcSink, StreamingIpcSink};
