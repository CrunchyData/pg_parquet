//! Postgres tuples to Arrow record batches, and back.
//!
//! There is one copy of this code in a cluster, and it is the one inside `pg_arrow_base.so`.
//!
//! This crate is not for a provider to link. It is where the conversion lives so that the
//! *extension* can own it: `pg_arrow_base` compiles it in and drives it from its own `COPY`
//! path, and a provider never sees a `TupleDesc` or a `Datum` at all. What crosses into a
//! provider is a `RecordBatch`, over the Arrow C Data Interface — which is a stable ABI, so a
//! provider may be built against a different arrow-rs version than the base, and encodes
//! nothing about Postgres.
//!
//! That split is what keeps the rule the architecture is built on — no duplicated code —
//! honest in both directions. Everything that knows how a `numeric` becomes a `Decimal128`, how
//! a domain unwraps, how a `timestamptz` is normalized, is here, once. Everything that knows
//! how a file is framed is in the provider, once. Two providers installed in the same cluster
//! share one copy of the first and have one copy each of the second, and neither contains a
//! line of the other's.
//!
//! The crate is a library rather than part of the extension crate for one reason: it speaks
//! `pg_sys`, so it has to build against each supported Postgres major, and keeping it separate
//! is what lets that happen without the extension's SQL entity discovery walking 5000 lines of
//! conversion code on every build.
//!
//! What is here is format-agnostic by construction: nothing in this crate knows what file the
//! batches came from or are going to.

/// Arrow arrays to Postgres datums.
pub mod arrow_to_pg;
/// Offsets and null buffers shared by the array builders.
pub mod arrow_utils;
/// Matching a file's columns to a table's, and the casts that makes reading it possible.
pub mod coerce;
/// Postgres column definitions from a file's Arrow schema.
pub mod infer;
/// The widening rules a `COPY FROM` applies to an integer column.
pub mod integer_cast;
/// Whether a file's columns are matched to a table's by position or by name.
pub mod match_by;
/// Postgres datums to Arrow arrays.
pub mod pg_to_arrow;
/// Tuple descriptors, attribute collection, and the array/composite/domain type helpers.
pub mod pgrx_utils;
/// The Arrow schema of a Postgres tuple.
pub mod schema;
/// The Postgres types that have no Arrow counterpart, and the mapping rules for the ones that
/// do: numeric precision and scale, timestamps, geometry, maps, the text fallback.
pub mod type_compat;
