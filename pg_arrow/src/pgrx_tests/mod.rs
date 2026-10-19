//! The three tiers together.
//!
//! These are the tests that cannot live in `arrow-ipc-format`: each one goes in through SQL,
//! down through `pg_arrow_base`'s dispatch and `pg_object_store`'s file client, and back out
//! through the C descriptor. That means they need **both** of the other extensions installed in
//! the test cluster, which `cargo pgrx test` will not do by itself:
//!
//! ```sh
//! PG_CONFIG=~/.pgrx/18.*/pgrx-install/bin/pg_config
//! (cd ../pg_object_store && cargo pgrx install --package pg_object_store --pg-config $PG_CONFIG)
//! (cd ../pg_arrow_base  && cargo pgrx install --package pg_arrow_base  --pg-config $PG_CONFIG)
//! cargo pgrx test pg18 --package pg_arrow
//! ```
//!
//! The suites are:
//!
//! - [`boundary`] — the descriptor and the dispatch that reaches it.
//! - [`copy_type_roundtrip`] — every Postgres type out to a file and back, which is the claim
//!   that `format 'arrow'` is a peer of `format 'parquet'` and not a subset of it.
//! - [`copy_endpoints`] — the places a `COPY` can read from and write to.
//! - [`copy_pg_rules`] — the parts of `COPY` that belong to Postgres rather than to a format.
//! - [`parallel_copy`] — a `COPY` several processes take part in, in both directions.

mod boundary;
mod common;
mod copy_endpoints;
mod copy_pg_rules;
mod copy_type_roundtrip;
mod parallel_copy;
