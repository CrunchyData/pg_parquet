# What moved here from pg_parquet, and what changed on the way

Against `pg_parquet` 0.6.0, which now links `object-store-core` instead of carrying this code
(`aykut-bozkurt/pg_parquet`, branch `extract-object-store`).

| pg_parquet | here |
|---|---|
| `src/object_store.rs` | `object-store-core/src/error.rs` |
| `src/object_store/object_store_cache.rs` | `object-store-core/src/cache.rs` |
| `src/object_store/client_options.rs` | `object-store-core/src/client_options.rs` |
| `src/object_store/aws.rs` | `object-store-core/src/stores/aws.rs` |
| `src/object_store/azure.rs` | `object-store-core/src/stores/azure.rs` |
| `src/object_store/gcs.rs` | `object-store-core/src/stores/gcs.rs` |
| `src/object_store/http.rs` | `object-store-core/src/stores/http.rs` |
| `src/object_store/local_file.rs` | `object-store-core/src/stores/local_file.rs` |
| `ParsedUriInfo`, `uri_as_string`, `object_store_base_uri` (`src/arrow_parquet/uri_utils.rs`) | `object-store-core/src/uri.rs` |
| `ensure_access_privilege_to_uri` (`src/arrow_parquet/uri_utils.rs:405`) | `pg_object_store/src/privilege.rs` |
| `PG_BACKEND_TOKIO_RUNTIME` (`src/lib.rs:33`) | `object-store-core/src/runtime.rs` |
| `object_store_cache::parquet_test::*` (test-only UDFs) | `pg_object_store/src/udfs.rs`, no longer test-only |

## Deliberate differences

**Nothing panics in the core crate.** Every entry point returns `Result<_, String>`. The code
this came from ran inside a Postgres backend, where pgrx turns a panic into `ereport(ERROR)` and
the transaction aborts; a library cannot assume that. The extension crate is where a failure
becomes an `ereport` again, and the vtable is where it becomes a return code.

**`ParsedUriInfo` lost its Postgres half.** The temporary file behind `COPY ... TO STDOUT` and
the pipe behind `COPY ... TO PROGRAM` are COPY features, not object store access; they stayed
with whoever implements COPY. What is left — `(uri, bucket, path, scheme)` — is `ParsedUri`.
For the same reason, `ensure_access_privilege_to_uri` here does not handle
`pg_execute_server_program`: a program is not a uri.

**The roles are renamed.** `pg_object_store` creates `object_store_read` /
`object_store_write`, not `parquet_object_store_read` / `parquet_object_store_write`, because
they were never about parquet. `pg_parquet` reaches a uri by calling this extension now, so
these are the roles that govern it; its `0.6.0--0.7.0` update grants the new role to every
member of the old one and then drops the old one if nothing else holds it.

**The cache is `thread_local!`, not `static mut`.** Same lifetime in practice — a backend is
single-threaded and a parallel worker is a separate process — but it needs no `static mut`
and no `#[allow(static_mut_refs)]`.

**The cache-inspection functions are no longer test-only.** They were
`#[cfg(feature = "pg_test")]` in `object_store_cache.rs:151`, so `object_store.cache_items()`
in a production build is *new* surface rather than a move. `cache_clear` and
`cache_expire_bucket` are restricted to superusers, since they are session-wide side effects.

**Expiry no longer ereports from the core crate.** A library has no business deciding where a
trace message goes, so the host installs a sink (`runtime::set_trace_sink`); the extension
points it at `ereport(DEBUG2)`, which is what `object_store_cache.rs` did inline.

**`object_store.enable_store_cache` is new.** There was no way to opt out of the per-session
client cache.

## Still to move, and where it goes

Nothing in this repository depends on these; they are listed so the boundary is legible.

- **Multi-process reads.** `get_ranges` serves one backend pulling the ranges it wants, which is
  what a `COPY FROM` does today. A split-scoped read — one worker, one row group — is a
  `pg_arrow_base` entry point already, and a parallel `COPY FROM` drives it from
  `pg_arrow_base/src/parallel_copy_from.rs`, so there is nothing left for either side.

Nothing to move for one of upstream's: **giving up the upload of an interrupted `COPY TO`**
(`src/arrow_parquet/parquet_single_upload.rs`). A write handle here is in the handle table with
an xact-end callback and a backend-wide `on_proc_exit` one behind it (`writer.rs`), so an
aborted statement or a killed backend aborts the multipart upload it was in the middle of —
which is the whole of what upstream added a module for.

Done since this list was written: **the multi-process multipart upload.** The single-writer case
is `put_begin` / `put_part` / `put_finish` / `put_abort` in the main table, wrapping
`object_store::buffered::BufWriter`, which buffers below 10 MiB and starts a multipart upload
above it. `get_or_create_multipart_store`'s own case — several *processes* uploading parts of one
file, which is what a parallel `COPY TO` of a single file needs — is the second table
(`PgObjectStoreExtV1`, hung off the main one's first reserved slot): `multipart_begin` /
`multipart_put_part` / `multipart_complete` / `multipart_abort`, plus the `delete` the main table
never had and the per-worker part files of a local destination need. What crosses the boundary
there is the store's own identifiers rather than a handle, since the upload is not local to any
one backend. The *stitching* — rebasing row group offsets, writing the footer — is format-specific
and belongs in the provider either way.

Also done: the ranged `AsyncFileReader` (`ParquetObjectStoreReader`) is
gone, because `pg_parquet` is handed a byte source by `pg_arrow_base` and no longer holds an
`ObjectStore` at all — it links only `object-store-client`, for the UDFs that read a footer from
a uri. `parquet.list` is gone too: `arrow.list` dispatches on the registry and names the format
of every file it lists, and `object_store.list` here is the raw form. Both render a location the
way the prefix was written, so a listing can be fed straight back to a read.
