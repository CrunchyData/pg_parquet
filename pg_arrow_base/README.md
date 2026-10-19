# pg_arrow_base

The substrate every Arrow-backed file format extension is written against.

```
tier 3  FORMAT PROVIDERS       pg_parquet | pg_arrow | pg_avro | pg_geoarrow
tier 2  pg_arrow_base          <- this: the COPY hook, the format registry, pg<->arrow mapping
tier 1  pg_object_store        uris, credentials, s3|azure|gcs|http|file, one client cache
```

The placement test for any line of code: **providers never touch Postgres internals,
pg_arrow_base never touches a credential.**

Tier 2 owns what is the same for every format and everything that touches Postgres. It reaches
an object store by calling tier 1's vtable rather than linking it, so a cluster with four
format extensions installed still has one credential cache, one tokio runtime and one set of
connection pools.

## Layout

| path | what |
|---|---|
| `format-registry/` | a library with no pgrx dependency: the `FormatProvider` trait, the registry, and the C ABI a provider in its own `.so` registers over. A provider depends on this and on nothing else of ours. |
| `pg-arrow-convert/` | Postgres tuples to Arrow record batches and back: tuple descriptors, datums, type oids, the numeric and timestamp and geometry mapping rules. Compiled into the extension below, and *not* for a provider to link — a provider never sees a `TupleDesc`. This is the one copy of this code in a cluster. |
| `object-store-client/` (tier 1's repo) | the vtable ABI and the marshalling over it, linked rather than copied: `pg_object_store` publishes the same struct this crate declares, so a layout skew between the two sides is impossible rather than checked. `pg_arrow_base/src/object_store.rs` is only the base's two decisions on top of it — how tier 1 is found, and how loud a failure is. |
| `pg_arrow_base/` | the extension: the `arrow.*` GUCs, the `arrow` schema and its functions, the `ProcessUtility` hook, the tier-1 client, and the registry vtable providers resolve. |

## State

What works today: the registry and its ABI, format resolution with on-demand provider loading,
the `arrow` schema, reading a file's schema, metadata and shape through any registered
provider, both halves of reaching tier 1 — whole-object and ranged reads, and a streaming
writer that holds a `pg_object_store` upload handle rather than a buffer — both directions of
`COPY`, and `CREATE TABLE ... FROM` a file:

```sql
COPY orders TO 's3://bucket/orders.parquet' WITH (format 'parquet', compression 'zstd');
COPY (SELECT * FROM orders WHERE day = '2026-10-17') TO '/data/day.arrow' WITH (format 'arrow');

COPY orders FROM 's3://bucket/orders.parquet' WITH (format 'parquet');
COPY orders (id, total) FROM 's3://bucket/day=*/part-*.parquet'
    WITH (format 'parquet', match_by 'name') WHERE total > 0;
```

The statement becomes the `SELECT` core `COPY` would have run, the executor produces the rows,
and the tuples become `RecordBatch`es through `pg-arrow-convert/` and a file through whichever
provider the format named — streamed into the object store as it is produced when the format
can stream, and written in one `PUT` when its container cannot. `pg_stat_progress_copy` reports
it, a failed statement leaves no file rather than a truncated one, and none of it knows what
parquet is.

A `COPY FROM` runs the other way round the same way: the provider's batches become the
Postgres binary `COPY` stream and core reads that, so defaults, generated columns, `WHERE`,
constraints, triggers, partition routing, `ON ERROR` and `FREEZE` are core's — the same code
that runs them for a csv. A uri with a `*` in it reads every file that matches, and `match_by`
decides whether the file's columns line up with the table's by position or by name.

A table can also be defined by a file, and filled from it in the same statement:

```sql
CREATE TABLE orders () WITH (load_from = 's3://bucket/orders.parquet');       -- columns and rows
CREATE TABLE orders () WITH (definition_from = '/data/day.arrow');            -- columns only
```

The statement is *rewritten* rather than carried out here: the file's schema becomes a column
list, Postgres creates the table from it, and a `load_from` then runs the `COPY FROM` the user
would have written next. So `IF NOT EXISTS`, tablespaces, event triggers and ownership are all
still core's, and a struct field in the file becomes a composite type in the `arrow_structs`
schema — which belongs to no extension, because a column's type has to outlive a
`DROP EXTENSION`.

Now that both directions are served, `arrow.enable_copy_hooks` defaults to **on**; a `COPY` or
`CREATE TABLE` the extension does not recognize is still passed on untouched.

A `COPY TO` can also be told how large a file may get, and writes as many as it takes:

```sql
COPY orders TO 's3://bucket/orders.parquet' WITH (format 'parquet', file_size_bytes '512MB');
-- s3://bucket/orders.parquet/data_0.parquet, data_1.parquet, ...
```

Whole files rather than slices of one, so each reads on its own and the set reads back with a
`*`. A `COPY ... TO STDOUT` or `TO PROGRAM` is one byte stream and cannot be split, which is the
one place the option is refused.

A folder destination is also where parallelism comes from. `arrow.max_parallel_workers` — 0 by
default, so nothing changes until a cluster says so — lets several processes fill one folder:
each claims ctid ranges of the table and writes the rows of a range into files of its own.

```sql
SET arrow.max_parallel_workers TO 4;
COPY orders TO 's3://bucket/orders.parquet' WITH (format 'parquet', file_size_bytes '512MB');
```

Only a plain, non-temporary heap of at least 1024 pages is split, and only into a folder: a
single file is written by one writer from its header to its footer, so there is nothing to hand
a second process. Making *that* parallel needs the multi-process upload behind tier 1's reserved
slots and a provider that can rebase a part's offsets; `MIGRATION.md` tracks what moves where.

The same setting splits a `COPY FROM`, where what is handed around is the decoding: a file's
splits — a parquet row group, an Arrow IPC record batch — are strided across the workers, and the
rows come back to this process already encoded.

```sql
SET arrow.max_parallel_workers TO 4;
COPY orders FROM 's3://bucket/orders.parquet' WITH (format 'parquet');
```

The rows are inserted here, in the order the file holds them, because only the process running
the statement may write into a relation — so a trigger, a default, an identity or a generated
column keeps the statement in this process, as does a format that cannot read ranges, a
`FROM STDIN`, a `FROM PROGRAM` and a relation that is not a plain heap. There is nothing to
divide below two splits either, so a small file is read as it always was.

One gap worth knowing about: `pg_stat_progress_copy.bytes_total` is reported for a `COPY FROM`
only when every file being read knows its row count. A format whose reader streams from a
`ByteSource` without a row count in its header leaves the column NULL rather than reporting an
extrapolation from nothing.

## Installing

```sh
cargo pgrx install --package pg_arrow_base --release --pg-config "$(which pg_config)"
```

```sql
CREATE EXTENSION pg_arrow_base CASCADE;  -- CASCADE picks up pg_object_store
```

Preloading via `shared_preload_libraries = 'pg_arrow_base, pg_object_store'` is recommended but
not required. Preloaded, the registry vtable exists before any provider looks for it; otherwise
the first provider to load finds the slot empty and `load_file`s the base itself.

A provider extension is installed the same way and registers itself when its library loads,
which happens on demand the first time a session names its format.

## SQL

Every function below is dispatched on the registry, so a format that installs later gets them
for free. `format` is optional: if it is omitted the uri's extension decides, and if that is
ambiguous or unknown the function says so rather than guessing.

| function | notes |
|---|---|
| `arrow.formats() -> (format, file_extensions)` | the formats *loaded in this session*. Empty does not mean nothing is installed — it means nothing has been used yet. |
| `arrow.schema(uri [, format]) -> (field_position, field_name, arrow_type, nullable)` | one row per top-level field |
| `arrow.kv_metadata(uri [, format]) -> (key, value)` | `value` is NULL for a key stored without one, which is not an empty string |
| `arrow.file_info(uri [, format]) -> (format, size_bytes, num_rows, num_splits, split_label, compression, format_version)` | every column but the first may be NULL: a format is allowed not to know |
| `arrow.list(uri) -> (location, size_bytes, format)` | uri as a prefix. `format` is NULL for a file no loaded provider claims, which is how to see that half a prefix is unreadable |

Reading a uri needs `object_store_read`, or `pg_read_server_files` for a path on the server —
`pg_object_store`'s roles, enforced by `pg_object_store`. See its README.

## GUCs

| name | default | what |
|---|---|---|
| `arrow.enable_copy_hooks` | `on` | whether a `COPY` naming a uri and a registered format is served here. Off installs no `ProcessUtility` hook at all, and it has to be on when the library is *loaded*, so a cluster that wants it adds `pg_arrow_base` to `shared_preload_libraries`. |
| `arrow.default_format` | *(empty)* | the format to assume when a statement names none and the uri's extension implies none. Empty means make no assumption: a wrong guess reads a whole file before failing to parse it. |

## Writing a provider

Implement one trait. A provider is handed the bytes of a file and hands back the bytes of a
file; it sees no uri, no credential, no `pg_sys`, no `TupleDesc`.

```rust
pub trait FormatProvider: Send + Sync {
    fn name(&self) -> String;
    fn file_extensions(&self) -> Vec<String>;
    fn validate_option(&self, name: &str, value: &str) -> Result<()>;
    fn schema(&self, data: &[u8]) -> Result<SchemaRef>;
    fn kv_metadata(&self, data: &[u8]) -> Result<Vec<(String, Option<String>)>>;
    fn file_info(&self, data: &[u8]) -> Result<FileInfo>;
    fn open_reader(&self, data: Bytes, options: &FormatOptions)
        -> Result<Box<dyn RecordBatchReader + Send>>;
    fn open_writer(&self, schema: SchemaRef, options: &FormatOptions)
        -> Result<Box<dyn RecordBatchSink>>;

    // optional, defaulted: a format that can avoid the whole file says so here
    fn can_read_ranges(&self) -> bool { false }
    fn open_ranged_reader(&self, source: Arc<dyn ByteSource>, options: &FormatOptions,
                          splits: Option<Range<i64>>)
        -> Result<Box<dyn RecordBatchReader + Send>>;
    fn can_stream_write(&self) -> bool { false }
    fn open_streaming_writer(&self, schema: SchemaRef, options: &FormatOptions,
                             sink: Box<dyn ByteSink>) -> Result<Box<dyn StreamingSink>>;
}
```

Then register it. A provider in its own extension — which is every real one — cannot hand the
base a `Box<dyn FormatProvider>` even though both are Rust and both link `format-registry`:
Rust has no stable ABI and a pgrx extension is a `cdylib`, so `nm -D --defined-only` on one
shows its Postgres entry points and nothing else. So registration goes over a C vtable, and the
base wraps the result back into a `dyn FormatProvider` (`format_registry::abi::CProvider`) so
everything above the registry is written once.

See `pg_arrow_base/include/pg_arrow_base.h` for the ABI and the full contract, and
`format-registry/src/abi.rs` for the Rust side. The data path is the Arrow C Data Interface,
which means a provider may be built against a different arrow-rs version than the base.

### What a provider owes the base

- Nothing ereports and nothing longjmps out; a failure is a return code and a message.
- `PGARROW_UNSUPPORTED` for a question the format cannot answer, so the base can render NULL
  instead of failing a query.
- Everything returned through an out-parameter is palloc'd in the caller's context. There is no
  free function: the caller's context teardown is the free.
- An `ArrowSchema` or `ArrowArray` passed in is consumed.
- Exactly one terminator runs for a writer handle — `finish_writer`, `finish_streaming_writer`,
  or `close_writer` if the statement failed first. A `finish_` call closes the handle whether it
  succeeded or not; `close_writer` is only called on a handle that was never finished. All
  tolerate a null handle.
- A callback the base installs (`get_ranges`, `write`) returns a code and no message: the base
  keeps the real message on its own side and reports that one rather than a copy of it.

### Whole files, and the two ways around them

The baseline path hands a provider the whole file and takes the whole file back. That is
correct, and it is not how `pg_parquet` reads a 40 GB file: it fetches the footer, then
coalesced ranges per row group. So provider ABI v2 adds a way out at each end, and a provider
may implement either, both or neither:

- **`open_ranged_reader`** takes a `ByteSource` instead of a buffer and fetches what it needs.
  `splits` restricts the read to a half-open range of row groups / record batches / blocks,
  which is what gives one worker of a parallel `COPY FROM` its share; `None` is the whole file.
  A format that cannot restrict a read must report `can_read_ranges() == false` rather than
  ignore the range, since reading everything in every worker would multiply the rows.
- **`open_streaming_writer`** takes a `ByteSink` and writes the file into it as it goes, so a
  `COPY TO` of any size never materializes its output. Under the sink is tier 1's
  `put_begin`/`put_part`/`put_finish`, which is a single `PUT` for a small file and a multipart
  upload for a large one.

Both sides of that are implemented here (`pg_arrow_base/src/io.rs`): a source is a uri and a
size, a sink is a tier 1 write handle. Neither reports — a failure in a callback invoked from
inside a provider's frames travels back as a value, because an `ereport` there would longjmp
past the provider's cleanup. `COPY TO` drives the sink half and `COPY FROM` the source half, so
both are exercised by the suite rather than only specified.

## Building

Same libclang and ICU recipe as `pg_object_store`; see its README. In short:

```sh
pip install --target /tmp/libclangpkg libclang
export LIBCLANG_PATH=/tmp/libclangpkg/clang/native
export BINDGEN_EXTRA_CLANG_ARGS="-I/usr/lib/gcc/$(gcc -dumpmachine)/$(gcc -dumpversion)/include -I/path/to/icu/include"
export CFLAGS="-I/path/to/icu/include"
```

## Testing

```sh
cargo test -p format-registry          # the registry, option parsing, extension inference
cargo pgrx test pg18 --package pg_arrow_base
```

The `#[pg_test]`s cover what one extension can cover alone: the schema, the GUCs, the
published vtable, and the error messages for an unknown or uninferable format. Anything that
reads a file needs `pg_object_store` installed in the same cluster, since the base reaches an
object store only through tier 1; those paths are covered from the provider repositories, which
install both.

`pg_object_store` has to be installed **without** `pg_test`, since the control file requires it
and `CREATE EXTENSION ... CASCADE` installs it first:

```sh
(cd ../pg_object_store && cargo pgrx install --package pg_object_store --no-default-features \
    --features pg18 --pg-config ~/.pgrx/18.6/pgrx-install/bin/pg_config)
```

A `pg_test` build of it owns the `tests` schema that pgrx's test functions go in, so this
extension's script then fails with `schema tests is not a member of extension` and every later
test reports `Could not obtain test mutex`. Running `cargo pgrx test` over there leaves exactly
that build installed.
