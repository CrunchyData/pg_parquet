# pg_arrow

The Arrow IPC formats — file (Feather v2) and stream — for Postgres.

```
tier 3  FORMAT PROVIDERS       pg_parquet | pg_arrow | pg_avro | pg_geoarrow
                                            ^ this
tier 2  pg_arrow_base          the COPY hook, the format registry, pg<->arrow mapping
tier 1  pg_object_store        uris, credentials, s3|azure|gcs|http|file, one client cache
```

This extension ships **no SQL at all**: no functions, no schema, no types. `COPY ... WITH
(format 'arrow')`, `arrow.schema()`, `arrow.file_info()` and the rest already exist in
`pg_arrow_base` and are dispatched on its registry, so adding a format adds no surface. What is
here is the format and the descriptor that registers it — about as small as a tier-3 extension
gets, which is the point: `pg_avro` will be the same shape.

A provider never sees a uri, a credential, a `TupleDesc` or a `pg_sys`. It is handed the bytes
of a file and hands back Arrow, which is why the whole format lives in a crate with no pgrx
dependency and is tested without a Postgres anywhere.

## Layout

| path | what |
|---|---|
| `arrow-ipc-format/` | the format: the `FormatProvider` implementation, over `arrow-ipc`, and the byte-source adapter a ranged read goes through. No pgrx, no `unsafe`. |
| `pg_arrow/` | the extension: a `static PgArrowProviderV1`, the C shims behind it, and the `_PG_init` that registers it with tier 2. |

## Installing

```sh
cargo pgrx install --package pg_arrow --release --pg-config "$(which pg_config)"
```

```sql
CREATE EXTENSION pg_arrow CASCADE;  -- CASCADE picks up pg_arrow_base and pg_object_store
```

Nothing needs to be preloaded. The first time a session names `format 'arrow'` — or a `.arrow`
uri that the format can be inferred from — tier 2 `load_file`s `$libdir/pg_arrow`, which
registers the provider. A session that never mentions arrow never loads it. The other order
works too: loaded first, this extension finds the registry slot empty and loads the base.

## Using it

Everything below is `pg_arrow_base`'s SQL, reaching this provider through the registry.

```sql
SELECT * FROM arrow.schema('s3://bucket/events.arrow');
SELECT * FROM arrow.file_info('/data/events.arrow');
SELECT * FROM arrow.kv_metadata('az://container/events.arrow');

COPY events TO 's3://bucket/events.arrow' WITH (format 'arrow', compression 'zstd');
COPY events FROM 's3://bucket/day=*/part-*.arrow' WITH (format 'arrow', match_by 'name');
```

Both directions of `COPY` are this format's as much as they are parquet's: the same hook, the
same type mapping, the same endpoints — a uri on any store tier 1 reaches, a local path,
`STDIN`, `STDOUT` and `PROGRAM` — and `CREATE TABLE ... WITH (load_from = ...)` as well.

### Reading in pieces, and in several processes

An IPC *file* is read through tier 2's byte source rather than downloaded: the footer first,
then only the record batches that are wanted. That is what makes a range of its batches a read
of its own, so `arrow.max_parallel_workers` splits a `COPY FROM` of one — the workers decode
the batches and the process running the statement inserts every row, in the order the file holds
them.

```sql
SET arrow.max_parallel_workers TO 4;
COPY events FROM '/data/events.arrow' WITH (format 'arrow');
```

A *stream* has no footer, so there is nothing to count and nothing to divide: such a file is
read forwards in one process, which is reported as a NULL `num_splits` rather than as a
failure.

### Options

| option | values | default | what |
|---|---|---|---|
| `variant` | `file`, `stream` | `file` | which IPC container to write. The file variant has a footer and can be read out of order; the stream variant is a bare sequence of messages. Not inferred from the uri — a provider does not see one. |
| `compression` | `none`, `lz4`, `zstd` | `none` | per-buffer compression inside each record batch. `lz4` is the LZ4 frame format, which is what the IPC spec means by LZ4. |
| `compression_level` | an integer | arrow's default | zstd only, and zstd's levels include negative ones. Arrow range-checks it. |

On read nothing is configurable and nothing needs to be: which of the two variants a file is
comes from its bytes — an IPC file starts with `ARROW1` and a stream does not — rather than from
an option or from the uri's extension. A `.arrows` file containing the file variant reads
correctly.

### What `arrow.file_info()` can and cannot say

```
format | size_bytes | num_rows | num_splits | split_label    | compression | format_version
arrow  |     102400 |   [NULL] |          3 | record batch   |      [NULL] | Arrow IPC file
```

The NULLs are deliberate and documented, not oversights:

- **`num_rows`**: an IPC footer lists where each record batch *is*, not how many rows it has. A
  row count means decoding every block, which is not what a question about a file's shape
  should cost.
- **`compression`**: per-buffer, recorded in each record batch rather than once for the file. A
  file may legally mix codecs between batches, so there is no single honest answer.
- **`num_splits`** is known for the file variant, from the footer's block list. A stream has no
  footer, so it is NULL there — counting the messages would mean reading the whole thing.

`format_version` reports which container it is, since arrow-rs's readers do not expose the IPC
metadata version.

## Building

Same libclang and ICU recipe as the other two; see `pg_object_store`'s README. `format-registry`
comes from the `pg_arrow_base` repository over ssh, and `.cargo/config.toml` sets
`git-fetch-with-cli` so that `git`'s own ssh configuration is what resolves it.

## Testing

```sh
cargo test -p arrow-ipc-format        # the format: round trips, compression, options, bad input
```

The integration tests go in through SQL and down through all three tiers, so they need the
other two extensions installed in the test cluster. `cargo pgrx test` will not do that for you:

```sh
PG_CONFIG=~/.pgrx/18.6/pgrx-install/bin/pg_config
(cd ../pg_object_store && cargo pgrx install --package pg_object_store --no-default-features --features pg18 --pg-config $PG_CONFIG)
(cd ../pg_arrow_base  && cargo pgrx install --package pg_arrow_base  --no-default-features --features pg18 --pg-config $PG_CONFIG)
cargo pgrx test pg18 --package pg_arrow
```

The test cluster preloads both and turns `arrow.enable_copy_hooks` on, which is how the two
`COPY` tests get a `ProcessUtility` hook to exercise. If a prerequisite is missing, Postgres
fails to start and says which library it could not find.

Install a prerequisite **without** `pg_test`, as above. A `pg_test` build of it owns the `tests`
schema that pgrx's own test functions go in, so `CREATE EXTENSION ... CASCADE` installs that
one first and this extension's script then fails with `schema tests is not a member of
extension`, followed by `Could not obtain test mutex` for every later test. Running
`cargo pgrx test` in a prerequisite's repository leaves exactly that build installed, so
re-install it plainly afterwards.
