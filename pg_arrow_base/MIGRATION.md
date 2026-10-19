# What moves here from pg_parquet, and in what order

Against `pg_parquet` 0.6.0. Tier 1's half of the split is tracked in `pg_object_store`'s
`MIGRATION.md`; this is the tier-2 half.

## Already here

| pg_parquet | here |
|---|---|
| `parquet.list` (`src/parquet_udfs/list.rs`) | `arrow.list`, dispatched on the registry |
| `parquet.schema` (`src/parquet_udfs/schema.rs`) | `arrow.schema`, for any format |
| `parquet.kv_metadata` (`src/parquet_udfs/metadata.rs:185`) | `arrow.kv_metadata` |
| the common subset of `parquet.file_metadata` (`metadata.rs:143`) | `arrow.file_info` |
| `ENABLE_PARQUET_COPY_HOOK` (`src/parquet_copy_hook/hook.rs:43`) | `arrow.enable_copy_hooks` |
| `init_parquet_copy_hook` (`hook.rs:49`), `call_next_process_utility` (`hook.rs:163`) | `pg_arrow_base/src/copy_hook.rs` |
| `copy_stmt_uri`, `copy_stmt_get_option`, `option_matches`, `count_option` (`copy_utils.rs`) | `pg_arrow_base/src/copy_stmt.rs`, format-agnostic |
| `validate_copy_to_options` / `validate_copy_from_options` (`copy_utils.rs:39`) | split: the core-option list is here, every format-specific option is `FormatProvider::validate_option` |

## Still to move, in dependency order

### 1. The pg <-> arrow type mapping — **done**

`src/type_compat/`, `src/arrow_parquet/{pg_to_arrow,arrow_to_pg}{,.rs}` and
`src/arrow_parquet/schema{,.rs}`: about 6.5k lines, and *none* of it is parquet-specific. A
numeric is an Arrow `Decimal128` whatever file it is going into. This is the biggest single
reason tier 2 exists, and until it is here every format would have to reimplement it.

It is the `pg-arrow-convert` crate, which `pg_arrow_base` links; a provider never sees it.
`schema/parse_schema.rs` landed as `pg-arrow-convert/src/schema.rs` with its field-id context
removed: a `PARQUET:field_id` is stamped by the provider that wants one, from the field paths
of the finished schema, so the base's schema builder knows no format's metadata keys.

### 2. The COPY execution path — **done, both directions**

`src/parquet_copy_hook/{copy_to,copy_from,copy_to_dest_receiver,copy_progress}.rs`, plus
`copy_from_stdin.rs`, `copy_to_stdout.rs`, `copy_to_program.rs` and `copy_from_program.rs`.
Depends on (1). Both directions being here is what flipped `arrow.enable_copy_hooks` to on by
default.

Here now, format-agnostic and driving the registry:

- `copy_to.rs` — the statement becomes the `SELECT` core COPY would have run, planned and run
  in a portal whose `DestReceiver` is ours, so the executor keeps doing permissions,
  snapshots, partitions and row security.
- `copy_to_dest_receiver.rs` — the `#[repr(C)]` receiver and the batching: 1024 rows, or
  sooner if a column would outgrow an `i32` offset.
- `batch_writer.rs` — tuples to `RecordBatch` to a provider, over either shape of writer:
  streaming into tier 1's upload when `can_stream_write`, buffered whole when the format's
  container cannot stream.
- `copy_progress.rs` — `pg_stat_progress_copy` for the direction core cannot see, counting the
  bytes at the sink rather than asking tier 1 what it has uploaded.

And for the other direction:

- `copy_from.rs` — the rows the provider produces are encoded as the Postgres *binary copy
  format* and fed to `BeginCopyFrom`'s read callback, so core's `CopyFrom` inserts them.
  Defaults, generated columns, `WHERE`, constraints, triggers, partition routing, `ON ERROR`
  and `FREEZE` are therefore core's, running the same code they run for a csv. The `WHERE`
  clause is transformed here because the statement never reaches `DoCopy`.
- `batch_reader.rs` — a provider's reader to `RecordBatch`es to datums, over either shape of
  reader: `open_ranged_reader` when the format reports `can_read_ranges`, the whole buffer when
  it does not. A uri with a `*` is expanded by listing the literal prefix in front of it, since
  tier 1 lists a prefix and knows nothing about patterns.
- `coerce_schema.rs` landed as `pg-arrow-convert/src/coerce.rs`, with its messages reworded off
  parquet: it is the file-to-table column matching and the casts, which is (1)'s problem.

One asymmetry worth recording: `pg_stat_progress_copy.bytes_total` is extrapolated from the
rows encoded so far and is only reported when every file knows its row count, which a ranged
reader's format need not.

The `STDIN`/`STDOUT`/`PROGRAM` variants are COPY features rather than object store access and
stay in tier 2, which is why `pg_object_store`'s `ParsedUri` dropped `for_program()` and
`for_std_inout()`. They are `endpoint.rs` here: one mechanism for all four, since none of the
three ends is somewhere a provider can be pointed at. A temporary file Postgres owns is written
or read in the provider's usual way, and the bytes are copied at the far end — to the protocol
stream as `CopyData`, or through the pipe `OpenPipeStream` returns. The privilege check splits
with them: a uri is tier 1's question, `pg_execute_server_program` is asked here because this
hook runs before the `standard_ProcessUtility` that would otherwise ask it, and `STDIN`/`STDOUT`
need nothing. `pg_parquet`'s `copy_to_stdout.rs`, `copy_from_stdin.rs`, `copy_to_program.rs`
and `copy_from_program.rs` all collapse into it.

### 3. `CREATE TABLE ... FROM '<uri>'` — **done**

`src/parquet_copy_hook/create_table.rs` is `pg_arrow_base/src/create_table.rs`, dispatched from
the same `ProcessUtility` hook: the `load_from` / `definition_from` reloptions, the validations,
the `IF NOT EXISTS` races, and the rewriting of the statement so that Postgres creates the
table.

`src/arrow_parquet/schema/infer_schema.rs` is `pg-arrow-convert/src/infer.rs` — it is (1) run
backwards, so it belongs with (1) and not with the hook. Two changes in the move:

- `infer_columns_from_uri` became `infer_columns_from_schema`: reading the file is
  `batch_reader::file_schema`'s job, over `FormatProvider::schema` or a ranged reader, and the
  inference is then a function of an Arrow schema alone.
- the composite types go in `arrow_structs` rather than `parquet_structs`. `crunchy_map` keeps
  its name: it is a type a user may already have tables using, and not ours to rename.

The `load_from` half runs the `COPY FROM` from (2) against a synthesized `CopyStmt` rather than
having a load path of its own.

### 4. v2 of the provider ABI, then parquet's reader — **done, as the ABI**

The baseline path hands a provider the whole file. The parquet reader does not want that: it
reads the footer, then coalesced ranges per row group, which is the difference between reading
a 40 GB file and failing to. Provider ABI v2 is the way out, and it is in place:

- `open_ranged_reader` — the provider is handed a `PgArrowByteSourceV1` instead of a buffer and
  pulls the ranges it needs. Backed by `pg_object_store`'s plural `get_ranges`, which is why
  that form is in tier 1's vtable; the base's implementation is `UriSource` in
  `pg_arrow_base/src/io.rs`.
- The same call's `split_begin` / `split_end` — a split-scoped read, so a parallel `COPY FROM`
  can hand one worker one row group. There is nothing to move for this one: `pg_parquet` reads
  in the one backend the statement is in.
- `open_streaming_writer` / `finish_streaming_writer` — the provider writes into a
  `PgArrowByteSinkV1` as it goes, so `COPY TO` never buffers the file. Under the sink is tier
  1's `put_begin`/`put_part`/`put_finish`, which is a single `PUT` below 10 MiB and a multipart
  upload above it; the base's implementation is `WriterSink` in the same file. This replaces
  the single-process half of `src/arrow_parquet/parquet_shared_upload.rs`. The *multi-process*
  half — several workers uploading parts of one file — still needs tier 1's two remaining
  reserved slots.

Every half now has its caller: `COPY TO` drives `open_streaming_writer` through
`batch_writer.rs`, `COPY FROM` drives `open_ranged_reader` through `batch_reader.rs`, and the
split-scoped form of the same call is what a parallel `COPY FROM` hands each worker — item 7.

### 5. Splitting a `COPY TO` into several files — **done**

`src/parquet_copy_hook/copy_to_split_dest_receiver.rs` and the `file_size_bytes` option are
`pg_arrow_base/src/split.rs` plus two dozen lines of the dest receiver. The uri becomes a folder
and the files under it are `data_0`, `data_1`, … in the order they were written, which is
pg_parquet's own layout, so a reader that globs a folder reads the same set.

Three differences from pg_parquet, all deliberate:

- there is no wrapper dest receiver. pg_parquet's split receiver forwards to child receivers
  because the child is where a parquet writer lives; here the *writer* is the thing that is
  closed and reopened, so the splitting is a `finish_current_file` and an `open_writer` on the
  one receiver.
- a child keeps the suffix the statement wrote — `orders.parquet.zst` splits into
  `data_0.parquet.zst` — because a provider is told the name of the file it is writing and reads
  its own meaning out of it. pg_parquet hard-codes `.parquet`.
- the threshold is measured against the bytes tier 1 has been handed, which is exact for a
  streaming format. A buffered one has no file yet, so it is measured against the Arrow bytes it
  was given instead and splits early rather than late. `file_size_bytes` is refused for the ends
  that are not a uri, since a byte stream has nowhere to put a second file.

### 6. Parallel `COPY TO` — **the folder destination is done**

Upstream's `src/parquet_copy_hook/parallel_copy_to.rs` is `pg_arrow_base/src/parallel_copy_to.rs`
plus the shared file-id claim in `split.rs`, behind `arrow.max_parallel_workers`.

Of upstream's three destination shapes, only the folder one — `SplitFiles` — is here, and it is
the one that needs nothing of a format: a file of a folder is written whole, header to footer, by
whoever writes it, so the participants write files of their own rather than parts of one that
somebody has to splice. The leader renders a
`COPY (SELECT ... WHERE ctid >= ... AND ctid < ...) TO '<folder>'` per range and the participants
claim ranges out of DSM; the shared chunk carries that one statement, the range cursor, the file
id cursor and the row count. No `@PART_URI@`, no part prefix, no upload id, no `shm_mq`
participant report and no cleanup registry, because each file is finished — or discarded — by the
writer that opened it.

Upstream's `NUMBER_VALUED_COPY_TO_OPTIONS` / `STRING_VALUED_COPY_TO_OPTIONS` have no equivalent:
the base reads every option with `defGetString` and hands a provider text, so a range's statement
renders each one as a quoted literal.

Upstream's own coverage of this — `src/pgrx_tests/parallel_copy_to.rs`, and the
`copy_cleanup.rs` that goes with the shapes below — is written against a parquet file it reads
back, so it belongs in a provider's suite and not here; what is here is covered by this repo's
`#[pg_test]`s over the fake format.

Still to come are upstream's other two shapes — `SharedUpload` over tier 1's
`PgObjectStoreExtV1`, and `LocalFile` with the part-file splice — both of which need the provider
ABI to grow per-sub-file metadata and a splice entry point, since they are the shapes where the
footer's offsets have to be rebased.

### 7. Parallel `COPY FROM` — **done**

Upstream's `src/parquet_copy_hook/parallel_copy_from.rs` is
`pg_arrow_base/src/parallel_copy_from.rs`, behind the same `arrow.max_parallel_workers`. It needed
nothing of the provider ABI that item 4 did not already put there: `ranged_file_info`'s
`num_splits` is how the leader counts what there is to hand out, and `open_ranged_reader`'s split
range is how a worker reads its own share. A format that reads no ranges, or a file of one
that records no splits — an Arrow IPC stream has no footer to count them in — is read in one
process, which is the honest answer rather than every worker reading the whole file.

What is upstream's and is *not* here: nothing of the design, and one liberty of the shape. The
splits are strided across the workers, the rows come back over one `shm_mq` per worker as a
tagged stream — rows, then a per-split summary — the leader is the only process that inserts and
hands core a binary `COPY` stream it frames itself, and it drains the worker whose split it is
waiting for first. Where it differs is the payload: a worker is handed the provider's *name*, the
statement's format options, the column names and the resolved file list through DSM, because the
format lives in another library than the base and the pointer the leader holds is not one a
worker can be given. Upstream, which is one extension, ships the statement.

Upstream's `src/pgrx_tests/parallel_copy_from.rs` reads parquet files back and is a provider's
suite's to carry; the three `#[pg_test]`s here prove the same properties over the fake format —
every row once in file order, every file of a wildcard read, and a statement the path cannot
split read in this process.

The worker's encoder is `CopyFromStream` itself, unframed (`batch_reader.rs`'s `framed` flag): a
worker's rows are a piece of the leader's stream, so the header, the trailer and
`pg_stat_progress_copy` belong to the leader and not to each piece of it.

## Stays in pg_parquet, deliberately

- Row group statistics, bloom filter settings, column chunk offsets: `parquet.metadata`,
  `parquet.stats` (`src/parquet_udfs/stats.rs`) and the rest of `parquet.file_metadata`. No
  other format has them, and `arrow.file_info` is the common subset on purpose rather than by
  accident.
- `field_ids` and the Iceberg field-id machinery (`src/arrow_parquet/field_ids.rs`), now
  `parquet-format/src/field_ids.rs` in that repo. The base stamps no format's metadata keys.
- `parquet.kv_metadata`, which stayed alongside `arrow.kv_metadata` rather than being replaced
  by it: a parquet key is bytes and may not be valid utf-8, while the registry's form is text,
  because a format whose metadata is not bytes should not have to pretend it is.
- Compression codecs and levels, `parquet_version`, `row_group_size`: format options, which is
  why `validate_option` is the provider's and not the base's.
- ~~`match_by` (`src/arrow_parquet/match_by.rs`)~~ moved: it is about matching a file's columns
  to a tuple's, so it is `pg-arrow-convert`'s, and `match_by` is a base-owned `COPY` option
  rather than one a provider is asked to validate.
- `geospatial_stats.rs`: the bounding boxes a parquet writer puts in a column chunk's
  statistics, which is file layout. The *type* half of it did move — PostGIS geometry to and
  from Arrow's canonical WKB extension type is `pg-arrow-convert/src/type_compat/geometry.rs`,
  because a geometry column is a geometry column whatever file it lands in.
- the cache-inspection UDFs of `src/parquet_udfs/object_store_cache.rs` went past this tier
  rather than into it: a client cache is tier 1's, so they are `object_store.cache_items()`,
  `cache_clear()` and `cache_expire_bucket()`.
