# pg_object_store

Object store access for Postgres extensions: uri parsing, credential chains, clients, roles.

This is tier 1 of the split of `pg_parquet` into reusable layers. It owns everything about
*reaching* an object store and nothing about file formats:

```
tier 3  FORMAT PROVIDERS       pg_parquet | pg_arrow | pg_avro | pg_geoarrow
tier 2  pg_arrow_base          the COPY hook, the format registry, pg<->arrow mapping
tier 1  pg_object_store        uris, credentials, s3|azure|gcs|http|file, one client cache
```

The placement test for any line of code: **providers never touch Postgres internals,
pg_arrow_base never touches a credential.**

## Layout

| path | what |
|---|---|
| `object-store-core/` | a library with no pgrx dependency: uri parsing, the credential chains, the store builders, the client cache. Returns `Result<_, String>` and reports nothing itself. |
| `pg_object_store/` | the extension: the GUCs, the roles and the privilege check over them, the SQL functions, and the C vtable other extensions resolve. |

The split is what lets a second extension vendor `object-store-core` without also inheriting a
second copy of the GUC and role *definitions*, which Postgres only permits one of per cluster.

## Installing

```sh
cargo pgrx install --package pg_object_store --release --pg-config "$(which pg_config)"
```

```sql
CREATE EXTENSION pg_object_store;
```

Preloading via `shared_preload_libraries = 'pg_object_store'` is recommended but not required:
the extension installs no hooks, so it is correct whether it is preloaded or dlopened on
demand by a dependent. Preloading means the vtable exists before anything asks for it and the
first query of a session does not pay for the load.

## Roles

A superuser is unrestricted. Everyone else needs the privileges of a role, chosen by what is
being reached and in which direction — the same shape as Postgres' own rule for local paths:

| target | reading | writing |
|---|---|---|
| a path on the server | `pg_read_server_files` | `pg_write_server_files` |
| a remote uri | `object_store_read` | `object_store_write` |

Both roles are created by `CREATE EXTENSION` if they do not already exist, and are left alone
if they do, so grants survive a reinstall.

## SQL

| function | notes |
|---|---|
| `object_store.parse_uri(uri) -> (scheme, bucket, path)` | resolves no credentials, reaches no network. The first thing to reach for when a uri is rejected and the message is not obvious. |
| `object_store.exists(uri) -> bool` | read role |
| `object_store.size(uri) -> bigint` | NULL if there is no such object. read role |
| `object_store.list(uri) -> (location, size, last_modified)` | uri is treated as a prefix. `location` is a uri in the form the prefix was written in, so it can be read as it stands. read role |
| `object_store.get(uri) -> bytea` | read role |
| `object_store.get_range(uri, offset, length) -> bytea` | read role |
| `object_store.put(uri, bytea) -> bigint` | replaces anything already there. write role |
| `object_store.normalize_uri(uri) -> text` | the uri as the store sees it |
| `object_store.cache_items() -> (scheme, bucket, expires_at)` | the clients cached in this session |
| `object_store.cache_clear()` | superuser |
| `object_store.cache_expire_bucket(bucket)` | superuser |

`get` and `put` materialize into a `bytea`, so an object has to fit in one: a varlena is capped
at 1 GB. This surface is for metadata — a manifest, a `catalog.json`, Iceberg metadata, probing
a prefix before starting work — not for data files. Bulk readers want the vtable.

## GUCs

| name | default | what |
|---|---|---|
| `object_store.enable_store_cache` | `on` | whether a client, once built, is reused for the rest of the session. Building one resolves credentials, which for s3 can mean a round trip to the metadata service or to STS. Turn it off when credentials can be revoked out from under a long-lived session. |

Clients are cached per `(scheme, bucket)` and rebuilt when their credentials expire. Local
paths are never cached.

## Credentials

Resolved from the environment of the *Postgres server process*, with each vendor's own config
files as the fallback. `object-store-core` reads those config files itself, because the
`object_store` crate has no way to.

- **s3** — `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`,
  `AWS_ENDPOINT_URL`, `AWS_REGION`, `AWS_SHARED_CREDENTIALS_FILE`, `AWS_CONFIG_FILE`,
  `AWS_PROFILE`, `AWS_ALLOW_HTTP`, `AWS_<HTTP CLIENT OPTION>`
- **azure** — `AZURE_STORAGE_ACCOUNT`, `AZURE_STORAGE_KEY`, `AZURE_STORAGE_CONNECTION_STRING`,
  `AZURE_STORAGE_SAS_TOKEN`, `AZURE_CONFIG_FILE`, `AZURE_STORAGE_ENDPOINT`,
  `AZURE_ALLOW_HTTP`, `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`,
  `AZURE_FEDERATED_TOKEN_FILE`, `AZURE_<HTTP CLIENT OPTION>`, and `~/.azure/config`
- **gcs** — `GOOGLE_SERVICE_ACCOUNT_KEY`, `GOOGLE_SERVICE_ACCOUNT_PATH`,
  `GOOGLE_<HTTP CLIENT OPTION>`
- **http** — `ALLOW_HTTP`, `HTTP_<HTTP CLIENT OPTION>`

Supported uri spellings: `s3://bucket/key`, `https://bucket.s3.amazonaws.com/key`,
`https://s3.amazonaws.com/bucket/key`, `az://container/key`, `azure://container/key`,
`https://account.blob.core.windows.net/container/key`, `gs://bucket/key`,
`https://host/path`, and a bare local path.

## Using it from another extension

Three modes, in increasing coupling:

### A. SQL

Call the functions above over SPI. No linking, no ABI, no version coupling, and it works from
PL/pgSQL and from an FDW. Bounded by the `bytea` ceiling.

### B. Vendor `object-store-core`

```toml
object-store-core = { git = "https://github.com/CrunchyData/pg_object_store" }
```

The full typed Rust API. The cost is a copy: your extension gets its own tokio runtime, its own
credential cache and its own connection pools, and a `COPY` to parquet and a `COPY` to avro
against the same bucket resolve credentials twice. Never define the GUCs or the roles from a
vendoring extension — Postgres permits one definition per cluster, and the second one errors.

### C. The vtable

```c
#include "pg_object_store.h"

void **slot = find_rendezvous_variable(PG_OBJECT_STORE_RENDEZVOUS_KEY);
PgObjectStoreApiV1 *api = *slot;

if (api == NULL)
{
    load_file("$libdir/pg_object_store", false);  /* lazy load */
    api = *slot;
}

if (api == NULL || api->magic != PG_OBJECT_STORE_API_MAGIC ||
    api->version != PG_OBJECT_STORE_API_VERSION)
    ereport(ERROR, (errmsg("pg_object_store v1 is not available")));
```

One credential cache, one runtime and one connection pool for the whole cluster no matter how
many extensions read object storage. See `pg_object_store/include/pg_object_store.h` for the
full ABI and `pg_object_store/src/vtable.rs` for the rules it keeps — in short: every call
blocks, no call ereports, returned memory is palloc'd in the caller's context, no Rust panic
crosses the boundary, `get_ranges` is plural on purpose, and `check_privilege` is in the vtable
so that no consumer can be the one that forgot it.

Why a C ABI at all: Rust has no stable ABI and a pgrx extension is a `cdylib`, so `nm -D
--defined-only` on a built extension shows only its Postgres entry points. Nothing in Rust is
callable across the `.so` boundary.

## Building

`cargo pgrx` needs libclang for bindgen, and bindgen needs to reach the headers
`pg_locale.h` includes, which on a Postgres built `--with-icu` means the ICU headers. On a host
with no distro clang:

```sh
pip install --target /tmp/libclangpkg libclang   # manylinux build, loads against old glibc
export LIBCLANG_PATH=/tmp/libclangpkg/clang/native
# the pip wheel ships no resource headers, so point clang at gcc's for stddef.h, and at ICU
export BINDGEN_EXTRA_CLANG_ARGS="-I/usr/lib/gcc/$(gcc -dumpmachine)/$(gcc -dumpversion)/include -I/path/to/icu/include"
# BINDGEN_EXTRA_CLANG_ARGS does not reach the cc crate compiling pgrx-cshim.c, so repeat ICU
export CFLAGS="-I/path/to/icu/include"
```
