# pg_parquet

> Copy from/to Parquet files in PostgreSQL!

[![CI lints and tests](https://github.com/aykut-bozkurt/pg_parquet/actions/workflows/ci.yml/badge.svg)](https://github.com/aykut-bozkurt/pg_parquet/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/aykut-bozkurt/pg_parquet/graph/badge.svg?token=SVDGPEAP51)](https://codecov.io/gh/aykut-bozkurt/pg_parquet)

`pg_parquet` is a PostgreSQL extension that allows you to read and write Parquet files, which are located in `S3` or `file system`, from PostgreSQL via `COPY TO/FROM` commands. It heavily uses [Apache Arrow](https://arrow.apache.org/rust/arrow/) project to read and write Parquet files and [pgrx](https://github.com/pgcentralfoundation/pgrx) project to extend PostgreSQL's `COPY` command.

```sql
-- Copy a query result into Parquet in S3
COPY (SELECT * FROM table) TO 's3://mybucket/data.parquet' WITH (format 'parquet');

-- Load data from Parquet in S3
COPY table FROM 's3://mybucket/data.parquet' WITH (format 'parquet');
```

## Quick Reference
- [Installation From Source](#installation-from-source)
- [Usage](#usage)
  - [Copy FROM/TO Parquet files TO/FROM Postgres tables](#copy-tofrom-parquet-files-fromto-postgres-tables)
  - [Inspect Parquet schema](#inspect-parquet-schema)
  - [Inspect Parquet metadata](#inspect-parquet-metadata)
- [Object Store Support](#object-store-support)
- [Copy Options](#copy-options)
- [Configuration](#configuration)
- [Supported Types](#supported-types)
  - [Nested Types](#nested-types)
- [Postgres Support Matrix](#postgres-support-matrix)

## Installation From Source
After installing `Postgres`, you need to set up `rustup`, `cargo-pgrx` to build the extension.

```bash
# install rustup
> curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# install cargo-pgrx
> cargo install cargo-pgrx

# configure pgrx
> cargo pgrx init --pg17 $(which pg_config)

# append the extension to shared_preload_libraries in ~/.pgrx/data-17/postgresql.conf 
> echo "shared_preload_libraries = 'pg_parquet'" >> ~/.pgrx/data-17/postgresql.conf

# run cargo-pgrx to build and install the extension
> cargo pgrx run

# create the extension in the database
psql> "CREATE EXTENSION pg_parquet;"
```

## Usage
There are mainly 3 things that you can do with `pg_parquet`:
1. You can export Postgres tables/queries to Parquet files,
2. You can ingest data from Parquet files to Postgres tables,
3. You can inspect the schema and metadata of Parquet files.

### COPY to/from Parquet files from/to Postgres tables
You can use PostgreSQL's `COPY` command to read and write Parquet files. Below is an example of how to write a PostgreSQL table, with complex types, into a Parquet file and then to read the Parquet file content back into the same table.

```sql
-- create composite types
CREATE TYPE product_item AS (id INT, name TEXT, price float4);
CREATE TYPE product AS (id INT, name TEXT, items product_item[]);

-- create a table with complex types
CREATE TABLE product_example (
    id int,
    product product,
    products product[],
    created_at TIMESTAMP,
    updated_at TIMESTAMPTZ
);

-- insert some rows into the table
insert into product_example values (
    1,
    ROW(1, 'product 1', ARRAY[ROW(1, 'item 1', 1.0), ROW(2, 'item 2', 2.0), NULL]::product_item[])::product,
    ARRAY[ROW(1, NULL, NULL)::product, NULL],
    now(),
    '2022-05-01 12:00:00-04'
);

-- copy the table to a parquet file
COPY product_example TO '/tmp/product_example.parquet' (FORMAT 'parquet', COMPRESSION 'gzip');

-- show table
SELECT * FROM product_example;

-- copy the parquet file to the table
COPY product_example FROM '/tmp/product_example.parquet';

-- show table
SELECT * FROM product_example;
```

### Inspect Parquet schema
You can call `SELECT * FROM parquet.schema(<uri>)` to discover the schema of the Parquet file at given uri.

### Inspect Parquet metadata
You can call `SELECT * FROM parquet.metadata(<uri>)` to discover the detailed metadata of the Parquet file, such as column statistics, at given uri.

You can call `SELECT * FROM parquet.file_metadata(<uri>)` to discover file level metadata of the Parquet file, such as format version, at given uri.

You can call `SELECT * FROM parquet.kv_metadata(<uri>)` to query custom key-value metadata of the Parquet file at given uri.

## Object Store Support
`pg_parquet` supports reading and writing Parquet files from/to `S3` object store. Only the uris with `s3://` scheme is supported.

You can either set the following environment variables or use shared configuration files to access to the object store:
- `AWS_ACCESS_KEY_ID`: the access key ID of the AWS account,
- `AWS_SECRET_ACCESS_KEY`: the secret access key of the AWS account,
- `AWS_REGION`: the default region of the AWS account.

You can set config and credentials file paths with `AWS_CONFIG_FILE` and `AWS_SHARED_CREDENTIALS_FILE` environment variables. The default config and credentials file paths are `~/.aws/config` and `~/.aws/credentials`. You can also set profile name with `AWS_PROFILE` environment variable. The default profile name is `default`.

## Copy Options
`pg_parquet` supports the following options in the `COPY TO` command:
- `format parquet`: you need to specify this option to read or write Parquet files which does not end with `.parquet[.<compression>]` extension. (This is the only option that `COPY FROM` command supports.),
- `row_group_size <size>`: the number of rows in each row group while writing Parquet files. The default row group size is `100000`,
- `compression <compression>`: the compression format to use while writing Parquet files. The supported compression formats are `uncompressed`, `snappy`, `gzip`, `brotli`, `lz4`, `lz4raw` and `zstd`. The default compression format is `uncompressed`. If not specified, the compression format is determined by the file extension.

## Configuration
There is currently only one GUC parameter to enable/disable the `pg_parquet`:
- `pg_parquet.enable_copy_hooks`: you can set this parameter to `on` or `off` to enable or disable the `pg_parquet` extension. The default value is `on`.

## Supported Types
`pg_parquet` has rich type support, including PostgreSQL's primitive, array, and composite types. Below is the table of the supported types in PostgreSQL and their corresponding Parquet types.

| PostgreSQL Type   | Parquet Physical Type     | Logical Type     |
|-------------------|---------------------------|------------------|
| `bool`            | BOOLEAN                   |                  |
| `smallint`        | INT16                     |                  |
| `integer`         | INT32                     |                  |
| `bigint`          | INT64                     |                  |
| `real`            | FLOAT                     |                  |
| `oid`             | INT32                     |                  |
| `double`          | DOUBLE                    |                  |
| `numeric`(1)      | FIXED_LEN_BYTE_ARRAY(16)  | DECIMAL(128)     |
| `text`            | BYTE_ARRAY                | STRING           |
| `json`            | BYTE_ARRAY                | STRING           |
| `bytea`           | BYTE_ARRAY                |                  |
| `date` (2)        | INT32                     | DATE             |
| `timestamp`       | INT64                     | TIMESTAMP_MICROS |
| `timestamptz` (3) | INT64                     | TIMESTAMP_MICROS |
| `time`            | INT64                     | TIME_MICROS      |
| `timetz`(3)       | INT64                     | TIME_MICROS      |
| `geometry`(4)     | BYTE_ARRAY                |                  |

### Nested Types
| PostgreSQL Type   | Parquet Physical Type     | Logical Type     |
|-------------------|---------------------------|------------------|
| `composite`       | GROUP                     | STRUCT           |
| `array`           | element's physical type   | LIST             |
| `crunchy_map`(5)  | GROUP                     | MAP              |

> [!WARNING]
> - (1) The `numeric` types with <= `38` precision is represented as `FIXED_LEN_BYTE_ARRAY(16)` with `DECIMAL(128)` logical type. The `numeric` types with > `38` precision is represented as `BYTE_ARRAY` with `STRING` logical type.
> - (2) The `date` type is represented according to `Unix epoch` when writing to Parquet files. It is converted back according to `PostgreSQL epoch` when reading from Parquet files.
> - (3) The `timestamptz` and `timetz` types are adjusted to `UTC` when writing to Parquet files. They are converted back with `UTC` timezone when reading from Parquet files.
> - (4) The `geometry` type is represented as `BYTE_ARRAY` encoded as `WKB` when `postgis` extension is created. Otherwise, it is represented as `BYTE_ARRAY` with `STRING` logical type.
> - (5) The `crunchy_map` type is represented as `GROUP` with `MAP` logical type when `crunchy_map` extension is created. Otherwise, it is represented as `BYTE_ARRAY` with `STRING` logical type. This extension is only available at [Crunchy Bridge for Analytics](https://www.crunchydata.com/products/crunchy-bridge-for-analytics).

> [!WARNING]
> Any type that does not have a corresponding Parquet type will be represented, as a fallback mechanism, as `BYTE_ARRAY` with `STRING` logical type. e.g. `enum`

## Postgres Support Matrix
`pg_parquet` is tested with the following PostgreSQL versions:
| PostgreSQL Major Version | Supported |
|--------------------------|-----------|
| 17                       |    ✅     |
| 16                       |    ✅     |
