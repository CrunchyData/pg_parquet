# pg_parquet

> Copy from/to Parquet files in PostgreSQL!

[![CI lints and tests](https://github.com/CrunchyData/pg_parquet/actions/workflows/ci.yml/badge.svg)](https://github.com/CrunchyData/pg_parquet/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/CrunchyData/pg_parquet/graph/badge.svg?token=6BPS0DSKJ2)](https://codecov.io/gh/CrunchyData/pg_parquet)

`pg_parquet` is a PostgreSQL extension that allows you to read and write [Parquet files](https://parquet.apache.org), which are located in `S3`, `Azure Blob Storage`, `Google Cloud Storage`, `http(s) endpoints` or `file system`, from PostgreSQL via `COPY TO/FROM` commands. It depends on [Apache Arrow](https://arrow.apache.org/rust/arrow/) project to read and write Parquet files and [pgrx](https://github.com/pgcentralfoundation/pgrx) project to extend PostgreSQL's `COPY` command.

```sql
-- Copy a query result into Parquet in S3
COPY (SELECT * FROM table) TO 's3://mybucket/data.parquet' WITH (format 'parquet');

-- Load data from Parquet in S3
COPY table FROM 's3://mybucket/data.parquet' WITH (format 'parquet');
```

## Quick Reference
- [Installation From Source](#installation-from-source)
- [Usage](#usage)
  - [Copy FROM/TO Parquet files TO/FROM Postgres tables](#copy-fromto-parquet-files-tofrom-postgres-tables)
  - [Create Postgres tables from Parquet files](#create-postgres-tables-from-parquet-files)
  - [COPY FROM/TO Parquet stdin/stdout TO/FROM Postgres tables)](#copy-fromto-parquet-stdinstdout-tofrom-postgres-tables)
  - [COPY FROM/TO Parquet program stream TO/FROM Postgres tables)](#copy-fromto-parquet-program-stream-tofrom-postgres-tables)
  - [Inspect Parquet schema](#inspect-parquet-schema)
  - [Inspect Parquet metadata](#inspect-parquet-metadata)
  - [Inspect Parquet column statistics](#inspect-parquet-column-statistics)
  - [List and read Parquet files from uri pattern](#list-and-read-parquet-files-from-uri-pattern)
  - [Monitor COPY progress](#monitor-copy-progress)
- [Object Store Support](#object-store-support)
- [Copy Options](#copy-options)
- [Configuration](#configuration)
- [Supported Types](#supported-types)
  - [Nested Types](#nested-types)
- [Postgres Support Matrix](#postgres-support-matrix)

## Installation From Source
After installing `Postgres`, you need to set up `rustup`, `cargo-pgrx` to build the extension.

```bash
# clone repo
> git clone https://github.com/CrunchyData/pg_parquet.git
> cd pg_parquet

# install rustup
> curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# set cargo-pgrx (should be the same as pgrx dep in Cargo.toml) and pg versions
> export CARGO_PGRX_VERSION=0.19.3
> export PG_MAJOR=19

# install cargo-pgrx
> cargo install --force --locked cargo-pgrx@"${CARGO_PGRX_VERSION}"

# configure pgrx
> cargo pgrx init --pg"${PG_MAJOR}" $(which pg_config)

# append the extension to shared_preload_libraries 
> echo "shared_preload_libraries = 'pg_parquet'" >> ~/.pgrx/data-"${PG_MAJOR}"/postgresql.conf

# initialize a data directory, build and install the extension (to the targets specified by configured pg_config), then connects to a session
> cargo pgrx run --features pg"${PG_MAJOR}"
# alternatively you can only build and install the extension (pass --release flag for production binary)
> cargo pgrx install --release --features pg"${PG_MAJOR}"

# create the extension in the database
psql> "CREATE EXTENSION pg_parquet;"
```

> [!NOTE]
> `pgrx` installs the extension into the directories of the `pg_config` that it is configured with, which are owned by `root` for a packaged `Postgres`. `cargo pgrx install` can be told to use `sudo` for that, but `cargo pgrx run` has no such flag and instead needs those directories to be writable, as shown below. Alternatively, `cargo pgrx init --pg"${PG_MAJOR}" download` has `pgrx` build its own `Postgres` under `~/.pgrx`, which belongs to you and needs no root permissions at all.

```bash
# either install the extension with sudo
> cargo pgrx install --release --sudo --features pg"${PG_MAJOR}"

# or make the installation directories writable once
> sudo chmod a+rwx $(pg_config --pkglibdir)         \
                   $(pg_config --pkglibdir)/bitcode \
                   $(pg_config --sharedir)/extension

# the files of a packaged pg_parquet are still owned by root after that, and they are
# overwritten in place rather than recreated, so remove them to be able to install over
> sudo rm -f $(pg_config --pkglibdir)/pg_parquet* \
             $(pg_config --sharedir)/extension/pg_parquet*
```

## Usage
There are mainly 3 things that you can do with `pg_parquet`:
1. You can export Postgres tables/queries to Parquet files, stdin/stdout or a program's stream,
2. You can ingest data from Parquet files to Postgres tables with type coercion and schema inference,
3. You can inspect the schema and metadata of Parquet files.

### COPY from/to Parquet files to/from Postgres tables
You can use PostgreSQL's `COPY` command to read and write from/to Parquet files. Below is an example of how to write a PostgreSQL table, with complex types, into a Parquet file and then to read the Parquet file content back into the same table.

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
COPY product_example TO '/tmp/product_example.parquet' (format 'parquet', compression 'gzip');

-- show table
SELECT * FROM product_example;

-- copy the parquet file to the table
COPY product_example FROM '/tmp/product_example.parquet';

-- show table
SELECT * FROM product_example;
```

### Create Postgres tables from Parquet files
You can use `CREATE TABLE () WITH (definition_from = <uri>)` command to **infer** the columns of Postgres tables.
You can even infer + populate the table via `CREATE TABLE () WITH (load_from = <uri>)`.

```sql
-- create table with inferred columns and populated rows
CREATE TABLE product_inferred_example () WITH (load_from = '/tmp/product_example.parquet');

-- show table
SELECT * FROM product_inferred_example;
```

> [!NOTE]
> If the inferred column is of composite type, a new type will be created named as `parquet_structs.struct_<hash>`. Hash is determined by
field types and names of the composite type. The type is created by the user that runs the command, so that user needs `CREATE` privilege
on the `parquet_structs` schema, which the extension grants to `public`. The generated types are not dropped together with the table.
The `parquet_structs` schema belongs to the extension, so `DROP EXTENSION pg_parquet` reports the dependency while any table still has such
a column, and `DROP EXTENSION pg_parquet CASCADE` drops those columns along with the types. Drop or alter the columns first.

> [!NOTE]
> Postgres has no unsigned integers, so an unsigned column is inferred as the next wider type e.g. `uint32` as `bigint` and `uint64`
> as `numeric(20,0)`. A 16 byte fixed size binary column is inferred as `uuid`, which is the type that `COPY FROM` reads it into.
> A `map` column requires the [crunchy_map](https://github.com/CrunchyData/postgres-map) extension. The command
> errors out for a parquet type that has no Postgres counterpart e.g. `Duration` or `Decimal256`, in which case you can create the table
> with explicit column definitions and then `COPY FROM` the parquet file.

### COPY from/to Parquet stdin/stdout to/from Postgres tables

You can use `COPY` command to read and write Parquet stream from/to standard input and output. Below is an example usage (you have to specify `format = parquet`):

```bash
psql -d pg_parquet -p 28817 -h localhost -c "create table product_example_reconstructed (like product_example);"
 CREATE TABLE

psql -d pg_parquet -p 28817 -h localhost -c "copy product_example to stdout (format parquet);" | psql -d pg_parquet -p 28817 -h localhost -c "copy product_example_reconstructed from stdin (format parquet);"
 COPY 2
```

### COPY from/to Parquet program stream to/from Postgres tables

You can use `COPY` command to read and write Parquet stream from/to a program's input and output. Below is an example usage (you have to specify `format = parquet`):

```bash
psql -d pg_parquet -p 28817 -h localhost -c "copy product_example_reconstructed to program 'cat > /tmp/test.parquet' (format parquet);"
 COPY 2

psql -d pg_parquet -p 28817 -h localhost -c "copy product_example_reconstructed from program 'cat /tmp/test.parquet' (format parquet);"
 COPY 2
```


### Inspect Parquet schema
You can call `SELECT * FROM parquet.schema(<uri>)` to discover the schema of the Parquet file at given uri.

```sql
SELECT * FROM parquet.schema('/tmp/product_example.parquet') LIMIT 10;
             uri              |     name     | type_name  | type_length | repetition_type | num_children | converted_type | scale | precision | field_id | logical_type 
------------------------------+--------------+------------+-------------+-----------------+--------------+----------------+-------+-----------+----------+--------------
 /tmp/product_example.parquet | arrow_schema |            |             |                 |            5 |                |       |           |          | 
 /tmp/product_example.parquet | id           | INT32      |             | OPTIONAL        |              |                |       |           |        0 | 
 /tmp/product_example.parquet | product      |            |             | OPTIONAL        |            3 |                |       |           |        1 | 
 /tmp/product_example.parquet | id           | INT32      |             | OPTIONAL        |              |                |       |           |        2 | 
 /tmp/product_example.parquet | name         | BYTE_ARRAY |             | OPTIONAL        |              | UTF8           |       |           |        3 | STRING
 /tmp/product_example.parquet | items        |            |             | OPTIONAL        |            1 | LIST           |       |           |        4 | LIST
 /tmp/product_example.parquet | list         |            |             | REPEATED        |            1 |                |       |           |          | 
 /tmp/product_example.parquet | element        |            |             | OPTIONAL        |            3 |                |       |           |        5 | 
 /tmp/product_example.parquet | id           | INT32      |             | OPTIONAL        |              |                |       |           |        6 | 
 /tmp/product_example.parquet | name         | BYTE_ARRAY |             | OPTIONAL        |              | UTF8           |       |           |        7 | STRING
(10 rows)
```

### Inspect Parquet metadata
You can call `SELECT * FROM parquet.metadata(<uri>)` to discover the detailed metadata of the Parquet file, such as column statistics, at given uri.

```sql
SELECT uri, row_group_id, row_group_num_rows, row_group_num_columns, row_group_bytes, column_id, file_offset, num_values, path_in_schema, type_name FROM parquet.metadata('/tmp/product_example.parquet') LIMIT 1;
             uri              | row_group_id | row_group_num_rows | row_group_num_columns | row_group_bytes | column_id | file_offset | num_values | path_in_schema | type_name 
------------------------------+--------------+--------------------+-----------------------+-----------------+-----------+-------------+------------+----------------+-----------
 /tmp/product_example.parquet |            0 |                  1 |                    13 |             842 |         0 |           0 |          1 | id             | INT32
(1 row)
```

```sql
SELECT stats_null_count, stats_distinct_count, stats_min, stats_max, compression, encodings, index_page_offset, dictionary_page_offset, data_page_offset, total_compressed_size, total_uncompressed_size FROM parquet.metadata('/tmp/product_example.parquet') LIMIT 1;
 stats_null_count | stats_distinct_count | stats_min | stats_max |    compression     |        encodings         | index_page_offset | dictionary_page_offset | data_page_offset | total_compressed_size | total_uncompressed_size 
------------------+----------------------+-----------+-----------+--------------------+--------------------------+-------------------+------------------------+------------------+-----------------------+-------------------------
                0 |                      | 1         | 1         | GZIP(GzipLevel(6)) | PLAIN,RLE,RLE_DICTIONARY |                   |                      4 |               42 |                   101 |                      61
(1 row)
```

The `stats_geospatial` column reports the bounding box and the geometry types that Parquet computed for a geospatial column, as `jsonb`. It is `NULL` for the columns that have no geospatial statistics, which are all of the columns except the `geometry` and `geography` ones. The geometry types are the ISO WKB codes that the [Parquet geospatial specification](https://github.com/apache/parquet-format/blob/master/Geospatial.md) lists, e.g. `1` is a point and `2` is a linestring. The `zmin`, `zmax`, `mmin` and `mmax` bounds are only reported for the geometries that have those dimensions. A PostGIS type that the Parquet geospatial specification does not cover, e.g. a curved, triangulated or polyhedral one, is refused rather than written, as described [below](#postgis-types-the-specification-does-not-cover). A `geography` column always reports its geometry types, but its bounding box is only reported when all of its geometries are points: the edges of a `geography` are geodesics, which can reach beyond the box that the coordinates of their endpoints give.

```sql
SELECT path_in_schema, stats_geospatial FROM parquet.metadata('/tmp/geo_example.parquet') WHERE stats_geospatial IS NOT NULL;
 path_in_schema |                                     stats_geospatial
----------------+--------------------------------------------------------------------------------------------
 location       | {"bbox": {"xmax": 5.0, "xmin": 1.0, "ymax": 6.0, "ymin": 2.0}, "geospatial_types": [1, 2]}
(1 row)
```

You can call `SELECT * FROM parquet.file_metadata(<uri>)` to discover file level metadata of the Parquet file, such as format version, at given uri.

```sql
SELECT * FROM parquet.file_metadata('/tmp/product_example.parquet')
             uri              | created_by | num_rows | num_row_groups | format_version 
------------------------------+------------+----------+----------------+----------------
 /tmp/product_example.parquet | pg_parquet |        1 |              1 | 1
(1 row)
```

You can call `SELECT * FROM parquet.kv_metadata(<uri>)` to query custom key-value metadata of the Parquet file at given uri.

```sql
SELECT uri, encode(key, 'escape') as key, encode(value, 'escape') as value FROM parquet.kv_metadata('/tmp/product_example.parquet');
             uri              |     key      |    value
------------------------------+--------------+---------------------
 /tmp/product_example.parquet | ARROW:schema | /////5gIAAAQAAAA ...
(1 row)
```

### Inspect Parquet column statistics

You can call `SELECT * FROM parquet.column_stats(<uri>)` to discover the column statistics of the Parquet file, such as min and max value for the column, at given uri.

```sql
SELECT * FROM parquet.column_stats('/tmp/product_example.parquet')
 column_id | field_id |         stats_min          |         stats_max          | stats_null_count | stats_distinct_count 
-----------+----------+----------------------------+----------------------------+------------------+----------------------
         4 |        7 | item 1                     | item 2                     |                1 |                     
         6 |       11 | 1                          | 1                          |                1 |                     
         7 |       12 |                            |                            |                2 |                     
        10 |       17 |                            |                            |                2 |                     
         0 |        0 | 1                          | 1                          |                0 |                     
        11 |       18 | 2025-03-11 14:01:22.045739 | 2025-03-11 14:01:22.045739 |                0 |                     
         3 |        6 | 1                          | 2                          |                1 |                     
        12 |       19 | 2022-05-01 19:00:00+03     | 2022-05-01 19:00:00+03     |                0 |                     
         8 |       15 |                            |                            |                2 |                     
         5 |        8 | 1                          | 2                          |                1 |                     
         9 |       16 |                            |                            |                2 |                     
         1 |        2 | 1                          | 1                          |                0 |                     
         2 |        3 | product 1                  | product 1                  |                0 |                     
(13 rows)
```

### List and read Parquet files from uri pattern

You can call `SELECT * FROM parquet.list(<uri_pattern>)` to see all uris that matches with the uri pattern.
Uri pattern can resolve `**` for directories and `*` for words in the uri. A file that holds no byte is not among the uris a pattern matches, since a Parquet file is its footer at the very least: the marker an object store browser writes for a folder it creates and the `_SUCCESS` of a job that wrote one are reported rather than read, so that a pattern covering one of them reads the files next to it instead of failing over it. No other character is pattern syntax, so a `?` or a `[...]` in a uri is part of the name of the object it is written as rather than a pattern that matches a name next to it.


```sql
COPY (SELECT i FROM generate_series(1, 1000000) i) TO '/tmp/some/test.parquet' with (file_size_bytes '1MB');
COPY 1000000

SELECT * FROM parquet.list('/tmp/some/**/*.parquet');
                  uri                  |  size
---------------------------------------+---------
 /tmp/some/test.parquet/data_4.parquet |  100162
 /tmp/some/test.parquet/data_3.parquet | 1486916
 /tmp/some/test.parquet/data_2.parquet | 1486916
 /tmp/some/test.parquet/data_0.parquet | 1486920
 /tmp/some/test.parquet/data_1.parquet | 1486916
(5 rows)

```

Uri pattern is also supported by `COPY FROM` for all supported object stores except `http(s)` endpoints. A folder that a `COPY TO` with `file_size_bytes` wrote is read this way too: the folder holds no object of its own name, so `COPY FROM '<folder>/*.parquet'` is what reads the files in it.

A `COPY FROM` reads an object of a store over several requests, and every one of them names the version of the object that the read was opened on, so an object that is replaced while it is being read fails the copy rather than giving the rows of two objects at once.
```sql
COPY (SELECT i FROM generate_series(1, 1000000) i) TO 's3://testbucket/some/test.parquet' with (file_size_bytes '1MB');
COPY 1000000

CREATE TABLE test(a int);
CREATE TABLE

COPY test FROM 's3://testbucket/some/**/*.parquet';
COPY 1000000
```

### Monitor COPY progress

The progress of an ongoing `COPY TO/FROM` a Parquet file is reported to the [pg_stat_progress_copy](https://www.postgresql.org/docs/current/progress-reporting.html#COPY-PROGRESS-REPORTING) view.

```sql
SELECT relid::regclass, command, type, bytes_processed, bytes_total, tuples_processed
FROM pg_stat_progress_copy;
 relid | command   | type     | bytes_processed | bytes_total | tuples_processed
-------+-----------+----------+-----------------+-------------+------------------
 test  | COPY FROM | CALLBACK |       318767104 |   390000021 |         31876709
(1 row)
```

The byte counters mean different things for the two directions:
- For `COPY TO`, `bytes_processed` is the number of the bytes that are already written to the Parquet file(s). `bytes_total` is not reported since the size of a Parquet file is not known before it is written.
- For `COPY FROM`, `bytes_processed` and `bytes_total` count the bytes of the uncompressed binary stream that `pg_parquet` feeds to Postgres, which is unrelated to the size of the Parquet file. `bytes_total` is extrapolated from the rows that are read so far, so it is an estimation until the last row of the file is read. The estimation is corrected after every record batch, hence it can move in both directions when the rows of the file differ in size.

## Object Store Support
`pg_parquet` supports reading and writing Parquet files from/to `S3`, `Azure Blob Storage`, `http(s)` and `Google Cloud Storage` object stores. The host of an `s3://`, `az://` or `gs://` uri is the bucket or the container that holds the object rather than an endpoint, so such a uri is written without a port and the endpoint is configured per store below. Only an `http(s)://` uri names its endpoint, and keeps the port of it.

The uri of an object is parsed as the url standard asks it to be parsed, which removes a tab, a newline and a carriage return in it and trims the whitespace around it, so a uri holding one of those characters is refused rather than copied into an object other than the one it names. A space inside the path is kept, so an object whose name holds one is written and read as it is.

The object of a uri is named by the whole path of that uri, and a `#` or a `?` ends that path rather than belonging to it: `s3://bucket/report#2026.parquet` names the object `report`. A uri holding one of those characters is rejected, since it names an object other than the one it was written for, and an object whose name holds one is named by writing the character as `%23` or `%3F`. A query that is meant for the store rather than for the object, as the version id or the signature of a presigned uri is, is sent to no store. A local path is a path and not a uri, so both characters are part of the name of the file there.

> [!NOTE]
> To be able to write into a object store location, you need to grant `parquet_object_store_write` role to your current postgres user.
> Similarly, to read from an object store location, you need to grant `parquet_object_store_read` role to your current postgres user.

The credentials of an object store are read from the environment of the postgres process, as described below for each store, and never from the uri: a uri that carries credentials before its host is rejected rather than signed with the credentials of the environment while its own are only written into the logs. A session can make its requests with credentials of its own instead, by naming a profile that the administrator mapped to its role, which is described under [Session Profiles](#session-profiles), or by putting them in parameters of its own where the server allows that, which is described under [Credentials In Parameters](#credentials-in-parameters).

#### S3 Storage

The simplest way to configure object storage is by creating the standard `~/.aws/credentials` and `~/.aws/config` files:

```bash
$ cat ~/.aws/credentials
[default]
aws_access_key_id = AKIAIOSFODNN7EXAMPLE
aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

$ cat ~/.aws/config 
[default]
region = eu-central-1
```

Alternatively, you can use the following environment variables when starting postgres to configure the S3 client:
- `AWS_ACCESS_KEY_ID`: the access key ID of the AWS account
- `AWS_SECRET_ACCESS_KEY`: the secret access key of the AWS account
- `AWS_SESSION_TOKEN`: the session token for the AWS account
- `AWS_REGION`: the default region of the AWS account
- `AWS_ENDPOINT_URL`: the endpoint
- `AWS_SHARED_CREDENTIALS_FILE`: an alternative location for the credentials file **(only via environment variables)**
- `AWS_CONFIG_FILE`: an alternative location for the config file **(only via environment variables)**
- `AWS_PROFILE`: the name of the profile from the credentials and config file (default profile name is `default`) **(only via environment variables)**
- `AWS_ALLOW_HTTP`: allows http endpoints **(only via environment variables)**
- `AWS_<HTTP CLIENT OPTION>`: an [http client option](#http-client-options), e.g. `AWS_TIMEOUT` **(only via environment variables)**

Config source priority order is shown below:
1. Environment variables,
2. Config file.

Supported S3 uri formats are shown below:
- s3:// \<bucket\> / \<path\>
- https:// \<bucket\>.s3.amazonaws.com / \<path\>
- https:// s3.amazonaws.com / \<bucket\> / \<path\>

Supported authorization methods' priority order is shown below:
1. Temporary session tokens by assuming roles,
2. Long term credentials.

#### Azure Blob Storage

The simplest way to configure object storage is by creating the standard [`~/.azure/config`](https://learn.microsoft.com/en-us/cli/azure/azure-cli-configuration?view=azure-cli-latest) file:

```bash
$ cat ~/.azure/config
[storage]
account = devstoreaccount1
key = Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==
```

Alternatively, you can use the following environment variables when starting postgres to configure the Azure Blob Storage client:
- `AZURE_STORAGE_ACCOUNT`: the storage account name of the Azure Blob
- `AZURE_STORAGE_KEY`: the storage key of the Azure Blob
- `AZURE_STORAGE_CONNECTION_STRING`: the connection string for the Azure Blob (overrides any other config)
- `AZURE_STORAGE_SAS_TOKEN`: the storage SAS token for the Azure Blob
- `AZURE_TENANT_ID`: the tenant id for client secret or workload identity auth **(only via environment variables)**
- `AZURE_CLIENT_ID`: the client id for client secret or workload identity auth **(only via environment variables)**
- `AZURE_FEDERATED_TOKEN_FILE`: the file system path to the federated token file to use for workload identity auth **(only via environment variables)**
- `AZURE_CLIENT_SECRET`: the client secret for client secret auth **(only via environment variables)**
- `AZURE_STORAGE_ENDPOINT`: the endpoint **(only via environment variables)**
- `AZURE_CONFIG_FILE`: an alternative location for the config file **(only via environment variables)**
- `AZURE_CONFIG_DIRECTORY`: the directory the config files of the [session profiles](#session-profiles) are kept in **(only via environment variables)**
- `AZURE_ALLOW_HTTP`: allows http endpoints **(only via environment variables)**
- `AZURE_<HTTP CLIENT OPTION>`: an [http client option](#http-client-options), e.g. `AZURE_TIMEOUT` **(only via environment variables)**

Config source priority order is shown below:
1. Connection string (read from environment variable or config file),
2. Environment variables,
3. Config file.

Supported Azure Blob Storage uri formats are shown below:
- az:// \<container\> / \<path\>
- azure:// \<container\> / \<path\>
- https:// \<account\>.blob.core.windows.net / \<container\> / \<path\>

Supported authorization methods' priority order is shown below:
1. Bearer token via client secret,
2. Sas token,
3. Storage key.

#### Http(s) Storage

Only `https` uris are supported by default. You can set `ALLOW_HTTP` environment variable to allow `http` uris.

An `http(s)` endpoint takes an object in a single request rather than in an upload of several parts, so a `COPY TO` such an endpoint buffers the whole file in memory and writes at most 10 MiB. A larger table is written with a `file_size_bytes` below that size, which writes it as a folder of files, and each file of that folder is then read back by its own name, since a uri pattern does not apply to an `http(s)` endpoint.

You can also use the following environment variables when starting postgres to configure the http(s) client:
- `HTTP_<HTTP CLIENT OPTION>`: an [http client option](#http-client-options), e.g. `HTTP_TIMEOUT` **(only via environment variables)**

#### Google Cloud Storage

The simplest way to configure object storage is by creating a json config file like [`~/.config/gcloud/application_default_credentials.json`] (can be generated by `gcloud auth application-default login`):

```bash
$ cat ~/.config/gcloud/application_default_credentials.json
{
  "gcs_base_url": "http://localhost:4443",     
  "disable_oauth": true,
  "client_email": "",
  "private_key_id": "",
  "private_key": ""
}
```

Alternatively, you can use the following environment variables when starting postgres to configure the Google Cloud Storage client:
- `GOOGLE_SERVICE_ACCOUNT_KEY`: json serialized service account key **(only via environment variables)**
- `GOOGLE_SERVICE_ACCOUNT_PATH`: an alternative location for the config file **(only via environment variables)**
- `GOOGLE_SERVICE_ACCOUNT_DIRECTORY`: the directory the service account keys of the [session profiles](#session-profiles) are kept in **(only via environment variables)**
- `GOOGLE_<HTTP CLIENT OPTION>`: an [http client option](#http-client-options), e.g. `GOOGLE_TIMEOUT` **(only via environment variables)**

Supported Google Cloud Storage uri formats are shown below:
- gs:// \<bucket\> / \<path\>

#### Session Profiles

The credentials above are the ones every session of the server shares, since they are read from the environment of the postgres process and the config files of the cloud sdks. A session can make its object store requests with another set of credentials instead, by naming the profile they are kept under:

```sql
SET pg_parquet.profile_name TO 'analytics';
COPY products TO 's3://analytics-lake/products.parquet';
```

A profile is a name and never a secret, so nothing that a session sets or that the mapping holds is a credential: the credentials themselves stay in the files the administrator keeps them in, which is what keeps them out of the server log, out of `pg_stat_activity`, out of `pg_stat_statements` and out of a dump. Where a profile is read from depends on the store:

- `s3://`: the `[profile <name>]` section of `~/.aws/config` or `AWS_CONFIG_FILE`, read by the aws sdk the way `AWS_PROFILE` is read. Note that a profile using `credential_process` or `sso` is not read, since `pg_parquet` builds the sdk without those features,
- `az://`: a config file named after the profile in `AZURE_CONFIG_DIRECTORY`, so `AZURE_CONFIG_DIRECTORY=/etc/pg_parquet/azure` makes the profile `analytics` the file `/etc/pg_parquet/azure/analytics`. That file is in the same format as the one the server reads, an ordinary `[storage]` section, and it takes every config parameter listed above plus `tenant_id`, `client_id`, `client_secret`, `federated_token_file`, `endpoint` and `allow_http`,
- `gs://`: a service account key named after the profile in `GOOGLE_SERVICE_ACCOUNT_DIRECTORY`, so `GOOGLE_SERVICE_ACCOUNT_DIRECTORY=/etc/pg_parquet/gcs` makes the profile `analytics` the key `/etc/pg_parquet/gcs/analytics.json`.

For azure and gcs alike the rule is one: a profile is a file named after it in the directory the server keeps the credentials of the profiles in. A server that names no such directory keeps them next to the credentials it reads its own from, i.e. in the directory of `AZURE_CONFIG_FILE` and of `GOOGLE_SERVICE_ACCOUNT_PATH`, so azure profiles work with no directory configured at all while a server whose own gcs key is inline in `GOOGLE_SERVICE_ACCOUNT_KEY` must name one. The directory is the server's own either way, which is what keeps a session to the credentials that were put there, and a profile is a name rather than a path, which is what keeps it to that directory.

A profile is read on its own rather than before the environment: a profile whose file is missing fails the copy instead of falling back to the credentials of the server, which would otherwise hand a session an identity it did not ask for. A local path and an `http(s)` endpoint are read with no credentials at all, so a profile does not apply to them and is ignored rather than refused.

Which profiles a role may name is a table that only a superuser and the `parquet_profile_manager` role may write:

```sql
GRANT parquet_profile_manager TO lake_admin;

INSERT INTO parquet.user_profile_mapping (userid, profiles)
VALUES ('analytics_team', '{"aws_profiles": ["analytics"], "gcs_profiles": ["analytics"]}');
```

A row is read for every role the session has the privileges of, so a profile mapped to a group role is one each of its members may name. The mapping is read on every copy rather than when the GUC was set, so a mapping that is taken away stops the next copy of a session that already named the profile. `pg_parquet.profile_name` is settable by any role for that reason: the GUC says which profile a session wants and the mapping is what says whether it may have it, which is also why a role carrying the GUC in `pg_db_role_setting` grants it nothing. A session reads the profiles it may name from `parquet.my_profiles`, and `parquet.profile_is_allowed('s3', 'analytics')` answers for one of them. A superuser names any profile, since it reads the credential files of the server anyway.

##### End to end

The administrator keeps the credentials where each sdk already reads them, one profile per identity:

```ini
# /etc/pg_parquet/aws/config -- AWS_CONFIG_FILE, one section per profile
[profile analytics]
region=us-east-1
aws_access_key_id=...
aws_secret_access_key=...

[profile reporting]
region=us-east-1
aws_access_key_id=...
aws_secret_access_key=...
```

```ini
# /etc/pg_parquet/azure/analytics -- AZURE_CONFIG_DIRECTORY is /etc/pg_parquet/azure, so
# this is the profile "analytics", in the same format as the server's own config file
[storage]
account=analyticslake
key=...
```

```
# /etc/pg_parquet/gcs/analytics.json -- GOOGLE_SERVICE_ACCOUNT_DIRECTORY is
# /etc/pg_parquet/gcs, so this is the service account key of the profile "analytics"
```

Then, in the database, the mapping. Note the two shapes: `analytics_team` reaches all three clouds, `reporting_team` reaches one.

```sql
GRANT parquet_profile_manager TO lake_admin;    -- lake_admin writes the mapping from here on

-- all three clouds at once, under one profile name
INSERT INTO parquet.user_profile_mapping (userid, profiles) VALUES
  ('analytics_team', '{"aws_profiles":   ["analytics"],
                       "azure_profiles": ["analytics"],
                       "gcs_profiles":   ["analytics"]}');

-- s3 only, under a profile of its own
INSERT INTO parquet.user_profile_mapping (userid, profiles) VALUES
  ('reporting_team', '{"aws_profiles": ["reporting"]}');

GRANT parquet_object_store_read, parquet_object_store_write TO analytics_team, reporting_team;
GRANT analytics_team TO alice;      -- the mapping is read through role membership
GRANT reporting_team TO bob;
```

`alice` names the profile once and reaches all three stores, because the same name is mapped for each:

```sql
SET pg_parquet.profile_name TO 'analytics';

COPY orders TO 's3://analytics-lake/orders.parquet';   -- [profile analytics] of the aws config
COPY orders TO 'az://analytics-lake/orders.parquet';   -- /etc/pg_parquet/azure/analytics
COPY orders TO 'gs://analytics-lake/orders.parquet';   -- /etc/pg_parquet/gcs/analytics.json

SELECT * FROM parquet.my_profiles;
   mapped_to    |                                   profiles
----------------+-------------------------------------------------------------------------------
 analytics_team | {"aws_profiles": ["analytics"], "azure_profiles": ["analytics"], "gcs_profiles": ["analytics"]}
```

`bob` is mapped for `s3://` only, so the same name on another scheme is refused rather than resolved against the server's own credentials:

```sql
SET pg_parquet.profile_name TO 'reporting';
COPY orders TO 's3://reporting-lake/orders.parquet';   -- COPY 1000
COPY orders TO 'gs://reporting-lake/orders.parquet';
-- ERROR:  permission denied for profile "reporting"
-- DETAIL: parquet.user_profile_mapping does not map the session to the profile for a gs store. ...

SET pg_parquet.profile_name TO 'analytics';            -- a profile mapped to another role
COPY orders TO 's3://analytics-lake/orders.parquet';
-- ERROR:  permission denied for profile "analytics"

RESET pg_parquet.profile_name;                         -- back to the server's own credentials
COPY orders TO 's3://shared-lake/orders.parquet';      -- COPY 1000
```

Because one GUC names one profile, a session that reaches several clouds at once needs the **same profile name in each of them** — which is the administrator's choice in all three places, since the azure and gcs files are named when the credentials are written and an aws profile can be aliased. A session whose clouds are named differently selects them one at a time.

#### Credentials In Parameters

A session can also put the credentials themselves into parameters, rather than naming a profile they are kept under. **This is unsafe and off by default**, because a credential set this way travels through a SQL statement and so reaches every place a statement reaches:

- the server log, under `log_statement = 'all'`, and under any setting of it when the statement fails, since `log_min_error_statement` defaults to `error` and writes the `STATEMENT:` line,
- `pg_stat_activity.query`, which a member of `pg_read_all_stats` reads for every session,
- `pg_stat_statements`, if it is installed,
- the error reports of the session, and whatever the client logs them into,
- the client's own history, e.g. `~/.psql_history`,
- and the shared memory of a parallel copy: the workers make their requests with the credentials of the session that started them, which postgres carries to them by serializing the parameters of that session into the shared memory segment of the copy.

Only a superuser opens them, since whether the credentials of the sessions of a server may be written into its log is the server's decision and not a session's:

```sql
-- in postgresql.conf, which is the way to open them for the sessions of the server
pg_parquet.allow_credentials_in_parameters = on
```

```sql
SET pg_parquet.aws_access_key_id TO '...';
SET pg_parquet.aws_secret_access_key TO '...';
COPY products TO 's3://a-bucket-of-ones-own/products.parquet';
```

The parameters are, by store:

- `s3://`: `pg_parquet.aws_access_key_id`, `pg_parquet.aws_secret_access_key`, `pg_parquet.aws_session_token`, `pg_parquet.aws_region`, `pg_parquet.aws_endpoint_url`,
- `az://`: `pg_parquet.azure_storage_account`, `pg_parquet.azure_storage_key`, `pg_parquet.azure_storage_sas_token`, `pg_parquet.azure_storage_connection_string`, `pg_parquet.azure_storage_endpoint`,
- `gs://`: `pg_parquet.gcs_service_account_key`.

They are read **ahead of** [a profile](#session-profiles) and of the environment both: a session that set `pg_parquet.aws_access_key_id` makes its `s3://` requests with what it set, whichever profile it named, and a credential that does not work fails the copy rather than falling back to the credentials of the server. Setting `pg_parquet.allow_credentials_in_parameters` back to `off` stops them being read at once, including in a session that already set them. A store built from them is not cached, so replacing a parameter replaces the identity the next copy is made with.

What is done to keep a credential from outliving the session that set it:

- a credential is refused from `ALTER ROLE ... SET` and `ALTER DATABASE ... SET`, whose settings are kept in `pg_db_role_setting` — a catalog **every role of the cluster may read** and `pg_dumpall` writes out,
- and from `postgresql.conf` and `ALTER SYSTEM`, for the same reason: a parameter of the server is readable by every session of it,
- and inside a security restricted operation, so a `SECURITY DEFINER` function cannot set one on a caller's behalf,
- and the parameters holding a credential have no row in `pg_settings` and do not appear in `SHOW ALL`.

> [!WARNING]
> The last of those hides a credential, it does not keep it: `SHOW pg_parquet.aws_secret_access_key` and `current_setting('pg_parquet.aws_secret_access_key')` read it in full, from any session that set it. Nothing here makes a credential in a parameter safe from the log — only the places it is refused from are closed. A deployment that cannot have credentials in its log should leave this off and name [a profile](#session-profiles) instead.

To keep the credential out of the server log while using these, a session needs `log_parameter_max_length_on_error = 0`, which is the default and which a session may set for itself, and the server needs `log_parameter_max_length = 0` if `log_statement` or `log_min_duration_statement` is on, which only a superuser may set. Both only help a credential that is sent as a bind parameter rather than written into the statement text.

#### Http Client Options

Each object storage client has its own http client, which can be configured via environment variables that are named after the storage prefix, e.g. `AWS_TIMEOUT` for S3 and `AZURE_TIMEOUT` for Azure Blob Storage. The supported options are shown below:
- `TIMEOUT`: the timeout for a single request, including the time it takes to read the response body. Defaults to 30 seconds. Slow links or large `file_size_bytes` values might need a higher value,
- `CONNECT_TIMEOUT`: the timeout for only establishing the connection. Defaults to 5 seconds,
- `READ_TIMEOUT`: the timeout for reading the response body after the response headers arrived,
- `PROXY_URL`: the proxy to send the requests through,
- `PROXY_CA_CERTIFICATE`: a PEM encoded CA certificate to trust when connecting to the proxy,
- `PROXY_EXCLUDES`: a comma separated list of hosts that should not be sent through the proxy,
- `ALLOW_INVALID_CERTIFICATES`: skips certificate validation. **Insecure, it allows man in the middle attacks**,
- `DISABLE_SYSTEM_CERTIFICATES`: does not trust the certificates of the operating system,
- `USER_AGENT`: the user agent header to send with the requests,
- `DEFAULT_CONTENT_TYPE`: the content type header to send with the requests,
- `HTTP1_ONLY`, `HTTP2_ONLY`: forces a single http version,
- `HTTP2_KEEP_ALIVE_INTERVAL`, `HTTP2_KEEP_ALIVE_TIMEOUT`, `HTTP2_KEEP_ALIVE_WHILE_IDLE`, `HTTP2_MAX_FRAME_SIZE`: tune the http2 connections,
- `POOL_IDLE_TIMEOUT`, `POOL_MAX_IDLE_PER_HOST`: tune the connection pool,
- `RANDOMIZE_ADDRESSES`: shuffles the resolved addresses of a host before connecting.

Durations are given with a unit, e.g. `30s`, `5m` or `1m30s`. Booleans are given as `true` or `false`.

A request that an object store is slow to answer is retried, which takes minutes by default before it gives up. A statement that is waiting on one is still cancellable while that happens: `Ctrl-C`, `statement_timeout`, `pg_cancel_backend` and `pg_terminate_backend` end it without waiting for the retries to run out.

### PostGIS Types The Specification Does Not Cover

PostGIS has geometry types that the [Parquet geospatial specification](https://github.com/apache/parquet-format/blob/master/Geospatial.md) does not define: the curved ones (`CIRCULARSTRING`, `COMPOUNDCURVE`, `CURVEPOLYGON`, `MULTICURVE`, `MULTISURFACE`), and `TIN`, `POLYHEDRALSURFACE` and `TRIANGLE`. Their WKB type codes are `8`, `9`, `10`, `11`, `12`, `16`, `15` and `17`, where the specification defines `1` to `7` and the same codes plus 1000 per extra dimension.

Such a geometry is **refused** rather than written:

```sql
COPY (SELECT 'CIRCULARSTRING(0 0, 1 1, 2 0)'::geometry) TO '/tmp/x.parquet';
ERROR:  cannot write a circular string to a parquet geospatial column
DETAIL: The GEOMETRY and GEOGRAPHY logical types are defined over the geometry types of the
        parquet geospatial specification, which the curved types and tin, polyhedral surface
        and triangle are not among, so a reader of the specification refuses the whole column
        one of them is written to. ...
```

A column is written with the `GEOMETRY` or `GEOGRAPHY` logical type, which promises the types the specification defines, and a reader of the specification refuses **the whole column** one of these is in rather than the row: a column of one `CIRCULARSTRING` among points would be unreadable to it entirely, the points included, while `pg_parquet` read the same file back without noticing. So the copy ends at the geometry rather than at the reader.

To write these, convert them to a type the specification defines first. `ST_CurveToLine` replaces a curved geometry with linestrings and polygons, `ST_Multi` turns a `TIN` or a `POLYHEDRALSURFACE` into a `MULTIPOLYGON` that keeps every one of its faces, and `ST_UnaryUnion` turns a `TRIANGLE` into a `POLYGON`. Note that `ST_Dump` of a `TIN` gives `TRIANGLE`s, which are refused in their turn, and that `ST_Multi` of a `TRIANGLE` gives a `TIN`. A `bytea` column holding `ST_AsBinary` of the geometry is the other way to carry the original bytes, which is written with no geospatial logical type and leaves a reader to make of them what it can.

## Copy Options
`pg_parquet` supports the following options in the `COPY TO` command:
- `format parquet`: you need to specify this option to read or write Parquet files which does not end with `.parquet[.<compression>]` extension,
- `file_size_bytes <string>`: the total file size per Parquet file. When set, the parquet files, with target size, are created under parent directory (named the same as file name). By default, when not specified, a single file is generated without creating a parent folder. You can specify total bytes without unit like `file_size_bytes 2000000` or with unit (KB, MB, or GB) like `file_size_bytes '1MB'`. A file ends at a row group boundary, so it can exceed the target by up to one row group, and `row_group_size_bytes` is capped at the target so that a row group cannot be larger than the file it is written into. A folder that already holds something is refused, since a copy of it writes over the files of whoever wrote it before and removes the ones it does not write over: pass `overwrite true` to replace what is in the folder, which once the copy wrote its own files removes the files that an earlier and larger copy left behind, so that reading the folder back does not return the rows of two copies at once. A folder that is not there yet, or is there and empty, is written without the option. A copy that wrote files and then failed, or whose transaction rolled back, leaves them where they are, since writing them is no part of the transaction: the folder holds something afterwards and the copy that follows it is one that replaces a folder, option and all. A uri that already names an object is no folder destination and such a copy is refused: the files of it would be written under a name that is taken, which an object store is free to accept and then hide, leaving the folder to read as the object it was written over. A copy that finds no row writes one file holding nothing but the schema, the way a copy without `file_size_bytes` writes the single file it was given, so that the folder is one a `COPY FROM` reads no row from rather than one it finds no file in. A folder that another copy is writing is refused, which the copy knows by taking an advisory lock on the destination for the length of its transaction: two copies at once number their files the same way and each removes the files it takes for a previous copy's, which left the folder holding one copy's rows or a part of each while both reported the rows they wrote. The second copy is not made to wait, since a turn to write over the rows the first one just wrote is no turn worth waiting for, and two copies waiting on two folders in opposite orders would deadlock. A transaction that copies one folder more than once is not refused by its own lock. The lock holds within one PostgreSQL instance, so a folder that another instance, or anything else, writes at the same time still needs a lock of the application's own,
- `overwrite <boolean>`: whether the copy may replace what its folder destination already holds. `false` by default, which refuses a folder with anything in it rather than writing over the files of the copy that wrote it before. Knowing what the folder holds means listing it, so a destination that answers a listing with an error, which is what a credential that may write but not read gets, is written as it was before the option existed rather than refused. Only a folder destination takes it, which is the one `file_size_bytes` writes: a single file destination is replaced the way `COPY TO` has always replaced one,
- `field_ids <string>`: fields ids that are assigned to the fields in Parquet file schema. By default, no field ids are assigned. Pass `auto` to let pg_parquet generate field ids. You can pass a json string to explicitly pass the field ids, like `'{"id": 1}'`. A nested column takes a json object rather than an id, whose `__root_field_id` key is the id of the column itself and whose other keys are its nested fields, the fields of a composite type by name, the element of an array as `element`, and the entries of a map as `key` and `val`, e.g. `'{"friends": {"__root_field_id": 2, "element": {"__root_field_id": 3, "name": 4}}}'`. A field the mapping leaves out is written without a field id. `__root_field_id` is reserved, so a nested field cannot be named after it,
- `row_group_size <int64>`: the number of rows in each row group while writing Parquet files. The default row group size is `122880`,
- `row_group_size_bytes <int64>`: the total byte size of rows in each row group while writing Parquet files. The default row group size bytes is `row_group_size * 1024`,
- `compression <string>`: the compression format to use while writing Parquet files. The supported compression formats are `uncompressed`, `snappy`, `gzip`, `brotli`, `lz4`, `lz4_raw` and `zstd`. `lz4` is the compression format that the Parquet format deprecated, so prefer `lz4_raw` unless a reader needs the deprecated one. The default compression format is `snappy`. If not specified, the compression format is determined by the file extension,
- `compression_level <int>`: the compression level to use while writing Parquet files. The supported compression levels are only supported for `gzip`, `zstd` and `brotli` compression formats. The default compression level is `6` for `gzip (0-10)`, `1` for `zstd (1-22)` and `1` for `brotli (0-11)`,
- `parquet_version <string>`: writer version of the Parquet file. By default, it is set to `v1` to be more interoperable with common query engines. (some are not able to read v2 files) You can set it to `v2` to unlock some of the new encodings,
- `bloom_filter <string>`: the columns that a bloom filter is written for. By default, no bloom filters are written. Pass `all` to write one for every column, or a json string like `'{"id": true}'` to write one only for the given columns. A bloom filter lets a reader skip a row group that cannot contain a value, which speeds up equality filters on high cardinality columns at the cost of a larger file,
- `bloom_filter_fpp <float>`: the false positive probability of the bloom filters, between `1e-62` and `1` exclusive. The default is `0.05`. A lower value makes the filters larger but makes them skip more row groups. Note that a filter can take up to 128MiB per column, which a value near the lower bound reaches with a single row,
- `dictionary <string>`: the columns that are dictionary encoded. By default, all columns are. Pass `none` to disable dictionary encoding for every column, or a json string like `'{"id": false}'` to disable it only for the given columns. Dictionary encoding is a win for low cardinality columns, but it costs memory and time for the columns that are mostly distinct.

> [!NOTE]
> A bloom filter only helps the readers that push equality filters down to the Parquet file, like `DuckDB`, `Spark` or `DataFusion`. `COPY FROM` reads every row of a file, so it never reads a bloom filter. A selected column is a top level column, which expands to all of its leaf columns when it is of a nested type.

`pg_parquet` supports the following options in the `COPY FROM` command:
- `format parquet`: you need to specify this option to read or write Parquet files which does not end with `.parquet[.<compression>]` extension,
- `match_by <string>`: method to match Parquet file fields to PostgreSQL table columns. The available methods are `position` and `name`. The default method is `position`. You can set it to `name` to match the columns by their name rather than by their position in the schema (default). Match by `name` is useful when field order differs between the Parquet file and the table, but their names match. It also matches the fields of the composite type columns by name.

## Configuration
The following GUC parameters are available:
- `pg_parquet.enable_copy_hooks`: you can set this parameter to `on` or `off` to enable or disable the `pg_parquet` extension. The default value is `on`.
- `pg_parquet.profile_name`: the profile the session's object store credentials are read from, as described under [Session Profiles](#session-profiles). The default is unset, which reads the credentials of the server's environment. Any role may set it, and `parquet.user_profile_mapping` is what says which profiles a role may name.
- `pg_parquet.allow_credentials_in_parameters`: you can set this parameter to `on` to let a session put the credentials of an object store into parameters of its own, as described under [Credentials In Parameters](#credentials-in-parameters). The default is `off`, and only a superuser may change it, since a credential set that way is written into the server log and read from `pg_stat_activity`. The credential parameters themselves are refused while this is off.
- `pg_parquet.max_parallel_workers`: the number of parallel workers a `COPY TO` or `COPY FROM` command may use. The default value is `0`, which keeps both of them single process. The workers come from the same pool as parallel query workers, so a request above `max_parallel_workers` is capped to it. A command that the pool cannot spare every worker it asked for runs with the ones it was given. A `COPY FROM` gains less from a large number of workers than a `COPY TO` does, because the workers only decode the row groups of the file while the leader inserts every row it hands out.
  - For `COPY TO`, each worker writes a range of the table's pages, and the ranges end up in the destination without being re-encoded. For an `s3://`, `az://` or `gs://` destination, every worker uploads the ranges it wrote as parts of the destination's own multipart upload and the backend only adds the file's footer, so each byte of the file is uploaded once, nothing is downloaded, and nothing of such a copy is written to local disk. A copy whose server crashes can leave an unfinished multipart upload behind, which the bucket's own rule for incomplete uploads is what removes. A part that is left behind this way holds the rows of the copy that wrote it, so a uri pattern that covers it skips it rather than reading those rows again, and reports it as a file the pattern does not read. The parts of that upload differ in size, which a store that requires all but the last part to be the same size, e.g. Cloudflare R2, rejects. The bytes of a range are uploaded as the range wrote them, header and footer included, so the destination holds a little under 1% of bytes nothing points at. For a local file, each worker writes a part file next to the destination instead, and the backend merges the parts into it. With `file_size_bytes` the destination is a folder of complete files, so each worker writes files of its own into it rather than parts of one file, and the file a worker is in the middle of is finished before its range ends, which leaves a few more and smaller files behind than a single process copy writes. The files of such a folder are numbered in the order the workers claim them, so the rows of the table are not in the order of the file names. Parallel `COPY TO` applies to a single non-temporary heap table copied to a plain local file, to a folder of files, or to one of those object stores; anything else, e.g. `COPY (SELECT ..) TO`, a `COPY TO PROGRAM` or `TO STDOUT`, or an `http://` uri, is copied by a single process. Note that each range writes its own row groups, so a parallel `COPY TO` ends up with more and smaller row groups than a single process one.
  - For `COPY FROM`, each worker decodes a subset of the row groups of the file and hands the rows to the backend that runs the command, which is the only one that writes them into the relation, in the order the file holds them. A uri with a wildcard stands for several files, whose row groups are one list the workers split between themselves the same way, each worker opening the file its next row group is in. Parallel `COPY FROM` applies to files, local, on an `s3://`, `az://` or `gs://` store, or at an `http(s)` endpoint that serves byte ranges, holding more than one row group together, read into a non-temporary heap table that has no row triggers and no attribute the command leaves to a default, an identity or a generation expression; anything else, e.g. a `COPY FROM PROGRAM`, is copied by a single process. Because only one process may write into the relation, the speedup is bounded by how fast that one process can insert, which the decoding of a few workers is usually enough to reach.

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
| `oid8`(6)         | INT64                     | INTEGER(64,false)|
| `double`          | DOUBLE                    |                  |
| `numeric`(1)      | FIXED_LEN_BYTE_ARRAY(16)  | DECIMAL(128)     |
| `text`            | BYTE_ARRAY                | STRING           |
| `json`            | BYTE_ARRAY                | JSON             |
| `jsonb`           | BYTE_ARRAY                | JSON             |
| `uuid`            | FIXED_LEN_BYTE_ARRAY(16)  | UUID             |
| `bytea`           | BYTE_ARRAY                |                  |
| `date` (2)        | INT32                     | DATE             |
| `timestamp`       | INT64                     | TIMESTAMP_MICROS |
| `timestamptz` (3) | INT64                     | TIMESTAMP_MICROS |
| `time`            | INT64                     | TIME_MICROS      |
| `timetz`(3)       | INT64                     | TIME_MICROS      |
| `geometry`(4)     | BYTE_ARRAY                | GEOMETRY         |
| `geography`(4)    | BYTE_ARRAY                | GEOGRAPHY        |

### Nested Types
| PostgreSQL Type   | Parquet Physical Type     | Logical Type     |
|-------------------|---------------------------|------------------|
| `composite`       | GROUP                     | STRUCT           |
| `array`(7)        | element's physical type   | LIST             |
| `crunchy_map`(5)  | GROUP                     | MAP              |

> [!WARNING]
> - (1) `numeric` type is written the smallest possible memory width to parquet file as follows:
>    * `numeric(P <= 9, S)` is represented as `INT32` with `DECIMAL` logical type
>    * `numeric(9 < P <= 18, S)` is represented as `INT64` with `DECIMAL` logical type
>    * `numeric(18 < P <= 38, S)` is represented as `FIXED_LEN_BYTE_ARRAY(9-16)` with `DECIMAL` logical type
>    * `numeric(38 < P, S)` is represented as `BYTE_ARRAY` with `STRING` logical type
>    * `numeric` is allowed by Postgres. (precision and scale not specified). These are represented by a default precision (38) and scale (9) instead of writing them as string. You get runtime error if your table tries to write a numeric value which is not allowed by the default precision and scale (29 integral digits before decimal point, 9 digits after decimal point). Reading into such a column keeps all the digits of the file's value, be it a decimal or a string, since Postgres stores an unbounded numeric at any precision and scale.
> - (2) The `date` type is represented according to `Unix epoch` when writing to Parquet files. It is converted back according to `PostgreSQL epoch` when reading from Parquet files.
> - (3) The `timestamptz` and `timetz` types are adjusted to `UTC` when writing to Parquet files. They are converted back with `UTC` timezone when reading from Parquet files.
> - (4) The `geometry` and `geography` types are represented as `BYTE_ARRAY` encoded as `WKB` with the `GEOMETRY` and `GEOGRAPHY` logical types, specified by the [parquet geospatial spec](https://github.com/apache/parquet-format/blob/master/Geospatial.md), when `postgis` extension is created. Otherwise, they are represented as `BYTE_ARRAY` with `STRING` logical type. The `crs` of the logical type is taken from the `srid` of the column's type modifier, e.g. `geometry(point, 3857)` is written with the `EPSG:3857` crs. A column whose `srid` is lon/lat on the `WGS84` ellipsoid is written with the default crs of the spec, which is `OGC:CRS84`, and a `geometry` column without an `srid` is written with the unset crs of the spec, which is `srid:0`. An `srid` that no authority in `spatial_ref_sys` claims is written with the engine specific `srid:<code>` form of the spec, e.g. `srid:990000`, so that it is not mistaken for an unset crs. An `srid` whose `auth_name` holds the `:` separator, or is `srid` itself, takes that same form, since its `authority:code` form would not read back as this `srid`. A `geography` column without an `srid` is written with the default crs, since PostGIS geographies are on the `WGS84` ellipsoid. The bounding box and the geometry types of each column are written as geospatial statistics, except for the bounding box of a `geography` column that has edges, as described [above](#inspect-parquet-metadata). A geometry whose type the specification does not cover is refused rather than written, as described [above](#postgis-types-the-specification-does-not-cover). On read, the `srid` of the geometries is restored from the `crs` of the logical type, resolving its `authority:code` form through `spatial_ref_sys` and taking the code of its `srid:<code>` form as the `srid` itself, and falls back to the `srid` of the target column's type modifier when the file has no `crs`.
> - (5) `crunchy_map` is dependent on functionality provided by [Crunchy Bridge](https://www.crunchydata.com/products/crunchy-bridge). The `crunchy_map` type is represented as `GROUP` with `MAP` logical type when `crunchy_map` extension is created. Otherwise, it is represented as `BYTE_ARRAY` with `STRING` logical type.
> - (6) The `oid8` type only exists on PostgreSQL 19 and later. It is written as an unsigned `INT64`, so its whole range round-trips.
> - (7) Only one dimensional arrays can be written, since a parquet `LIST` is one dimensional. A multi dimensional array value, e.g. `'{{1,2},{3,4}}'::int[]`, is rejected rather than silently flattened.

> [!NOTE]
> `infinity` and `-infinity` values of the `date`, `timestamp` and `timestamptz` types are written as the limits of the underlying `INT32`/`INT64`, which is how `COPY FROM` reads them back. A temporal value in the Parquet file that `PostgreSQL` cannot represent is rejected with an out of range error.

> [!WARNING]
> Any type that does not have a corresponding Parquet type will be represented, as a fallback mechanism, as `BYTE_ARRAY` with `STRING` logical type. e.g. `enum`

> [!NOTE]
> `COPY FROM` does not need an exact type match: a file's column is cast to the table's column type whenever `PostgreSQL` has a cast between them. That is what makes the files of the other writers readable, e.g. the `TIME_MILLIS`, `FIXED_LEN_BYTE_ARRAY` or wider than `DECIMAL(38)` columns of `Spark`, and the dictionary encoded (`pandas` categorical), view or null typed columns that the `Arrow` based writers record in their schema. An `INT96` timestamp column, which `Hive`, `Impala` and `Spark` write, records no resolution of its own, so it is read as microseconds, which is the resolution `PostgreSQL` stores and covers its whole timestamp range. A fractional column that is read into an integer column is rounded the way `PostgreSQL` rounds it, i.e. a `DECIMAL` half away from zero and a `FLOAT` half to even, rather than truncated toward zero.

## Postgres Support Matrix
`pg_parquet` supports the following PostgreSQL versions:
| PostgreSQL Major Version | Supported |
|--------------------------|-----------|
| 14                       |    ✅     |
| 15                       |    ✅     |
| 16                       |    ✅     |
| 17                       |    ✅     |
| 18                       |    ✅     |
| 19                       |    ✅     |
