# pg_parquet
`pg_parquet` is a PostgreSQL extension that allows you to read and write Parquet files, which are located in `S3` or `file system`, from PostgreSQL via `COPY TO/FROM` commands. It heavily uses [Apache Arrow](https://arrow.apache.org/rust/arrow/) project to read and write Parquet files and [pgrx](https://github.com/pgcentralfoundation/pgrx) project to extend PostgreSQL's `COPY` command.

## Usage
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
COPY product_example TO 'file:///tmp/product_example.parquet' (FORMAT 'parquet', CODEC 'gzip');

-- show table
SELECT * FROM product_example;

-- copy the parquet file to the table
COPY product_example FROM 'file:///tmp/product_example.parquet';

-- show table
SELECT * FROM product_example;
```

## Object Store Support
`pg_parquet` supports reading and writing Parquet files from/to `S3` object store. You can replace `file://`, as shown in above example, with `s3://` scheme to specify the location of the Parquet file in the `S3` object store.

### Configuration
You can set the following `AWS S3` environment variables properly to access to the object store:
- `AWS_ACCESS_KEY_ID`: the access key ID of the AWS account,
- `AWS_SECRET_ACCESS_KEY`: the secret access key of the AWS account,
- `AWS_REGION`: the default region of the AWS account.

## Supported Copy Options
`pg_parquet` supports the following options in the `COPY TO` command:
- `format parquet`: you need to specify this option to read or write Parquet files which does not end with `.parquet[.<codec>]` extension. (This is the only option that `COPY FROM` command supports.),
- `row_group_size <size>`: the number of rows in each row group while writing Parquet files. The default row group size is `100000`,
- `codec <codec>`: the compression codec to use while writing Parquet files. The supported codecs are `uncompressed`, `snappy`, `gzip`, `brotli`, `lz4`, `lz4raw` and `zstd`. The default codec is `uncompressed`. If not specified, the codec is determined by the file extension.

## Supported Types
`pg_parquet` has rich type support, including PostgreSQL's primitive, array, and composite types. Below is the table of the supported types in PostgreSQL and their corresponding Parquet types.

| PostgreSQL Type   | Parquet Physical Type | Logical Type     |
|-------------------|-----------------------|------------------|
| `bool`            | BOOLEAN               |                  |
| `smallint`        | INT16                 |                  |
| `integer`         | INT32                 |                  |
| `bigint`          | INT64                 |                  |
| `real`            | FLOAT                 |                  |
| `oid`             | INT32                 |                  |
| `double`          | DOUBLE                |                  |
| `numeric(38,8)`   | FIXED_LEN_BYTE_ARRAY  | DECIMAL(128)     |
| `"char"`          | BYTE_ARRAY            | STRING           |
| `varchar`         | BYTE_ARRAY            | STRING           |
| `bpchar`          | BYTE_ARRAY            | STRING           |
| `text`            | BYTE_ARRAY            | STRING           |
| `bytea`           | BYTE_ARRAY            |                  |
| `date`            | INT32                 | DATE             |
| `timestamp`       | INT64                 | TIMESTAMP_MICROS |
| `timestamptz`     | INT64                 | TIMESTAMP_MICROS |
| `time`            | INT64                 | TIME_MICROS      |
| `timetz`          | INT64                 | TIME_MICROS      |
| `interval`        | FIXED_LEN_BYTE_ARRAY  | INTERVAL(128)    |
| `bool[]`          | BOOLEAN               | LIST             |
| `smallint[]`      | INT16                 | LIST             |
| `integer[]`       | INT32                 | LIST             |
| `bigint[]`        | INT64                 | LIST             |
| `real[]`          | FLOAT                 | LIST             |
| `double[]`        | DOUBLE                | LIST             |
| `numeric(38,8)[]` | FIXED_LEN_BYTE_ARRAY  | LIST             |
| `"char"[]`        | BYTE_ARRAY            | LIST             |
| `varchar[]`       | BYTE_ARRAY            | LIST             |
| `bpchar[]`        | BYTE_ARRAY            | LIST             |
| `text[]`          | BYTE_ARRAY            | LIST             |
| `bytea[]`         | BYTE_ARRAY            | LIST             |
| `date[]`          | INT32                 | LIST             |
| `timestamp[]`     | INT64                 | LIST             |
| `timestamptz[]`   | INT64                 | LIST             |
| `time[]`          | INT64                 | LIST             |
| `timetz[]`        | INT64                 | LIST             |
| `interval[]`      | FIXED_LEN_BYTE_ARRAY  | LIST             |
| `composite`       | STRUCT                |                  |
