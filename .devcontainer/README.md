## Devcontainer
If you install devcontainer extension at vscode, you can spin up your development environment via pg_parquet devcontainer. All necessary components and dependencies would be installed in your environment after the spin-up. You can work in the container as if it is your development environment. (git, make, aws cli and postgres commands should work seamlessly) The container offers reproducible environment as we use the same dockerfile for CI and devcontainer.

## Prerequisites
1. Make sure to allocate >= ~10GB memory resource to docker.
2. Your `~/.aws` folder is mounted as a read only volume to pg_parquet container so that it can read/write buckets with your aws credentials. You are expected to set up your aws credentials before. (possible to read/write from/to **production** s3 buckets)

After opening the project inside the devcontainer, you can do the following steps:

1. Connect to the postgres server (build takes place at the first run)
```bash
cargo pgrx run
```
2. Create pg_parquet extension
```sql
pg_parquet=# create extension pg_parquet;
CREATE EXTENSION
```
3. Copy test data to parquet file
```sql
pg_parquet=# copy (select i from generate_series(1, 10) i) to '/tmp/test.parquet';
COPY 10
```
