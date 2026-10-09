-- error if the schema already exists
CREATE SCHEMA parquet_structs;
REVOKE ALL ON SCHEMA parquet_structs FROM public;
-- the composite types for the structs in a parquet file are created on demand
-- by the user that creates a table from the file, hence CREATE is needed here
GRANT USAGE, CREATE ON SCHEMA parquet_structs TO public;
