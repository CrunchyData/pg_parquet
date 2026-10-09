-- create roles for parquet object store read and write if they do not exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'parquet_object_store_read') THEN
        CREATE ROLE parquet_object_store_read;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'parquet_object_store_write') THEN
        CREATE ROLE parquet_object_store_write;
    END IF;
END $$;

-- error if the schema already exists
CREATE SCHEMA parquet;
REVOKE ALL ON SCHEMA parquet FROM public;
GRANT USAGE ON SCHEMA parquet TO public;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA parquet TO public;

-- error if the schema already exists
CREATE SCHEMA parquet_structs;
REVOKE ALL ON SCHEMA parquet_structs FROM public;
-- the composite types for the structs in a parquet file are created on demand
-- by the user that creates a table from the file, hence CREATE is needed here
GRANT USAGE, CREATE ON SCHEMA parquet_structs TO public;
