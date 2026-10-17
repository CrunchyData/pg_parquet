-- Only the schema this extension's own functions live in.
--
-- The roles that govern reaching a uri (object_store_read, object_store_write) are
-- pg_object_store's, and the schema the composite types of a file's structs are created in
-- (arrow_structs) is pg_arrow_base's. Creating either here would be a second copy of an object
-- the other tier owns.

-- error if the schema already exists
CREATE SCHEMA parquet;
REVOKE ALL ON SCHEMA parquet FROM public;
GRANT USAGE ON SCHEMA parquet TO public;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA parquet TO public;
