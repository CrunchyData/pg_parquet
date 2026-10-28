-- The roles that govern access to a remote uri. A non-superuser needs the privileges of one
-- of these to read from or write to an object store, which is the same shape as Postgres' own
-- pg_read_server_files / pg_write_server_files for local paths.
--
-- They are created if absent rather than unconditionally, so that a cluster that granted them
-- before this extension was installed -- or that installs and drops the extension more than
-- once -- keeps its grants.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'object_store_read') THEN
        CREATE ROLE object_store_read;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'object_store_write') THEN
        CREATE ROLE object_store_write;
    END IF;
END $$;

-- error if the schema already exists
CREATE SCHEMA object_store;
REVOKE ALL ON SCHEMA object_store FROM public;
GRANT USAGE ON SCHEMA object_store TO public;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA object_store TO public;
