-- The schema the format-agnostic functions live in. No roles are created here: reaching a uri
-- is governed by object_store_read / object_store_write, which pg_object_store owns, and a
-- second extension defining the same roles is how a cluster ends up with two answers to the
-- same question.
--
-- Errors if the schema already exists, which is deliberate: `arrow` is a short name and taking
-- one that a user already has would be worse than refusing to install.
CREATE SCHEMA arrow;
REVOKE ALL ON SCHEMA arrow FROM public;
GRANT USAGE ON SCHEMA arrow TO public;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA arrow TO public;

-- Where `CREATE TABLE ... FROM '<uri>'` puts the composite types a file's struct fields need.
-- They are created on demand, by whichever user runs the statement, so CREATE has to be here,
-- and they are deliberately not members of this extension: a table's column type must outlive
-- a DROP EXTENSION.
CREATE SCHEMA arrow_structs;
REVOKE ALL ON SCHEMA arrow_structs FROM public;
GRANT USAGE, CREATE ON SCHEMA arrow_structs TO public;
