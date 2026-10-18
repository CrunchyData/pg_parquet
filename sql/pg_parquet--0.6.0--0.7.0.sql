-- 0.7.0 is where pg_parquet stopped owning everything it used to. Reaching a uri is
-- pg_object_store's now and turning a file into Arrow is pg_arrow_base's, so this upgrade is
-- almost entirely subtraction: the objects below moved to another extension, and a second copy
-- of any of them is worse than none.
--
-- The five functions in the parquet schema are unchanged.

-- Both tiers have to be installed first. `requires` in the control file covers a fresh
-- `CREATE EXTENSION` and does nothing for an update, so this is where an existing installation
-- finds out.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_object_store') THEN
        RAISE EXCEPTION 'pg_parquet 0.7.0 needs the pg_object_store extension'
            USING HINT = 'run CREATE EXTENSION pg_object_store, then retry this update';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_arrow_base') THEN
        RAISE EXCEPTION 'pg_parquet 0.7.0 needs the pg_arrow_base extension'
            USING HINT = 'run CREATE EXTENSION pg_arrow_base, then retry this update';
    END IF;
END;
$$;

-- listing a uri has nothing to do with parquet. arrow.list(TEXT) is the closest equivalent --
-- it names the format of every file it lists -- and object_store.list(TEXT) is the plain one.
DROP FUNCTION IF EXISTS parquet."list"(TEXT);

-- The privilege to read or write a uri is object_store_read and object_store_write now.
-- Whoever held the parquet-named role is granted the pg_object_store one: losing the grant
-- quietly would surface as a permission error on the next COPY instead of here.
DO $$
DECLARE
    pair RECORD;
    member TEXT;
BEGIN
    FOR pair IN
        SELECT * FROM (VALUES
            ('parquet_object_store_read', 'object_store_read'),
            ('parquet_object_store_write', 'object_store_write')
        ) AS t(old_role, new_role)
    LOOP
        CONTINUE WHEN NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = pair.old_role);

        FOR member IN
            SELECT m.rolname
            FROM pg_auth_members a
            JOIN pg_roles r ON r.oid = a.roleid
            JOIN pg_roles m ON m.oid = a.member
            WHERE r.rolname = pair.old_role
        LOOP
            EXECUTE format('GRANT %I TO %I', pair.new_role, member);
        END LOOP;

        -- a role that still holds a privilege on some object cannot be dropped, and that
        -- privilege was granted by a user rather than by this extension: say so and move on
        BEGIN
            EXECUTE format('DROP ROLE %I', pair.old_role);
        EXCEPTION WHEN dependent_objects_still_exist THEN
            RAISE NOTICE 'role % is still referenced and was left in place', pair.old_role
                USING HINT = 'pg_parquet no longer reads it; drop it once nothing grants to it';
        END;
    END LOOP;
END;
$$;

-- The composite types for a file's structs are created in pg_arrow_base's arrow_structs, which
-- that extension created when it was installed. Types a user already has in here are theirs, so
-- the schema goes only while it is empty.
DO $$
BEGIN
    DROP SCHEMA IF EXISTS parquet_structs RESTRICT;
EXCEPTION WHEN dependent_objects_still_exist THEN
    RAISE NOTICE 'schema parquet_structs still holds types and was left in place'
        USING HINT = 'new struct types go to arrow_structs; drop the old schema once nothing reads from it';
END;
$$;
