-- create the role that administers which profiles a role may select if it does not exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'parquet_profile_manager') THEN
        CREATE ROLE parquet_profile_manager;
    END IF;
END $$;

-- user_profile_mapping maps a role to the profiles it may select with
-- pg_parquet.profile_name, as
--   {"aws_profiles": [...], "azure_profiles": [...], "gcs_profiles": [...]}
-- A row is read for every role the session has the privileges of, so a profile mapped to a
-- group role is selectable by each of its members.
--
-- Only parquet_profile_manager and a superuser may write the table, which is what keeps a
-- session from selecting a profile it was not mapped to: the GUC that names the profile is
-- settable by anyone, and this is the mapping that it is checked against.
CREATE TABLE parquet.user_profile_mapping (
    -- regrole rather than oid: it is written and dumped as the name of the role, so a
    -- restore resolves the role of this cluster rather than whichever role happens to
    -- hold the oid the dump was taken with, and a role that does not exist is refused
    -- when the row is written rather than mapping a profile to nobody
    userid regrole PRIMARY KEY,
    profiles jsonb NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT profiles_is_an_object CHECK (jsonb_typeof(profiles) = 'object'),
    -- a misspelled key would otherwise map no profile at all, which reads as a profile
    -- that is refused rather than as the typo that it is
    CONSTRAINT profiles_keys_are_known CHECK (
        profiles - ARRAY['aws_profiles', 'azure_profiles', 'gcs_profiles'] = '{}'::jsonb
    ),
    -- strict is what makes $.* the values of the object rather than the elements of the
    -- arrays it holds, which lax mode unwraps
    CONSTRAINT profiles_are_arrays_of_names CHECK (
        NOT (profiles @? 'strict $.* ? (@.type() != "array")')
        AND NOT (profiles @? 'strict $.*[*] ? (@.type() != "string")')
    )
);

-- the mapping is configuration rather than a part of the extension, so it is dumped
SELECT pg_catalog.pg_extension_config_dump('parquet.user_profile_mapping', '');

REVOKE ALL ON parquet.user_profile_mapping FROM public;
GRANT SELECT, INSERT, UPDATE, DELETE ON parquet.user_profile_mapping
    TO parquet_profile_manager;

-- my_profiles is the mapping as the session may select from it, which is the rows of the
-- roles it has the privileges of. The table itself is readable by the manager only.
--
-- The view is what reads the table on behalf of a session that has no privilege on it,
-- rather than a SECURITY DEFINER function: current_user in the body of such a function is
-- the role that defined it, so pg_has_role below would answer for the superuser that
-- installed the extension and every row of the table would be one of the session's. In a
-- view it is the session's own role, while the table is still read as the owner.
CREATE VIEW parquet.my_profiles AS
    SELECT m.userid AS mapped_to, m.profiles
    FROM parquet.user_profile_mapping m
    WHERE pg_catalog.pg_has_role(current_user, m.userid::oid, 'USAGE');

REVOKE ALL ON parquet.my_profiles FROM public;
GRANT SELECT ON parquet.my_profiles TO public;

-- profile_is_allowed tells whether the session may select a profile for a scheme. It
-- answers with a boolean about a profile name, so it tells a caller nothing that the
-- caller could not select anyway.
CREATE FUNCTION parquet.profile_is_allowed("scheme" TEXT, "profile" TEXT) RETURNS BOOLEAN
LANGUAGE sql
STABLE
AS $$
    SELECT EXISTS (
        SELECT 1
        FROM parquet.my_profiles m
        WHERE m.profiles -> (CASE "scheme"
                                 WHEN 's3' THEN 'aws_profiles'
                                 WHEN 'az' THEN 'azure_profiles'
                                 WHEN 'gs' THEN 'gcs_profiles'
                             END) ? "profile"
    )
$$;

REVOKE ALL ON FUNCTION parquet.profile_is_allowed(TEXT, TEXT) FROM public;
GRANT EXECUTE ON FUNCTION parquet.profile_is_allowed(TEXT, TEXT) TO public;
