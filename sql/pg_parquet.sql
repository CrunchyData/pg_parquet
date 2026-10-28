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

-- parquet key value metadata function
CREATE  FUNCTION parquet."kv_metadata"(
	"uri" TEXT
) RETURNS TABLE (
	"uri" TEXT,
	"key" BYTEA,
	"value" BYTEA
)
STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', 'kv_metadata_wrapper';

-- parquet metadata function
CREATE  FUNCTION parquet."metadata"(
	"uri" TEXT
) RETURNS TABLE (
	"uri" TEXT,
	"row_group_id" BIGINT,
	"row_group_num_rows" BIGINT,
	"row_group_num_columns" BIGINT,
	"row_group_bytes" BIGINT,
	"column_id" BIGINT,
	"file_offset" BIGINT,
	"num_values" BIGINT,
	"path_in_schema" TEXT,
	"type_name" TEXT,
	"stats_null_count" BIGINT,
	"stats_distinct_count" BIGINT,
	"stats_min" TEXT,
	"stats_max" TEXT,
	"stats_geospatial" JSONB,
	"compression" TEXT,
	"encodings" TEXT,
	"index_page_offset" BIGINT,
	"dictionary_page_offset" BIGINT,
	"data_page_offset" BIGINT,
	"total_compressed_size" BIGINT,
	"total_uncompressed_size" BIGINT
)
STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', 'metadata_wrapper';

-- parquet file metadata function
CREATE  FUNCTION parquet."file_metadata"(
	"uri" TEXT
) RETURNS TABLE (
	"uri" TEXT,
	"created_by" TEXT,
	"num_rows" BIGINT,
	"num_row_groups" BIGINT,
	"format_version" TEXT
)
STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', 'file_metadata_wrapper';

CREATE SCHEMA IF NOT EXISTS parquet;

-- parquet schema function
CREATE  FUNCTION parquet."schema"(
	"uri" TEXT
) RETURNS TABLE (
	"uri" TEXT,
	"name" TEXT,
	"type_name" TEXT,
	"type_length" TEXT,
	"repetition_type" TEXT,
	"num_children" INT,
	"converted_type" TEXT,
	"scale" INT,
	"precision" INT,
	"field_id" INT,
	"logical_type" TEXT
)
STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', 'schema_wrapper';

-- parquet stats function
CREATE FUNCTION parquet."column_stats"("uri" TEXT) RETURNS TABLE (
	"column_id" INT,
	"field_id" INT,
	"stats_min" TEXT,
	"stats_max" TEXT,
	"stats_null_count" BIGINT,
	"stats_distinct_count" BIGINT
) STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', 'column_stats_wrapper';
