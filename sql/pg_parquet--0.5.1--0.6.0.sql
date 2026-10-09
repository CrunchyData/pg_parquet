-- parquet metadata function, which now also reports the geospatial statistics of a column chunk
DROP FUNCTION parquet."metadata"(TEXT);
CREATE FUNCTION parquet."metadata"("uri" TEXT) RETURNS TABLE (
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
) STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', 'metadata_wrapper';
