#[pgrx::pg_schema]
mod tests {
    use crate::pgrx_tests::common::{
        extension_exists, extension_version, geospatial_crs_by_column, TestTable,
        LOCAL_TEST_FILE_PATH,
    };
    use crate::type_compat::geometry::{Geography, Geometry};
    use pgrx::{pg_test, Spi};

    fn postgis_missing() -> bool {
        !extension_exists("postgis") || *extension_version("postgis") < *"3.4"
    }

    fn create_postgis() {
        Spi::run("DROP EXTENSION IF EXISTS postgis CASCADE; CREATE EXTENSION postgis;").unwrap();
    }

    fn bbox_text(column: &str) -> Option<String> {
        let query = format!(
            "SELECT (SELECT (stats_geospatial->'bbox')::text
                     FROM parquet.metadata('{LOCAL_TEST_FILE_PATH}')
                     WHERE path_in_schema = '{column}'
                     ORDER BY row_group_id
                     LIMIT 1)"
        );
        Spi::get_one::<String>(&query).unwrap()
    }

    fn geospatial_types_text(column: &str) -> Option<String> {
        let query = format!(
            "SELECT (SELECT array_agg(t::int order by t::int)::text
                     FROM parquet.metadata('{LOCAL_TEST_FILE_PATH}') m,
                          jsonb_array_elements_text(m.stats_geospatial->'geospatial_types') t
                     WHERE m.path_in_schema = '{column}')"
        );
        Spi::get_one::<String>(&query).unwrap()
    }

    fn stats_geospatial_text(column: &str) -> Option<String> {
        let query = format!(
            "SELECT (SELECT stats_geospatial::text
                     FROM parquet.metadata('{LOCAL_TEST_FILE_PATH}')
                     WHERE path_in_schema = '{column}'
                     ORDER BY row_group_id
                     LIMIT 1)"
        );
        Spi::get_one::<String>(&query).unwrap()
    }

    fn logical_type_of(column: &str) -> Option<String> {
        let query = format!(
            "SELECT (SELECT logical_type
                     FROM parquet.schema('{LOCAL_TEST_FILE_PATH}')
                     WHERE name = '{column}')"
        );
        Spi::get_one::<String>(&query).unwrap()
    }

    // ---------------------------------------------------------------- piece 1

    #[pg_test]
    fn test_geo_all_wkb_types() {
        if postgis_missing() {
            return;
        }
        create_postgis();

        let test_table = TestTable::<Geometry>::new("geometry".into());
        test_table.insert(
            "INSERT INTO test_expected (a) VALUES
               (ST_GeomFromText('POINT(1 2)')),
               (ST_GeomFromText('LINESTRING(0 0, 1 1, 2 2)')),
               (ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0),(2 2, 2 4, 4 4, 4 2, 2 2))')),
               (ST_GeomFromText('MULTIPOINT((1 1),(3 3))')),
               (ST_GeomFromText('MULTILINESTRING((0 0, 1 1),(2 2, 3 3))')),
               (ST_GeomFromText('MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)),((5 5, 5 6, 6 6, 6 5, 5 5)))')),
               (ST_GeomFromText('GEOMETRYCOLLECTION(POINT(-1 -1), LINESTRING(7 7, 8 8))'));",
        );
        test_table.assert_expected_and_result_rows();

        assert_eq!(
            geospatial_types_text("a"),
            Some("{1,2,3,4,5,6,7}".to_string())
        );
        assert_eq!(
            bbox_text("a"),
            Some("{\"xmax\": 10.0, \"xmin\": -1.0, \"ymax\": 10.0, \"ymin\": -1.0}".to_string())
        );
    }

    #[pg_test]
    fn test_geo_zm_dimensions() {
        if postgis_missing() {
            return;
        }
        create_postgis();

        let test_table = TestTable::<Geometry>::new("geometry".into());
        test_table.insert(
            "INSERT INTO test_expected (a) VALUES
               (ST_GeomFromText('POINT Z (1 2 3)')),
               (ST_GeomFromText('POINT M (4 5 6)')),
               (ST_GeomFromText('POINT ZM (7 8 9 10)')),
               (ST_GeomFromText('LINESTRING ZM (0 0 0 0, 1 1 1 1)'));",
        );
        test_table.assert_expected_and_result_rows();

        // z and m ranges must appear once the geometries carry those dimensions
        assert_eq!(
            stats_geospatial_text("a"),
            Some(
                "{\"bbox\": {\"mmax\": 10.0, \"mmin\": 0.0, \"xmax\": 7.0, \"xmin\": 0.0, \
                 \"ymax\": 8.0, \"ymin\": 0.0, \"zmax\": 9.0, \"zmin\": 0.0}, \
                 \"geospatial_types\": [1001, 2001, 3001, 3002]}"
                    .to_string()
            )
        );
    }

    #[pg_test]
    fn test_geo_empty_geometries() {
        if postgis_missing() {
            return;
        }
        create_postgis();

        let test_table = TestTable::<Geometry>::new("geometry".into());
        test_table.insert(
            "INSERT INTO test_expected (a) VALUES
               (ST_GeomFromText('POINT EMPTY')),
               (ST_GeomFromText('LINESTRING EMPTY')),
               (ST_GeomFromText('POLYGON EMPTY')),
               (ST_GeomFromText('MULTIPOINT EMPTY')),
               (ST_GeomFromText('GEOMETRYCOLLECTION EMPTY'));",
        );
        test_table.assert_expected_and_result_rows();

        // empty geometries contribute their type but no bounds
        assert_eq!(bbox_text("a"), Some("null".to_string()));
        assert_eq!(geospatial_types_text("a"), Some("{1,2,3,4,7}".to_string()));
    }

    #[pg_test]
    fn test_geo_nonfinite_coordinates() {
        if postgis_missing() {
            return;
        }
        create_postgis();

        // postgis reads a wkb point of NaN coordinates back as POINT EMPTY, so only the written
        // statistics are asserted here
        let copy_to_query = format!(
            "COPY (SELECT ST_MakePoint('Infinity'::float8, '-Infinity'::float8) as a
                   UNION ALL SELECT ST_MakePoint('NaN'::float8, 'NaN'::float8))
             TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        );
        Spi::run(&copy_to_query).unwrap();

        // json has no infinity, so the bounding box keys are present but null
        assert_eq!(
            stats_geospatial_text("a"),
            Some(
                "{\"bbox\": {\"xmax\": null, \"xmin\": null, \"ymax\": null, \"ymin\": null}, \
                 \"geospatial_types\": [1]}"
                    .to_string()
            )
        );
    }

    #[pg_test]
    fn test_geo_unsupported_wkb_types() {
        if postgis_missing() {
            return;
        }
        create_postgis();

        let test_table = TestTable::<Geometry>::new("geometry".into());
        test_table.insert(
            "INSERT INTO test_expected (a) VALUES
               (ST_GeomFromText('CIRCULARSTRING(0 0, 1 1, 2 0)')),
               (ST_GeomFromText('COMPOUNDCURVE(CIRCULARSTRING(0 0, 1 1, 2 0), (2 0, 3 0))')),
               (ST_GeomFromText('CURVEPOLYGON(CIRCULARSTRING(0 0, 2 0, 2 2, 0 2, 0 0))')),
               (ST_GeomFromText('MULTICURVE((0 0, 1 1), CIRCULARSTRING(2 0, 3 1, 4 0))')),
               (ST_GeomFromText('MULTISURFACE(CURVEPOLYGON(CIRCULARSTRING(0 0, 2 0, 2 2, 0 2, 0 0)))')),
               (ST_GeomFromText('TIN(((0 0 0, 0 0 1, 0 1 0, 0 0 0)))')),
               (ST_GeomFromText('POLYHEDRALSURFACE(((0 0 0, 0 0 1, 0 1 1, 0 1 0, 0 0 0)))')),
               (ST_GeomFromText('TRIANGLE((0 0, 0 1, 1 1, 0 0))'));",
        );
        test_table.assert_expected_and_result_rows();

        // the bounder cannot walk these, so the whole column loses its statistics
        assert_eq!(stats_geospatial_text("a"), None);
    }

    #[pg_test]
    fn test_geo_unsupported_geometry_drops_statistics() {
        if postgis_missing() {
            return;
        }
        create_postgis();

        let test_table = TestTable::<Geometry>::new("geometry".into());
        test_table.insert(
            "INSERT INTO test_expected (a) VALUES
               (ST_GeomFromText('POINT(1 2)')),
               (ST_GeomFromText('TRIANGLE((0 0, 0 1, 1 1, 0 0))'));",
        );
        test_table.assert_expected_and_result_rows();

        assert_eq!(stats_geospatial_text("a"), None);
    }

    #[pg_test]
    fn test_geo_all_null_geometry_column() {
        if postgis_missing() {
            return;
        }
        create_postgis();

        let test_table = TestTable::<Geometry>::new("geometry".into());
        test_table.insert("INSERT INTO test_expected (a) VALUES (null), (null);");
        test_table.assert_expected_and_result_rows();

        assert_eq!(
            stats_geospatial_text("a"),
            Some("{\"bbox\": null, \"geospatial_types\": null}".to_string())
        );
    }

    #[pg_test]
    fn test_geo_geography_has_no_statistics() {
        if postgis_missing() {
            return;
        }
        create_postgis();

        let test_table = TestTable::<Geography>::new("geography".into());
        test_table.insert(
            "INSERT INTO test_expected (a) VALUES
               (ST_GeogFromText('POINT(1 2)')),
               (ST_GeogFromText('LINESTRING(3 4, 5 6)'));",
        );
        test_table.assert_expected_and_result_rows();

        // parquet only accumulates geospatial statistics for the geometry logical type
        assert_eq!(logical_type_of("a"), Some("GEOGRAPHY".to_string()));
        assert_eq!(stats_geospatial_text("a"), None);
    }

    // ---------------------------------------------------------------- piece 2

    #[pg_test]
    fn test_geo_many_geo_columns_in_one_copy() {
        if postgis_missing() {
            return;
        }
        create_postgis();

        let copy_to_query = format!(
            "COPY (SELECT ST_GeomFromText('POINT(1 2)') as g1,
                          ST_SetSRID(ST_MakePoint(3, 4), 4326)::geometry(point, 4326) as g2,
                          ST_SetSRID(ST_MakePoint(5, 6), 3857)::geometry(point, 3857) as g3,
                          ST_GeogFromText('POINT(7 8)') as gg1,
                          ST_GeogFromText('LINESTRING(9 10, 11 12)')::geography(linestring, 4326) as gg2,
                          ST_GeomFromText('POINT(1 2)')::bytea as b,
                          42 as i
                  )
             TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        );
        Spi::run(&copy_to_query).unwrap();

        assert_eq!(logical_type_of("g1"), Some("GEOMETRY".to_string()));
        assert_eq!(logical_type_of("g2"), Some("GEOMETRY".to_string()));
        assert_eq!(logical_type_of("g3"), Some("GEOMETRY".to_string()));
        assert_eq!(logical_type_of("gg1"), Some("GEOGRAPHY".to_string()));
        assert_eq!(logical_type_of("gg2"), Some("GEOGRAPHY".to_string()));
        assert_eq!(logical_type_of("b"), None);

        let crs_by_column = geospatial_crs_by_column();
        assert_eq!(crs_by_column.get("g1"), Some(&Some("srid:0".to_string())));
        assert_eq!(crs_by_column.get("g2"), Some(&None));
        assert_eq!(
            crs_by_column.get("g3"),
            Some(&Some("EPSG:3857".to_string()))
        );
        assert_eq!(crs_by_column.get("gg1"), Some(&None));
        assert_eq!(crs_by_column.get("gg2"), Some(&None));

        assert!(stats_geospatial_text("g1").is_some());
        assert!(stats_geospatial_text("g2").is_some());
        assert!(stats_geospatial_text("g3").is_some());
        assert_eq!(stats_geospatial_text("gg1"), None);
        assert_eq!(stats_geospatial_text("b"), None);
        assert_eq!(stats_geospatial_text("i"), None);
    }

    #[pg_test]
    fn test_geo_nested_composite_with_geo() {
        if postgis_missing() {
            return;
        }
        create_postgis();

        Spi::run(
            "DROP TYPE IF EXISTS geo_inner CASCADE;
             DROP TYPE IF EXISTS geo_outer CASCADE;
             CREATE TYPE geo_inner AS (g geometry, gg geography);
             CREATE TYPE geo_outer AS (label text, inner_geo geo_inner, geos geometry[]);",
        )
        .unwrap();

        let copy_to_query = format!(
            "COPY (SELECT row('x',
                              row(ST_GeomFromText('POINT(1 2)'),
                                  ST_GeogFromText('POINT(3 4)'))::geo_inner,
                              array[ST_GeomFromText('LINESTRING(5 6, 7 8)'), null]
                             )::geo_outer as o
                  )
             TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        );
        Spi::run(&copy_to_query).unwrap();

        let logical_types = Spi::get_one::<String>(&format!(
            "SELECT array_agg(name || '=' || coalesce(logical_type, '-') order by name)::text
             FROM parquet.schema('{LOCAL_TEST_FILE_PATH}')"
        ))
        .unwrap()
        .unwrap();
        assert_eq!(
            logical_types,
            "{arrow_schema=-,element=GEOMETRY,g=GEOMETRY,geos=LIST,gg=GEOGRAPHY,\
             inner_geo=-,label=STRING,list=-,o=-}"
        );

        let nested_stats = Spi::get_one::<String>(&format!(
            "SELECT array_agg(path_in_schema || '=' || coalesce(stats_geospatial::text, '-')
                              order by path_in_schema)::text
             FROM parquet.metadata('{LOCAL_TEST_FILE_PATH}')"
        ))
        .unwrap()
        .unwrap();
        // the nested geometry columns get their own statistics, the nested geography ones do not
        assert!(nested_stats.contains(
            "o.inner_geo.g={\\\"bbox\\\": {\\\"xmax\\\": 1.0, \\\"xmin\\\": 1.0, \
             \\\"ymax\\\": 2.0, \\\"ymin\\\": 2.0}"
        ));
        assert!(nested_stats.contains("o.inner_geo.gg=-"));

        Spi::run(
            "DROP TABLE IF EXISTS nested_result;
             CREATE TABLE nested_result (o geo_outer);",
        )
        .unwrap();
        Spi::run(&format!(
            "COPY nested_result FROM '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();

        let roundtripped = Spi::get_one::<String>(
            "SELECT ST_AsText(((t.o).inner_geo).g) || '|' || ST_AsText(((t.o).inner_geo).gg)
             FROM nested_result t",
        )
        .unwrap()
        .unwrap();
        assert_eq!(roundtripped, "POINT(1 2)|POINT(3 4)");
    }

    #[pg_test]
    fn test_geo_array_of_composite_with_geo() {
        if postgis_missing() {
            return;
        }
        create_postgis();

        Spi::run(
            "DROP TYPE IF EXISTS geo_pair CASCADE;
             CREATE TYPE geo_pair AS (g geometry(point, 4326), gg geography(point, 4326));",
        )
        .unwrap();

        let copy_to_query = format!(
            "COPY (SELECT array[
                              row(ST_SetSRID(ST_MakePoint(1, 2), 4326)::geometry(point, 4326),
                                  ST_GeogFromText('POINT(3 4)')::geography(point, 4326))::geo_pair,
                              null
                          ] as pairs
                  )
             TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        );
        Spi::run(&copy_to_query).unwrap();

        // the geometry inside the composite array keeps its own bounding box
        let nested_bbox_written = Spi::get_one::<bool>(&format!(
            "SELECT count(*) = 1
             FROM parquet.metadata('{LOCAL_TEST_FILE_PATH}')
             WHERE (stats_geospatial->'bbox'->>'xmin')::float8 = 1
               AND (stats_geospatial->'bbox'->>'ymin')::float8 = 2"
        ))
        .unwrap()
        .unwrap();
        assert!(nested_bbox_written);

        Spi::run(
            "DROP TABLE IF EXISTS pairs_result;
             CREATE TABLE pairs_result (pairs geo_pair[]);",
        )
        .unwrap();
        Spi::run(&format!(
            "COPY pairs_result FROM '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();

        let roundtripped =
            Spi::get_one::<String>("SELECT ST_AsText((t.pairs[1]).g) FROM pairs_result t")
                .unwrap()
                .unwrap();
        assert_eq!(roundtripped, "POINT(1 2)");
    }

    // ---------------------------------------------------------------- piece 3

    #[pg_test]
    fn test_geo_copy_syntaxes() {
        if postgis_missing() {
            return;
        }
        create_postgis();

        Spi::run(
            "DROP TABLE IF EXISTS geo_src;
             CREATE TABLE geo_src (id int, g geometry(point, 4326), gg geography(point, 4326));
             INSERT INTO geo_src VALUES
               (1, ST_SetSRID(ST_MakePoint(1, 2), 4326), ST_GeogFromText('POINT(3 4)')),
               (2, null, null);",
        )
        .unwrap();

        // COPY table TO
        Spi::run(&format!(
            "COPY geo_src TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();
        assert_eq!(logical_type_of("g"), Some("GEOMETRY".to_string()));
        assert_eq!(logical_type_of("gg"), Some("GEOGRAPHY".to_string()));

        // COPY table FROM, match_by position (the default)
        Spi::run(
            "DROP TABLE IF EXISTS geo_dst;
             CREATE TABLE geo_dst (id int, g geometry(point, 4326), gg geography(point, 4326));",
        )
        .unwrap();
        Spi::run(&format!(
            "COPY geo_dst FROM '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();
        assert_eq!(
            Spi::get_one::<i64>("SELECT count(*) FROM geo_dst WHERE g IS NOT NULL")
                .unwrap()
                .unwrap(),
            1
        );

        // COPY table FROM, match_by name with the columns permuted
        Spi::run(
            "DROP TABLE IF EXISTS geo_dst_named;
             CREATE TABLE geo_dst_named (gg geography(point, 4326), id int, g geometry(point, 4326));",
        )
        .unwrap();
        Spi::run(&format!(
            "COPY geo_dst_named FROM '{LOCAL_TEST_FILE_PATH}'
             WITH (format parquet, match_by name);"
        ))
        .unwrap();
        assert_eq!(
            Spi::get_one::<String>("SELECT ST_AsText(g) FROM geo_dst_named WHERE g IS NOT NULL")
                .unwrap()
                .unwrap(),
            "POINT(1 2)"
        );

        // COPY table (columns) TO
        Spi::run(&format!(
            "COPY geo_src (gg, g) TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();
        assert_eq!(logical_type_of("g"), Some("GEOMETRY".to_string()));
        assert_eq!(logical_type_of("gg"), Some("GEOGRAPHY".to_string()));

        // COPY (query) TO with an expression that loses the typmod
        Spi::run(&format!(
            "COPY (SELECT ST_Centroid(g) as g FROM geo_src) TO '{LOCAL_TEST_FILE_PATH}'
             WITH (format parquet);"
        ))
        .unwrap();
        assert_eq!(logical_type_of("g"), Some("GEOMETRY".to_string()));
        // an expression loses the type modifier, so the crs falls back to the unknown srid
        let crs_by_column = geospatial_crs_by_column();
        assert_eq!(crs_by_column.get("g"), Some(&Some("srid:0".to_string())));

        // COPY TO/FROM through PROGRAM and STDOUT are covered elsewhere; here we only need the
        // geospatial type to survive a row_group_size of one
        Spi::run(&format!(
            "COPY geo_src TO '{LOCAL_TEST_FILE_PATH}'
             WITH (format parquet, row_group_size 1, compression zstd);"
        ))
        .unwrap();
        let row_groups = Spi::get_one::<i64>(&format!(
            "SELECT count(*) FROM parquet.metadata('{LOCAL_TEST_FILE_PATH}')
             WHERE path_in_schema = 'g'"
        ))
        .unwrap()
        .unwrap();
        assert_eq!(row_groups, 2);
        let per_row_group = Spi::get_one::<String>(&format!(
            "SELECT array_agg(coalesce(stats_geospatial::text, '-') order by row_group_id)::text
             FROM parquet.metadata('{LOCAL_TEST_FILE_PATH}')
             WHERE path_in_schema = 'g'"
        ))
        .unwrap()
        .unwrap();
        // the row group that only holds the null geometry has no bounds
        assert_eq!(
            per_row_group,
            "{\"{\\\"bbox\\\": {\\\"xmax\\\": 1.0, \\\"xmin\\\": 1.0, \
             \\\"ymax\\\": 2.0, \\\"ymin\\\": 2.0}, \\\"geospatial_types\\\": [1]}\",\
             \"{\\\"bbox\\\": null, \\\"geospatial_types\\\": null}\"}"
        );
    }

    #[pg_test]
    fn test_geo_geometry_geography_cross_load() {
        if postgis_missing() {
            return;
        }
        create_postgis();

        // a geography file read back into a geometry column and the reverse
        Spi::run(
            "DROP TABLE IF EXISTS geog_only;
             CREATE TABLE geog_only (a geography);
             INSERT INTO geog_only VALUES (ST_GeogFromText('POINT(1 2)'));",
        )
        .unwrap();
        Spi::run(&format!(
            "COPY geog_only TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();

        Spi::run(
            "DROP TABLE IF EXISTS geom_target;
             CREATE TABLE geom_target (a geometry);",
        )
        .unwrap();
        Spi::run(&format!(
            "COPY geom_target FROM '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();
        // a geography is lon/lat, so the geometry it is read into gets srid 4326
        assert_eq!(
            Spi::get_one::<String>("SELECT ST_AsEWKT(a) FROM geom_target")
                .unwrap()
                .unwrap(),
            "SRID=4326;POINT(1 2)"
        );

        // bytea target
        Spi::run(
            "DROP TABLE IF EXISTS bytea_target;
             CREATE TABLE bytea_target (a bytea);",
        )
        .unwrap();
        Spi::run(&format!(
            "COPY bytea_target FROM '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();
        assert_eq!(
            Spi::get_one::<i64>("SELECT count(*) FROM bytea_target WHERE a IS NOT NULL")
                .unwrap()
                .unwrap(),
            1
        );
    }

    // ---------------------------------------------------------------- piece 4

    #[pg_test]
    fn test_geo_srid_matrix() {
        if postgis_missing() {
            return;
        }
        create_postgis();

        let copy_to_query = format!(
            "COPY (SELECT ST_SetSRID(ST_MakePoint(1, 2), 0)::geometry(point, 0) as srid0,
                          ST_SetSRID(ST_MakePoint(1, 2), 4326)::geometry(point, 4326) as srid4326,
                          ST_SetSRID(ST_MakePoint(1, 2), 4269)::geometry(point, 4269) as srid4269,
                          ST_SetSRID(ST_MakePoint(1, 2), 3857)::geometry(point, 3857) as srid3857,
                          ST_SetSRID(ST_MakePoint(1, 2), 998999)::geometry(point, 998999) as srid_user_max,
                          ST_GeogFromText('POINT(1 2)')::geography(point, 4326) as geog4326,
                          ST_GeogFromText('POINT(1 2)') as geog_notypmod
                  )
             TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        );
        Spi::run(&copy_to_query).unwrap();

        let crs_by_column = geospatial_crs_by_column();

        assert_eq!(
            crs_by_column.get("srid0"),
            Some(&Some("srid:0".to_string()))
        );
        assert_eq!(crs_by_column.get("srid4326"), Some(&None));
        assert_eq!(
            crs_by_column.get("srid4269"),
            Some(&Some("EPSG:4269".to_string()))
        );
        assert_eq!(
            crs_by_column.get("srid3857"),
            Some(&Some("EPSG:3857".to_string()))
        );
        assert_eq!(
            crs_by_column.get("srid_user_max"),
            Some(&Some("srid:0".to_string()))
        );
        assert_eq!(crs_by_column.get("geog4326"), Some(&None));
        assert_eq!(crs_by_column.get("geog_notypmod"), Some(&None));
    }

    #[pg_test]
    fn test_geo_srid_restored_on_read() {
        if postgis_missing() {
            return;
        }
        create_postgis();

        let copy_to_query = format!(
            "COPY (SELECT ST_SetSRID(ST_MakePoint(1, 2), 4326)::geometry(point, 4326) as lonlat,
                          ST_SetSRID(ST_MakePoint(1, 2), 4269)::geometry(point, 4269) as authority,
                          ST_SetSRID(ST_MakePoint(1, 2), 998999)::geometry(point, 998999) as unknown_srid,
                          ST_SetSRID(ST_MakePoint(1, 2), 4326) as no_typmod,
                          ST_GeogFromText('POINT(1 2)')::geography(point, 4326) as geog
                  )
             TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        );
        Spi::run(&copy_to_query).unwrap();

        Spi::run(
            "DROP TABLE IF EXISTS srid_restore;
             CREATE TABLE srid_restore (lonlat geometry, authority geometry,
                                        unknown_srid geometry, no_typmod geometry,
                                        geog geography);",
        )
        .unwrap();
        Spi::run(&format!(
            "COPY srid_restore FROM '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();

        let srids = Spi::get_one::<String>(
            "SELECT format('%s,%s,%s,%s,%s', ST_SRID(lonlat), ST_SRID(authority),
                                             ST_SRID(unknown_srid), ST_SRID(no_typmod),
                                             ST_SRID(geog))
             FROM srid_restore",
        )
        .unwrap()
        .unwrap();

        // the lon/lat crs is omitted in the file and comes back as "OGC:CRS84", the authority
        // form resolves through spatial_ref_sys, and an srid that postgis cannot name is written
        // as an unset crs, which leaves the geometry with the unknown srid
        assert_eq!(srids, "4326,4269,0,0,4326");

        let wkt = Spi::get_one::<String>("SELECT ST_AsText(authority) FROM srid_restore")
            .unwrap()
            .unwrap();
        assert_eq!(wkt, "POINT(1 2)");

        // the target column's own crs fills in for the columns whose crs the file leaves unset
        Spi::run(
            "DROP TABLE IF EXISTS srid_from_typmod;
             CREATE TABLE srid_from_typmod (lonlat geometry(point, 4326),
                                            authority geometry(point, 4269),
                                            unknown_srid geometry(point, 998999),
                                            no_typmod geometry(point, 3857),
                                            geog geography(point, 4326));",
        )
        .unwrap();
        Spi::run(&format!(
            "COPY srid_from_typmod FROM '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();

        let srids = Spi::get_one::<String>(
            "SELECT format('%s,%s,%s,%s,%s', ST_SRID(lonlat), ST_SRID(authority),
                                             ST_SRID(unknown_srid), ST_SRID(no_typmod),
                                             ST_SRID(geog))
             FROM srid_from_typmod",
        )
        .unwrap()
        .unwrap();
        assert_eq!(srids, "4326,4269,998999,3857,4326");
    }

    #[pg_test]
    fn test_geo_srid_restored_in_nested_types() {
        if postgis_missing() {
            return;
        }
        create_postgis();

        Spi::run(
            "DROP TYPE IF EXISTS geo_srid_pair CASCADE;
             CREATE TYPE geo_srid_pair AS (g geometry(point, 3857), gg geography(point, 4326));
             DROP TABLE IF EXISTS nested_srid;
             CREATE TABLE nested_srid (gs geometry(point, 4269)[], pair geo_srid_pair);
             INSERT INTO nested_srid
             VALUES (array[ST_SetSRID(ST_MakePoint(1, 2), 4269)],
                     row(ST_SetSRID(ST_MakePoint(3, 4), 3857),
                         ST_GeogFromText('POINT(5 6)'))::geo_srid_pair);",
        )
        .unwrap();

        let copy_to_query =
            format!("COPY nested_srid TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);");
        Spi::run(&copy_to_query).unwrap();

        Spi::run("DELETE FROM nested_srid;").unwrap();
        Spi::run(&format!(
            "COPY nested_srid FROM '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();

        // an array element and a composite field each carry their own crs
        let srids = Spi::get_one::<String>(
            "SELECT format('%s,%s,%s', ST_SRID(gs[1]), ST_SRID((pair).g), ST_SRID((pair).gg))
             FROM nested_srid",
        )
        .unwrap()
        .unwrap();
        assert_eq!(srids, "4269,3857,4326");
    }

    // postgis 3.6.4 crashes the backend in GetSysCacheOid for any geography whose srid is not
    // 4326 when it runs on pg19, so the test cannot run there
    #[cfg(not(feature = "pg19"))]
    #[pg_test]
    fn test_geo_geography_with_projected_srid() {
        if postgis_missing() {
            return;
        }
        create_postgis();

        // a geography constrained to a non lon/lat srid still claims spherical edges
        let copy_to_query = format!(
            "COPY (SELECT ST_SetSRID(ST_MakePoint(1, 2), 4269)::geography(point, 4269) as geog
                  )
             TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        );
        Spi::run(&copy_to_query).unwrap();

        assert_eq!(logical_type_of("geog"), Some("GEOGRAPHY".to_string()));
        let crs_by_column = geospatial_crs_by_column();
        assert_eq!(
            crs_by_column.get("geog"),
            Some(&Some("EPSG:4269".to_string()))
        );

        Spi::run(
            "DROP TABLE IF EXISTS projected_geog;
             CREATE TABLE projected_geog (geog geography);",
        )
        .unwrap();
        Spi::run(&format!(
            "COPY projected_geog FROM '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();

        // st_geogfromwkb would have made it lon/lat, so the srid comes back through a geometry
        let srid = Spi::get_one::<i32>("SELECT ST_SRID(geog) FROM projected_geog")
            .unwrap()
            .unwrap();
        assert_eq!(srid, 4269);
    }

    #[pg_test]
    fn test_geo_values_carry_a_different_srid_than_the_typmod() {
        if postgis_missing() {
            return;
        }
        create_postgis();

        // an unconstrained geometry column whose values are lon/lat is still written as srid:0,
        // and mixed srids in one column are not detectable at all
        let copy_to_query = format!(
            "COPY (SELECT ST_SetSRID(ST_MakePoint(1, 2), 4326) as a
                   UNION ALL
                   SELECT ST_SetSRID(ST_MakePoint(3, 4), 3857)
                  )
             TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        );
        Spi::run(&copy_to_query).unwrap();

        let crs_by_column = geospatial_crs_by_column();
        assert_eq!(crs_by_column.get("a"), Some(&Some("srid:0".to_string())));

        // the srid survives in the wkb payload, so the roundtrip is lossless anyway
        Spi::run(
            "DROP TABLE IF EXISTS mixed_srid;
             CREATE TABLE mixed_srid (a geometry);",
        )
        .unwrap();
        Spi::run(&format!(
            "COPY mixed_srid FROM '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();
        // ST_AsBinary writes plain wkb, so the per value srid is lost on the way out
        let srids = Spi::get_one::<String>(
            "SELECT array_agg(ST_SRID(a) order by ST_SRID(a))::text FROM mixed_srid",
        )
        .unwrap()
        .unwrap();
        assert_eq!(srids, "{0,0}");
    }

    #[pg_test]
    fn test_geo_spatial_ref_sys_without_auth_name() {
        if postgis_missing() {
            return;
        }
        create_postgis();

        Spi::run(
            "INSERT INTO spatial_ref_sys (srid, auth_name, auth_srid, srtext, proj4text)
             VALUES (990001, null, null, '', '');",
        )
        .unwrap();

        let copy_to_query = format!(
            "COPY (SELECT ST_SetSRID(ST_MakePoint(1, 2), 990001)::geometry(point, 990001) as a)
             TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        );
        Spi::run(&copy_to_query).unwrap();

        let crs_by_column = geospatial_crs_by_column();
        // an srid with no authority has no crs to report
        assert_eq!(crs_by_column.get("a"), Some(&Some("srid:0".to_string())));
    }

    #[pg_test]
    fn test_geo_duplicate_spatial_ref_sys_entry() {
        if postgis_missing() {
            return;
        }
        create_postgis();

        // spatial_ref_sys has a primary key on srid, so a second authority for one srid is not
        // possible, but auth_name can hold anything, including the separator itself
        Spi::run(
            "INSERT INTO spatial_ref_sys (srid, auth_name, auth_srid, srtext, proj4text)
             VALUES (990002, 'weird:name', 7, '', '');",
        )
        .unwrap();

        let copy_to_query = format!(
            "COPY (SELECT ST_SetSRID(ST_MakePoint(1, 2), 990002)::geometry(point, 990002) as a)
             TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        );
        Spi::run(&copy_to_query).unwrap();

        let crs_by_column = geospatial_crs_by_column();
        assert_eq!(
            crs_by_column.get("a"),
            Some(&Some("weird:name:7".to_string()))
        );
    }

    // ---------------------------------------------------------------- piece 5

    #[pg_test]
    fn test_geo_postgis_in_a_non_default_schema() {
        if postgis_missing() {
            return;
        }

        Spi::run(
            "DROP EXTENSION IF EXISTS postgis CASCADE;
             DROP SCHEMA IF EXISTS gis CASCADE;
             CREATE SCHEMA gis;
             CREATE EXTENSION postgis SCHEMA gis;",
        )
        .unwrap();

        // a plain copy must keep working even though postgis is not on the search path
        Spi::run("SET search_path TO pg_catalog, public;").unwrap();
        Spi::run(&format!(
            "COPY (SELECT 1 as a) TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();

        let copy_to_query = format!(
            "COPY (SELECT gis.ST_SetSRID(gis.ST_MakePoint(1, 2), 4326)::gis.geometry(point, 4326) as a)
             TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        );
        Spi::run(&copy_to_query).unwrap();

        assert_eq!(logical_type_of("a"), Some("GEOMETRY".to_string()));

        Spi::run("SET search_path TO \"$user\", public;").unwrap();
    }

    #[pg_test]
    fn test_geo_domain_over_geometry() {
        if postgis_missing() {
            return;
        }
        create_postgis();

        Spi::run(
            "DROP DOMAIN IF EXISTS geom_domain CASCADE;
             CREATE DOMAIN geom_domain AS geometry(point, 4326);",
        )
        .unwrap();

        let copy_to_query = format!(
            "COPY (SELECT ST_SetSRID(ST_MakePoint(1, 2), 4326)::geom_domain as a)
             TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        );
        Spi::run(&copy_to_query).unwrap();

        // a domain over geometry is not the geometry type itself, so it falls back to text
        // instead of being written as a geospatial column
        assert_eq!(logical_type_of("a"), Some("STRING".to_string()));
        assert_eq!(stats_geospatial_text("a"), None);
    }

    #[pg_test]
    fn test_geo_copy_inside_plpgsql_and_cursor() {
        if postgis_missing() {
            return;
        }
        create_postgis();

        Spi::run(&format!(
            "DO $$
             BEGIN
               EXECUTE 'COPY (SELECT ST_SetSRID(ST_MakePoint(1, 2), 4326)::geometry(point, 4326) as a)
                         TO ''{LOCAL_TEST_FILE_PATH}'' WITH (format parquet)';
             END $$;"
        ))
        .unwrap();
        assert_eq!(logical_type_of("a"), Some("GEOMETRY".to_string()));

        // a copy driven while another portal is open exercises the spi reentrancy of the crs
        // lookup
        Spi::run(&format!(
            "DO $$
             DECLARE
               c cursor for select srid from spatial_ref_sys order by srid limit 3;
               s int;
             BEGIN
               OPEN c;
               LOOP
                 FETCH c INTO s;
                 EXIT WHEN NOT FOUND;
                 EXECUTE 'COPY (SELECT ST_SetSRID(ST_MakePoint(1, 2), 4326)::geometry(point, 4326) as a)
                           TO ''{LOCAL_TEST_FILE_PATH}'' WITH (format parquet)';
               END LOOP;
               CLOSE c;
             END $$;"
        ))
        .unwrap();
        assert_eq!(logical_type_of("a"), Some("GEOMETRY".to_string()));
    }

    #[pg_test]
    fn test_geo_postgis_dropped_between_copies() {
        if postgis_missing() {
            return;
        }
        create_postgis();

        Spi::run(&format!(
            "COPY (SELECT ST_GeomFromText('POINT(1 2)') as a)
             TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();
        assert_eq!(logical_type_of("a"), Some("GEOMETRY".to_string()));

        // the cached postgis context must not survive the extension going away
        Spi::run("DROP EXTENSION postgis CASCADE;").unwrap();
        Spi::run(&format!(
            "COPY (SELECT 1 as a) TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();
        assert_eq!(logical_type_of("a"), None);

        create_postgis();
        Spi::run(&format!(
            "COPY (SELECT ST_GeomFromText('POINT(1 2)') as a)
             TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        ))
        .unwrap();
        assert_eq!(logical_type_of("a"), Some("GEOMETRY".to_string()));
    }
    #[pg_test]
    fn test_geo_unsupported_geometry_types_one_by_one() {
        if postgis_missing() {
            return;
        }
        create_postgis();

        // postgis types that the parquet geospatial specification does not cover. the bounder
        // misreads most of their wkb instead of rejecting it, so none of them may produce
        // statistics.
        let unboundable_wkts = [
            "TRIANGLE((0 0, 0 1, 1 1, 0 0))",
            "TIN(((0 0 0, 0 0 1, 0 1 0, 0 0 0)))",
            "POLYHEDRALSURFACE(((0 0 0, 0 0 1, 0 1 1, 0 1 0, 0 0 0)))",
            "CIRCULARSTRING(0 0, 1 1, 2 0)",
            "COMPOUNDCURVE(CIRCULARSTRING(0 0, 1 1, 2 0), (2 0, 3 0))",
            "CURVEPOLYGON(CIRCULARSTRING(0 0, 2 0, 2 2, 0 2, 0 0))",
            "MULTICURVE((0 0, 1 1), CIRCULARSTRING(2 0, 3 1, 4 0))",
            "MULTISURFACE(CURVEPOLYGON(CIRCULARSTRING(0 0, 2 0, 2 2, 0 2, 0 0)))",
            "GEOMETRYCOLLECTION(TRIANGLE((0 0, 0 1, 1 1, 0 0)))",
        ];

        for wkt in unboundable_wkts {
            let copy_to_query = format!(
                "COPY (SELECT ST_GeomFromText('POINT(1 2)') as a
                       UNION ALL SELECT ST_GeomFromText('{wkt}'))
                 TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
            );
            Spi::run(&copy_to_query).unwrap();

            assert_eq!(stats_geospatial_text("a"), None, "{wkt}");
        }
    }

    #[pg_test]
    fn test_geo_antimeridian_bounds() {
        if postgis_missing() {
            return;
        }
        create_postgis();

        let copy_to_query = format!(
            "COPY (SELECT ST_GeomFromText('POINT(-179 0)')::geometry(point, 4326) as a
                   UNION ALL SELECT ST_GeomFromText('POINT(179 0)')::geometry(point, 4326))
             TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        );
        Spi::run(&copy_to_query).unwrap();

        assert_eq!(
            bbox_text("a"),
            Some("{\"xmax\": 179.0, \"xmin\": -179.0, \"ymax\": 0.0, \"ymin\": 0.0}".to_string())
        );
    }
}
