#[pgrx::pg_schema]
mod tests {
    use pgrx::{datum::UnboxDatum, pg_sys, pg_test, IntoDatum, Spi};

    use crate::{pgrx_tests::common::LOCAL_TEST_FILE_PATH, pgrx_utils::AnyArrayDatum};

    #[pg_test]
    fn test_nested_copy_to_stmts() {
        let create_func_command = "
            CREATE OR REPLACE FUNCTION copy_to(url text)
            RETURNS text
            LANGUAGE plpgsql
            AS $function$
            DECLARE
            BEGIN
                EXECUTE format($$COPY (SELECT s FROM generate_series(1,3) s) TO %L WITH (format 'parquet')$$, url);
                RETURN 'success';
            END;
            $function$;
        ";
        Spi::run(create_func_command).unwrap();

        let create_table_command = "CREATE TABLE exports (id int, url text);";
        Spi::run(create_table_command).unwrap();

        let insert_query =
            "insert into exports values ( 1, '/tmp/pg_parquet_test1.parquet'), ( 2, '/tmp/pg_parquet_test2.parquet');";
        Spi::run(insert_query).unwrap();

        let nested_copy_command =
            "COPY (SELECT copy_to(url) as copy_to_result FROM exports) TO '/tmp/pg_parquet_test3.parquet';";
        Spi::run(nested_copy_command).unwrap();

        let create_table_command = "
            CREATE TABLE file1_result (s int);
            CREATE TABLE file3_result (copy_to_result text);
        ";
        Spi::run(create_table_command).unwrap();

        let copy_from_command = "
            COPY file1_result FROM '/tmp/pg_parquet_test1.parquet';
            COPY file3_result FROM '/tmp/pg_parquet_test3.parquet';
        ";
        Spi::run(copy_from_command).unwrap();

        let select_command = "SELECT * FROM file1_result ORDER BY s;";
        let result1 = Spi::connect(|client| {
            let mut results = Vec::new();
            let tup_table = client.select(select_command, None, &[]).unwrap();

            for row in tup_table {
                let s = row["s"].value::<i32>();
                results.push(s.unwrap().unwrap());
            }

            results
        });

        assert_eq!(vec![1, 2, 3], result1);

        let select_command = "SELECT * FROM file3_result;";
        let result3 = Spi::connect(|client| {
            let mut results = Vec::new();
            let tup_table = client.select(select_command, None, &[]).unwrap();

            for row in tup_table {
                let copy_to_result = row["copy_to_result"].value::<&str>();
                results.push(copy_to_result.unwrap().unwrap());
            }

            results
        });

        assert_eq!(vec!["success"; 2], result3);
    }

    #[pg_test]
    #[should_panic(expected = "violates not-null constraint")]
    fn test_copy_not_null_table() {
        let create_table = "CREATE TABLE test_table (x int NOT NULL)";
        Spi::run(create_table).unwrap();

        // first copy non-null value to file
        let copy_to = format!("COPY (SELECT 1 as x) TO '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_to).unwrap();

        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();

        let result = Spi::get_one::<i32>("SELECT x FROM test_table")
            .unwrap()
            .unwrap();
        assert_eq!(result, 1);

        // then copy null value to file
        let copy_to = format!("COPY (SELECT NULL::int as x) TO '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_to).unwrap();

        // this should panic
        let copy_from = format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_from).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "COPY FROM parquet with generated columns is not supported")]
    fn test_copy_from_by_position_with_generated_columns_not_supported() {
        Spi::run("DROP TABLE IF EXISTS test_table;").unwrap();

        Spi::run("CREATE TABLE test_table (a int, b int generated always as (10) stored, c text);")
            .unwrap();

        let copy_from_query =
            format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);");
        Spi::run(copy_from_query.as_str()).unwrap();
    }

    #[pg_test]
    fn test_with_generated_and_dropped_columns() {
        Spi::run("DROP TABLE IF EXISTS test_table;").unwrap();

        Spi::run("CREATE TABLE test_table (a int, b int generated always as (10) stored, c text);")
            .unwrap();

        Spi::run("ALTER TABLE test_table DROP COLUMN a;").unwrap();

        Spi::run("INSERT INTO test_table (c) VALUES ('test');").unwrap();

        let copy_to_query = format!(
            "COPY (SELECT * FROM test_table) TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);"
        );
        Spi::run(copy_to_query.as_str()).unwrap();

        let expected = vec![(Some(10), Some("test".to_string()))];

        Spi::run("TRUNCATE test_table;").unwrap();

        let copy_from_query = format!(
            "COPY test_table FROM '{LOCAL_TEST_FILE_PATH}' WITH (format parquet, match_by 'name');"
        );
        Spi::run(copy_from_query.as_str()).unwrap();

        let select_command = "SELECT b, c FROM test_table ORDER BY b, c;";
        let result = Spi::connect(|client| {
            let mut results = Vec::new();
            let tup_table = client.select(select_command, None, &[]).unwrap();

            for row in tup_table {
                let b = row["b"].value::<i32>();
                let c = row["c"].value::<String>();
                results.push((b.unwrap(), c.unwrap()));
            }

            results
        });

        for (expected, actual) in expected.into_iter().zip(result) {
            assert_eq!(expected.0, actual.0);
            assert_eq!(expected.1, actual.1);
        }
    }

    #[pg_test]
    fn test_by_position_with_dropped_column() {
        Spi::run("DROP TABLE IF EXISTS test_table;").unwrap();

        Spi::run("CREATE TABLE test_table (a int, b text, c int);").unwrap();

        Spi::run("ALTER TABLE test_table DROP COLUMN b;").unwrap();

        Spi::run("INSERT INTO test_table (a, c) VALUES (1, 3);").unwrap();

        let copy_to_query =
            format!("COPY test_table TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);");
        Spi::run(&copy_to_query).unwrap();

        Spi::run("TRUNCATE test_table;").unwrap();

        let copy_from_query =
            format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);");
        Spi::run(&copy_from_query).unwrap();

        let result = Spi::get_two::<i32, i32>("SELECT a, c FROM test_table").unwrap();
        assert_eq!(result, (Some(1), Some(3)));
    }

    #[pg_test]
    fn test_with_dropped_attribute_in_composite_type() {
        Spi::run("DROP TABLE IF EXISTS test_table;").unwrap();
        Spi::run("DROP TYPE IF EXISTS dropped_attr_type;").unwrap();

        Spi::run("CREATE TYPE dropped_attr_type AS (a int, b text, c int);").unwrap();

        Spi::run("ALTER TYPE dropped_attr_type DROP ATTRIBUTE b;").unwrap();

        Spi::run("CREATE TABLE test_table (x dropped_attr_type, y dropped_attr_type[]);").unwrap();

        Spi::run(
            "INSERT INTO test_table VALUES (row(1, 3)::dropped_attr_type, \
             array[row(4, 6)::dropped_attr_type]);",
        )
        .unwrap();

        let copy_to_query =
            format!("COPY test_table TO '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);");
        Spi::run(&copy_to_query).unwrap();

        Spi::run("TRUNCATE test_table;").unwrap();

        let copy_from_query =
            format!("COPY test_table FROM '{LOCAL_TEST_FILE_PATH}' WITH (format parquet);");
        Spi::run(&copy_from_query).unwrap();

        let result =
            Spi::get_two::<String, String>("SELECT x::text, y::text FROM test_table").unwrap();
        assert_eq!(
            result,
            (Some("(1,3)".to_string()), Some("{\"(4,6)\"}".to_string()))
        );
    }

    #[pg_test]
    fn test_with_column_names() {
        let create_table = "create table test_table(id int, name text);";
        Spi::run(create_table).unwrap();

        let insert_data = "insert into test_table values (1, 'ali'), (2, 'veli');";
        Spi::run(insert_data).unwrap();

        let copy_to_parquet = format!("copy test_table(id, name) to '{LOCAL_TEST_FILE_PATH}';");
        Spi::run(&copy_to_parquet).unwrap();

        let copy_from_parquet = format!("copy test_table(id, name) from '{LOCAL_TEST_FILE_PATH}';");
        Spi::run(&copy_from_parquet).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "column \"nonexistent\" does not exist")]
    fn test_copy_to_with_nonexistent_column_names() {
        let create_table = "create table test_table(id int, name text);";
        Spi::run(create_table).unwrap();

        let copy_to_parquet = format!("copy test_table(nonexistent) to '{LOCAL_TEST_FILE_PATH}';");
        Spi::run(&copy_to_parquet).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "column \"nonexistent\" of relation \"test_table\" does not exist")]
    fn test_copy_from_with_nonexistent_column_names() {
        let create_table = "create table test_table(id int, name text);";
        Spi::run(create_table).unwrap();

        let copy_from_parquet =
            format!("copy test_table(nonexistent) from '{LOCAL_TEST_FILE_PATH}';");
        Spi::run(&copy_from_parquet).unwrap();
    }

    #[pg_test]
    fn test_with_where_clause() {
        let create_table = "create table test_table(id int);";
        Spi::run(create_table).unwrap();

        let copy_to_parquet = format!(
            "copy (select i as id from generate_series(1,5) i) to '{LOCAL_TEST_FILE_PATH}';"
        );
        Spi::run(&copy_to_parquet).unwrap();

        let copy_from_parquet =
            format!("copy test_table from '{LOCAL_TEST_FILE_PATH}' where id > 2;");
        Spi::run(&copy_from_parquet).unwrap();

        let result = Spi::connect(|client| {
            let tup_table = client
                .select("select * from test_table;", None, &[])
                .unwrap();
            let mut results = Vec::new();

            for row in tup_table {
                let id = row["id"].value::<i32>().unwrap().unwrap();
                results.push(id);
            }

            results
        });
        assert_eq!(result, vec![3, 4, 5]);
    }

    #[pg_test]
    #[should_panic(expected = "duplicate attribute \"a\" is not allowed in parquet schema")]
    fn test_with_duplicate_column_name() {
        Spi::run(format!("copy (select 1 as a, 2 as a) to '{LOCAL_TEST_FILE_PATH}';").as_str())
            .unwrap();
    }

    #[pg_test]
    fn test_with_quoted_table_name() {
        let create_table = "create table \"test _ta23BLe\"(\"id _asdsadasd343d\" int);";
        Spi::run(create_table).unwrap();

        let copy_to_parquet = format!("copy \"test _ta23BLe\" to '{LOCAL_TEST_FILE_PATH}';");
        Spi::run(&copy_to_parquet).unwrap();

        let copy_from_parquet = format!("copy \"test _ta23BLe\" from '{LOCAL_TEST_FILE_PATH}';");
        Spi::run(&copy_from_parquet).unwrap();
    }

    #[pg_test]
    fn test_with_qualified_table_name() {
        let create_schema = "create schema test_schema;";
        Spi::run(create_schema).unwrap();

        let create_table = "create table test_schema.test_table (id int);";
        Spi::run(create_table).unwrap();

        let copy_to_parquet = format!("copy test_schema.test_table to '{LOCAL_TEST_FILE_PATH}';");
        Spi::run(&copy_to_parquet).unwrap();

        let copy_from_parquet =
            format!("copy test_schema.test_table from '{LOCAL_TEST_FILE_PATH}';");
        Spi::run(&copy_from_parquet).unwrap();

        // test the same with search_path
        let set_search_path = "set search_path to test_schema;";
        Spi::run(set_search_path).unwrap();

        let copy_to_parquet = format!("copy test_table to '{LOCAL_TEST_FILE_PATH}';");
        Spi::run(&copy_to_parquet).unwrap();

        let copy_from_parquet = format!("copy test_table from '{LOCAL_TEST_FILE_PATH}';");
        Spi::run(&copy_from_parquet).unwrap();
    }

    #[pg_test]
    fn test_copy_with_program_not_hooked() {
        let create_table = "create table test_table(id int);";
        Spi::run(create_table).unwrap();

        let insert_data = "insert into test_table select i from generate_series(1, 10) i;";
        Spi::run(insert_data).unwrap();

        let copy_to_parquet = "copy test_table to program 'cat' with (format csv);";
        Spi::run(copy_to_parquet).unwrap();
    }

    #[cfg(pre_pg19)]
    #[pg_test]
    #[should_panic(expected = "cannot copy from partitioned table \"partitioned_table\"")]
    fn test_copy_to_partitioned_table() {
        let create_table = "create table partitioned_table(id int) partition by range (id);";
        Spi::run(create_table).unwrap();

        let create_partition =
            "create table partitioned_table_1 partition of partitioned_table for values from (1) to (11);";
        Spi::run(create_partition).unwrap();

        let insert_data = "insert into partitioned_table select i from generate_series(1, 10) i;";
        Spi::run(insert_data).unwrap();

        // success via query form
        let copy_to_parquet =
            format!("copy (select * from partitioned_table) to '{LOCAL_TEST_FILE_PATH}';");
        Spi::run(&copy_to_parquet).unwrap();

        let total_rows = Spi::get_one::<i64>("select count(*) from partitioned_table;")
            .unwrap()
            .unwrap();
        assert_eq!(total_rows, 10);

        // should fail with partitioned table
        let copy_to_parquet = format!("copy partitioned_table to '{LOCAL_TEST_FILE_PATH}';");
        Spi::run(&copy_to_parquet).unwrap();
    }

    // PG19 allows COPY TO directly from a partitioned table
    #[cfg(not(pre_pg19))]
    #[pg_test]
    fn test_copy_to_partitioned_table() {
        let create_table = "create table partitioned_table(id int) partition by range (id);";
        Spi::run(create_table).unwrap();

        let create_partition =
            "create table partitioned_table_1 partition of partitioned_table for values from (1) to (6);";
        Spi::run(create_partition).unwrap();

        // a sub-partitioned partition, to make sure every level is visited
        let create_partition =
            "create table partitioned_table_2 partition of partitioned_table for values from (6) to (11) partition by range (id);";
        Spi::run(create_partition).unwrap();

        let create_partition =
            "create table partitioned_table_2_1 partition of partitioned_table_2 for values from (6) to (11);";
        Spi::run(create_partition).unwrap();

        let insert_data = "insert into partitioned_table select i from generate_series(1, 10) i;";
        Spi::run(insert_data).unwrap();

        let copy_to_parquet = format!("copy partitioned_table to '{LOCAL_TEST_FILE_PATH}';");
        Spi::run(&copy_to_parquet).unwrap();

        let create_table = "create table copied_table(id int);";
        Spi::run(create_table).unwrap();

        let copy_from_parquet = format!("copy copied_table from '{LOCAL_TEST_FILE_PATH}';");
        Spi::run(&copy_from_parquet).unwrap();

        let result = Spi::get_one::<i64>(
            "select count(*) from copied_table c
             join partitioned_table p using (id);",
        )
        .unwrap()
        .unwrap();
        assert_eq!(result, 10);

        let total_rows = Spi::get_one::<i64>("select count(*) from copied_table;")
            .unwrap()
            .unwrap();
        assert_eq!(total_rows, 10);
    }

    // PG19 rejects system columns in the COPY FROM WHERE clause
    #[cfg(not(pre_pg19))]
    #[pg_test]
    #[should_panic(expected = "system columns are not supported in COPY FROM WHERE conditions")]
    fn test_copy_from_where_clause_with_system_column() {
        let create_table = "create table test_table(id int);";
        Spi::run(create_table).unwrap();

        let copy_to_parquet = format!(
            "copy (select i as id from generate_series(1,5) i) to '{LOCAL_TEST_FILE_PATH}';"
        );
        Spi::run(&copy_to_parquet).unwrap();

        let copy_from_parquet =
            format!("copy test_table from '{LOCAL_TEST_FILE_PATH}' where ctid is not null;");
        Spi::run(&copy_from_parquet).unwrap();
    }

    // a whole-row reference is not a system column reference
    #[cfg(not(pre_pg19))]
    #[pg_test]
    fn test_copy_from_where_clause_with_whole_row_reference() {
        let create_table = "create table test_table(id int);";
        Spi::run(create_table).unwrap();

        let copy_to_parquet = format!(
            "copy (select i as id from generate_series(1,5) i) to '{LOCAL_TEST_FILE_PATH}';"
        );
        Spi::run(&copy_to_parquet).unwrap();

        let copy_from_parquet =
            format!("copy test_table from '{LOCAL_TEST_FILE_PATH}' where test_table is not null;");
        Spi::run(&copy_from_parquet).unwrap();

        let total_rows = Spi::get_one::<i64>("select count(*) from test_table;")
            .unwrap()
            .unwrap();
        assert_eq!(total_rows, 5);
    }

    #[pg_test]
    #[should_panic(expected = "cannot copy from view \"test_view\"")]
    fn test_copy_to_view() {
        let create_table = "create table test_table(id int);";
        Spi::run(create_table).unwrap();

        let create_view = "create view test_view as select * from test_table;";
        Spi::run(create_view).unwrap();

        let insert_data = "insert into test_table select i from generate_series(1, 10) i;";
        Spi::run(insert_data).unwrap();

        // success via query form
        let copy_to_parquet = format!("copy (select * from test_view) to '{LOCAL_TEST_FILE_PATH}'");
        Spi::run(&copy_to_parquet).unwrap();

        let total_rows = Spi::get_one::<i64>("select count(*) from test_view;")
            .unwrap()
            .unwrap();
        assert_eq!(total_rows, 10);

        // should fail with view
        let copy_to_parquet = format!("copy test_view to '{LOCAL_TEST_FILE_PATH}';");
        Spi::run(&copy_to_parquet).unwrap();
    }

    #[pg_test]
    fn test_copy_to_with_row_level_security() {
        let create_table = "create table test_table(username text);";
        Spi::run(create_table).unwrap();

        let insert_data = "insert into test_table values ('postgres'), ('dummy'), ('test_role');";
        Spi::run(insert_data).unwrap();

        let create_role = "create role test_role;";
        Spi::run(create_role).unwrap();

        let grant_server_file_write = "grant pg_write_server_files to test_role;";
        Spi::run(grant_server_file_write).unwrap();

        let grant_table = "grant all on test_table to test_role;";
        Spi::run(grant_table).unwrap();

        let create_username_policy =
            "create policy test_policy on test_table using (username = current_user);";
        Spi::run(create_username_policy).unwrap();

        let enable_rls = "alter table test_table enable row level security;";
        Spi::run(enable_rls).unwrap();

        let set_role = "set role test_role;";
        Spi::run(set_role).unwrap();

        let copy_to_parquet = format!("copy test_table to '{LOCAL_TEST_FILE_PATH}';");
        Spi::run(&copy_to_parquet).unwrap();

        let reset_role = "reset role;";
        Spi::run(reset_role).unwrap();

        let truncate_table = "truncate test_table;";
        Spi::run(truncate_table).unwrap();

        let copy_from_parquet = format!("copy test_table from '{LOCAL_TEST_FILE_PATH}';");
        Spi::run(&copy_from_parquet).unwrap();

        let total_rows = Spi::get_one::<i64>("select count(*) from test_table;")
            .unwrap()
            .unwrap();

        assert_eq!(total_rows, 1);
    }

    #[pg_test]
    #[should_panic(expected = "COPY FROM not supported with row-level security")]
    fn test_copy_from_with_row_level_security() {
        let create_table = "create table test_table(username text);";
        Spi::run(create_table).unwrap();

        let insert_data = "insert into test_table values ('postgres'), ('dummy'), ('test_role');";
        Spi::run(insert_data).unwrap();

        let copy_to_parquet = format!("copy test_table to '{LOCAL_TEST_FILE_PATH}';");
        Spi::run(&copy_to_parquet).unwrap();

        let create_role = "create role test_role;";
        Spi::run(create_role).unwrap();

        let grant_server_file_write = "grant pg_read_server_files to test_role;";
        Spi::run(grant_server_file_write).unwrap();

        let grant_table = "grant all on test_table to test_role;";
        Spi::run(grant_table).unwrap();

        let create_username_policy =
            "create policy test_policy on test_table using (username = current_user);";
        Spi::run(create_username_policy).unwrap();

        let enable_rls = "alter table test_table enable row level security;";
        Spi::run(enable_rls).unwrap();

        let set_role = "set role test_role;";
        Spi::run(set_role).unwrap();

        let copy_from_parquet = format!("copy test_table from '{LOCAL_TEST_FILE_PATH}';");
        Spi::run(&copy_from_parquet).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "permission denied to COPY to a file")]
    fn test_with_no_write_files_privilege() {
        let create_table = "create table test_table(id int);";
        Spi::run(create_table).unwrap();

        let create_role = "create role test_role;";
        Spi::run(create_role).unwrap();

        let grant_role = "grant ALL ON ALL TABLES IN SCHEMA public TO test_role;";
        Spi::run(grant_role).unwrap();

        let set_role = "set role test_role;";
        Spi::run(set_role).unwrap();

        let copy_to_parquet = format!("copy test_table to '{LOCAL_TEST_FILE_PATH}';");
        Spi::run(&copy_to_parquet).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "permission denied to COPY from a file")]
    fn test_with_no_read_files_privilege() {
        let create_table = "create table test_table(id int);";
        Spi::run(create_table).unwrap();

        let create_role = "create role test_role;";
        Spi::run(create_role).unwrap();

        let grant_role = "grant ALL ON ALL TABLES IN SCHEMA public TO test_role;";
        Spi::run(grant_role).unwrap();

        let set_role = "set role test_role;";
        Spi::run(set_role).unwrap();

        let copy_from_parquet = format!("copy test_table from '{LOCAL_TEST_FILE_PATH}';");
        Spi::run(&copy_from_parquet).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "permission denied for table test_table")]
    fn test_copy_to_with_no_table_privilege() {
        let create_table = "create table test_table(id int);";
        Spi::run(create_table).unwrap();

        let create_role = "create role test_role;";
        Spi::run(create_role).unwrap();

        let grant_role = "grant pg_write_server_files TO test_role;";
        Spi::run(grant_role).unwrap();

        let set_role = "set role test_role;";
        Spi::run(set_role).unwrap();

        let copy_to_parquet = format!("copy test_table to '{LOCAL_TEST_FILE_PATH}';");
        Spi::run(&copy_to_parquet).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "permission denied for table test_table")]
    fn test_copy_from_with_no_table_privilege() {
        let copy_to_parquet = format!("copy (select 1) to '{LOCAL_TEST_FILE_PATH}';");
        Spi::run(&copy_to_parquet).unwrap();

        let create_table = "create table test_table(id int);";
        Spi::run(create_table).unwrap();

        let create_role = "create role test_role;";
        Spi::run(create_role).unwrap();

        let grant_role = "grant pg_read_server_files TO test_role;";
        Spi::run(grant_role).unwrap();

        let set_role = "set role test_role;";
        Spi::run(set_role).unwrap();

        let copy_from_parquet = format!("copy test_table from '{LOCAL_TEST_FILE_PATH}';");
        Spi::run(&copy_from_parquet).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "column \"a\" has a 2-dimensional array value")]
    fn test_copy_to_multi_dimensional_array() {
        let create_table = "create table test_table(a int[]);";
        Spi::run(create_table).unwrap();

        let insert_data = "insert into test_table values ('{{1,2},{3,4}}');";
        Spi::run(insert_data).unwrap();

        let copy_to_parquet = format!("copy test_table to '{LOCAL_TEST_FILE_PATH}';");
        Spi::run(&copy_to_parquet).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "column \"a\" has a 3-dimensional array value")]
    fn test_copy_to_multi_dimensional_array_in_composite() {
        let create_type = "create type dim_comp as (a int[]);";
        Spi::run(create_type).unwrap();

        let create_table = "create table test_table(c dim_comp);";
        Spi::run(create_table).unwrap();

        let insert_data = "insert into test_table values (row('{{{1},{2}},{{3},{4}}}'::int[]));";
        Spi::run(insert_data).unwrap();

        let copy_to_parquet = format!("copy test_table to '{LOCAL_TEST_FILE_PATH}';");
        Spi::run(&copy_to_parquet).unwrap();
    }

    #[pg_test]
    fn test_copy_one_dimensional_and_empty_arrays() {
        let create_table = "create table test_table(a int[]);";
        Spi::run(create_table).unwrap();

        let insert_data = "insert into test_table values ('{1,2,3}'), ('{}'), (null);";
        Spi::run(insert_data).unwrap();

        let copy_to_parquet = format!("copy test_table to '{LOCAL_TEST_FILE_PATH}';");
        Spi::run(&copy_to_parquet).unwrap();

        let truncate = "truncate test_table;";
        Spi::run(truncate).unwrap();

        let copy_from_parquet = format!("copy test_table from '{LOCAL_TEST_FILE_PATH}';");
        Spi::run(&copy_from_parquet).unwrap();

        let result = Spi::get_one::<String>(
            "select string_agg(coalesce(a::text, 'null'), ',' order by a nulls last) from test_table;",
        )
        .unwrap();
        assert_eq!(result, Some("{},{1,2,3},null".into()));
    }

    #[pg_test]
    fn test_any_array_datum_is_the_datum_it_was_unboxed_from() {
        // the multi-dimensional array check reads an attribute through FromDatum, so UnboxDatum,
        // which get_by_index asks for, has no other caller to exercise it
        let array = unsafe { <AnyArrayDatum as UnboxDatum>::unbox(pgrx::datum::Datum::null()) };

        assert_eq!(array.into_datum(), Some(pg_sys::Datum::null()));
    }

    #[pg_test]
    #[should_panic(expected = "cannot copy from sequence \"test_sequence\"")]
    fn test_copy_to_sequence() {
        Spi::run("create sequence test_sequence;").unwrap();

        // success via query form
        let copy_to_parquet =
            format!("copy (select last_value from test_sequence) to '{LOCAL_TEST_FILE_PATH}';");
        Spi::run(&copy_to_parquet).unwrap();

        // should fail with the sequence itself
        let copy_to_parquet = format!("copy test_sequence to '{LOCAL_TEST_FILE_PATH}';");
        Spi::run(&copy_to_parquet).unwrap();
    }

    // PG 18 is the first version to read a populated matview rather than refuse the relation
    // kind outright, which pg_parquet follows
    #[cfg(not(pre_pg18))]
    #[pg_test]
    fn test_copy_to_materialized_view() {
        Spi::run("create table test_table(id int);").unwrap();
        Spi::run("insert into test_table select i from generate_series(1, 10) i;").unwrap();
        Spi::run("create materialized view test_matview as select * from test_table;").unwrap();

        let copy_to_parquet = format!("copy test_matview to '{LOCAL_TEST_FILE_PATH}';");
        Spi::run(&copy_to_parquet).unwrap();

        Spi::run("create table matview_result(id int);").unwrap();
        let copy_from_parquet = format!("copy matview_result from '{LOCAL_TEST_FILE_PATH}';");
        Spi::run(&copy_from_parquet).unwrap();

        let total_rows = Spi::get_one::<i64>("select count(*) from matview_result;")
            .unwrap()
            .unwrap();
        assert_eq!(total_rows, 10);
    }

    #[cfg(pre_pg18)]
    #[pg_test]
    #[should_panic(expected = "cannot copy from materialized view \"test_matview\"")]
    fn test_copy_to_materialized_view() {
        Spi::run("create table test_table(id int);").unwrap();
        Spi::run("insert into test_table select i from generate_series(1, 10) i;").unwrap();
        Spi::run("create materialized view test_matview as select * from test_table;").unwrap();

        let copy_to_parquet = format!("copy test_matview to '{LOCAL_TEST_FILE_PATH}';");
        Spi::run(&copy_to_parquet).unwrap();
    }

    // a matview created WITH NO DATA holds no rows to read, which PG 18 is the first to say
    // rather than to return an empty copy for
    #[cfg(not(pre_pg18))]
    #[pg_test]
    #[should_panic(expected = "cannot copy from unpopulated materialized view \"test_matview\"")]
    fn test_copy_to_unpopulated_materialized_view() {
        Spi::run("create table test_table(id int);").unwrap();
        Spi::run("create materialized view test_matview as select * from test_table with no data;")
            .unwrap();

        let copy_to_parquet = format!("copy test_matview to '{LOCAL_TEST_FILE_PATH}';");
        Spi::run(&copy_to_parquet).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "cannot copy from foreign table \"test_foreign_table\"")]
    fn test_copy_to_foreign_table() {
        Spi::run("create extension if not exists file_fdw;").unwrap();
        Spi::run("create server test_file_server foreign data wrapper file_fdw;").unwrap();

        std::fs::write("/tmp/pg_parquet_test_foreign_table.csv", "1\n2\n3\n")
            .expect("failed to write the csv the foreign table reads");

        Spi::run(
            "create foreign table test_foreign_table (id int)
             server test_file_server
             options (filename '/tmp/pg_parquet_test_foreign_table.csv', format 'csv');",
        )
        .unwrap();

        // success via query form
        let copy_to_parquet =
            format!("copy (select * from test_foreign_table) to '{LOCAL_TEST_FILE_PATH}';");
        Spi::run(&copy_to_parquet).unwrap();

        // should fail with the foreign table itself
        let copy_to_parquet = format!("copy test_foreign_table to '{LOCAL_TEST_FILE_PATH}';");
        Spi::run(&copy_to_parquet).unwrap();
    }
}
