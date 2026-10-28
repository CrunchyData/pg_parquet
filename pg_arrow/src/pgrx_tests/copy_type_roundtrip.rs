//! Every Postgres type out to an Arrow IPC file and back.
//!
//! This is the suite behind the claim that `format 'arrow'` is a peer of `format 'parquet'`
//! rather than a subset of it. None of the mapping being exercised is this crate's: the
//! tuple-to-Arrow direction is `pg-arrow-convert` in tier 2, and what this format contributes
//! is only whether the resulting array survives being written and read as IPC. Which is
//! exactly why the coverage has to be here and not next door — the format crate never sees a
//! Postgres type.
//!
//! The comparison is on `a::text`, for the reason given in [`super::common`].

#[pgrx::pg_schema]
mod tests {
    use pgrx::{pg_test, Spi};

    use crate::pgrx_tests::common::{FileCleanup, Roundtrip};

    /// One type, one set of values, out and back.
    fn round_trip(typename: &str, values: &str) {
        Roundtrip::new(typename).insert(values).assert_round_trips();
    }

    /// A type and its array in one test, with the array carrying a NULL element, a NULL array
    /// and an empty array — the three cases an offsets buffer gets wrong separately.
    fn round_trip_with_array(typename: &str, values: &str, array_values: &str) {
        round_trip(typename, values);
        round_trip(&format!("{typename}[]"), array_values);
    }

    #[pg_test]
    fn test_int2() {
        round_trip_with_array(
            "int2",
            "(1), (2), (null)",
            "(array[1,2,null]::int2[]), (null), (array[1]::int2[]), (array[]::int2[])",
        );
    }

    #[pg_test]
    fn test_int4() {
        round_trip_with_array(
            "int4",
            "(1), (2), (null)",
            "(array[1,2,null]), (null), (array[1]), (array[]::int4[])",
        );
    }

    #[pg_test]
    fn test_int8() {
        round_trip_with_array(
            "int8",
            "(1), (2), (null), (9223372036854775807), (-9223372036854775808)",
            "(array[1,2,null]::int8[]), (null), (array[]::int8[])",
        );
    }

    #[pg_test]
    fn test_float4() {
        // nan and the two infinities have no Arrow of their own: they are the float32 bit
        // patterns, and what is being checked is that nothing in the path normalises them
        round_trip_with_array(
            "float4",
            "(1.0), (2.23213123), (null), ('nan'), ('infinity'), ('-infinity')",
            "(array[1.123,2.2,null,'nan','infinity','-infinity']::float4[]), (null), (array[]::float4[])",
        );
    }

    #[pg_test]
    fn test_float8() {
        round_trip_with_array(
            "float8",
            "(1.0), (2.23213123), (null), ('nan'), ('infinity'), ('-infinity')",
            "(array[1.123,2.2,null,'nan','infinity','-infinity']::float8[]), (null), (array[]::float8[])",
        );
    }

    #[pg_test]
    fn test_bool() {
        round_trip_with_array(
            "bool",
            "(false), (true), (null)",
            "(array[false,true,null]), (null), (array[]::bool[])",
        );
    }

    #[pg_test]
    fn test_text() {
        round_trip_with_array(
            "text",
            "('asd'), (''), (null), ('a lot of text that is longer than any inline buffer')",
            "(array['asd','',null]), (null), (array[]::text[])",
        );
    }

    #[pg_test]
    fn test_varchar() {
        round_trip_with_array(
            "varchar",
            "('asd'), (null), ('')",
            "(array['asd',null,'']::varchar[]), (null), (array[]::varchar[])",
        );
    }

    #[pg_test]
    fn test_varchar_with_typmod() {
        // the length is the type's business and not the file's: what comes back is checked
        // against the column, so a value that no longer fits would be an error here
        round_trip("varchar(10)", "('asd'), (null), ('0123456789')");
    }

    #[pg_test]
    fn test_bpchar() {
        // the blank padding is part of the value, which is why this is compared as text
        round_trip_with_array(
            "bpchar(10)",
            "('asd'), (null), ('')",
            "(array['asd',null,'']::bpchar(10)[]), (null)",
        );
    }

    #[pg_test]
    fn test_name() {
        round_trip_with_array(
            "name",
            "('asd'), (null), ('')",
            "(array['asd',null,'']::name[]), (null), (array[]::name[])",
        );
    }

    #[pg_test]
    fn test_char() {
        round_trip_with_array(
            "\"char\"",
            "('a'), (null)",
            "(array['a',null]::\"char\"[]), (null), (array[]::\"char\"[])",
        );
    }

    #[pg_test]
    fn test_bytea() {
        round_trip_with_array(
            "bytea",
            "('\\x0102'::bytea), (''::bytea), (null), ('hello'::bytea)",
            "(array['\\x0102'::bytea, null]), (null), (array[]::bytea[])",
        );
    }

    #[pg_test]
    fn test_uuid() {
        round_trip_with_array(
            "uuid",
            "('f0d2ed4d-8d95-4a4f-9dd4-cc5f1b3b0a08'), (null)",
            "(array['f0d2ed4d-8d95-4a4f-9dd4-cc5f1b3b0a08'::uuid, null]), (null), (array[]::uuid[])",
        );
    }

    #[pg_test]
    fn test_all_null_column() {
        // a column with nothing but NULLs still has to carry its type, which is the case where
        // a reader that infers from the data would get it wrong
        round_trip_with_array("uuid", "(null), (null)", "(null), (null)");
    }

    #[pg_test]
    fn test_oid() {
        round_trip_with_array(
            "oid",
            "(1), (2), (null), (4294967295)",
            "(array[1,2,null]::oid[]), (null), (array[]::oid[])",
        );
    }

    #[pg_test]
    fn test_date() {
        round_trip_with_array(
            "date",
            "('2022-05-01'), ('2022-05-02'), (null)",
            "(array['2022-05-01', null]::date[]), (null), (array[]::date[])",
        );
    }

    #[pg_test]
    fn test_date_limits_and_infinity() {
        round_trip(
            "date",
            "('4713-01-01 BC'), ('5874897-12-31 AD'), ('infinity'), ('-infinity')",
        );
    }

    #[pg_test]
    fn test_time() {
        round_trip_with_array(
            "time",
            "('13:00:00'), ('00:00:00'), ('23:59:59.999999'), (null)",
            "(array['13:00:00', null]::time[]), (null), (array[]::time[])",
        );
    }

    #[pg_test]
    fn test_timetz() {
        // the Arrow mapping of a timetz is the instant in UTC, so the offset a user typed is
        // not what comes back and the instant is. Compared after normalising both sides.
        Roundtrip::new("timetz")
            .compare_by("(a AT TIME ZONE 'UTC')::text")
            .insert("('13:00:00-05'), ('13:00:00+03'), ('13:00:00'), (null)")
            .assert_round_trips();
    }

    #[pg_test]
    fn test_timetz_array() {
        Roundtrip::new("timetz[]")
            .compare_by(
                "(SELECT array_agg(e AT TIME ZONE 'UTC' ORDER BY i) FROM unnest(a) WITH ORDINALITY t(e, i))::text",
            )
            .insert("(array['13:00:00-05'::timetz, null]), (null), (array[]::timetz[])")
            .assert_round_trips();
    }

    #[pg_test]
    fn test_timestamp() {
        round_trip_with_array(
            "timestamp",
            "('2022-05-01 13:00:00'), ('2022-05-02 00:00:00'), (null)",
            "(array['2022-05-01 13:00:00'::timestamp, null]), (null), (array[]::timestamp[])",
        );
    }

    #[pg_test]
    fn test_timestamp_limits_and_infinity() {
        // Arrow counts microseconds from the Unix epoch in an i64, which runs out around the
        // year 294246 — earlier than the latest timestamp Postgres itself holds
        round_trip(
            "timestamp",
            "('4713-01-01 00:00:00 BC'), ('294246-12-31 23:59:59.999999 AD'), ('infinity'), ('-infinity')",
        );
    }

    #[pg_test]
    #[should_panic(expected = "is out of range for an Arrow \"timestamp\" column")]
    fn test_a_timestamp_past_the_arrow_range_is_refused() {
        // the value is legal in the column it came from, so the refusal has to name the limit
        // that was hit rather than let the microsecond count wrap
        round_trip("timestamp", "('294276-12-31 23:59:59.999999 AD')");
    }

    #[pg_test]
    fn test_timestamptz() {
        round_trip_with_array(
            "timestamptz",
            "('2022-05-01 13:00:00-05'), ('2022-05-02 00:00:00+03'), (null)",
            "(array['2022-05-01 13:00:00-05'::timestamptz, null]), (null), (array[]::timestamptz[])",
        );
    }

    #[pg_test]
    fn test_timestamptz_is_an_instant_and_not_a_wall_clock() {
        // the session's timezone decides how a timestamptz is *printed*, not what it is, so the
        // instant has to come back the same out of a session that prints it differently
        let uri = crate::pgrx_tests::common::test_file("instant.arrow");
        let _cleanup = FileCleanup::new(&uri);

        Spi::run(&format!(
            "SET TIME ZONE 'UTC';
             CREATE TABLE written (a timestamptz);
             INSERT INTO written VALUES ('2022-05-01 13:00:00+00');
             COPY written TO '{uri}' WITH (format 'arrow');
             SET TIME ZONE 'America/New_York';
             CREATE TABLE came_back (a timestamptz);
             COPY came_back FROM '{uri}' WITH (format 'arrow');"
        ))
        .unwrap();

        let same =
            Spi::get_one::<bool>("SELECT (SELECT a FROM written) = (SELECT a FROM came_back)")
                .unwrap();

        assert_eq!(same, Some(true));
    }

    #[pg_test]
    fn test_interval() {
        // no Arrow interval matches Postgres's months-days-microseconds triple, so this one
        // crosses as text; the text is the whole of the value and is what is compared
        round_trip_with_array(
            "interval",
            "('2 years 3 minutes 3 seconds'), ('-1 day'), (null)",
            "(array['2 years 3 minutes 3 seconds'::interval, null]), (null), (array[]::interval[])",
        );
    }

    #[pg_test]
    fn test_json() {
        round_trip_with_array(
            "json",
            "('{\"id\": 12, \"name\": \"Doe\"}'), ('[1,2,3]'), ('null'), (null)",
            "(array['{\"id\": 12}'::json, null]), (null), (array[]::json[])",
        );
    }

    #[pg_test]
    fn test_json_keeps_its_text_as_written() {
        // a json is text, not a document: the key order and the whitespace are the value, and a
        // round trip that reformatted them would lose information a jsonb never had
        Roundtrip::new("json")
            .insert("('{\"b\": 1,   \"a\":  2}')")
            .assert_round_trips();
    }

    #[pg_test]
    fn test_jsonb() {
        round_trip_with_array(
            "jsonb",
            "('{\"id\": 12, \"name\": \"Doe\"}'), ('[1,2,3]'), ('null'), (null)",
            "(array['{\"id\": 12}'::jsonb, null]), (null), (array[]::jsonb[])",
        );
    }

    #[pg_test]
    fn test_small_numeric() {
        round_trip_with_array(
            "numeric(8,5)",
            "(123.12345), (-1.00000), (0), (null)",
            "(array[123.12345, null]::numeric(8,5)[]), (null), (array[]::numeric(8,5)[])",
        );
    }

    #[pg_test]
    fn test_numeric_with_the_largest_decimal_precision() {
        // 38 digits is the widest a decimal128 holds, which is the mapping this uses
        round_trip(
            "numeric(38,4)",
            "(12345678901234567890123456789012.3456), (-0.0001), (null)",
        );
    }

    #[pg_test]
    fn test_large_numeric_crosses_as_text() {
        // wider than a decimal128, so the mapping is text rather than an error
        round_trip(
            "numeric(100,4)",
            "(1234567890123456789012345678901234567890.1234), (null)",
        );
    }

    // a numeric with a typmod is decoded in place, which leaves a special value to the general
    // path, where it is refused the same way an unbounded one is
    #[pg_test]
    #[should_panic(expected = "Special numeric values like NaN, Inf, -Inf are not allowed")]
    fn test_bounded_numeric_nan() {
        round_trip("numeric(9,4)", "('NaN')");
    }

    #[pg_test]
    fn test_unbounded_numeric() {
        // a numeric with no typmod has no scale to carry, so it crosses at the base's default
        // scale of 9 and comes back padded to it. Both sides are compared at that scale.
        Roundtrip::new("numeric")
            .compare_by("a::numeric(30,9)")
            .insert("(123.24535), (0), (-1), (null)")
            .assert_round_trips();

        Roundtrip::new("numeric[]")
            .compare_by("a::numeric(30,9)[]")
            .insert("(array[123.24535, null]::numeric[]), (null), (array[]::numeric[])")
            .assert_round_trips();
    }

    #[pg_test]
    fn test_enum() {
        Spi::run("DROP TYPE IF EXISTS color CASCADE; CREATE TYPE color AS ENUM ('red', 'green');")
            .unwrap();

        round_trip_with_array(
            "color",
            "('red'), ('green'), (null)",
            "(array['red','green',null]::color[]), (null), (array[]::color[])",
        );
    }

    #[pg_test]
    fn test_bit_and_varbit() {
        round_trip_with_array(
            "bit(3)",
            "('101'), (null)",
            "(array['101'::bit(3), null]), (null)",
        );

        round_trip_with_array(
            "varbit",
            "('101'), (''), (null)",
            "(array['101'::varbit, null]), (null), (array[]::varbit[])",
        );
    }

    #[pg_test]
    fn test_composite() {
        Spi::run("DROP TYPE IF EXISTS pair CASCADE; CREATE TYPE pair AS (id int, label text);")
            .unwrap();

        round_trip_with_array(
            "pair",
            "(row(1,'one')::pair), (row(2,null)::pair), (null)",
            "(array[row(1,'one')::pair, null]), (null), (array[]::pair[])",
        );
    }

    #[pg_test]
    fn test_nested_composite() {
        Spi::run(
            "DROP TYPE IF EXISTS parent, child CASCADE;
             CREATE TYPE child AS (id int);
             CREATE TYPE parent AS (id int, child child, children child[]);",
        )
        .unwrap();

        round_trip_with_array(
            "parent",
            "(row(1, row(10)::child, array[row(10)::child, null])::parent),
             (row(2, null, null)::parent),
             (null)",
            "(array[row(1, row(10)::child, array[row(10)::child, null])::parent, null]), (null)",
        );
    }

    #[pg_test]
    fn test_domain_over_composite() {
        Spi::run(
            "DROP DOMAIN IF EXISTS positive_pair CASCADE;
             DROP TYPE IF EXISTS pair CASCADE;
             CREATE TYPE pair AS (id int, label text);
             CREATE DOMAIN positive_pair AS pair CHECK ((value).id > 0);",
        )
        .unwrap();

        round_trip("positive_pair", "(row(1,'one')::pair), (null)");
    }

    #[pg_test]
    #[should_panic(expected = "violates check constraint")]
    fn test_a_domain_constraint_is_checked_on_the_way_in() {
        // the file is not where a constraint lives, so a value that was legal in the table it
        // came from has to be rejected by the table it is going into
        Spi::run(
            "DROP DOMAIN IF EXISTS positive_int CASCADE;
             CREATE DOMAIN positive_int AS int CHECK (value > 0);",
        )
        .unwrap();

        let uri = crate::pgrx_tests::common::test_file("domain.arrow");
        let _cleanup = FileCleanup::new(&uri);

        Spi::run(&format!(
            "COPY (SELECT -1 AS a) TO '{uri}' WITH (format 'arrow');
             CREATE TABLE constrained (a positive_int);
             COPY constrained FROM '{uri}' WITH (format 'arrow');"
        ))
        .unwrap();
    }

    #[pg_test]
    fn test_a_compressed_file_round_trips_the_same_way() {
        // compression is a body-level property of an IPC file and the reader picks it up from
        // the record batch itself, so no option is needed on the way back in
        for codec in ["zstd", "lz4"] {
            Roundtrip::new("text")
                .with_to_options(&format!("format 'arrow', compression '{codec}'"))
                .insert("('asd'), (null), (repeat('x', 10000))")
                .assert_round_trips();
        }
    }

    #[pg_test]
    fn test_a_wide_row_of_every_type_at_once() {
        // one file with every mapping in it, which is the case where a schema that is right
        // field by field can still be wrong as a whole
        Spi::run(
            "DROP TYPE IF EXISTS parent, child CASCADE;
             CREATE TYPE child AS (id int);
             CREATE TYPE parent AS (id int, child child, children child[]);",
        )
        .unwrap();

        let uri = crate::pgrx_tests::common::test_file("wide.arrow");
        let _cleanup = FileCleanup::new(&uri);

        // three columns are spelled in the form they come back in, so that this test compares
        // every column as one text value: the unbounded numeric at the scale it crosses at, the
        // numeric array with the typmod on the array rather than on its elements, and the timetz
        // at the UTC offset its Arrow mapping is. Each of those is a test of its own above.
        let columns = "11::int2 AS a, array[11::int2, null] AS a_array,
                       232::int4 AS b, array[232, null] AS b_array,
                       2342::int8 AS c, array[2342::int8, null] AS c_array,
                       12.34::float4 AS d, array[12.34::float4, null] AS d_array,
                       123.325::float8 AS e, array[123.325::float8, null] AS e_array,
                       123.245350000::numeric AS f, array[123.245350000::numeric, null] AS f_array,
                       123.24535::numeric(8,5) AS g, array[123.24535, null]::numeric(8,5)[] AS g_array,
                       false AS h, array[false, null] AS h_array,
                       '2022-05-05'::date AS i, array['2022-05-05'::date, null] AS i_array,
                       '2022-05-05 13:00:00'::timestamp AS j, array['2022-05-05 13:00:00'::timestamp, null] AS j_array,
                       '2022-05-05 13:00:00-05'::timestamptz AS k, array['2022-05-05 13:00:00-05'::timestamptz, null] AS k_array,
                       '13:00:00'::time AS l, array['13:00:00'::time, null] AS l_array,
                       '13:00:00+00'::timetz AS m, array['13:00:00+00'::timetz, null] AS m_array,
                       '2 years 3 minutes'::interval AS n, array['2 years'::interval, null] AS n_array,
                       'a'::\"char\" AS o, array['a'::\"char\", null] AS o_array,
                       'hello'::text AS p, array['hello', null] AS p_array,
                       'hello'::bytea AS q, array['hello'::bytea, null] AS q_array,
                       'hello'::varchar AS r, array['hello'::varchar, null] AS r_array,
                       'hello'::bpchar AS s, array['hello'::bpchar, null] AS s_array,
                       'hello'::name AS t, array['hello'::name, null] AS t_array,
                       '{\"id\": 12}'::json AS u, array['{\"id\": 12}'::json, null] AS u_array,
                       '{\"id\": 12}'::jsonb AS v, array['{\"id\": 12}'::jsonb, null] AS v_array,
                       123::oid AS w, array[123::oid, null] AS w_array,
                       'f0d2ed4d-8d95-4a4f-9dd4-cc5f1b3b0a08'::uuid AS x, array['f0d2ed4d-8d95-4a4f-9dd4-cc5f1b3b0a08'::uuid, null] AS x_array,
                       row(1, row(10)::child, array[row(10)::child, null])::parent AS y,
                       array[row(1, row(10)::child, array[row(10)::child, null])::parent, null] AS y_array";

        Spi::run(&format!(
            "CREATE TABLE wide AS SELECT {columns};
             COPY wide TO '{uri}' WITH (format 'arrow');
             CREATE TABLE wide_back (LIKE wide);
             COPY wide_back FROM '{uri}' WITH (format 'arrow');"
        ))
        .unwrap();

        // the whole row as one value: a single mismatch anywhere fails this. The aliases are
        // spelled out because a column of this table is named `w`, and a column name wins over
        // a table alias in a whole-row reference.
        let same = Spi::get_one::<bool>(
            "SELECT (SELECT row_out FROM wide row_out)::text
                  = (SELECT row_back FROM wide_back row_back)::text",
        )
        .unwrap();

        // both rows, to say which column of the 28 moved
        let rows = Spi::get_one::<String>(
            "SELECT (SELECT row_out FROM wide row_out)::text || E'\n came back as \n' ||
                    (SELECT row_back FROM wide_back row_back)::text",
        )
        .unwrap()
        .unwrap_or_default();

        assert_eq!(same, Some(true), "{rows}");
    }
}
