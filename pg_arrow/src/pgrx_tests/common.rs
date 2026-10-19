//! What the COPY suites share: a scratch file, and a round trip that compares two tables.
//!
//! The comparison is on the *text* of each value rather than on a Rust datum, which is how
//! these suites avoid depending on `pg-arrow-convert`. That matters for more than the
//! dependency graph: a type whose Arrow mapping is text — an enum, a bit string, an interval,
//! an unbounded numeric — crosses through its own input and output functions, and the thing a
//! user cares about is that `a::text` survives. Comparing the text is comparing the claim.

use pgrx::Spi;
use std::path::Path;

/// A path under the temp directory that belongs to this backend. The pid keeps concurrent test
/// runs out of each other's files, which a shared `/tmp/pg_arrow_test.arrow` would not.
pub(crate) fn test_file(name: &str) -> String {
    std::env::temp_dir()
        .join(format!("pg_arrow_copy_{}_{name}", std::process::id()))
        .to_string_lossy()
        .into_owned()
}

/// Removes a path when it goes out of scope, and before it is used, so that a test which fails
/// partway through does not hand a stale file to the next one.
pub(crate) struct FileCleanup {
    path: String,
}

impl FileCleanup {
    pub(crate) fn new(path: &str) -> Self {
        remove(path);

        Self {
            path: path.to_string(),
        }
    }
}

impl Drop for FileCleanup {
    fn drop(&mut self) {
        remove(&self.path);
    }
}

fn remove(path: &str) {
    let path = Path::new(path);

    // a uri that expanded to a directory of files is cleaned up the same way
    std::fs::remove_dir_all(path).ok();
    std::fs::remove_file(path).ok();
}

/// One column of one type, out to a file and back into a second table.
///
/// `test_expected` holds what was written and `test_result` what came back, which is the shape
/// the parquet suites use; keeping it means a test can be read side by side with its parquet
/// counterpart.
pub(crate) struct Roundtrip {
    typename: String,
    uri: String,
    to_options: String,
    from_options: String,
    compare: String,
}

impl Roundtrip {
    pub(crate) fn new(typename: &str) -> Self {
        let roundtrip = Roundtrip {
            typename: typename.to_string(),
            uri: test_file("roundtrip.arrow"),
            to_options: "format 'arrow'".to_string(),
            from_options: "format 'arrow'".to_string(),
            // the value as the type's own output function writes it. NULL stays NULL, and a
            // NULL compares equal to a NULL here, which is what a round trip should preserve.
            compare: "a::text".to_string(),
        };

        Spi::run("DROP TABLE IF EXISTS test_expected, test_result;").unwrap();

        for table in ["test_expected", "test_result"] {
            Spi::run(&format!("CREATE TABLE {table} (a {typename});")).unwrap();
        }

        roundtrip
    }

    /// The expression the two tables are compared by, for a type whose text is allowed to
    /// change in a round trip. A `timetz` is the case that needs it: the Arrow mapping is an
    /// instant in UTC, so the offset a user typed is not what comes back, while the instant is.
    pub(crate) fn compare_by(mut self, compare: &str) -> Self {
        self.compare = compare.to_string();
        self
    }

    pub(crate) fn with_to_options(mut self, options: &str) -> Self {
        self.to_options = options.to_string();
        self
    }

    pub(crate) fn insert(self, values: &str) -> Self {
        Spi::run(&format!("INSERT INTO test_expected (a) VALUES {values};")).unwrap();
        self
    }

    /// Writes the file, reads it back, and fails with the values that differ.
    pub(crate) fn assert_round_trips(&self) {
        let _cleanup = FileCleanup::new(&self.uri);

        self.copy_to();
        self.copy_from();

        self.assert_tables_agree();
    }

    fn copy_to(&self) {
        Spi::run(&format!(
            "COPY (SELECT a FROM test_expected) TO '{}' WITH ({});",
            self.uri, self.to_options
        ))
        .unwrap();
    }

    fn copy_from(&self) {
        Spi::run(&format!(
            "COPY test_result FROM '{}' WITH ({});",
            self.uri, self.from_options
        ))
        .unwrap();
    }

    /// Multiset equality, in one query, in both directions.
    ///
    /// `EXCEPT ALL` both ways rather than an ordered comparison: a row that came back doubled
    /// and a row that went missing are different bugs and this tells them apart, while an
    /// `ORDER BY` would need a sort key the type may not have.
    fn assert_tables_agree(&self) {
        let difference = format!(
            "SELECT string_agg(side || ' ' || coalesce(v, '<null>'), ', ') FROM (
                 (SELECT 'only in what was written:' AS side, ({compare})::text AS v
                    FROM test_expected
                  EXCEPT ALL
                  SELECT 'only in what was written:', ({compare})::text FROM test_result)
                 UNION ALL
                 (SELECT 'only in what came back:', ({compare})::text FROM test_result
                  EXCEPT ALL
                  SELECT 'only in what came back:', ({compare})::text FROM test_expected)
             ) d",
            compare = self.compare
        );

        let difference = Spi::get_one::<String>(&difference)
            .expect("failed to compare the two tables")
            .unwrap_or_default();

        assert!(
            difference.is_empty(),
            "a {} did not survive the round trip: {difference}",
            self.typename
        );
    }
}

/// pgrx starts the test instance on an ephemeral port, so a test that shells out to psql has to
/// ask the backend which port it ended up on.
pub(crate) fn test_pg_port() -> String {
    Spi::get_one::<String>("SELECT current_setting('port')")
        .expect("failed to read the test instance port")
        .expect("port setting is unset")
}
