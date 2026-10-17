//! That the parquet format reaches `pg_arrow_base`'s registry.
//!
//! The format itself is tested in `parquet-format` with no Postgres anywhere, and the base's
//! statement forms are tested in `pg_arrow_base` against a fake provider. What is left for this
//! file is the join: the descriptor this extension publishes, under the name the base dispatches
//! on, with the capabilities the base asks about before it picks a path.

#[pgrx::pg_schema]
mod tests {
    use pgrx::{pg_test, Spi};

    #[pg_test]
    fn test_parquet_is_registered_with_the_base() {
        // the extension's own SQL is created by the test harness; the base's is not, and
        // arrow.formats() is where the registry becomes visible
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_arrow_base CASCADE").unwrap();

        // one row, so a second registration of the same format under a different descriptor
        // would be visible here rather than only in whichever one the base happened to keep
        let extensions = Spi::get_one::<String>(
            "SELECT array_to_string(file_extensions, ',') FROM arrow.formats()
             WHERE format = 'parquet'",
        )
        .unwrap();

        assert_eq!(extensions.as_deref(), Some("parquet"));
    }
}
