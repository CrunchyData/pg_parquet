//! That 0.6.0's two GUC names are still real settings.
//!
//! The forwarding to `arrow.*` is not asserted here because it happens once, when the library
//! loads, and a `#[pg_test]` runs in a backend that already loaded it. What this file does
//! cover is the failure these names would otherwise have: a dotted name nothing defines is a
//! placeholder, so a `SET` of it succeeds and does nothing at all.

#[pgrx::pg_schema]
mod tests {
    use pgrx::{pg_test, Spi};

    #[pg_test]
    fn test_the_0_6_0_guc_names_are_still_defined() {
        // the library is loaded: this test function is in it
        let enabled =
            Spi::get_one::<String>("SELECT current_setting('pg_parquet.enable_copy_hooks')")
                .expect("pg_parquet.enable_copy_hooks is not defined");

        assert_eq!(enabled.as_deref(), Some("on"));

        let workers =
            Spi::get_one::<String>("SELECT current_setting('pg_parquet.max_parallel_workers')")
                .expect("pg_parquet.max_parallel_workers is not defined");

        assert_eq!(workers.as_deref(), Some("0"));
    }

    // what MarkGUCPrefixReserved buys: without it this SET succeeds as a placeholder and the
    // session goes on believing the hook is off
    #[pg_test]
    #[should_panic(
        expected = "invalid configuration parameter name \"pg_parquet.enabl_copy_hooks\""
    )]
    fn test_a_typo_under_the_prefix_is_rejected() {
        Spi::run("SET pg_parquet.enabl_copy_hooks = off").unwrap();
    }
}
