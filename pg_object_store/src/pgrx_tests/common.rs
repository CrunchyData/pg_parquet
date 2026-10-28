//! Scaffolding the test modules share.

use pgrx::prelude::*;

/// Runs a statement and gives back the message of the error it raised, or `None` if it
/// succeeded. A test that needs to see more than one failure cannot use
/// `#[pg_test(error = ...)]`, which ends at the first one.
pub fn error_of(statement: &str) -> Option<String> {
    Spi::run(
        "CREATE OR REPLACE FUNCTION pg_temp.error_of(statement text) RETURNS text
         LANGUAGE plpgsql AS $fn$
         BEGIN
             EXECUTE statement;
             RETURN NULL;
         EXCEPTION WHEN others THEN
             RETURN SQLERRM;
         END;
         $fn$",
    )
    .expect("failed to define the error-capturing helper");

    Spi::get_one_with_args::<String>("SELECT pg_temp.error_of($1)", &[statement.into()])
        .expect("the helper itself should never error")
}

/// Builds the store for a uri, by asking whether an object in it exists. The answer is thrown
/// away: what these tests are after is the store that resolving the uri leaves behind in the
/// cache, and a uri in a bucket that cannot be reached does that just as well as one that can.
pub fn resolve_store(uri: &str) {
    error_of(&format!("SELECT object_store.exists('{uri}')"));
}
