//! What a recognized `COPY` becomes once the hook has resolved it.
//!
//! [`crate::copy_stmt::ArrowCopyStmt`] is what the statement *said*; this is what it *means*:
//! the uri, the provider that will produce or read the file, and the options that provider
//! accepted. The separation matters because the dest receiver runs long after the hook's frame
//! is gone — it is a C struct Postgres calls back into — so the decisions have to be carried
//! forward as owned data rather than re-derived from a parse tree that may no longer be around.

use std::sync::Arc;

use format_registry::options::FormatOptions;
use format_registry::provider::FormatProvider;
use pg_arrow_convert::match_by::MatchBy;

use crate::endpoint::Endpoint;

/// A validated `COPY ... TO '<uri>'`.
///
/// `uri` is where the provider writes, which is the statement's uri for a `TO '<uri>'` and a
/// temporary file for a `TO STDOUT` or `TO PROGRAM`. `endpoint` is where those bytes then go,
/// and is what makes the difference a byte copy at one end rather than a second write path.
pub(crate) struct CopyToPlan {
    pub(crate) uri: String,
    pub(crate) endpoint: Endpoint,
    pub(crate) provider: Arc<dyn FormatProvider>,
    pub(crate) options: FormatOptions,
    /// `file_size_bytes`, parsed. `Some` makes `uri` a folder the files are written under rather
    /// than the file being written: see [`crate::split`].
    pub(crate) file_size_bytes: Option<i64>,
}

/// A validated `COPY ... FROM '<uri>'`.
///
/// `match_by` is the base's own option rather than a format's: whether a file's columns line up
/// with a table's by position or by name is a question about the two schemas, and the answer
/// would be the same for every format.
pub(crate) struct CopyFromPlan {
    pub(crate) uri: String,
    /// Where the bytes come from, by the same split as [`CopyToPlan`]: a `FROM STDIN` or a
    /// `FROM PROGRAM` fills the temporary file `uri` names before the provider opens it.
    pub(crate) endpoint: Endpoint,
    pub(crate) provider: Arc<dyn FormatProvider>,
    pub(crate) options: FormatOptions,
    pub(crate) match_by: MatchBy,
}
