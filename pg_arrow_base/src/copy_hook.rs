//! The `ProcessUtility` hook that serves `COPY ... TO/FROM <end> WITH (format '<x>')`.
//!
//! ## What is here
//!
//! Deciding whether a statement is ours ([`crate::copy_stmt::recognize`]), resolving and
//! on-demand loading the provider, validating its options and the base's own, the privilege
//! check against the end it names ([`crate::endpoint`], which is a uri, the client or a
//! program), and then carrying the statement out: `TO` through [`crate::copy_to`]
//! and [`crate::copy_to_dest_receiver`], `FROM` through [`crate::copy_from`] and
//! [`crate::batch_reader`].
//!
//! Both directions are served, and the two are symmetric in the one way that matters: the
//! executor does the Postgres-side work in each — producing the rows for a `TO`, consuming them
//! for a `FROM` — and this extension only ever handles finished tuples and bytes.
//!
//! A statement that is declined here is declined before anything happens, which is what lets a
//! cluster install the extension without changing what any existing `COPY` means.

use std::ffi::{c_char, CStr};
use std::str::FromStr;
use std::sync::Arc;

use format_registry::provider::FormatProvider;
use pg_arrow_convert::match_by::MatchBy;
use pgrx::pg_sys::{
    copyObjectImpl, standard_ProcessUtility, CommandTag, DestReceiver, InvalidOid,
    ParamListInfoData, PlannedStmt, ProcessUtility_hook, ProcessUtility_hook_type, QueryCompletion,
    QueryEnvironment,
};
use pgrx::prelude::*;

use crate::copy_plan::{CopyFromPlan, CopyToPlan};
use crate::copy_stmt::{recognize, ArrowCopyStmt};
use crate::endpoint::Endpoint;
use crate::ENABLE_COPY_HOOKS;
use crate::{
    copy_from, copy_progress, copy_stmt, copy_to, copy_to_dest_receiver, create_table, endpoint,
    parallel_copy_from, parallel_copy_to, providers, split,
};

static mut PREV_PROCESS_UTILITY_HOOK: ProcessUtility_hook_type = None;

/// Chains ourselves onto whatever hook is already there. Called from `_PG_init` only, where
/// Postgres guarantees we are alone, and only when the hook is enabled: an installed hook that
/// declines every statement still costs every utility statement in the cluster a call.
pub(crate) fn install() {
    #[allow(static_mut_refs)]
    unsafe {
        if ProcessUtility_hook.is_some() {
            PREV_PROCESS_UTILITY_HOOK = ProcessUtility_hook;
        }

        ProcessUtility_hook = Some(arrow_copy_hook);
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn call_next_process_utility(
    p_stmt: *mut PlannedStmt,
    query_string: *const c_char,
    read_only_tree: bool,
    context: u32,
    params: *mut ParamListInfoData,
    query_env: *mut QueryEnvironment,
    dest: *mut DestReceiver,
    completion_tag: *mut QueryCompletion,
) {
    unsafe {
        match PREV_PROCESS_UTILITY_HOOK {
            Some(prev_hook) => prev_hook(
                p_stmt,
                query_string,
                read_only_tree,
                context,
                params,
                query_env,
                dest,
                completion_tag,
            ),
            None => standard_ProcessUtility(
                p_stmt,
                query_string,
                read_only_tree,
                context,
                params,
                query_env,
                dest,
                completion_tag,
            ),
        }
    }
}

/// Everything that can be decided about a statement before any tuple moves.
///
/// Worth keeping separate from the execution it precedes: these are the failures a user hits
/// while writing the statement, they are the ones that have to name the right option and the
/// right format, and none of them should depend on whether the file exists.
fn validate(copy_stmt: &ArrowCopyStmt) -> (Arc<dyn FormatProvider>, MatchBy, Option<i64>) {
    let provider = providers::resolve(
        copy_stmt.format.as_deref(),
        copy_stmt.endpoint.path_for_inference(),
    );

    // before the format's own options, because a core option a format cannot honour is the
    // user's mistake about what the statement means rather than about what the format takes
    if let Some(name) = copy_stmt.unhonoured_core_options.first() {
        let direction = if copy_stmt.is_from { "from" } else { "to" };

        ereport!(
            PgLogLevel::ERROR,
            PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
            format!(
                "{name} is not a valid option for \"copy {direction} {}\".",
                provider.name()
            ),
            format!(
                "\"{name}\" describes a text or csv file, and the {} format is neither.",
                provider.name()
            )
        );
    }

    for (name, value) in copy_stmt.options.iter() {
        if let Err(message) = provider.validate_option(name, value) {
            ereport!(
                PgLogLevel::ERROR,
                PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
                message,
                format!(
                    "\"{name}\" was passed to the {} provider because COPY does not have an \
                     option by that name.",
                    provider.name()
                )
            );
        }
    }

    // a statement that is going to be refused should be refused for the first reason that
    // applies, and "you may not write there" is more useful than an option complaint about an
    // endpoint you cannot reach
    copy_stmt.endpoint.check_privilege(copy_stmt.is_from);

    (provider, match_by(copy_stmt), file_size_bytes(copy_stmt))
}

/// `match_by`, which is the base's own option: valid only for a `COPY FROM`, since there is
/// nothing to match a file's columns against when the file is the thing being written.
fn match_by(copy_stmt: &ArrowCopyStmt) -> MatchBy {
    let Some(match_by) = copy_stmt.match_by.as_deref() else {
        return MatchBy::default();
    };

    if !copy_stmt.is_from {
        ereport!(
            PgLogLevel::ERROR,
            PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
            "match_by is only meaningful for COPY FROM",
            "A COPY TO writes the columns it was given, in the order it was given them."
        );
    }

    MatchBy::from_str(match_by).unwrap_or_else(|message| {
        ereport!(
            PgLogLevel::ERROR,
            PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
            message,
            "Allowed values are: position, name."
        );

        unreachable!("ereport at ERROR does not return");
    })
}

/// `file_size_bytes`, the base's other option: a `COPY TO` that writes several files under the
/// uri instead of one file at it.
///
/// Refused for the ends that are not a uri, and that is not a limitation to be lifted later: a
/// `TO STDOUT` or a `TO PROGRAM` is one byte stream, and there is nowhere for a second file to
/// go.
fn file_size_bytes(copy_stmt: &ArrowCopyStmt) -> Option<i64> {
    let written = copy_stmt.file_size_bytes.as_deref()?;

    if copy_stmt.is_from {
        ereport!(
            PgLogLevel::ERROR,
            PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
            "file_size_bytes is only meaningful for COPY TO",
            "A COPY FROM reads the files its uri names, however many and however large they are."
        );
    }

    if !matches!(copy_stmt.endpoint, Endpoint::Uri(_)) {
        ereport!(
            PgLogLevel::ERROR,
            PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
            "file_size_bytes needs a uri to write the files under",
            "A COPY TO STDOUT or TO PROGRAM is a single byte stream, which has no place to put a \
             second file."
        );
    }

    let bytes = split::parse_file_size(written).unwrap_or_else(|message| {
        ereport!(
            PgLogLevel::ERROR,
            PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
            message,
            "A size is a byte count, or a number with a kB, MB or GB unit."
        );

        unreachable!("ereport at ERROR does not return");
    });

    Some(bytes)
}

/// Carries out a `COPY ... TO '<uri>'`, and returns the number of rows written.
///
/// The receiver is created before the portal and freed after it on every path, including the
/// error one: an upload that is still open when the statement fails has to be *discarded*
/// rather than completed, and the only thing that knows there is one is the receiver.
fn process_copy_to(
    p_stmt: &PgBox<PlannedStmt>,
    query_string: &CStr,
    params: &PgBox<ParamListInfoData>,
    query_env: &PgBox<QueryEnvironment>,
    plan: CopyToPlan,
) -> u64 {
    // a statement whose destination is a folder can be written by several processes at once,
    // each filling files of that folder. One that is not eligible is carried out below.
    if let Some(nprocessed) = parallel_copy_to::try_parallel_copy_to(p_stmt, &plan) {
        return nprocessed;
    }

    // read before the plan is moved into the receiver, which owns it from here on
    let endpoint = plan.endpoint.clone();

    let arrow_dest = copy_to_dest_receiver::create(plan);

    let dest = unsafe { PgBox::<DestReceiver>::from_pg(arrow_dest as _) };

    let relation_oid = if copy_stmt::has_relation(p_stmt) {
        copy_stmt::relation_oid(p_stmt)
    } else {
        // COPY (SELECT ...) TO has no relation to report against
        InvalidOid
    };

    PgTryBuilder::new(|| {
        copy_progress::start_copy_to(relation_oid, &endpoint);

        copy_to::execute_with_dest_receiver(p_stmt, query_string, params, query_env, &dest)
    })
    .catch_others(|cause| cause.rethrow())
    .finally(|| {
        copy_progress::end();

        unsafe { (*arrow_dest).cleanup() };
    })
    .execute()
}

/// Validates a recognized statement and carries it out, in whichever direction it goes.
///
/// Separate from the hook because a parallel `COPY TO` renders a statement per ctid range and
/// runs it through here: `ProcessUtility` would open a subtransaction, which a parallel
/// operation cannot do.
pub(crate) fn execute_recognized(
    p_stmt: &PgBox<PlannedStmt>,
    query_string: &CStr,
    params: &PgBox<ParamListInfoData>,
    query_env: &PgBox<QueryEnvironment>,
    stmt: ArrowCopyStmt,
) -> u64 {
    let (provider, match_by, file_size_bytes) = validate(&stmt);

    // a `STDOUT`/`STDIN`/`PROGRAM` statement passes its bytes through a temporary file, and the
    // file has to outlive the statement that writes or reads it: dropping it here is what
    // removes it, on the error path as well
    let (uri, _temp_file) = endpoint::resolve(&stmt.endpoint);

    if stmt.is_from {
        let plan = CopyFromPlan {
            uri,
            endpoint: stmt.endpoint,
            provider,
            options: stmt.options,
            match_by,
        };

        process_copy_from(p_stmt, query_string, query_env, plan)
    } else {
        let plan = CopyToPlan {
            uri,
            endpoint: stmt.endpoint,
            provider,
            options: stmt.options,
            file_size_bytes,
        };

        process_copy_to(p_stmt, query_string, params, query_env, plan)
    }
}

/// Carries out a `COPY ... FROM '<uri>'`, and returns the number of rows core inserted.
///
/// The stream is popped on every path for the same reason the dest receiver is freed on every
/// path: it holds open readers, and on the error path the statement is over whether or not the
/// file was finished.
pub(crate) fn process_copy_from(
    p_stmt: &PgBox<PlannedStmt>,
    query_string: &CStr,
    query_env: &PgBox<QueryEnvironment>,
    plan: CopyFromPlan,
) -> u64 {
    // the plan holds an `Arc<dyn FormatProvider>`, which is `Send + Sync` but not `UnwindSafe`:
    // a trait object may contain interior mutability. Nothing of it is observed after an error —
    // the whole plan is dropped either way — so the assertion is the honest one to make
    PgTryBuilder::new(std::panic::AssertUnwindSafe(move || {
        copy_from::execute_copy_from(p_stmt, query_string, query_env, plan)
    }))
    .catch_others(|cause| cause.rethrow())
    .finally(|| {
        copy_from::pop_stream();

        // and the workers, if the statement had any: on the error path one of them may still be
        // waiting on a queue nothing is reading any more
        parallel_copy_from::pop_source();
    })
    .execute()
}

#[pg_guard]
#[allow(clippy::too_many_arguments)]
extern "C-unwind" fn arrow_copy_hook(
    p_stmt: *mut PlannedStmt,
    query_string: *const c_char,
    read_only_tree: bool,
    context: u32,
    params: *mut ParamListInfoData,
    query_env: *mut QueryEnvironment,
    dest: *mut DestReceiver,
    completion_tag: *mut QueryCompletion,
) {
    let planned = unsafe { PgBox::from_pg(p_stmt) };

    // the GUC is read per statement, not just at install time, so that turning it off disables
    // the hook for the rest of the session rather than needing a reconnect
    let hooks_enabled = ENABLE_COPY_HOOKS.get();

    let recognized = hooks_enabled.then(|| recognize(&planned)).flatten();

    let recognized_create = hooks_enabled
        .then(|| create_table::recognize(&planned))
        .flatten();

    if let Some(stmt) = recognized_create {
        // a `CREATE TABLE ... FROM` is carried out by rewriting it, and `read_only_tree` says
        // the tree belongs to a plan cache that will run it again
        let mut planned = if read_only_tree {
            unsafe { PgBox::<PlannedStmt>::from_pg(copyObjectImpl(p_stmt as _) as _) }
        } else {
            planned
        };

        create_table::execute(
            &mut planned,
            unsafe { CStr::from_ptr(query_string) },
            context,
            params,
            query_env,
            dest,
            completion_tag,
            stmt,
        );

        return;
    }

    if let Some(stmt) = recognized {
        let query_string_cstr = unsafe { CStr::from_ptr(query_string) };
        let params = unsafe { PgBox::from_pg(params) };
        let query_env = unsafe { PgBox::from_pg(query_env) };

        let nprocessed =
            execute_recognized(&planned, query_string_cstr, &params, &query_env, stmt);

        // core COPY reports the rows it moved, and a client that parses the tag for a count
        // should not be able to tell which code moved them
        let mut completion_tag = unsafe { PgBox::from_pg(completion_tag) };

        if !completion_tag.is_null() {
            completion_tag.nprocessed = nprocessed;
            completion_tag.commandTag = CommandTag::CMDTAG_COPY;
        }

        return;
    }

    // `from_pg` borrows rather than owns, so the tree is still the caller's to pass on
    drop(planned);

    call_next_process_utility(
        p_stmt,
        query_string,
        read_only_tree,
        context,
        params,
        query_env,
        dest,
        completion_tag,
    );
}
