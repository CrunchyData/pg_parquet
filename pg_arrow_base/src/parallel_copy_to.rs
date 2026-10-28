//! `COPY ... TO '<uri>' WITH (file_size_bytes '...')`, written by several processes at once.
//!
//! The destination of such a statement is a folder, and a file of a folder is written whole —
//! header, rows, footer — by whoever writes it. That is what makes this the shape of a parallel
//! `COPY TO` that needs nothing of a format: the participants do not write parts of one file
//! that somebody has to splice afterwards, they write files of their own, and a uri a single
//! process can write is a uri a crowd can fill a folder of.
//!
//! What is shared is therefore small: the ctid ranges still to claim, the next file id, and the
//! row count. Each participant claims a range, runs the statement the leader rendered for it —
//! an ordinary `COPY (SELECT ... WHERE ctid >= ... AND ctid < ...) TO '<folder>'` — and the rows
//! of that range end up in files of the folder. A range that writes its own files also finishes
//! them, so a failure has nothing half-written to hand back: the writer of the file discards it
//! the way a single process statement's does.
//!
//! A statement into a single file is still single process. Making one of those parallel means
//! uploading parts of one object from several processes — tier 1 has the primitive for it,
//! `PgObjectStoreExtV1` — and then rebasing whatever offsets the format's footer holds, which is
//! the provider's business rather than the base's. This file deliberately stops short of that.

use std::ffi::{c_char, CStr, CString};
use std::panic::AssertUnwindSafe;
use std::rc::Rc;

use pgrx::pg_sys::{
    self, dsm_segment, pg_atomic_fetch_add_u32, pg_atomic_fetch_add_u64, pg_atomic_init_u32,
    pg_atomic_init_u64, pg_atomic_read_u64, pg_atomic_uint32, pg_atomic_uint64, pg_class_aclcheck,
    quote_identifier, quote_literal_cstr, quote_qualified_identifier, shm_toc, AclResult, AsPgCStr,
    CmdType, GetUserId, Node, NodeTag, PlannedStmt, RawStmt, ACL_SELECT, HEAP_TABLE_AM_OID,
    RELKIND_RELATION, RELPERSISTENCE_TEMP,
};
use pgrx::{pg_guard, GucSetting, PgBox, PgList, PgMemoryContexts, PgRelation, PgTryBuilder};

use crate::copy_plan::CopyToPlan;
use crate::endpoint::Endpoint;
use crate::pg_compat::strVal;
use crate::split::{self, FileIdClaim};
use crate::{copy_hook, copy_stmt};

/// How many workers one `COPY TO` may ask for. 0, the default, keeps every statement single
/// process, because a cluster that installs the extension should not have the shape of its
/// `COPY` change until it says so.
pub static MAX_PARALLEL_WORKERS: GucSetting<i32> = GucSetting::<i32>::new(0);

pub(crate) const MAX_PARALLEL_WORKERS_LIMIT: i32 = 64;

/// How many workers this statement may ask for.
///
/// The server hands parallel workers out of the `max_parallel_workers` pool, so a request above
/// it is never met in full; asking for what cannot be given only makes the statement wait.
pub(crate) fn requested_parallel_workers() -> i32 {
    // read as copies: a reference to a mutable static is not allowed
    let (pool, slots) = unsafe { (pg_sys::max_parallel_workers, pg_sys::max_worker_processes) };

    MAX_PARALLEL_WORKERS.get().min(pool).min(slots)
}

/// Ranges per participant, on average. More ranges than participants is what keeps everyone busy
/// when the pages are not evenly filled; each range costs a statement of its own and at least one
/// file, so there is no point in very many.
const RANGES_PER_PARTICIPANT: u32 = 4;

/// Below this many pages the fan-out costs more than it saves.
const MIN_PARALLEL_BLOCKS: u32 = 1024;

const PARALLEL_COPY_TO_SHARED_KEY: u64 = 0xA770_0001;

/// Resolved by name out of this library, so the symbol has to stay.
const PARALLEL_COPY_TO_WORKER_FUNCTION: &CStr = c"pg_arrow_base_parallel_copy_to_worker";

const PARALLEL_COPY_TO_LIBRARY: &CStr = c"pg_arrow_base";

// filled in by whichever participant claims the range
const FIRST_BLOCK_PLACEHOLDER: &str = "@FIRST_BLOCK@";
const LAST_BLOCK_PLACEHOLDER: &str = "@LAST_BLOCK@";

const PLACEHOLDERS: [&str; 2] = [FIRST_BLOCK_PLACEHOLDER, LAST_BLOCK_PLACEHOLDER];

/// The DSM payload. The statement every range runs is stored right after it in the same chunk,
/// so a participant needs no second key to find it.
#[repr(C)]
struct ParallelCopyToShared {
    /// The next ctid range a participant may claim.
    next_range: pg_atomic_uint32,
    nranges: u32,
    pages_per_range: u32,
    /// The next id a participant may name a file of the folder after.
    next_file_id: pg_atomic_uint32,
    /// How many rows the participants copied, which is the only thing that says how large the
    /// statement was: its files are finished by the participants themselves, so there is nothing
    /// left at the end for the leader to count.
    nprocessed: pg_atomic_uint64,
    statement_len: u32,
}

impl ParallelCopyToShared {
    fn size_with_statement(statement: &str) -> usize {
        std::mem::size_of::<Self>() + statement.len()
    }

    unsafe fn write_statement(shared: *mut Self, statement: &str) {
        (*shared).statement_len = statement.len() as u32;

        std::ptr::copy_nonoverlapping(
            statement.as_ptr(),
            shared.add(1) as *mut u8,
            statement.len(),
        );
    }

    unsafe fn statement(shared: *const Self) -> String {
        let bytes = std::slice::from_raw_parts(
            shared.add(1) as *const u8,
            (*shared).statement_len as usize,
        );

        std::str::from_utf8(bytes)
            .expect("parallel COPY TO shared state is not valid utf-8")
            .to_string()
    }
}

/// How a participant claims the id of the next file of the folder.
struct DsmFileIdClaim(*mut ParallelCopyToShared);

impl FileIdClaim for DsmFileIdClaim {
    fn claim_file_id(&self) -> i64 {
        unsafe { pg_atomic_fetch_add_u32(&mut (*self.0).next_file_id, 1) as i64 }
    }
}

/// Runs the statement with parallel workers, or returns `None` when it is not one this path can
/// handle — in which case the caller carries it out in this process, as it always did.
pub(crate) fn try_parallel_copy_to(p_stmt: &PgBox<PlannedStmt>, plan: &CopyToPlan) -> Option<u64> {
    let nworkers = requested_parallel_workers();

    if nworkers <= 0 {
        return None;
    }

    // a single file is written by one writer from its header to its footer, so there is nothing
    // to hand a second process. Only a folder destination splits into whole files.
    plan.file_size_bytes?;

    // and the folder has to be a uri: a STDOUT or a PROGRAM is one byte stream, which `validate`
    // has already refused `file_size_bytes` for
    if !matches!(plan.endpoint, Endpoint::Uri(_)) {
        return None;
    }

    let relation = parallel_copy_to_relation(p_stmt)?;

    // every tuple the snapshot can see lives below this block, which is the assumption the
    // heap's own parallel scan makes as well
    let nblocks = unsafe {
        pg_sys::RelationGetNumberOfBlocksInFork(relation.as_ptr(), pg_sys::ForkNumber::MAIN_FORKNUM)
    };

    if nblocks < MIN_PARALLEL_BLOCKS {
        return None;
    }

    let statement = render_range_copy_to_statement(p_stmt, &relation, plan)?;

    let nparticipants = nworkers as u32 + 1;
    let pages_per_range = (nblocks / (nparticipants * RANGES_PER_PARTICIPANT)).max(1);
    let nranges = nblocks.div_ceil(pages_per_range);

    Some(run_parallel_copy_to(
        nworkers,
        nranges,
        pages_per_range,
        &statement,
    ))
}

/// The relation the statement reads, but only when it is one this path can split.
///
/// The rows are claimed as ctid ranges of a single heap, so anything else — a query, a view, a
/// foreign table, a partitioned table — falls back to the single process path.
fn parallel_copy_to_relation(p_stmt: &PgBox<PlannedStmt>) -> Option<PgRelation> {
    // `COPY (SELECT ...) TO` has no relation to split. This is also what stops a range's own
    // statement from recursing back into this path.
    if !copy_stmt::has_relation(p_stmt) {
        return None;
    }

    // a parallel worker cannot start a parallel operation of its own
    if unsafe { pg_sys::IsInParallelMode() } {
        return None;
    }

    let relation = unsafe {
        PgRelation::with_lock(
            copy_stmt::relation_oid(p_stmt),
            copy_stmt::lock_mode(p_stmt),
        )
    };

    let relation_form = unsafe { *relation.rd_rel };

    // a ctid range is a heap concept, and a temporary relation is not visible to a worker
    if relation_form.relkind != RELKIND_RELATION as c_char
        || relation_form.relam != HEAP_TABLE_AM_OID
        || relation_form.relpersistence == RELPERSISTENCE_TEMP as c_char
    {
        return None;
    }

    // row level security rewrites the scan into a subquery holding the policy quals, which a
    // ctid qual can no longer be pushed into as a tid range scan
    if relation_form.relrowsecurity || relation_form.relforcerowsecurity {
        return None;
    }

    // the statement of a range reads ctid, which needs select on the relation itself, while the
    // command it comes from only needs it on the columns it names. A role that holds the column
    // privileges alone keeps the single process path, rather than being refused the copy it is
    // allowed to run
    let may_read_the_relation = unsafe {
        pg_class_aclcheck(relation.oid(), GetUserId(), ACL_SELECT as _) == AclResult::ACLCHECK_OK
    };

    if !may_read_the_relation {
        return None;
    }

    Some(relation)
}

/// Sets up the shared memory, launches the workers, claims ranges in the leader as well, and
/// returns the number of rows every participant copied.
fn run_parallel_copy_to(nworkers: i32, nranges: u32, pages_per_range: u32, statement: &str) -> u64 {
    let (pcxt, shared) = unsafe {
        pg_sys::EnterParallelMode();

        let pcxt = pg_sys::CreateParallelContext(
            PARALLEL_COPY_TO_LIBRARY.as_ptr(),
            PARALLEL_COPY_TO_WORKER_FUNCTION.as_ptr(),
            nworkers,
        );

        let shared_size = ParallelCopyToShared::size_with_statement(statement);

        // `shm_toc_estimate_chunk` and `shm_toc_estimate_keys` are macros, so they are spelled
        // out. The chunk is rounded up because `shm_toc_allocate` aligns what it hands out, and
        // estimating too much shared memory is harmless.
        (*pcxt).estimator.space_for_chunks += shared_size.next_multiple_of(64);
        (*pcxt).estimator.number_of_keys += 1;

        // serializes the leader's snapshot, GUCs, combo cids and transaction state into the
        // segment, which is what lets a worker see exactly the rows the leader sees
        pg_sys::InitializeParallelDSM(pcxt);

        let shared =
            pg_sys::shm_toc_allocate((*pcxt).toc, shared_size) as *mut ParallelCopyToShared;

        pg_atomic_init_u32(&mut (*shared).next_range, 0);
        pg_atomic_init_u32(&mut (*shared).next_file_id, 0);
        pg_atomic_init_u64(&mut (*shared).nprocessed, 0);
        (*shared).nranges = nranges;
        (*shared).pages_per_range = pages_per_range;
        ParallelCopyToShared::write_statement(shared, statement);

        pg_sys::shm_toc_insert((*pcxt).toc, PARALLEL_COPY_TO_SHARED_KEY, shared as _);

        pg_sys::LaunchParallelWorkers(pcxt);

        (pcxt, shared)
    };

    let nprocessed = PgTryBuilder::new(|| {
        // the leader claims ranges too, so a statement that could not launch a single worker
        // still runs, just without the fan-out
        claim_and_copy_ranges(shared);

        unsafe { pg_sys::WaitForParallelWorkersToFinish(pcxt) };

        // read while the segment is still mapped, and only once every participant has finished,
        // which is what makes the rows all of them copied visible to this read
        unsafe { pg_atomic_read_u64(&mut (*shared).nprocessed) }
    })
    // terminates the workers and waits for them to exit, which an interrupted statement has to
    // do before it reports anything: a worker still running would keep writing files of a
    // folder the statement has already failed on
    .finally(|| unsafe { pg_sys::DestroyParallelContext(pcxt) })
    .execute();

    unsafe { pg_sys::ExitParallelMode() };

    nprocessed
}

/// The entry point of a worker. Resolved by name out of this library, so the symbol has to stay.
#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn pg_arrow_base_parallel_copy_to_worker(
    _seg: *mut dsm_segment,
    toc: *mut shm_toc,
) {
    let shared = unsafe {
        pg_sys::shm_toc_lookup(toc, PARALLEL_COPY_TO_SHARED_KEY, false) as *mut ParallelCopyToShared
    };

    claim_and_copy_ranges(shared);
}

/// Claims ctid ranges one by one and copies the rows of each into files of the folder, until no
/// range is left.
fn claim_and_copy_ranges(shared: *mut ParallelCopyToShared) {
    // the participants fill one folder without knowing of each other, so the ids they name
    // their files after are claimed from shared memory. The claim is given up before this
    // returns, since it points into the shared memory of this statement alone.
    split::install_file_id_claim(Rc::new(DsmFileIdClaim(shared)));

    let copy_ranges = AssertUnwindSafe(|| copy_claimed_ranges(shared));

    PgTryBuilder::new(copy_ranges)
        .finally(split::uninstall_file_id_claim)
        .execute();
}

fn copy_claimed_ranges(shared: *mut ParallelCopyToShared) {
    let (nranges, pages_per_range) = unsafe { ((*shared).nranges, (*shared).pages_per_range) };

    let statement = unsafe { ParallelCopyToShared::statement(shared) };

    // every range's statement leaves allocations behind, so they are freed per range rather
    // than held until the whole copy is over
    let mut range_memory_context = PgMemoryContexts::new("pg_arrow_base parallel copy to range");

    loop {
        let range = unsafe { pg_atomic_fetch_add_u32(&mut (*shared).next_range, 1) };

        if range >= nranges {
            break;
        }

        // the ranges cover every block of the relation and the last one reaches past its end,
        // which costs nothing: a tuple written into a new block after the snapshot was taken is
        // not visible to it anyway
        let first_block = range * pages_per_range;
        let last_block = first_block + pages_per_range;

        let range_statement = statement
            .replace(FIRST_BLOCK_PLACEHOLDER, &first_block.to_string())
            .replace(LAST_BLOCK_PLACEHOLDER, &last_block.to_string());

        let nprocessed = unsafe {
            let nprocessed =
                range_memory_context.switch_to(|_| execute_copy_to_statement(&range_statement));

            range_memory_context.reset();

            nprocessed
        };

        unsafe { pg_atomic_fetch_add_u64(&mut (*shared).nprocessed, nprocessed as i64) };
    }
}

/// Runs one range's statement in this backend.
///
/// The hook's own path is called directly rather than going through `ProcessUtility`: a parallel
/// operation cannot open the subtransaction `SPI` would want.
fn execute_copy_to_statement(statement: &str) -> u64 {
    let statement =
        CString::new(statement).expect("a rendered COPY TO statement has an interior nul byte");

    unsafe {
        let raw_stmts = PgList::<RawStmt>::from_pg(pg_sys::pg_parse_query(statement.as_ptr()));

        let raw_stmt = raw_stmts
            .get_ptr(0)
            .expect("a rendered COPY TO statement parses into one statement");

        let mut p_stmt = PgBox::<PlannedStmt>::alloc_node(NodeTag::T_PlannedStmt);
        p_stmt.commandType = CmdType::CMD_UTILITY;
        p_stmt.canSetTag = true;
        p_stmt.utilityStmt = (*raw_stmt).stmt;
        p_stmt.stmt_location = (*raw_stmt).stmt_location;
        p_stmt.stmt_len = (*raw_stmt).stmt_len;

        let p_stmt = PgBox::<PlannedStmt>::from_pg(p_stmt.into_pg());

        let params = PgBox::<pg_sys::ParamListInfoData>::from_pg(std::ptr::null_mut());
        let query_env = PgBox::<pg_sys::QueryEnvironment>::from_pg(std::ptr::null_mut());

        let stmt = copy_stmt::recognize(&p_stmt)
            .expect("a rendered COPY TO statement is one this extension serves");

        copy_hook::execute_recognized(&p_stmt, &statement, &params, &query_env, stmt)
    }
}

/// Renders the statement a single ctid range is copied with, leaving the range bounds as
/// placeholders for whoever claims it.
///
/// The destination is the folder itself, as the statement wrote it, and so is `file_size_bytes`:
/// a range writes files of the folder exactly as a single process statement would, and the file
/// ids are what keep two ranges from naming the same one.
fn render_range_copy_to_statement(
    p_stmt: &PgBox<PlannedStmt>,
    relation: &PgRelation,
    plan: &CopyToPlan,
) -> Option<String> {
    let attributes = render_attribute_list(p_stmt);
    let options = render_copy_to_options(plan);

    let relation_name = unsafe {
        let relation_name = quote_qualified_identifier(
            relation.namespace().as_pg_cstr(),
            relation.name().as_pg_cstr(),
        );

        cstr_to_string(relation_name)
    };

    let folder = quote_literal(&plan.uri);

    // the bounds of a range are filled in by replacing the text of a placeholder in the
    // rendered statement, so a column, a relation, a schema or an option value whose name holds
    // that text would be replaced along with them and rewrite the statement into a command of
    // its own. Such a copy is left to the single process path
    let rendered = [
        attributes.as_str(),
        relation_name.as_str(),
        options.as_str(),
        folder.as_str(),
    ];

    if rendered.iter().any(|rendered| {
        PLACEHOLDERS
            .iter()
            .any(|placeholder| rendered.contains(placeholder))
    }) {
        return None;
    }

    // ONLY, because the single process path builds the `SELECT` without the inheritance flag
    // for a plain relation, and an inheritance child is not copied along there either
    Some(format!(
        "COPY (SELECT {attributes} FROM ONLY {relation_name} WHERE \
         ctid >= '({FIRST_BLOCK_PLACEHOLDER},0)'::tid AND \
         ctid < '({LAST_BLOCK_PLACEHOLDER},0)'::tid) \
         TO {folder} ({options})"
    ))
}

/// The column list the statement wrote, or `*` when it named no columns.
fn render_attribute_list(p_stmt: &PgBox<PlannedStmt>) -> String {
    let attribute_list = unsafe { PgList::<Node>::from_pg(copy_stmt::attribute_list(p_stmt)) };

    if attribute_list.is_empty() {
        return "*".into();
    }

    attribute_list
        .iter_ptr()
        .map(|attribute| {
            let attribute_name = strVal(attribute);

            unsafe { cstr_to_string(quote_identifier(attribute_name.as_pg_cstr())) }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// The options of the range's statement: the resolved format and `file_size_bytes`, plus the
/// format's own options as the statement wrote them.
///
/// Every value is rendered as a quoted literal, which is not a simplification: the base reads
/// an option with `defGetString` and hands a provider text either way, so a number written as a
/// literal is the same option. The format is rendered from the resolved provider rather than
/// from what the statement said, because a statement that named none had it inferred from the
/// uri — and while the folder a range writes into is that same uri, saying so is cheaper than
/// relying on it.
fn render_copy_to_options(plan: &CopyToPlan) -> String {
    let mut rendered = vec![format!("format {}", quote_literal(&plan.provider.name()))];

    if let Some(file_size_bytes) = plan.file_size_bytes {
        rendered.push(format!(
            "file_size_bytes {}",
            quote_literal(&file_size_bytes.to_string())
        ));
    }

    for (name, value) in plan.options.iter() {
        // a reserved option is the base's own and is never one the statement wrote
        if name.starts_with("__") {
            continue;
        }

        rendered.push(format!("{name} {}", quote_literal(value)));
    }

    rendered.join(", ")
}

fn quote_literal(value: &str) -> String {
    unsafe { cstr_to_string(quote_literal_cstr(value.as_pg_cstr())) }
}

unsafe fn cstr_to_string(cstr: *const c_char) -> String {
    CStr::from_ptr(cstr)
        .to_str()
        .expect("a quoted string is valid utf-8")
        .to_string()
}
