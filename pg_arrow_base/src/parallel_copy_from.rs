//! `COPY ... FROM '<uri>'`, decoded by several processes at once.
//!
//! What costs the time in a `COPY FROM` of a wide file is the decoding, not the inserting, and
//! the decoding is the half that does not have to happen in the process running the command: a
//! split of a file — a parquet row group, an Arrow IPC record batch — is decodable on its own.
//! So the splits are handed to parallel workers, each worker owning the splits whose index is
//! its own modulo the number of workers, and the rows come back to the leader over shared
//! memory queues already encoded as a binary `COPY` stream.
//!
//! The leader stays the only process that inserts, because Postgres does not let a worker write
//! into a relation, and it inserts the rows in the order the file holds them. It therefore drains
//! the worker whose split it is waiting for first: visiting the workers in worker order instead
//! lets a worker whose splits come later fill the buffer on its own, which leaves the leader
//! handing core the stream one queue message at a time and costs more the more workers there are.
//!
//! Nothing here is format-specific. A format takes part by answering two questions the provider
//! ABI already asks — how many splits a file has, through `ranged_file_info`, and what the rows
//! of one split are, through `open_ranged_reader` — and a format that answers neither is read in
//! one process, as are a `FROM STDIN` or a `FROM PROGRAM`, a store that serves no byte range, and
//! a relation whose writes the leader may not perform in parallel mode.

use std::collections::VecDeque;
use std::ffi::{c_char, c_void, CStr};
use std::sync::Arc;

use format_registry::options::FormatOptions;
use format_registry::provider::FormatProvider;
use pgrx::pg_sys::{
    self, dsm_segment, pg_atomic_init_u32, pg_atomic_read_u32, pg_atomic_uint32,
    pg_atomic_write_u32, shm_mq, shm_mq_handle, shm_mq_iovec, shm_mq_result, shm_toc, Oid,
    PlannedStmt, HEAP_TABLE_AM_OID, RELKIND_RELATION, RELPERSISTENCE_TEMP,
};
use pgrx::{check_for_interrupts, pg_guard, PgBox, PgMemoryContexts, PgRelation, PgTupleDesc};

use crate::batch_reader::{self, CopyFromStream};
use crate::copy_plan::CopyFromPlan;
use crate::endpoint::Endpoint;
use crate::io::Place;
use crate::parallel_copy_to::requested_parallel_workers;
use crate::{copy_progress, copy_stmt, pg_compat, providers};

/// Below this many splits there is not enough of the file to hand around.
const MIN_PARALLEL_SPLITS: i64 = 2;

/// How long a worker sleeps between two looks at the number of workers the leader launched,
/// which it only has to wait for at all when it starts before the leader got to write it.
const WAIT_FOR_NWORKERS_INTERVAL: i64 = 100;

/// The queue one worker hands its rows to the leader through. It only has to cover what a worker
/// encodes between two of the leader's visits, since the leader moves everything that arrived
/// into its own memory, so it is kept small enough that even the largest worker count fits into
/// a modest dynamic shared memory area.
const QUEUE_SIZE: usize = 256 * 1024;

/// How much of the stream the leader holds in its own memory, over all workers. The rows have to
/// reach core in file order, so a worker that owns a later split would otherwise sit on a full
/// queue until the leader got to it; this buffer is what buys that out.
const MAX_PENDING_BYTES: usize = 64 * 1024 * 1024;

/// The largest piece of the stream that travels as one queue message.
const MAX_CHUNK_SIZE: usize = 64 * 1024;

const PARALLEL_COPY_FROM_SHARED_KEY: u64 = 0xA770_0002;
const PARALLEL_COPY_FROM_QUEUES_KEY: u64 = 0xA770_0003;

/// Resolved by name out of this library, so the symbol has to stay.
const PARALLEL_COPY_FROM_WORKER_FUNCTION: &CStr = c"pg_arrow_base_parallel_copy_from_worker";

const PARALLEL_COPY_FROM_LIBRARY: &CStr = c"pg_arrow_base";

// a split's rows arrive as DATA messages and are closed by an END message, which is what tells
// the leader to move on to the next split's queue
const MESSAGE_TAG_DATA: u8 = 0;
const MESSAGE_TAG_END_OF_SPLIT: u8 = 1;

/// The payload of an END message: how many rows and how many bytes of row data the split turned
/// out to hold, which is all the leader needs to keep `pg_stat_progress_copy` honest.
#[repr(C)]
#[derive(Clone, Copy, Default)]
struct SplitSummary {
    rows: i64,
    bytes: i64,
}

/// The DSM payload. The split count of each file follows it in the same chunk, and then four
/// nul-separated string sections: the uris, the column names, the format name and the format's
/// options as alternating names and values.
#[repr(C)]
struct ParallelCopyFromShared {
    /// How many workers the command ended up with, which is 0 until they are launched.
    nworkers: pg_atomic_uint32,
    /// The splits of all the files together, which is what the workers divide between them.
    nsplits: u32,
    nfiles: u32,
    relation_oid: Oid,
    match_by_name: bool,
    uris_len: u32,
    attribute_names_len: u32,
    format_len: u32,
    options_len: u32,
}

/// The strings of the payload, in the order they are stored.
struct SharedStrings {
    uris: String,
    attribute_names: String,
    format: String,
    options: String,
}

impl SharedStrings {
    fn len(&self) -> usize {
        self.uris.len() + self.attribute_names.len() + self.format.len() + self.options.len()
    }
}

impl ParallelCopyFromShared {
    fn size_with_files(strings: &SharedStrings, nfiles: usize) -> usize {
        std::mem::size_of::<Self>() + nfiles * std::mem::size_of::<u32>() + strings.len()
    }

    unsafe fn write_files(shared: *mut Self, strings: &SharedStrings, splits_per_file: &[u32]) {
        (*shared).nfiles = splits_per_file.len() as u32;
        (*shared).uris_len = strings.uris.len() as u32;
        (*shared).attribute_names_len = strings.attribute_names.len() as u32;
        (*shared).format_len = strings.format.len() as u32;
        (*shared).options_len = strings.options.len() as u32;

        std::ptr::copy_nonoverlapping(
            splits_per_file.as_ptr(),
            Self::splits(shared),
            splits_per_file.len(),
        );

        let mut at = Self::strings(shared) as *mut u8;

        for section in [
            &strings.uris,
            &strings.attribute_names,
            &strings.format,
            &strings.options,
        ] {
            std::ptr::copy_nonoverlapping(section.as_ptr(), at, section.len());
            at = at.add(section.len());
        }
    }

    /// Where the split count of each file sits. Right after the header, whose own size keeps
    /// the counts aligned.
    unsafe fn splits(shared: *mut Self) -> *mut u32 {
        shared.add(1) as *mut u32
    }

    unsafe fn strings(shared: *const Self) -> *const u8 {
        (shared.add(1) as *const u32).add((*shared).nfiles as usize) as *const u8
    }

    unsafe fn splits_per_file(shared: *mut Self) -> Vec<u32> {
        std::slice::from_raw_parts(Self::splits(shared), (*shared).nfiles as usize).to_vec()
    }

    unsafe fn uris(shared: *const Self) -> Vec<String> {
        Self::items_at(Self::strings(shared), (*shared).uris_len)
    }

    unsafe fn attribute_names(shared: *const Self) -> Vec<String> {
        let at = Self::strings(shared).add((*shared).uris_len as usize);

        Self::items_at(at, (*shared).attribute_names_len)
    }

    unsafe fn format(shared: *const Self) -> String {
        let at = Self::strings(shared)
            .add((*shared).uris_len as usize + (*shared).attribute_names_len as usize);

        Self::string_at(at, (*shared).format_len)
    }

    unsafe fn options(shared: *const Self) -> FormatOptions {
        let at = Self::strings(shared).add(
            (*shared).uris_len as usize
                + (*shared).attribute_names_len as usize
                + (*shared).format_len as usize,
        );

        let items = Self::items_at(at, (*shared).options_len);

        let mut options = FormatOptions::new();

        for pair in items.chunks(2) {
            let [name, value] = pair else {
                panic!("parallel COPY FROM options are truncated");
            };

            options.push(name.clone(), value.clone());
        }

        options
    }

    unsafe fn items_at(bytes: *const u8, len: u32) -> Vec<String> {
        let section = Self::string_at(bytes, len);

        if section.is_empty() {
            return vec![];
        }

        section.split('\0').map(|item| item.into()).collect()
    }

    unsafe fn string_at(bytes: *const u8, len: u32) -> String {
        let bytes = std::slice::from_raw_parts(bytes, len as usize);

        std::str::from_utf8(bytes)
            .expect("parallel COPY FROM shared state is not valid utf-8")
            .to_string()
    }
}

/// What a worker sent and the leader has not handed to core yet.
enum PendingChunk {
    Rows(Vec<u8>),
    EndOfSplit(SplitSummary),
}

/// The leader's end of a parallel `COPY FROM`. It frames the binary stream core consumes and
/// fills it from the workers' queues, taking the splits in file order so that the rows arrive in
/// the order the single process path would read them.
pub(crate) struct ParallelCopyFromSource {
    pcxt: *mut pg_sys::ParallelContext,
    handles: Vec<*mut shm_mq_handle>,
    /// Per worker, what arrived from it and is not handed to core yet.
    pending: Vec<VecDeque<PendingChunk>>,
    pending_bytes: usize,
    /// How many splits each worker closed, and whether it detached.
    closed_splits: Vec<u32>,
    detached: Vec<bool>,
    nsplits: u32,
    total_rows: Option<i64>,
    current_split: u32,
    buffer: Vec<u8>,
    offset: usize,
    started: bool,
    finished: bool,
    rows_received: i64,
    row_bytes_received: i64,
    stream_bytes_received: i64,
}

impl ParallelCopyFromSource {
    /// Makes the next bytes of the stream available, and returns false once it is over.
    fn fill(&mut self) -> bool {
        if self.finished {
            return false;
        }

        if !self.started {
            self.started = true;

            // signature, flags and an empty header extension
            let mut header = b"\x50\x47\x43\x4f\x50\x59\x0a\xff\x0d\x0a\x00".to_vec();
            header.extend_from_slice(&0_i32.to_be_bytes());
            header.extend_from_slice(&0_i32.to_be_bytes());

            self.stream_bytes_received += header.len() as i64;
            self.hand_out(header);

            return true;
        }

        loop {
            // taking everything that already arrived, rather than only what the split core
            // waits for needs, is what keeps every worker busy
            self.drain_available();

            if self.current_split >= self.nsplits {
                self.finished = true;

                self.stream_bytes_received += 2;
                self.hand_out((-1_i16).to_be_bytes().to_vec());

                self.report_bytes_total();

                return true;
            }

            let worker = self.current_worker();

            let Some(chunk) = self.pending[worker].pop_front() else {
                // nothing of the split core waits for arrived yet
                self.receive(worker, false);
                continue;
            };

            match chunk {
                PendingChunk::Rows(rows) => {
                    self.pending_bytes -= rows.len();
                    self.stream_bytes_received += rows.len() as i64;
                    self.hand_out(rows);

                    return true;
                }
                PendingChunk::EndOfSplit(summary) => {
                    self.rows_received += summary.rows;
                    self.row_bytes_received += summary.bytes;
                    self.current_split += 1;

                    self.report_bytes_total();
                }
            }
        }
    }

    /// Makes the given bytes the next ones core reads. Only called with the buffer already
    /// drained, so they replace it rather than being copied into it.
    fn hand_out(&mut self, bytes: Vec<u8>) {
        debug_assert_eq!(self.not_yet_copied_bytes(), 0);

        self.buffer = bytes;
        self.offset = 0;
    }

    fn current_worker(&self) -> usize {
        self.current_split as usize % self.handles.len()
    }

    /// Moves everything the workers already sent into the leader's own memory, up to the
    /// buffer's limit, so that a worker only ever waits on the leader falling behind and not on
    /// the order the rows have to be handed out in.
    ///
    /// The workers are visited starting with the one whose split core is waiting for, so that
    /// the buffer goes to the rows that are needed first.
    fn drain_available(&mut self) {
        let nworkers = self.handles.len();
        let first = self.current_worker();

        for offset in 0..nworkers {
            let worker = (first + offset) % nworkers;

            while self.pending_bytes < MAX_PENDING_BYTES {
                if !self.receive(worker, true) {
                    break;
                }
            }
        }
    }

    /// Takes one message off a worker's queue, waiting for it unless `nowait`, and returns
    /// whether a message arrived.
    fn receive(&mut self, worker: usize, nowait: bool) -> bool {
        if self.detached[worker] {
            return false;
        }

        let mut nbytes = 0;
        let mut data = std::ptr::null_mut();

        let result =
            unsafe { pg_sys::shm_mq_receive(self.handles[worker], &mut nbytes, &mut data, nowait) };

        if result == shm_mq_result::SHM_MQ_WOULD_BLOCK {
            return false;
        }

        if result == shm_mq_result::SHM_MQ_DETACHED {
            self.detached[worker] = true;

            // a worker that failed has already put its error on its error queue, and this is
            // what re-raises it here, where it is the statement's error rather than a worker
            // having gone missing
            check_for_interrupts!();

            // a worker detaches once it closed every split it was given, so a detach with a
            // split still expected and no error behind it is one that is unaccounted for
            if self.closed_splits[worker]
                < splits_of_worker(self.nsplits, worker, self.handles.len())
            {
                pgrx::error!(
                    "a parallel COPY FROM worker exited before it read every split it was given"
                );
            }

            return false;
        }

        let message = unsafe { std::slice::from_raw_parts(data as *const u8, nbytes) };

        let (tag, payload) = message
            .split_first()
            .expect("parallel COPY FROM message is empty");

        match *tag {
            MESSAGE_TAG_END_OF_SPLIT => {
                self.closed_splits[worker] += 1;

                self.pending[worker]
                    .push_back(PendingChunk::EndOfSplit(read_split_summary(payload)));
            }
            _ => {
                self.pending_bytes += payload.len();

                self.pending[worker].push_back(PendingChunk::Rows(payload.to_vec()));
            }
        }

        true
    }

    /// Extrapolates the size of the whole binary stream from the splits that arrived so far,
    /// the way the single process path does. A format that reports no row count gets no total.
    fn report_bytes_total(&self) {
        if self.finished {
            copy_progress::update_copy_from_bytes_total(self.stream_bytes_received);
            return;
        }

        let Some(total_rows) = self.total_rows else {
            return;
        };

        if self.rows_received == 0 {
            return;
        }

        let rows_left = (total_rows - self.rows_received).max(0) as i128;

        let bytes_left = self.row_bytes_received as i128 * rows_left / self.rows_received as i128;

        let bytes_total = (self.stream_bytes_received as i128 + bytes_left).min(i64::MAX as i128);

        copy_progress::update_copy_from_bytes_total(bytes_total as i64);
    }

    fn not_yet_copied_bytes(&self) -> usize {
        self.buffer.len() - self.offset
    }

    fn reset_buffer(&mut self) {
        self.buffer.clear();
        self.offset = 0;
    }

    fn copy_to_outbuf(&mut self, len: usize, outbuf: pgrx::void_mut_ptr) {
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.buffer.as_ptr().add(self.offset),
                outbuf as *mut u8,
                len,
            );
        }

        self.offset += len;
    }

    /// Detaches the queues, waits for the workers and tears the parallel context down.
    ///
    /// Called on the error path as well, where a worker may still be running and blocked on a
    /// queue the leader stopped reading: detaching is what wakes it, and the detach of a queue
    /// is what a worker reads as the command being over.
    fn finish(&mut self) {
        unsafe {
            for handle in self.handles.drain(..) {
                pg_sys::shm_mq_detach(handle);
            }

            pg_sys::WaitForParallelWorkersToFinish(self.pcxt);
            pg_sys::DestroyParallelContext(self.pcxt);
            pg_sys::ExitParallelMode();
        }
    }
}

/// How many of the command's splits the given worker was given.
fn splits_of_worker(nsplits: u32, worker: usize, nworkers: usize) -> u32 {
    let worker = worker as u32;
    let nworkers = nworkers as u32;

    nsplits.saturating_sub(worker).div_ceil(nworkers)
}

fn read_split_summary(payload: &[u8]) -> SplitSummary {
    assert!(
        payload.len() == std::mem::size_of::<SplitSummary>(),
        "parallel COPY FROM split summary is truncated"
    );

    let mut summary = SplitSummary::default();

    unsafe {
        std::ptr::copy_nonoverlapping(
            payload.as_ptr(),
            &mut summary as *mut SplitSummary as *mut u8,
            payload.len(),
        )
    };

    summary
}

// One source per statement, and a stack for the same reason `copy_from` keeps one: a `COPY FROM`
// can run inside a trigger fired by another. The callback core invokes has no argument to carry
// state in, so the top of this is how it finds the source it belongs to.
static mut PARALLEL_COPY_FROM_SOURCE_STACK: Vec<ParallelCopyFromSource> = Vec::new();

fn peek_source() -> Option<&'static mut ParallelCopyFromSource> {
    #[allow(static_mut_refs)]
    unsafe {
        PARALLEL_COPY_FROM_SOURCE_STACK.last_mut()
    }
}

fn push_source(source: ParallelCopyFromSource) {
    #[allow(static_mut_refs)]
    unsafe {
        PARALLEL_COPY_FROM_SOURCE_STACK.push(source)
    };
}

/// Tears the statement's parallel source down. Called on the error path too, where the stack may
/// be empty — a failure before the launch never pushed one — so an empty stack is not itself
/// something to report.
pub(crate) fn pop_source() {
    #[allow(static_mut_refs)]
    let source = unsafe { PARALLEL_COPY_FROM_SOURCE_STACK.pop() };

    if let Some(mut source) = source {
        source.finish();
    }
}

/// Core's input callback for the leader of a parallel `COPY FROM`: it hands core the stream the
/// workers decoded.
#[pg_guard]
pub(crate) extern "C-unwind" fn copy_parallel_arrow_data_to_buffer(
    outbuf: pgrx::void_mut_ptr,
    _minread: i32,
    maxread: i32,
) -> i32 {
    let source =
        peek_source().unwrap_or_else(|| pgrx::error!("there is no parallel COPY FROM source"));

    let mut bytes_size = source.not_yet_copied_bytes();

    while bytes_size == 0 {
        source.reset_buffer();

        if !source.fill() {
            return 0;
        }

        bytes_size = source.not_yet_copied_bytes();
    }

    let bytes_size_to_copy = std::cmp::min(maxread as usize, bytes_size);
    source.copy_to_outbuf(bytes_size_to_copy, outbuf);

    bytes_size_to_copy as _
}

/// Sets a parallel `COPY FROM` up and pushes the leader's end of it, so that the caller reads
/// the rows the workers decode instead of decoding them itself.
///
/// `false` is a statement this path cannot serve, in which case the caller reads it in this
/// process as it always did.
pub(crate) fn try_parallel_copy_from(
    p_stmt: &PgBox<PlannedStmt>,
    plan: &CopyFromPlan,
    relation: &PgRelation,
) -> bool {
    let nworkers = requested_parallel_workers();

    if nworkers <= 0 || !is_eligible(plan, relation) {
        return false;
    }

    let Some(files) = parallel_copy_from_files(plan) else {
        return false;
    };

    let nsplits = files.iter().map(|file| file.nsplits).sum::<i64>();

    // a total only means something when every file told us its row count: a partial one would
    // make the progress view read as though the statement were further along than it is
    let total_rows = files.iter().map(|file| file.num_rows).sum::<Option<i64>>();

    if nsplits < MIN_PARALLEL_SPLITS {
        return false;
    }

    // a worker without a split of its own would only cost a queue
    let nworkers = nworkers.min(nsplits.min(i32::MAX as i64) as i32);

    let strings = SharedStrings {
        uris: files
            .iter()
            .map(|file| file.uri.as_str())
            .collect::<Vec<_>>()
            .join("\0"),
        attribute_names: copy_stmt::attribute_names(p_stmt).join("\0"),
        format: plan.provider.name(),
        options: plan
            .options
            .iter()
            .flat_map(|(name, value)| [name, value])
            .collect::<Vec<_>>()
            .join("\0"),
    };

    let splits_per_file = files
        .iter()
        .map(|file| file.nsplits as u32)
        .collect::<Vec<_>>();

    // the leader writes the rows itself and so needs an xid, which it can no longer be assigned
    // once it is in parallel mode
    unsafe { pg_sys::GetCurrentFullTransactionId() };

    let (pcxt, handles) = unsafe {
        pg_sys::EnterParallelMode();

        let pcxt = pg_sys::CreateParallelContext(
            PARALLEL_COPY_FROM_LIBRARY.as_ptr(),
            PARALLEL_COPY_FROM_WORKER_FUNCTION.as_ptr(),
            nworkers,
        );

        let shared_size = ParallelCopyFromShared::size_with_files(&strings, files.len());
        let queues_size = QUEUE_SIZE * nworkers as usize;

        // `shm_toc_estimate_chunk` and `shm_toc_estimate_keys` are macros, so they are spelled
        // out. The chunks are rounded up because `shm_toc_allocate` aligns what it hands out,
        // and estimating too much shared memory is harmless.
        (*pcxt).estimator.space_for_chunks +=
            shared_size.next_multiple_of(64) + queues_size.next_multiple_of(64);
        (*pcxt).estimator.number_of_keys += 2;

        // serializes the leader's snapshot, GUCs, combo cids and transaction state into the
        // segment, which is what lets a worker read exactly the file the leader would read
        pg_sys::InitializeParallelDSM(pcxt);

        let shared =
            pg_sys::shm_toc_allocate((*pcxt).toc, shared_size) as *mut ParallelCopyFromShared;

        // how many workers the command ends up with is only known once they are launched, which
        // is after a worker may already be reading this, so it starts out as the 0 the workers
        // wait on rather than as the number the command asked for
        pg_atomic_init_u32(&mut (*shared).nworkers, 0);

        (*shared).nsplits = nsplits as u32;
        (*shared).relation_oid = relation.oid();
        (*shared).match_by_name =
            matches!(plan.match_by, pg_arrow_convert::match_by::MatchBy::Name);
        ParallelCopyFromShared::write_files(shared, &strings, &splits_per_file);

        pg_sys::shm_toc_insert((*pcxt).toc, PARALLEL_COPY_FROM_SHARED_KEY, shared as _);

        let queues = pg_sys::shm_toc_allocate((*pcxt).toc, queues_size);

        for worker in 0..nworkers as usize {
            let queue = pg_sys::shm_mq_create(queue_at(queues, worker), QUEUE_SIZE);
            pg_sys::shm_mq_set_receiver(queue, pg_sys::MyProc);
        }

        pg_sys::shm_toc_insert((*pcxt).toc, PARALLEL_COPY_FROM_QUEUES_KEY, queues);

        pg_sys::LaunchParallelWorkers(pcxt);

        let nworkers_launched = (*pcxt).nworkers_launched;

        // the leader cannot take a split of its own, since it is the only process that may
        // write into the relation, so a command that was given no worker at all is left to read
        // the whole file by itself
        if nworkers_launched < 1 {
            pg_sys::DestroyParallelContext(pcxt);
            pg_sys::ExitParallelMode();

            return false;
        }

        // the splits are handed out by the number of workers the command ends up with rather
        // than by the number it asked for, which is what lets a command the pool is too busy to
        // fill run with the workers it was given. The workers wait for this.
        pg_atomic_write_u32(&mut (*shared).nworkers, nworkers_launched as u32);

        // postgres only launches the workers it registered without a failure, and a registration
        // that fails fails every one after it, so the workers of the command are the first
        // `nworkers_launched` of the ones it asked for
        let handles = (0..nworkers_launched as usize)
            .map(|worker| {
                pg_sys::shm_mq_attach(
                    queue_at(queues, worker) as *mut shm_mq,
                    (*pcxt).seg,
                    (*pcxt).worker.add(worker).read().bgwhandle,
                )
            })
            .collect::<Vec<_>>();

        (pcxt, handles)
    };

    let nworkers = handles.len();

    push_source(ParallelCopyFromSource {
        pcxt,
        handles,
        pending: (0..nworkers).map(|_| VecDeque::new()).collect(),
        pending_bytes: 0,
        closed_splits: vec![0; nworkers],
        detached: vec![false; nworkers],
        nsplits: nsplits as u32,
        total_rows,
        current_split: 0,
        buffer: Vec::new(),
        offset: 0,
        started: false,
        finished: false,
        rows_received: 0,
        row_bytes_received: 0,
        stream_bytes_received: 0,
    });

    true
}

/// One of the files the statement reads: where it is, and how much of it there is to read.
struct ParallelCopyFromFile {
    uri: String,
    nsplits: i64,
    num_rows: Option<i64>,
}

/// The files the statement reads, in the order the single process path reads them, which is the
/// order the rows have to reach core in.
///
/// `None` is a uri this path cannot divide, which leaves both the reading of it and the error it
/// deserves to the single process path.
fn parallel_copy_from_files(plan: &CopyFromPlan) -> Option<Vec<ParallelCopyFromFile>> {
    if let Some(file) = parallel_copy_from_file(plan, &plan.uri) {
        return Some(vec![file]);
    }

    // the single process path only reads a uri as a pattern once reading it as a file of its own
    // failed, which is the order kept here
    if !plan.uri.contains('*') {
        return None;
    }

    let files = batch_reader::matching_uris(&plan.uri)?
        .into_iter()
        .map(|file_uri| parallel_copy_from_file(plan, &file_uri))
        .collect::<Option<Vec<_>>>()?;

    // a pattern that matches nothing is an error of the single process path to report
    (!files.is_empty()).then_some(files)
}

/// Reads the footer of a single file, which is what says how much of it there is for the workers
/// to divide. `None` is a uri that is not a file the format can divide.
fn parallel_copy_from_file(plan: &CopyFromPlan, uri: &str) -> Option<ParallelCopyFromFile> {
    let (nsplits, num_rows) = batch_reader::file_splits(&plan.provider, &Place::object(uri))?;

    Some(ParallelCopyFromFile {
        uri: uri.to_string(),
        nsplits,
        num_rows,
    })
}

/// Whether the rows of the file may be decoded by workers while the leader writes them.
///
/// Only the decoding is handed around, so anything that would make the leader's own writes
/// illegal in parallel mode, or that would make a worker read a different file than the leader,
/// falls back to the single process path.
fn is_eligible(plan: &CopyFromPlan, relation: &PgRelation) -> bool {
    // a worker reads the file itself, so there has to be a file rather than a byte stream. A
    // `STDIN` or a `PROGRAM` is staged into a temporary file of this backend, which is a file
    // only this backend knows the name of.
    if !matches!(plan.endpoint, Endpoint::Uri(_)) {
        return false;
    }

    // a parallel worker cannot start a parallel operation of its own
    if unsafe { pg_sys::IsInParallelMode() } {
        return false;
    }

    let relation_form = unsafe { *relation.rd_rel };

    // a temporary relation is not visible to a worker, and a relation that is not a plain heap
    // routes its writes through code the leader may not reach in parallel mode
    if relation_form.relkind != RELKIND_RELATION as c_char
        || relation_form.relam != HEAP_TABLE_AM_OID
        || relation_form.relpersistence == RELPERSISTENCE_TEMP as c_char
    {
        return false;
    }

    // a trigger, which is also how a foreign key is enforced, runs arbitrary statements, and a
    // default expression may be volatile, and neither may do so in parallel mode
    if relation_form.relhastriggers {
        return false;
    }

    relation.tuple_desc().iter().all(|attribute| {
        !attribute.atthasdef && attribute.attidentity == 0 && attribute.attgenerated == 0
    })
}

/// The address of one worker's queue inside the chunk that holds all of them.
fn queue_at(queues: *mut c_void, worker: usize) -> *mut c_void {
    unsafe { (queues as *mut u8).add(worker * QUEUE_SIZE) as _ }
}

/// The entry point of a worker. Resolved by name out of this library, so the symbol has to stay.
#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn pg_arrow_base_parallel_copy_from_worker(
    seg: *mut dsm_segment,
    toc: *mut shm_toc,
) {
    let shared = unsafe {
        pg_sys::shm_toc_lookup(toc, PARALLEL_COPY_FROM_SHARED_KEY, false)
            as *mut ParallelCopyFromShared
    };

    let queues = unsafe { pg_sys::shm_toc_lookup(toc, PARALLEL_COPY_FROM_QUEUES_KEY, false) };

    let worker = unsafe { pg_sys::ParallelWorkerNumber } as usize;

    let handle = unsafe {
        let queue = queue_at(queues, worker) as *mut shm_mq;
        pg_sys::shm_mq_set_sender(queue, pg_sys::MyProc);

        pg_sys::shm_mq_attach(queue, seg, std::ptr::null_mut())
    };

    decode_splits(shared, worker, handle);

    unsafe { pg_sys::shm_mq_detach(handle) };
}

/// How many workers the command ended up with, which says which splits of it are this worker's
/// own. The leader cannot write it before it launches the workers, so a worker that reads it
/// first waits for it — which the leader only takes the launch itself to answer. An interrupt,
/// the leader's own death among them, ends the wait as well.
fn wait_for_nworkers(shared: *mut ParallelCopyFromShared) -> usize {
    loop {
        let nworkers = unsafe { pg_atomic_read_u32(&mut (*shared).nworkers) };

        if nworkers > 0 {
            return nworkers as usize;
        }

        check_for_interrupts!();

        unsafe { pg_sys::pg_usleep(WAIT_FOR_NWORKERS_INTERVAL) };
    }
}

/// Encodes every split this worker owns into the binary `COPY` representation and hands it to
/// the leader, one split at a time.
fn decode_splits(shared: *mut ParallelCopyFromShared, worker: usize, handle: *mut shm_mq_handle) {
    let nworkers = wait_for_nworkers(shared);

    let (nsplits, relation_oid, match_by) = unsafe {
        (
            (*shared).nsplits,
            (*shared).relation_oid,
            if (*shared).match_by_name {
                pg_arrow_convert::match_by::MatchBy::Name
            } else {
                pg_arrow_convert::match_by::MatchBy::Position
            },
        )
    };

    let uris = unsafe { ParallelCopyFromShared::uris(shared) };
    let splits_per_file = unsafe { ParallelCopyFromShared::splits_per_file(shared) };
    let attribute_names = unsafe { ParallelCopyFromShared::attribute_names(shared) };
    let format = unsafe { ParallelCopyFromShared::format(shared) };
    let options = unsafe { ParallelCopyFromShared::options(shared) };

    // by name, which is how the leader resolved it: the provider lives in another library and
    // the pointer the leader holds is that library's, loaded into this backend by this call
    let provider = providers::resolve(Some(&format), "");

    // the leader already holds a lock that conflicts with anything that could change the
    // relation, and a worker shares its leader's lock group, so this cannot block
    let relation = unsafe { PgRelation::with_lock(relation_oid, pg_sys::AccessShareLock as _) };

    let tupledesc =
        copy_stmt::create_filtered_tupledesc_for_attribute_names(&attribute_names, &relation);

    // every split's reader leaves its own allocations behind, so they are freed per split rather
    // than held until the command ends
    let mut split_memory_context = PgMemoryContexts::new("pg_arrow_base parallel copy from split");

    // the splits of all the files make up one list that the workers stride over, so this worker
    // owns the splits at its own offset of it
    let mut owned_split = worker as u32;

    while owned_split < nsplits {
        let (file, split) = file_of_split(&splits_per_file, owned_split);

        let keep_going = unsafe {
            let keep_going = split_memory_context.switch_to(|_| {
                decode_split(
                    provider.clone(),
                    &uris[file],
                    &options,
                    match_by,
                    &tupledesc,
                    split as i64,
                    handle,
                )
            });

            split_memory_context.reset();

            keep_going
        };

        if !keep_going {
            break;
        }

        owned_split += nworkers as u32;
    }
}

/// Which file of the statement the given split of all of them together belongs to, and which
/// split of that file it is.
fn file_of_split(splits_per_file: &[u32], split: u32) -> (usize, u32) {
    let mut split_of_file = split;

    for (file, nsplits) in splits_per_file.iter().enumerate() {
        if split_of_file < *nsplits {
            return (file, split_of_file);
        }

        split_of_file -= nsplits;
    }

    panic!("parallel COPY FROM split {split} is past the files of the command");
}

/// Sends the rows of a single split, and closes it with how much it held. `false` once the leader
/// has stopped reading.
#[allow(clippy::too_many_arguments)]
fn decode_split(
    provider: Arc<dyn FormatProvider>,
    uri: &str,
    options: &FormatOptions,
    match_by: pg_arrow_convert::match_by::MatchBy,
    tupledesc: &PgTupleDesc,
    split: i64,
    handle: *mut shm_mq_handle,
) -> bool {
    let mut stream = CopyFromStream::open_split(
        provider,
        &Place::object(uri),
        options,
        match_by,
        tupledesc,
        split,
    );

    while stream.read_next() {
        for chunk in stream.encoded_bytes().chunks(MAX_CHUNK_SIZE) {
            if !send_message(handle, MESSAGE_TAG_DATA, chunk) {
                return false;
            }
        }

        stream.reset_buffer();
    }

    let summary = SplitSummary {
        rows: stream.rows_copied(),
        bytes: stream.row_bytes_copied(),
    };

    let summary = unsafe {
        std::slice::from_raw_parts(
            &summary as *const SplitSummary as *const u8,
            std::mem::size_of::<SplitSummary>(),
        )
    };

    send_message(handle, MESSAGE_TAG_END_OF_SPLIT, summary)
}

/// Puts one tagged message on the queue, waiting for the leader to make room. `false` once the
/// leader stopped reading, which is how a worker learns the statement it belongs to is over.
fn send_message(handle: *mut shm_mq_handle, tag: u8, payload: &[u8]) -> bool {
    let mut iov = [
        shm_mq_iovec {
            data: &tag as *const u8 as *const c_char,
            len: 1,
        },
        shm_mq_iovec {
            data: payload.as_ptr() as *const c_char,
            len: payload.len(),
        },
    ];

    pg_compat::shm_mq_sendv(handle, &mut iov) == shm_mq_result::SHM_MQ_SUCCESS
}

#[cfg(test)]
mod tests {
    use super::{file_of_split, splits_of_worker};

    #[test]
    fn test_the_splits_of_a_worker_are_the_ones_it_strides_over() {
        // 5 splits over 2 workers: 0, 2, 4 for the first and 1, 3 for the second
        assert_eq!(splits_of_worker(5, 0, 2), 3);
        assert_eq!(splits_of_worker(5, 1, 2), 2);

        // and a worker past the splits of the command owns none of them
        assert_eq!(splits_of_worker(1, 1, 2), 0);
    }

    #[test]
    fn test_a_split_of_the_command_belongs_to_one_file() {
        let splits_per_file = [2, 1, 3];

        assert_eq!(file_of_split(&splits_per_file, 0), (0, 0));
        assert_eq!(file_of_split(&splits_per_file, 1), (0, 1));
        assert_eq!(file_of_split(&splits_per_file, 2), (1, 0));
        assert_eq!(file_of_split(&splits_per_file, 5), (2, 2));
    }
}
