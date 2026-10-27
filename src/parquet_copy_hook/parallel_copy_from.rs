use std::ffi::{c_char, c_void, CStr};

use glob::Pattern;
use object_store::ObjectStoreScheme;

use pgrx::{
    pg_guard,
    pg_sys::{
        self, dsm_segment, pg_atomic_init_u32, pg_atomic_read_u32, pg_atomic_uint32,
        pg_atomic_write_u32, shm_mq, shm_mq_handle, shm_mq_iovec, shm_mq_result, shm_toc, Node,
        Oid, PlannedStmt, HEAP_TABLE_AM_OID, RELKIND_RELATION, RELPERSISTENCE_TEMP,
        TYPTYPE_COMPOSITE, TYPTYPE_DOMAIN, TYPTYPE_MULTIRANGE, TYPTYPE_RANGE,
    },
    PgBox, PgMemoryContexts, PgRelation, PgTupleDesc,
};

use crate::{
    arrow_parquet::{
        match_by::MatchBy,
        parquet_reader::ParquetReaderContext,
        uri_utils::{uri_as_string, ParquetRowGroupOpener, ParsedUriInfo},
    },
    parquet_copy_hook::{
        copy_from::{peek_parallel_copy_from_source, push_parallel_copy_from_source},
        copy_progress::update_copy_from_bytes_total,
        copy_utils::{
            copy_stmt_attribute_names, copy_stmt_is_std_inout, copy_stmt_program,
            create_filtered_tupledesc_for_attribute_names,
        },
    },
    parquet_udfs::list::list_uri,
    pgrx_utils::tuple_desc,
};

use super::parallel_copy_to::requested_parallel_workers;

// below this many row groups there is not enough of the file to hand around
const MIN_PARALLEL_ROW_GROUPS: u32 = 2;

// how long a worker sleeps between two looks at the number of workers the leader launched,
// which it only has to wait for at all when it starts before the leader got to write it
const WAIT_FOR_NWORKERS_INTERVAL: i64 = 100;

// the shared memory queue one worker hands its rows to the leader through. It only has to
// cover what a worker encodes between two of the leader's visits to it, since the leader
// moves everything that arrived into its own memory, so it is kept small enough that even
// the largest worker count fits into a modest dynamic shared memory area.
const QUEUE_SIZE: usize = 256 * 1024;

// how much of the copy stream the leader holds in its own memory, over all workers. The rows
// have to reach PG in the order the file has them, so a worker that owns a later row group
// would otherwise sit on a full queue until the leader got to it, which is what this buffer
// buys out: the workers stay a few row groups ahead of the rows PG is being handed.
const MAX_PENDING_BYTES: usize = 64 * 1024 * 1024;

// the largest piece of the copy stream that travels as one queue message
const MAX_CHUNK_SIZE: usize = 64 * 1024;

const PARALLEL_COPY_FROM_SHARED_KEY: u64 = 0xBEEF_0002;
const PARALLEL_COPY_FROM_QUEUES_KEY: u64 = 0xBEEF_0003;

const PARALLEL_COPY_FROM_WORKER_FUNCTION: &CStr = c"pg_parquet_parallel_copy_from_worker";

// message tags. A row group's rows arrive as DATA messages and are closed by an END message,
// which is what tells the leader to move on to the next row group's queue.
const MESSAGE_TAG_DATA: u8 = 0;
const MESSAGE_TAG_END_OF_ROW_GROUP: u8 = 1;

// the payload of an END message: how many rows and how many bytes of row data the row group
// turned out to hold, which is all the leader needs to keep pg_stat_progress_copy honest
#[repr(C)]
#[derive(Clone, Copy, Default)]
struct RowGroupSummary {
    rows: i64,
    bytes: i64,
}

// ParallelCopyFromShared is the DSM payload. The number of row groups of each file follows it
// in the same chunk, and then the uris of the files and the attribute names, in that order,
// both separated by nul bytes.
#[repr(C)]
struct ParallelCopyFromShared {
    // how many workers the command ended up with, which is 0 until they are launched
    nworkers: pg_atomic_uint32,
    // the row groups of all the files together, which is what the workers split between them
    nrowgroups: u32,
    nfiles: u32,
    relation_oid: pg_sys::Oid,
    match_by_name: bool,
    uris_len: u32,
    attribute_names_len: u32,
}

impl ParallelCopyFromShared {
    fn size_with_files(uris: &str, attribute_names: &str, nfiles: usize) -> usize {
        std::mem::size_of::<Self>()
            + nfiles * std::mem::size_of::<u32>()
            + uris.len()
            + attribute_names.len()
    }

    unsafe fn write_files(
        shared: *mut Self,
        uris: &str,
        attribute_names: &str,
        row_groups_per_file: &[u32],
    ) {
        (*shared).nfiles = row_groups_per_file.len() as u32;
        (*shared).uris_len = uris.len() as u32;
        (*shared).attribute_names_len = attribute_names.len() as u32;

        std::ptr::copy_nonoverlapping(
            row_groups_per_file.as_ptr(),
            Self::row_groups(shared),
            row_groups_per_file.len(),
        );

        let strings = Self::strings(shared) as *mut u8;
        std::ptr::copy_nonoverlapping(uris.as_ptr(), strings, uris.len());
        std::ptr::copy_nonoverlapping(
            attribute_names.as_ptr(),
            strings.add(uris.len()),
            attribute_names.len(),
        );
    }

    // row_groups is where the number of row groups of each file sits, which the header is
    // followed by because the header's own size keeps the numbers aligned
    unsafe fn row_groups(shared: *mut Self) -> *mut u32 {
        shared.add(1) as *mut u32
    }

    unsafe fn strings(shared: *const Self) -> *const u8 {
        (shared.add(1) as *const u32).add((*shared).nfiles as usize) as *const u8
    }

    unsafe fn row_groups_per_file(shared: *mut Self) -> Vec<u32> {
        std::slice::from_raw_parts(Self::row_groups(shared), (*shared).nfiles as usize).to_vec()
    }

    unsafe fn uris(shared: *const Self) -> Vec<String> {
        Self::string_at(Self::strings(shared), (*shared).uris_len)
            .split('\0')
            .map(|uri| uri.into())
            .collect()
    }

    unsafe fn attribute_names(shared: *const Self) -> Vec<String> {
        let strings = Self::strings(shared).add((*shared).uris_len as usize);

        let attribute_names = Self::string_at(strings, (*shared).attribute_names_len);

        if attribute_names.is_empty() {
            return vec![];
        }

        attribute_names
            .split('\0')
            .map(|name| name.into())
            .collect()
    }

    unsafe fn string_at(bytes: *const u8, len: u32) -> String {
        let bytes = std::slice::from_raw_parts(bytes, len as usize);

        std::str::from_utf8(bytes)
            .expect("parallel COPY FROM shared state is not valid utf8")
            .to_string()
    }
}

// what a worker sent and the leader did not hand to PG yet
enum PendingChunk {
    Rows(Vec<u8>),
    EndOfRowGroup(RowGroupSummary),
}

// ParallelCopyFromSource is the leader's end of the parallel COPY FROM. It frames the binary
// copy stream PG consumes and fills it from the workers' queues, taking the row groups in
// file order so that the rows arrive in the order the single process path would read them.
pub(crate) struct ParallelCopyFromSource {
    pcxt: *mut pg_sys::ParallelContext,
    handles: Vec<*mut shm_mq_handle>,
    // per worker, what arrived from it and is not handed to PG yet
    pending: Vec<std::collections::VecDeque<PendingChunk>>,
    pending_bytes: usize,
    // how many row groups each worker closed, and whether it detached
    closed_row_groups: Vec<u32>,
    detached: Vec<bool>,
    nrowgroups: u32,
    total_rows: i64,
    current_row_group: u32,
    buffer: Vec<u8>,
    offset: usize,
    started: bool,
    finished: bool,
    rows_received: i64,
    row_bytes_received: i64,
    stream_bytes_received: i64,
}

impl ParallelCopyFromSource {
    // fill makes the next bytes of the copy stream available, and returns false once the
    // stream is over.
    fn fill(&mut self) -> bool {
        if self.finished {
            return false;
        }

        if !self.started {
            self.started = true;

            /* Binary signature, flags field and no header extension */
            let mut header = b"\x50\x47\x43\x4f\x50\x59\x0a\xff\x0d\x0a\x00".to_vec();
            header.extend_from_slice(&0_i32.to_be_bytes());
            header.extend_from_slice(&0_i32.to_be_bytes());

            self.stream_bytes_received += header.len() as i64;
            self.hand_out(header);

            return true;
        }

        loop {
            // taking everything that already arrived, rather than only what the row group PG
            // waits for needs, is what keeps every worker busy
            self.drain_available();

            if self.current_row_group >= self.nrowgroups {
                self.finished = true;

                /* trailer */
                self.stream_bytes_received += 2;
                self.hand_out((-1_i16).to_be_bytes().to_vec());

                self.report_bytes_total();

                return true;
            }

            let worker = self.current_worker();

            let Some(chunk) = self.pending[worker].pop_front() else {
                // nothing of the row group PG waits for arrived yet
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
                PendingChunk::EndOfRowGroup(summary) => {
                    self.rows_received += summary.rows;
                    self.row_bytes_received += summary.bytes;
                    self.current_row_group += 1;

                    self.report_bytes_total();
                }
            }
        }
    }

    // hand_out makes the given bytes the next ones PG reads. It is only called with the
    // buffer already drained, so the bytes replace it rather than being copied into it.
    fn hand_out(&mut self, bytes: Vec<u8>) {
        debug_assert_eq!(self.not_yet_copied_bytes(), 0);

        self.buffer = bytes;
        self.offset = 0;
    }

    fn current_worker(&self) -> usize {
        self.current_row_group as usize % self.handles.len()
    }

    // drain_available moves everything the workers already sent into the leader's own memory,
    // up to the buffer's limit, so that a worker only ever waits on the leader falling behind
    // and not on the order the rows have to be handed out in.
    // The workers are visited starting with the one whose row group PG is waiting for, so
    // that the buffer goes to the rows that are needed first. Visiting them in worker order
    // instead lets a worker whose row groups come later fill the buffer on its own, which
    // leaves the leader handing the stream out one queue message at a time, one wakeup per
    // message, and costs more the more workers there are.
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

    // receive takes one message off a worker's queue, waiting for it unless nowait is set,
    // and returns whether a message arrived.
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

            // a worker detaches once it closed every row group it was given, so it is only
            // gone too early if one of its row groups is still expected
            if self.closed_row_groups[worker]
                < row_groups_of_worker(self.nrowgroups, worker, self.handles.len())
            {
                // a worker that gave up reports why through the error queue of the parallel
                // context, which it writes to before it detaches this queue. PG re-throws
                // what is in there, which is the error the copy has to fail with. It is
                // asked for here rather than left to the next interrupt check, because the
                // queue only has to be read for the leader to find the error, and the signal
                // that the worker sent along with it does not have to have arrived yet.
                // postgres 18 renamed HandleParallelMessages() to ProcessParallelMessages()
                #[cfg(any(feature = "pg18", feature = "pg19"))]
                unsafe {
                    pg_sys::ProcessParallelMessages()
                };
                #[cfg(not(any(feature = "pg18", feature = "pg19")))]
                unsafe {
                    pg_sys::HandleParallelMessages()
                };

                panic!(
                    "parallel COPY FROM worker exited before it read every row group it was given"
                );
            }

            return false;
        }

        let message = unsafe { std::slice::from_raw_parts(data as *const u8, nbytes) };

        let (tag, payload) = message
            .split_first()
            .expect("parallel COPY FROM message is empty");

        match *tag {
            MESSAGE_TAG_END_OF_ROW_GROUP => {
                self.closed_row_groups[worker] += 1;

                self.pending[worker]
                    .push_back(PendingChunk::EndOfRowGroup(read_row_group_summary(payload)));
            }
            _ => {
                self.pending_bytes += payload.len();

                self.pending[worker].push_back(PendingChunk::Rows(payload.to_vec()));
            }
        }

        true
    }

    // report_bytes_total extrapolates the size of the whole binary copy stream from the row
    // groups that arrived so far, the same way the single process path does.
    fn report_bytes_total(&self) {
        if self.finished {
            update_copy_from_bytes_total(self.stream_bytes_received);
            return;
        }

        if self.rows_received == 0 {
            return;
        }

        let rows_left = (self.total_rows - self.rows_received).max(0) as i128;

        let bytes_left = self.row_bytes_received as i128 * rows_left / self.rows_received as i128;

        let bytes_total = (self.stream_bytes_received as i128 + bytes_left).min(i64::MAX as i128);

        update_copy_from_bytes_total(bytes_total as i64);
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

    // finish waits for the workers and tears the parallel context down. It is only called
    // once every row group arrived, so a worker that is still running at that point has
    // nothing left to send.
    pub(crate) fn finish(&mut self) {
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

// row_groups_of_worker is how many of the file's row groups the given worker was given
fn row_groups_of_worker(nrowgroups: u32, worker: usize, nworkers: usize) -> u32 {
    let worker = worker as u32;
    let nworkers = nworkers as u32;

    nrowgroups.saturating_sub(worker).div_ceil(nworkers)
}

fn read_row_group_summary(payload: &[u8]) -> RowGroupSummary {
    assert!(
        payload.len() == std::mem::size_of::<RowGroupSummary>(),
        "parallel COPY FROM row group summary is truncated"
    );

    let mut summary = RowGroupSummary::default();

    unsafe {
        std::ptr::copy_nonoverlapping(
            payload.as_ptr(),
            &mut summary as *mut RowGroupSummary as *mut u8,
            payload.len(),
        )
    };

    summary
}

// copy_parallel_parquet_data_to_buffer is the COPY FROM data source of the leader of a
// parallel COPY FROM. It hands PG the stream the workers decoded.
#[pg_guard]
pub(crate) extern "C-unwind" fn copy_parallel_parquet_data_to_buffer(
    outbuf: pgrx::void_mut_ptr,
    _minread: i32,
    maxread: i32,
) -> i32 {
    let source = peek_parallel_copy_from_source().expect("parallel COPY FROM source is not found");

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

// try_parallel_copy_from sets a parallel COPY FROM up and pushes the leader's end of it, so
// that the caller reads the rows the workers decode instead of decoding them itself. It
// returns false when the command is not eligible, in which case the caller falls back to the
// single process path.
pub(crate) fn try_parallel_copy_from(
    p_stmt: &PgBox<PlannedStmt>,
    uri_info: &ParsedUriInfo,
    relation: &PgRelation,
    match_by: MatchBy,
    where_clause: *mut Node,
) -> bool {
    let nworkers = requested_parallel_workers();

    if nworkers <= 0 || !parallel_copy_from_is_eligible(p_stmt, uri_info, relation, where_clause) {
        return false;
    }

    let Some(files) = parallel_copy_from_files(uri_info) else {
        return false;
    };

    let nrowgroups = files.iter().map(|file| file.nrowgroups).sum::<u32>();
    let total_rows = files.iter().map(|file| file.total_rows).sum::<i64>();

    if nrowgroups < MIN_PARALLEL_ROW_GROUPS {
        return false;
    }

    // a worker without a row group of its own would only cost a queue
    let nworkers = nworkers.min(nrowgroups as i32);

    let uris = files
        .iter()
        .map(|file| file.uri.as_str())
        .collect::<Vec<_>>()
        .join("\0");
    let row_groups_per_file = files.iter().map(|file| file.nrowgroups).collect::<Vec<_>>();

    let attribute_names = copy_stmt_attribute_names(p_stmt).join("\0");

    // the leader writes the rows itself and so needs an xid, which it can no longer be
    // assigned once it is in parallel mode
    unsafe { pg_sys::GetCurrentFullTransactionId() };

    let (pcxt, handles) = unsafe {
        pg_sys::EnterParallelMode();

        let pcxt = pg_sys::CreateParallelContext(
            c"pg_parquet".as_ptr(),
            PARALLEL_COPY_FROM_WORKER_FUNCTION.as_ptr(),
            nworkers,
        );

        let shared_size =
            ParallelCopyFromShared::size_with_files(&uris, &attribute_names, files.len());
        let queues_size = QUEUE_SIZE * nworkers as usize;

        // shm_toc_estimate_chunk and shm_toc_estimate_keys are macros, so they are spelled
        // out here. The chunks are rounded up because shm_toc_allocate aligns what it hands
        // out, and estimating too much shared memory is harmless.
        (*pcxt).estimator.space_for_chunks +=
            shared_size.next_multiple_of(64) + queues_size.next_multiple_of(64);
        (*pcxt).estimator.number_of_keys += 2;

        // serializes the leader's snapshot, GUCs, combo CIDs and transaction state into the
        // segment, which is what lets a worker read exactly the file the leader would read
        pg_sys::InitializeParallelDSM(pcxt);

        let shared =
            pg_sys::shm_toc_allocate((*pcxt).toc, shared_size) as *mut ParallelCopyFromShared;

        // how many workers the command ends up with is only known once they are launched,
        // which is after a worker may already be reading this, so it starts out as the 0 the
        // workers wait on rather than as the number the command asked for
        pg_atomic_init_u32(&mut (*shared).nworkers, 0);

        (*shared).nrowgroups = nrowgroups;
        (*shared).relation_oid = relation.oid();
        (*shared).match_by_name = matches!(match_by, MatchBy::Name);
        ParallelCopyFromShared::write_files(shared, &uris, &attribute_names, &row_groups_per_file);

        pg_sys::shm_toc_insert((*pcxt).toc, PARALLEL_COPY_FROM_SHARED_KEY, shared as _);

        let queues = pg_sys::shm_toc_allocate((*pcxt).toc, queues_size);

        for worker in 0..nworkers as usize {
            let queue = pg_sys::shm_mq_create(queue_at(queues, worker), QUEUE_SIZE);
            pg_sys::shm_mq_set_receiver(queue, pg_sys::MyProc);
        }

        pg_sys::shm_toc_insert((*pcxt).toc, PARALLEL_COPY_FROM_QUEUES_KEY, queues);

        pg_sys::LaunchParallelWorkers(pcxt);

        let nworkers_launched = (*pcxt).nworkers_launched;

        // the leader cannot take a row group of its own, since it is the only process that
        // may write into the relation, so a command that was given no worker at all is left
        // to read the whole file by itself
        if nworkers_launched < 1 {
            pg_sys::DestroyParallelContext(pcxt);
            pg_sys::ExitParallelMode();

            return false;
        }

        // the row groups are handed out by the number of workers the command ends up with
        // rather than by the number it asked for, which is what lets a command that the pool
        // is too busy to fill run with the workers it was given. The workers wait for this.
        pg_atomic_write_u32(&mut (*shared).nworkers, nworkers_launched as u32);

        // postgres only launches the workers it registered without a failure, and a
        // registration that fails fails every one after it, so the workers of the command are
        // the first nworkers_launched of the ones it asked for
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

    push_parallel_copy_from_source(ParallelCopyFromSource {
        pcxt,
        handles,
        pending: (0..nworkers).map(|_| Default::default()).collect(),
        pending_bytes: 0,
        closed_row_groups: vec![0; nworkers],
        detached: vec![false; nworkers],
        nrowgroups,
        total_rows,
        current_row_group: 0,
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

// ParallelCopyFromFile is one of the files the command reads: where it is, and how much of it
// there is to read.
struct ParallelCopyFromFile {
    uri: String,
    nrowgroups: u32,
    total_rows: i64,
}

// parallel_copy_from_files is the files the command reads, in the order the single process
// path reads them, which is the order the rows have to reach PG in. A uri that names a file of
// its own is that single file, and a pattern that stands for several is all of them. None is
// a uri the parallel path cannot read, which leaves both the reading of it and the error it
// deserves to the single process path.
fn parallel_copy_from_files(uri_info: &ParsedUriInfo) -> Option<Vec<ParallelCopyFromFile>> {
    if let Some(file) = parallel_copy_from_file(uri_info) {
        return Some(vec![file]);
    }

    // the single process path only reads a uri as a pattern once reading it as a file of its
    // own failed, which is the order kept here
    if !uri_info.path.as_ref().contains('*') || Pattern::try_from(uri_info).is_err() {
        return None;
    }

    let files = list_uri(uri_info)
        .into_iter()
        .map(|(file_uri, _)| {
            let file_uri_info = ParsedUriInfo::try_from(file_uri.as_str())
                .unwrap_or_else(|e| panic!("failed to parse file uri {file_uri}: {e}"));

            parallel_copy_from_file(&file_uri_info)
        })
        .collect::<Option<Vec<_>>>()?;

    // a pattern that matches nothing is an error of the single process path to report
    (!files.is_empty()).then_some(files)
}

// parallel_copy_from_file reads the footer of a single file, which is what says how much of it
// there is for the workers to split. None is a uri that is not a parquet file of its own.
fn parallel_copy_from_file(uri_info: &ParsedUriInfo) -> Option<ParallelCopyFromFile> {
    let (nrowgroups, total_rows) = ParquetRowGroupOpener::try_new(uri_info).ok()?.row_groups();

    Some(ParallelCopyFromFile {
        uri: uri_as_string(&uri_info.uri),
        nrowgroups,
        total_rows,
    })
}

// parallel_copy_from_is_eligible decides whether the rows of the file may be decoded by
// workers while the leader writes them. Only the decoding is handed around, so anything that
// would make the leader's own writes illegal in parallel mode, or that would make a worker
// read a different file than the leader, falls back to the single process path.
fn parallel_copy_from_is_eligible(
    p_stmt: &PgBox<PlannedStmt>,
    uri_info: &ParsedUriInfo,
    relation: &PgRelation,
    where_clause: *mut Node,
) -> bool {
    // a worker reads the file itself, so there has to be a file rather than a pipe
    if copy_stmt_program(p_stmt).is_some() || copy_stmt_is_std_inout(p_stmt) {
        return false;
    }

    // a worker reads its own row groups out of the uri itself, which it can only do for a
    // store that serves a range of a file. An http(s) endpoint is one of those: the single
    // process path already reads the footer and the column chunks by range, and object_store
    // refuses a range request that is answered with the whole file rather than with a 206,
    // so a server that cannot serve ranges fails a sequential copy just as much as a
    // parallel one. The list is an allowlist because the scheme enum is not exhaustive.
    if !matches!(
        uri_info.scheme,
        ObjectStoreScheme::Local
            | ObjectStoreScheme::AmazonS3
            | ObjectStoreScheme::MicrosoftAzure
            | ObjectStoreScheme::GoogleCloudStorage
            | ObjectStoreScheme::Http
    ) {
        return false;
    }

    // a parallel worker cannot start a parallel operation of its own
    if unsafe { pg_sys::IsInParallelMode() } {
        return false;
    }

    let relation_form = unsafe { *relation.rd_rel };

    // a temporary relation is not visible to a worker, and a relation that is not a plain
    // heap routes its writes through code the leader may not reach in parallel mode
    if relation_form.relkind != RELKIND_RELATION as c_char
        || relation_form.relam != HEAP_TABLE_AM_OID
        || relation_form.relpersistence == RELPERSISTENCE_TEMP as c_char
    {
        return false;
    }

    // a trigger, which is also how a foreign key is enforced, runs arbitrary statements, and
    // a default expression may be volatile, and neither may do so in parallel mode
    if relation_form.relhastriggers {
        return false;
    }

    // the leader evaluates the WHERE clause of the command and the CHECK constraints of the
    // relation for every row it writes, which a volatile function in either of them may not
    // do in parallel mode: nextval() refuses outright, and a plpgsql function runs its own
    // statements read write, which needs a command counter increment parallel mode forbids.
    // A stable or an immutable function is safe, since plpgsql runs the statements of one
    // read only, so an ordinary constraint keeps the command parallel
    if !where_clause.is_null() && unsafe { pg_sys::contain_volatile_functions(where_clause) } {
        return false;
    }

    if relation_has_volatile_check_constraint(relation) {
        return false;
    }

    let tupledesc = relation.tuple_desc();

    tupledesc.iter().all(|attribute| {
        !attribute.atthasdef
            && attribute.attidentity == 0
            && attribute.attgenerated == 0
            && !type_contains_domain(attribute.atttypid)
    })
}

// type_contains_domain is whether a value of the type can carry the constraints of a domain.
// Those constraints are evaluated by the leader too, but they are reached through the type
// cache rather than through the relation, so a type that holds a domain is left to the single
// process path whether that domain carries a volatile constraint or not. The domain can be the
// type itself, or it can sit inside an array, a composite or a range, each of which reaches
// the receive function of the type it holds, which is the domain's own.
fn type_contains_domain(typoid: Oid) -> bool {
    let typtype = unsafe { pg_sys::get_typtype(typoid) };

    if typtype == TYPTYPE_DOMAIN as c_char {
        return true;
    }

    // an array type is a base type, so its element type is asked for before the kind of the
    // type is looked at any further
    let element_typoid = unsafe { pg_sys::get_element_type(typoid) };

    if element_typoid != Oid::INVALID {
        return type_contains_domain(element_typoid);
    }

    if typtype == TYPTYPE_COMPOSITE as c_char {
        let tupledesc = tuple_desc(typoid, -1);

        return tupledesc
            .iter()
            .any(|attribute| !attribute.is_dropped() && type_contains_domain(attribute.atttypid));
    }

    if typtype == TYPTYPE_RANGE as c_char {
        return type_contains_domain(unsafe { pg_sys::get_range_subtype(typoid) });
    }

    if typtype == TYPTYPE_MULTIRANGE as c_char {
        return type_contains_domain(unsafe { pg_sys::get_multirange_range(typoid) });
    }

    false
}

// relation_has_volatile_check_constraint is whether any CHECK constraint of the relation calls
// a volatile function. A constraint that is NOT VALID counts, since it is still evaluated for
// the rows the command writes.
fn relation_has_volatile_check_constraint(relation: &PgRelation) -> bool {
    let constr = unsafe { (*relation.rd_att).constr };

    if constr.is_null() {
        return false;
    }

    let num_check = unsafe { (*constr).num_check } as usize;
    let check = unsafe { (*constr).check };

    (0..num_check).any(|i| {
        let expression_string = unsafe { (*check.add(i)).ccbin };

        if expression_string.is_null() {
            return false;
        }

        // the expression of a constraint is stored as the text of its node tree
        let expression = unsafe { pg_sys::stringToNode(expression_string) as *mut Node };

        unsafe { pg_sys::contain_volatile_functions(expression) }
    })
}

// queue_at is the address of one worker's queue inside the chunk that holds all of them
fn queue_at(queues: *mut c_void, worker: usize) -> *mut c_void {
    unsafe { (queues as *mut u8).add(worker * QUEUE_SIZE) as _ }
}

// pg_parquet_parallel_copy_from_worker is the entry point of a parallel COPY FROM worker. It
// is resolved by name out of pg_parquet's shared library, so its symbol has to stay.
#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn pg_parquet_parallel_copy_from_worker(
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

    decode_row_groups(shared, worker, handle);

    unsafe { pg_sys::shm_mq_detach(handle) };
}

// wait_for_nworkers is how many workers the command ended up with, which says which row
// groups of it are this worker's own. The leader cannot write it before it launches the
// workers, so a worker that reads it first waits for it, which the leader only takes the
// launch itself to answer. An interrupt, e.g. the leader's own death, ends the wait as well.
fn wait_for_nworkers(shared: *mut ParallelCopyFromShared) -> usize {
    loop {
        let nworkers = unsafe { pg_atomic_read_u32(&mut (*shared).nworkers) };

        if nworkers > 0 {
            return nworkers as usize;
        }

        pgrx::check_for_interrupts!();

        unsafe { pg_sys::pg_usleep(WAIT_FOR_NWORKERS_INTERVAL) };
    }
}

// decode_row_groups encodes every row group this worker owns into the binary copy
// representation and hands it to the leader, one row group at a time.
fn decode_row_groups(
    shared: *mut ParallelCopyFromShared,
    worker: usize,
    handle: *mut shm_mq_handle,
) {
    let nworkers = wait_for_nworkers(shared);

    let (nrowgroups, relation_oid, match_by) = unsafe {
        (
            (*shared).nrowgroups,
            (*shared).relation_oid,
            if (*shared).match_by_name {
                MatchBy::Name
            } else {
                MatchBy::Position
            },
        )
    };

    let uris = unsafe { ParallelCopyFromShared::uris(shared) };
    let row_groups_per_file = unsafe { ParallelCopyFromShared::row_groups_per_file(shared) };
    let attribute_names = unsafe { ParallelCopyFromShared::attribute_names(shared) };

    // the leader already holds a lock that conflicts with anything that could change the
    // relation, and a worker shares its leader's lock group, so this cannot block
    let relation = unsafe { PgRelation::with_lock(relation_oid, pg_sys::AccessShareLock as _) };

    let tupledesc = create_filtered_tupledesc_for_attribute_names(&attribute_names, &relation);

    // every row group's reader leaves its own allocations behind, so they are freed per row
    // group rather than held until the command ends
    let mut row_group_memory_context =
        PgMemoryContexts::new("pg_parquet parallel copy from row group");

    // the row groups of all the files make up one list that the workers stride over, so this
    // worker owns the row groups at its own offset of it
    let mut owned_row_group = worker as u32;

    // a file is opened once rather than once per row group of it: its footer is what says
    // where a row group's columns are, and reading it per row group would mean a request per
    // row group against an object store. A row group of a later file is never followed by one
    // of an earlier file, so one open file at a time is all a worker needs.
    let mut open_file: Option<(usize, ParquetRowGroupOpener)> = None;

    while owned_row_group < nrowgroups {
        let (file, row_group) = file_of_row_group(&row_groups_per_file, owned_row_group);

        if open_file.as_ref().map(|(open, _)| *open) != Some(file) {
            let uri = &uris[file];

            let uri_info = ParsedUriInfo::try_from(uri.as_str())
                .unwrap_or_else(|e| panic!("failed to parse uri {uri}: {e}"));

            let opener = ParquetRowGroupOpener::try_new(&uri_info)
                .unwrap_or_else(|e| panic!("failed to open uri {uri}: {e}"));

            open_file = Some((file, opener));
        }

        let opener = &open_file
            .as_ref()
            .expect("parallel COPY FROM worker opened no file")
            .1;

        let keep_going = unsafe {
            let keep_going = row_group_memory_context
                .switch_to(|_| decode_row_group(opener, match_by, &tupledesc, row_group, handle));

            row_group_memory_context.reset();

            keep_going
        };

        if !keep_going {
            break;
        }

        owned_row_group += nworkers as u32;
    }
}

// file_of_row_group is which file of the command the given row group of all of them together
// belongs to, and which row group of that file it is
fn file_of_row_group(row_groups_per_file: &[u32], row_group: u32) -> (usize, u32) {
    let mut row_group_of_file = row_group;

    for (file, nrowgroups) in row_groups_per_file.iter().enumerate() {
        if row_group_of_file < *nrowgroups {
            return (file, row_group_of_file);
        }

        row_group_of_file -= nrowgroups;
    }

    panic!("parallel COPY FROM row group {row_group} is past the files of the command");
}

// decode_row_group sends the rows of a single row group, and closes it with how much it held.
// It returns false once the leader stopped reading.
fn decode_row_group(
    opener: &ParquetRowGroupOpener,
    match_by: MatchBy,
    tupledesc: &PgTupleDesc,
    row_group: u32,
    handle: *mut shm_mq_handle,
) -> bool {
    let mut reader_context =
        ParquetReaderContext::new_for_row_group(opener, match_by, tupledesc, row_group as usize);

    while reader_context.read_parquet() {
        for chunk in reader_context.encoded_bytes().chunks(MAX_CHUNK_SIZE) {
            if !send_message(handle, MESSAGE_TAG_DATA, chunk) {
                return false;
            }
        }

        reader_context.reset_buffer();
    }

    let summary = RowGroupSummary {
        rows: reader_context.rows_copied(),
        bytes: reader_context.row_bytes_copied(),
    };

    let summary = unsafe {
        std::slice::from_raw_parts(
            &summary as *const RowGroupSummary as *const u8,
            std::mem::size_of::<RowGroupSummary>(),
        )
    };

    send_message(handle, MESSAGE_TAG_END_OF_ROW_GROUP, summary)
}

// send_message puts one tagged message on the queue, waiting for the leader to make room. It
// returns false once the leader stopped reading, which is how a worker learns that the
// command it belongs to is over.
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

    // postgres 15 gave shm_mq_sendv a force_flush argument
    #[cfg(feature = "pg14")]
    let result = unsafe { pg_sys::shm_mq_sendv(handle, iov.as_mut_ptr(), iov.len() as _, false) };
    #[cfg(not(feature = "pg14"))]
    let result =
        unsafe { pg_sys::shm_mq_sendv(handle, iov.as_mut_ptr(), iov.len() as _, false, true) };

    result == shm_mq_result::SHM_MQ_SUCCESS
}
