use std::{
    ffi::{c_char, c_void, CStr, CString},
    panic::AssertUnwindSafe,
    path::Path,
    rc::Rc,
    sync::Arc,
};

use bytes::Bytes;
use object_store::{
    multipart::{MultipartStore, PartId},
    path::Path as ObjectStorePath,
    MultipartId, ObjectStoreScheme,
};
use pgrx::{
    pg_guard,
    pg_sys::{
        self, defGetString, dsm_segment, pg_atomic_fetch_add_u32, pg_atomic_fetch_add_u64,
        pg_atomic_init_u32, pg_atomic_init_u64, pg_atomic_read_u32, pg_atomic_read_u64,
        pg_atomic_uint32, pg_atomic_uint64, pg_class_aclcheck, quote_identifier,
        quote_literal_cstr, quote_qualified_identifier, shm_mq, shm_mq_handle, shm_mq_iovec,
        shm_mq_result, shm_toc, AclResult, AsPgCStr, CmdType, CopyStmt, DefElem, GetUserId, Node,
        NodeTag, PlannedStmt, RawStmt, ACL_SELECT, HEAP_TABLE_AM_OID, RELKIND_RELATION,
        RELPERSISTENCE_TEMP,
    },
    GucSetting, PgBox, PgList, PgMemoryContexts, PgRelation, PgTryBuilder,
};

use crate::{
    arrow_parquet::{
        parquet_concat::concat_parquet_files,
        parquet_shared_upload::{
            complete_shared_upload, decode_range_metadata, encode_range_metadata,
            install_shared_upload, take_range_metadata, uninstall_shared_upload, PartClaim,
            ParticipantUpload, RangeUpload, Region, SharedUpload, CLAIM_OFFSET_BITS,
            CLAIM_OFFSET_MASK, CLAIM_PART_STEP,
        },
        uri_utils::{uri_as_string, ParsedUriInfo},
    },
    block_on,
    object_store::{object_store_cache::get_or_create_multipart_store, object_store_error_message},
    parquet_copy_hook::{
        copy_cleanup::{
            finish_unfinished_write, give_up_unfinished_write, register_unfinished_write,
            CleanupId, UnfinishedWrite, PART_URI_INFIX,
        },
        copy_to_split_dest_receiver::{
            install_file_id_claim, remove_files_of_a_previous_copy, uninstall_file_id_claim,
            FileIdClaim, INVALID_FILE_SIZE_BYTES,
        },
        copy_utils::{
            copy_stmt_attribute_list, copy_stmt_has_relation, copy_stmt_lock_mode,
            copy_stmt_relation_oid, copy_to_stmt_compression, copy_to_stmt_compression_level,
            copy_to_stmt_explicit_row_group_size, copy_to_stmt_file_size_bytes,
        },
        hook::process_copy_to_parquet,
        pg_compat::strVal,
    },
};

// MAX_PARALLEL_WORKERS is the number of parallel workers a single COPY TO parquet command
// may request. 0, the default, keeps COPY TO single process.
pub(crate) static MAX_PARALLEL_WORKERS: GucSetting<i32> = GucSetting::<i32>::new(0);

pub(crate) const MAX_PARALLEL_WORKERS_LIMIT: i32 = 64;

// requested_parallel_workers is how many workers a command may ask for. The server hands
// parallel workers out of the max_parallel_workers pool, so a request above it is never met
// in full, and a COPY FROM that is short of a single worker gives up on parallelism
// altogether, which makes a request above the pool slower than no request at all.
pub(crate) fn requested_parallel_workers() -> i32 {
    // read as copies, a reference to a mutable static is not allowed
    let (pool, slots) = unsafe { (pg_sys::max_parallel_workers, pg_sys::max_worker_processes) };

    MAX_PARALLEL_WORKERS.get().min(pool).min(slots)
}

// the number of ctid ranges a participant gets on average. More ranges than participants
// keeps everyone busy when the pages are not evenly filled, but every range is a part file
// that has to be spliced back in at the end.
const RANGES_PER_PARTICIPANT: u32 = 4;

// below this many pages the fan-out does not pay for the part files and the final splice
const MIN_PARALLEL_BLOCKS: u32 = 1024;

// the shared memory queue a worker hands what it uploaded to the leader through. Only the
// metadata of the rows travels through it, and only once a worker is done with every range it
// claimed, so a queue of this size is never the thing a worker waits on for long.
const QUEUE_SIZE: usize = 256 * 1024;

const PARALLEL_COPY_TO_SHARED_KEY: u64 = 0xBEEF_0001;
const PARALLEL_COPY_TO_QUEUES_KEY: u64 = 0xBEEF_0004;

const PARALLEL_COPY_TO_WORKER_FUNCTION: &CStr = c"pg_parquet_parallel_copy_to_worker";

// placeholders that the participant which claims a ctid range fills in
const FIRST_BLOCK_PLACEHOLDER: &str = "@FIRST_BLOCK@";
const LAST_BLOCK_PLACEHOLDER: &str = "@LAST_BLOCK@";
const PART_URI_PLACEHOLDER: &str = "@PART_URI@";

const PLACEHOLDERS: [&str; 3] = [
    FIRST_BLOCK_PLACEHOLDER,
    LAST_BLOCK_PLACEHOLDER,
    PART_URI_PLACEHOLDER,
];

// options the per range COPY statement renders as a bare number, since defGetInt64,
// defGetInt32 and defGetNumeric all reject a string argument
const NUMBER_VALUED_COPY_TO_OPTIONS: [&str; 3] =
    ["row_group_size", "row_group_size_bytes", "bloom_filter_fpp"];

// options the per range COPY statement renders as a quoted literal
const STRING_VALUED_COPY_TO_OPTIONS: [&str; 4] =
    ["parquet_version", "bloom_filter", "dictionary", "field_ids"];

// ParallelCopyToShared is the DSM payload. The per range COPY statement, the part uri prefix,
// the destination uri and the id of its upload are stored right after it in the same chunk,
// in that order.
#[repr(C)]
struct ParallelCopyToShared {
    // the next part of the destination's upload, in the high bits, together with the byte
    // offset of the destination that part covers, in the low bits. The two are claimed in a
    // single instruction because the parts of an upload are numbered by the order their bytes
    // have in the finished object, so a participant cannot take one without the other.
    next_part_and_offset: pg_atomic_uint64,
    // the next ctid range a participant may claim
    next_range: pg_atomic_uint32,
    nranges: u32,
    pages_per_range: u32,
    // whether the participants write into the destination's own upload, rather than each
    // range into a part of its own that the leader splices afterwards
    shared_upload: bool,
    // whether the participants write complete files into a folder destination, which is what
    // a copy with file_size_bytes writes, rather than one file the ranges make up together
    split_files: bool,
    // the next file id a participant of a folder destination may name a file after
    next_file_id: pg_atomic_uint32,
    // how many rows the participants of a folder destination copied, which is the only thing
    // that says how large such a copy was: its files are finished by the participants
    // themselves, so there is nothing left for the leader to count them in
    nprocessed: pg_atomic_uint64,
    statement_len: u32,
    part_prefix_len: u32,
    destination_len: u32,
    upload_id_len: u32,
}

impl ParallelCopyToShared {
    fn size_with_strings(strings: [&str; 4]) -> usize {
        std::mem::size_of::<Self>() + strings.iter().map(|string| string.len()).sum::<usize>()
    }

    // write_strings stores the per range COPY statement, the part uri prefix, the destination
    // uri and its upload id right after the struct, so that a participant needs no further
    // DSM keys
    unsafe fn write_strings(shared: *mut Self, strings: [&str; 4]) {
        let [statement, part_prefix, destination, upload_id] = strings;

        (*shared).statement_len = statement.len() as u32;
        (*shared).part_prefix_len = part_prefix.len() as u32;
        (*shared).destination_len = destination.len() as u32;
        (*shared).upload_id_len = upload_id.len() as u32;

        let mut at = shared.add(1) as *mut u8;

        for string in strings {
            std::ptr::copy_nonoverlapping(string.as_ptr(), at, string.len());
            at = at.add(string.len());
        }
    }

    unsafe fn statement(shared: *const Self) -> String {
        Self::string_at(shared, 0, (*shared).statement_len)
    }

    unsafe fn part_prefix(shared: *const Self) -> String {
        Self::string_at(shared, (*shared).statement_len, (*shared).part_prefix_len)
    }

    unsafe fn destination(shared: *const Self) -> String {
        let at = (*shared).statement_len + (*shared).part_prefix_len;

        Self::string_at(shared, at, (*shared).destination_len)
    }

    unsafe fn upload_id(shared: *const Self) -> String {
        let at = (*shared).statement_len + (*shared).part_prefix_len + (*shared).destination_len;

        Self::string_at(shared, at, (*shared).upload_id_len)
    }

    unsafe fn string_at(shared: *const Self, at: u32, len: u32) -> String {
        let bytes = (shared.add(1) as *const u8).add(at as usize);
        let bytes = std::slice::from_raw_parts(bytes, len as usize);

        std::str::from_utf8(bytes)
            .expect("parallel COPY TO shared state is not valid utf8")
            .to_string()
    }
}

// DsmPartClaim is how a participant claims the next part of the destination's upload
struct DsmPartClaim(*mut ParallelCopyToShared);

// the DSM of a copy is only reached from the backend that is running it, whose tokio runtime
// runs on the backend's own thread
unsafe impl Send for DsmPartClaim {}
unsafe impl Sync for DsmPartClaim {}

impl PartClaim for DsmPartClaim {
    fn claim(&self, len: usize) -> (u32, u64) {
        let claimed = unsafe {
            pg_atomic_fetch_add_u64(
                &mut (*self.0).next_part_and_offset,
                (CLAIM_PART_STEP + len as u64) as i64,
            )
        };

        let part = (claimed >> CLAIM_OFFSET_BITS) as u32;
        let offset = claimed & CLAIM_OFFSET_MASK;

        assert!(
            offset + len as u64 <= CLAIM_OFFSET_MASK,
            "the parallel COPY TO is writing more than the largest object the store takes"
        );

        (part, offset)
    }
}

// DsmFileIdClaim is how a participant claims the id of the next file of a folder destination
struct DsmFileIdClaim(*mut ParallelCopyToShared);

impl FileIdClaim for DsmFileIdClaim {
    fn claim_file_id(&self) -> i64 {
        unsafe { pg_atomic_fetch_add_u32(&mut (*self.0).next_file_id, 1) as i64 }
    }
}

// try_parallel_copy_to runs a COPY TO parquet command with parallel workers. Each
// participant claims ctid ranges of the table and writes the rows of each range where the
// destination takes them: into the parts of the destination's own multipart upload for an
// object store, so that every byte is uploaded once and nothing of the copy is written to
// local disk, and into a part file per range that the leader splices in range order for a
// local destination. It returns None when the command is not eligible, in which case the
// caller falls back to the single process path.
pub(crate) fn try_parallel_copy_to(
    p_stmt: &PgBox<PlannedStmt>,
    uri_info: &ParsedUriInfo,
) -> Option<u64> {
    let nworkers = requested_parallel_workers();

    if nworkers <= 0 {
        return None;
    }

    let relation = parallel_copy_to_relation(p_stmt)?;

    // all tuples the snapshot can see live below this block, which is the same assumption
    // the heap's own parallel scan makes
    let nblocks = unsafe {
        pg_sys::RelationGetNumberOfBlocksInFork(relation.as_ptr(), pg_sys::ForkNumber::MAIN_FORKNUM)
    };

    if nblocks < MIN_PARALLEL_BLOCKS {
        return None;
    }

    let statement = render_range_copy_to_statement(p_stmt, &relation, uri_info)?;

    let nparticipants = nworkers as u32 + 1;
    let pages_per_range = range_size_in_pages(p_stmt, &relation, nblocks, nparticipants);
    let nranges = nblocks.div_ceil(pages_per_range);

    // file_size_bytes makes the destination a folder the copy fills with files of its own,
    // which every participant writes complete files into rather than parts of a single one
    let split_files = copy_to_stmt_file_size_bytes(p_stmt) != INVALID_FILE_SIZE_BYTES;

    // opened last, once the command is known to take the parallel path, so that a command
    // that falls back to the single process path leaves no upload behind
    let destination = parallel_copy_to_destination(uri_info, split_files)?;

    let part_prefix = part_uri_prefix(&destination);

    // the cleanup below does not run when the backend exits in the middle of the copy, e.g.
    // on pg_terminate_backend, so what the copy wrote is also given up at process exit
    let cleanup = unfinished_write(&part_prefix, &destination).map(register_unfinished_write);

    // the destination holds the object store of the copy, which an error does not leave in a
    // state the cleanup below cannot handle: the only thing it does with it is to abort the
    // upload the copy opened
    let arguments = AssertUnwindSafe((&destination, statement.as_str(), part_prefix.as_str()));

    let nprocessed = PgTryBuilder::new(move || {
        let (destination, statement, part_prefix) = *arguments;

        run_parallel_copy_to(
            nworkers,
            nranges,
            pages_per_range,
            statement,
            part_prefix,
            destination,
            cleanup,
        )
    })
    .finally(|| {
        if let Some(cleanup) = cleanup {
            give_up_unfinished_write(cleanup)
        }
    })
    .execute();

    Some(nprocessed)
}

// parallel_copy_to_relation returns the relation the COPY TO command reads, but only when
// the command is one the parallel path can handle. The rows are claimed as ctid ranges of a
// single heap and each range is written as a part of its own, so anything else falls back to
// the single process path. Where the parts end up is checked separately, see
// parallel_copy_to_destination.
fn parallel_copy_to_relation(p_stmt: &PgBox<PlannedStmt>) -> Option<PgRelation> {
    // COPY (SELECT ..) TO has no relation to split into ctid ranges. This is also what stops
    // the per range COPY statements from recursing back into the parallel path.
    if !copy_stmt_has_relation(p_stmt) {
        return None;
    }

    // a parallel worker cannot start a parallel operation of its own
    if unsafe { pg_sys::IsInParallelMode() } {
        return None;
    }

    let relation = unsafe {
        PgRelation::with_lock(copy_stmt_relation_oid(p_stmt), copy_stmt_lock_mode(p_stmt))
    };

    let relation_form = unsafe { *relation.rd_rel };

    // ctid ranges are a heap concept, and a temporary relation is not visible to a worker
    if relation_form.relkind != RELKIND_RELATION as c_char
        || relation_form.relam != HEAP_TABLE_AM_OID
        || relation_form.relpersistence == RELPERSISTENCE_TEMP as c_char
    {
        return None;
    }

    // row level security rewrites the scan into a subquery holding the policy quals, which
    // a ctid range qual can no longer be pushed into as a tid range scan
    if relation_form.relrowsecurity || relation_form.relforcerowsecurity {
        return None;
    }

    // the statement of a range reads ctid, which needs select on the relation itself, while
    // the command it comes from only needs it on the columns it names. A role that holds the
    // column privileges alone keeps the single process path, rather than being refused the
    // copy it is allowed to run.
    let allowed = unsafe {
        pg_class_aclcheck(relation.oid(), GetUserId(), ACL_SELECT as _) == AclResult::ACLCHECK_OK
    };

    if !allowed {
        return None;
    }

    Some(relation)
}

// ParallelCopyToDestination is where what the participants wrote ends up
enum ParallelCopyToDestination<'a> {
    // spliced into a local file, which is also where the parts sit
    LocalFile(String),
    // written as complete files of the folder the copy was given, each by the participant
    // that wrote the rows in it, which is what a copy with file_size_bytes writes. Nothing
    // is spliced and no part is uploaded: a file of a folder destination is a destination of
    // its own as far as the range that writes it is concerned.
    SplitFiles(String),
    // written by the participants themselves, into the parts of the destination's own upload,
    // so that every byte of the copy is uploaded once and by the process that wrote it. The
    // leader only adds the footer that says where the rows of each range ended up.
    SharedUpload(SharedUploadDestination<'a>),
}

// SharedUploadDestination is the destination's own multipart upload, which every participant
// of the copy uploads the ranges it claims into
struct SharedUploadDestination<'a> {
    uri_info: &'a ParsedUriInfo,
    store: Arc<dyn MultipartStore>,
    path: ObjectStorePath,
    upload_id: MultipartId,
}

// start_shared_upload opens the destination's own upload, which the participants write the
// rows they copy straight into
fn start_shared_upload(uri_info: &ParsedUriInfo) -> Option<ParallelCopyToDestination<'_>> {
    // a store with no multipart upload interface is one no part of it can be uploaded by a
    // process other than the one that completes it, which the parallel path is built on
    let (store, path) = get_or_create_multipart_store(uri_info)?;

    let upload_id = block_on(store.create_multipart(&path)).unwrap_or_else(|e| {
        panic!(
            "failed to start the upload of \"{}\": {}",
            uri_info.uri,
            object_store_error_message(e)
        )
    });

    Some(ParallelCopyToDestination::SharedUpload(
        SharedUploadDestination {
            uri_info,
            store,
            path,
            upload_id,
        },
    ))
}

// parallel_copy_to_destination returns where the participants of a copy into the given uri
// write, or None when the uri is not one the parallel path can write.
fn parallel_copy_to_destination(
    uri_info: &ParsedUriInfo,
    split_files: bool,
) -> Option<ParallelCopyToDestination<'_>> {
    // a program takes its rows on a pipe, which no part of can be written on its own
    if uri_info.is_program {
        return None;
    }

    // a COPY TO STDOUT writes a temporary file that the single process path streams to the
    // client once the copy is over. The parallel path returns to the hook with the rows
    // already written, which skips that streaming and sends the client nothing, so the
    // destination is left to the single process path.
    if uri_info.is_std_inout() {
        return None;
    }

    // a file of a folder destination is written by a single range from its header to its
    // footer, the way a single process copy writes it, so any uri a single process copy can
    // write is one the participants can fill a folder of
    if split_files {
        return Some(ParallelCopyToDestination::SplitFiles(uri_as_string(
            &uri_info.uri,
        )));
    }

    match uri_info.scheme {
        ObjectStoreScheme::Local => {
            let output_file = uri_as_string(&uri_info.uri);

            // the splice writes the destination itself instead of going through the object
            // store, and it reports a directory destination in its own words, so the error
            // is left to the single process path
            if Path::new(&output_file).is_dir() {
                return None;
            }

            Some(ParallelCopyToDestination::LocalFile(output_file))
        }
        ObjectStoreScheme::AmazonS3
        | ObjectStoreScheme::MicrosoftAzure
        | ObjectStoreScheme::GoogleCloudStorage => {
            // a part uri is the destination uri with the part's name appended, which a
            // query or a fragment would end up inside of, naming the destination itself
            if uri_info.uri.query().is_some() || uri_info.uri.fragment().is_some() {
                return None;
            }

            start_shared_upload(uri_info)
        }
        // the remaining schemes, e.g. an http uri, take no multipart upload, which is what
        // the participants write the rows of their ranges into
        _ => None,
    }
}

// range_size_in_pages is how many pages a single range covers. The ranges are spread over the
// participants first, and then grown to hold the row group size the command asked for, because
// a range is written into a part file of its own and its rows cannot share a row group with
// another range's. The growth stops at a range per participant: a row group of the asked for
// size is worth coarser ranges, but not the fan-out itself.
fn range_size_in_pages(
    p_stmt: &PgBox<PlannedStmt>,
    relation: &PgRelation,
    nblocks: u32,
    nparticipants: u32,
) -> u32 {
    let pages_per_range = (nblocks / (nparticipants * RANGES_PER_PARTICIPANT)).max(1);

    // the default row group size is pg_parquet's own choice rather than something the
    // command asked for, so the fan-out is free to write smaller row groups than it
    let Some(row_group_size) = copy_to_stmt_explicit_row_group_size(p_stmt) else {
        return pages_per_range;
    };

    let (reltuples, relpages) = unsafe {
        let relation_form = *relation.rd_rel;

        (
            relation_form.reltuples as f64,
            relation_form.relpages as f64,
        )
    };

    // never analyzed, so there is no estimate to grow the ranges by
    if reltuples <= 0.0 || relpages <= 0.0 {
        return pages_per_range;
    }

    let pages_per_row_group = (row_group_size as f64 * relpages / reltuples).ceil();

    // a relation that cannot give every participant a row group of the asked for size writes
    // row groups short of it, the way the tail of a single process copy does. The row groups
    // never run over it, because the writer flushes one as soon as it fills.
    let pages_per_participant = (nblocks / nparticipants).max(1);

    pages_per_range.max((pages_per_row_group as u32).min(pages_per_participant))
}

// unfinished_write is what the cleanup of the copy gives up when the copy does not finish:
// the part files a local destination is spliced from, or the upload the participants of an
// object store destination write the parts of.
fn unfinished_write(
    part_prefix: &str,
    destination: &ParallelCopyToDestination,
) -> Option<UnfinishedWrite> {
    match destination {
        ParallelCopyToDestination::LocalFile(_) => {
            Some(UnfinishedWrite::PartFiles(part_prefix.to_string()))
        }
        // a folder destination has nothing of its own to give up here: the files in it are
        // created by the split dest receiver of the participant that writes them, which
        // registers and gives them up itself, the way a single process copy does
        ParallelCopyToDestination::SplitFiles(_) => None,
        ParallelCopyToDestination::SharedUpload(destination) => {
            Some(UnfinishedWrite::SharedUpload(
                destination.store.clone(),
                destination.path.clone(),
                destination.upload_id.clone(),
            ))
        }
    }
}

// part_uri_prefix is the uri the parts of the copy are named after. The parts sit next to
// the destination, which keeps a part of an object store destination in the object store as
// well, so that nothing of the copy is written to local disk. The leader pid is part of the
// name, so that two copies that run at the same time into the same destination cannot write
// each other's parts and splice a mix of the two into the destination.
fn part_uri_prefix(destination: &ParallelCopyToDestination) -> String {
    let leader_pid = unsafe { pg_sys::MyProcPid };

    match destination {
        ParallelCopyToDestination::LocalFile(output_file) => {
            format!("{output_file}.{leader_pid}")
        }
        // the ranges of a folder destination write files of the destination itself rather
        // than parts to be spliced, so there is no part to name
        ParallelCopyToDestination::SplitFiles(_) => String::new(),
        ParallelCopyToDestination::SharedUpload(destination) => {
            format!("{}.{leader_pid}", destination.uri_info.uri)
        }
    }
}

// run_parallel_copy_to sets up the DSM, launches the workers, claims ctid ranges in the
// leader as well, and splices the part files into the destination. It returns the number of
// rows written.
fn run_parallel_copy_to(
    nworkers: i32,
    nranges: u32,
    pages_per_range: u32,
    statement: &str,
    part_prefix: &str,
    destination: &ParallelCopyToDestination,
    cleanup: Option<CleanupId>,
) -> u64 {
    let shared_upload = match destination {
        ParallelCopyToDestination::SharedUpload(destination) => Some(destination),
        _ => None,
    };

    let split_files = matches!(destination, ParallelCopyToDestination::SplitFiles(_));

    // the uri a participant writes: the destination's own upload for a shared upload, and
    // the folder the files of a range go into for a folder destination
    let (upload_uri, upload_id) = match destination {
        ParallelCopyToDestination::SharedUpload(destination) => (
            uri_as_string(&destination.uri_info.uri),
            destination.upload_id.clone(),
        ),
        ParallelCopyToDestination::SplitFiles(folder) => (folder.clone(), String::new()),
        ParallelCopyToDestination::LocalFile(_) => (String::new(), String::new()),
    };

    let strings = [
        statement,
        part_prefix,
        upload_uri.as_str(),
        upload_id.as_str(),
    ];

    // the participants of a shared upload report what they uploaded over a queue of their
    // own, which only the workers need: the leader hands its own report over directly
    let nqueues = if shared_upload.is_some() {
        nworkers as usize
    } else {
        0
    };

    let (pcxt, shared, handles) = unsafe {
        pg_sys::EnterParallelMode();

        let pcxt = pg_sys::CreateParallelContext(
            c"pg_parquet".as_ptr(),
            PARALLEL_COPY_TO_WORKER_FUNCTION.as_ptr(),
            nworkers,
        );

        let shared_size = ParallelCopyToShared::size_with_strings(strings);
        let queues_size = QUEUE_SIZE * nqueues;

        // shm_toc_estimate_chunk and shm_toc_estimate_keys are macros, so they are spelled
        // out here. The chunks are rounded up because shm_toc_allocate aligns what it hands
        // out, and estimating too much shared memory is harmless.
        (*pcxt).estimator.space_for_chunks += shared_size.next_multiple_of(64);
        (*pcxt).estimator.number_of_keys += 1;

        if nqueues > 0 {
            (*pcxt).estimator.space_for_chunks += queues_size.next_multiple_of(64);
            (*pcxt).estimator.number_of_keys += 1;
        }

        // serializes the leader's snapshot, GUCs, combo CIDs and transaction state into the
        // segment, which is what lets a worker see exactly the rows the leader sees
        pg_sys::InitializeParallelDSM(pcxt);

        let shared =
            pg_sys::shm_toc_allocate((*pcxt).toc, shared_size) as *mut ParallelCopyToShared;

        pg_atomic_init_u32(&mut (*shared).next_range, 0);
        pg_atomic_init_u64(&mut (*shared).next_part_and_offset, 0);
        (*shared).nranges = nranges;
        (*shared).pages_per_range = pages_per_range;
        (*shared).shared_upload = shared_upload.is_some();
        (*shared).split_files = split_files;
        pg_atomic_init_u32(&mut (*shared).next_file_id, 0);
        pg_atomic_init_u64(&mut (*shared).nprocessed, 0);
        ParallelCopyToShared::write_strings(shared, strings);

        pg_sys::shm_toc_insert((*pcxt).toc, PARALLEL_COPY_TO_SHARED_KEY, shared as _);

        let queues = if nqueues > 0 {
            let queues = pg_sys::shm_toc_allocate((*pcxt).toc, queues_size);

            for worker in 0..nqueues {
                let queue = pg_sys::shm_mq_create(queue_at(queues, worker), QUEUE_SIZE);
                pg_sys::shm_mq_set_receiver(queue, pg_sys::MyProc);
            }

            pg_sys::shm_toc_insert((*pcxt).toc, PARALLEL_COPY_TO_QUEUES_KEY, queues);

            queues
        } else {
            std::ptr::null_mut()
        };

        pg_sys::LaunchParallelWorkers(pcxt);

        // only the workers of a shared upload have a queue, and only the ones that were
        // launched: a worker the pool was short of reports nothing because it runs nothing
        let nlaunched = if queues.is_null() {
            0
        } else {
            (*pcxt).nworkers_launched as usize
        };

        let handles = (0..nlaunched)
            .map(|worker| {
                pg_sys::shm_mq_attach(
                    queue_at(queues, worker) as *mut shm_mq,
                    (*pcxt).seg,
                    (*pcxt).worker.add(worker).read().bgwhandle,
                )
            })
            .collect::<Vec<_>>();

        (pcxt, shared, handles)
    };

    let claimer: Arc<dyn PartClaim> = Arc::new(DsmPartClaim(shared));

    let (participants, last_claim, nprocessed_by_participants, files_written) =
        PgTryBuilder::new(|| {
            // the leader claims ranges too, so a command that could not launch a single
            // worker still completes, just without the fan-out
            let leader = claim_and_copy_ranges(shared, &claimer);

            // a participant reports only once every range is claimed, which is also when the
            // leader is done claiming, so nothing of the leader's own share of the copy waits on
            // a report arriving
            let mut participants = receive_participant_uploads(&handles);

            participants.extend(leader);

            unsafe { pg_sys::WaitForParallelWorkersToFinish(pcxt) };

            // the claim of the leader's own last part is taken here, while the shared memory the
            // claims are handed out of is still mapped: the teardown below unmaps it, and
            // completing the upload comes after the teardown
            let last_claim = claimer.claim(0);

            // read here for the same reason, and only once every participant has exited, which
            // is what makes the rows every one of them copied visible to this read
            let nprocessed_by_participants =
                unsafe { pg_atomic_read_u64(&mut (*shared).nprocessed) };

            // the id the next file of a folder destination would have taken, which is how many
            // files the participants wrote between them
            let files_written = unsafe { pg_atomic_read_u32(&mut (*shared).next_file_id) } as i64;

            (
                participants,
                last_claim,
                nprocessed_by_participants,
                files_written,
            )
        })
        // terminates the workers and waits for them to exit, which an interrupted copy has
        // to do before its caller removes the part files, or a worker still running writes
        // a part file after the removal
        .finally(|| unsafe {
            for handle in &handles {
                pg_sys::shm_mq_detach(*handle);
            }

            pg_sys::DestroyParallelContext(pcxt);
        })
        .execute();

    unsafe { pg_sys::ExitParallelMode() };

    // every range wrote a part, even one that found no tuple at all, which holds nothing
    // but the schema and contributes no row group of its own
    let nprocessed = match destination {
        // the part files are spliced in range order, so the spliced file holds the rows of
        // range 0 first
        ParallelCopyToDestination::LocalFile(output_file) => {
            let part_files = (0..nranges)
                .map(|range| part_uri(part_prefix, range))
                .collect::<Vec<_>>();

            concat_parquet_files(&part_files, output_file)
                .unwrap_or_else(|e| panic!("failed to write \"{output_file}\": {e}"))
        }
        // nothing is spliced here either: the files of the folder are complete as the ranges
        // wrote them, so the rows the participants reported copying are the whole result
        ParallelCopyToDestination::SplitFiles(folder) => {
            // the folder is left holding the files of a previous and larger copy, which are
            // no part of this copy's result
            remove_files_of_a_previous_copy(folder, files_written);

            nprocessed_by_participants as i64
        }
        // nothing is spliced: the participants uploaded the rows they wrote into the
        // destination itself, so only the footer that covers them is left to write
        ParallelCopyToDestination::SharedUpload(upload) => {
            let nprocessed = block_on(complete_shared_upload(
                upload.store.clone(),
                upload.path.clone(),
                upload.upload_id.clone(),
                last_claim,
                participants,
                nranges,
            ))
            .unwrap_or_else(|e| panic!("failed to write \"{}\": {e}", upload.uri_info.uri));

            // the upload is the destination now, so the cleanup of the copy must not abort it
            finish_unfinished_write(
                cleanup.expect("the upload of a parallel COPY TO is not registered for cleanup"),
            );

            nprocessed
        }
    };

    nprocessed as u64
}

// queue_at is the address of one worker's queue inside the chunk that holds all of them
fn queue_at(queues: *mut c_void, worker: usize) -> *mut c_void {
    unsafe { (queues as *mut u8).add(worker * QUEUE_SIZE) as _ }
}

// receive_participant_uploads takes the report of every worker that has one, waiting for it.
// A worker sends its report once no range is left to claim, so the leader is only ever
// waiting on a worker that is still writing a range it already claimed.
fn receive_participant_uploads(handles: &[*mut shm_mq_handle]) -> Vec<ParticipantUpload> {
    handles
        .iter()
        .filter_map(|handle| receive_participant_upload(*handle))
        .collect()
}

// receive_participant_upload takes one worker's report off its queue. It returns None for a
// worker that detached without sending one, which a worker that failed does, and whose
// failure the leader learns of from WaitForParallelWorkersToFinish instead.
fn receive_participant_upload(handle: *mut shm_mq_handle) -> Option<ParticipantUpload> {
    let mut nbytes = 0;
    let mut data = std::ptr::null_mut();

    let nowait = false;
    let result = unsafe { pg_sys::shm_mq_receive(handle, &mut nbytes, &mut data, nowait) };

    if result != shm_mq_result::SHM_MQ_SUCCESS {
        return None;
    }

    let report = unsafe { std::slice::from_raw_parts(data as *const u8, nbytes) };

    Some(decode_participant_upload(report))
}

// pg_parquet_parallel_copy_to_worker is the entry point of a parallel COPY TO worker. It is
// resolved by name out of pg_parquet's shared library, so its symbol has to stay.
#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn pg_parquet_parallel_copy_to_worker(
    seg: *mut dsm_segment,
    toc: *mut shm_toc,
) {
    let shared = unsafe {
        pg_sys::shm_toc_lookup(toc, PARALLEL_COPY_TO_SHARED_KEY, false) as *mut ParallelCopyToShared
    };

    // a worker only has a queue when it writes into the destination's own upload, which is
    // the only case it has anything to report
    let handle = unsafe { (*shared).shared_upload }.then(|| unsafe {
        let queues = pg_sys::shm_toc_lookup(toc, PARALLEL_COPY_TO_QUEUES_KEY, false);
        let worker = pg_sys::ParallelWorkerNumber as usize;

        let queue = queue_at(queues, worker) as *mut shm_mq;
        pg_sys::shm_mq_set_sender(queue, pg_sys::MyProc);

        pg_sys::shm_mq_attach(queue, seg, std::ptr::null_mut())
    });

    let claimer: Arc<dyn PartClaim> = Arc::new(DsmPartClaim(shared));

    let upload = claim_and_copy_ranges(shared, &claimer);

    if let Some(handle) = handle {
        if let Some(upload) = upload {
            send_participant_upload(handle, &upload);
        }

        unsafe { pg_sys::shm_mq_detach(handle) };
    }
}

// send_participant_upload hands the leader what this worker uploaded. The report is larger
// than the queue when the worker has a tail to hand over, which shm_mq sends in chunks, so
// the worker waits here until the leader took all of it.
fn send_participant_upload(handle: *mut shm_mq_handle, upload: &ParticipantUpload) {
    let report = encode_participant_upload(upload);

    let mut iov = [shm_mq_iovec {
        data: report.as_ptr() as *const c_char,
        len: report.len(),
    }];

    // postgres 15 gave shm_mq_sendv a force_flush argument
    #[cfg(feature = "pg14")]
    let result = unsafe { pg_sys::shm_mq_sendv(handle, iov.as_mut_ptr(), iov.len() as _, false) };
    #[cfg(not(feature = "pg14"))]
    let result =
        unsafe { pg_sys::shm_mq_sendv(handle, iov.as_mut_ptr(), iov.len() as _, false, true) };

    // the leader stops reading when the command it belongs to is over, which is a failure
    // the leader itself reports, so there is nothing for the worker to do about it
    let _ = result;
}

// claim_and_copy_ranges claims ctid ranges one by one and writes the rows of each range into
// its own part, until no range is left. A copy into a destination that takes a shared upload
// writes the ranges into the destination's own upload instead, and returns what it uploaded,
// and a copy into a folder destination writes complete files of that folder.
fn claim_and_copy_ranges(
    shared: *mut ParallelCopyToShared,
    claimer: &Arc<dyn PartClaim>,
) -> Option<ParticipantUpload> {
    let upload = unsafe { (*shared).shared_upload }.then(|| {
        let destination = unsafe { ParallelCopyToShared::destination(shared) };
        let upload_id = unsafe { ParallelCopyToShared::upload_id(shared) };

        let uri_info = ParsedUriInfo::try_from(destination.as_str())
            .unwrap_or_else(|e| panic!("failed to parse uri \"{destination}\": {e}"));

        let (store, path) = get_or_create_multipart_store(&uri_info).unwrap_or_else(|| {
            panic!("\"{destination}\" cannot be written by several processes at once")
        });

        SharedUpload::new(store, path, upload_id, claimer.clone())
    });

    // the participants of a folder destination fill one folder without knowing of each
    // other, so the ids they name their files after are claimed from shared memory. The
    // claim is given up before this participant returns, since it points into the shared
    // memory of this copy alone.
    if unsafe { (*shared).split_files } {
        install_file_id_claim(Rc::new(DsmFileIdClaim(shared)));
    }

    let copy_ranges = AssertUnwindSafe(|| copy_claimed_ranges(shared, upload.as_ref()));

    PgTryBuilder::new(copy_ranges)
        .finally(uninstall_file_id_claim)
        .execute();

    let upload = upload?;

    Some(
        block_on(upload.finish())
            .unwrap_or_else(|e| panic!("failed to upload the rows of the parallel COPY TO: {e}")),
    )
}

// copy_claimed_ranges copies every range this participant claims, into the part of the
// range, into the destination's own upload, or into a file of the folder destination.
fn copy_claimed_ranges(shared: *mut ParallelCopyToShared, upload: Option<&SharedUpload>) {
    let (nranges, pages_per_range) = unsafe { ((*shared).nranges, (*shared).pages_per_range) };

    let split_files = unsafe { (*shared).split_files };

    let statement = unsafe { ParallelCopyToShared::statement(shared) };
    let part_prefix = unsafe { ParallelCopyToShared::part_prefix(shared) };

    // the folder the files of every range go into, which is the destination itself
    let folder = split_files.then(|| unsafe { ParallelCopyToShared::destination(shared) });

    // every range's COPY leaves its own allocations behind, so they are freed per range
    // rather than held until the command ends
    let mut range_memory_context = PgMemoryContexts::new("pg_parquet parallel copy to range");

    loop {
        let range = unsafe { pg_atomic_fetch_add_u32(&mut (*shared).next_range, 1) };

        if range >= nranges {
            break;
        }

        // the ranges cover every block of the relation, and the last one reaches past its
        // end, which costs nothing because a tuple written into a new block after the
        // snapshot was taken is not visible to it anyway
        let first_block = range * pages_per_range;
        let last_block = first_block + pages_per_range;

        // a range of a folder destination writes files of the folder itself, each complete
        // and named after an id no other range takes
        let part_uri = match folder.as_ref() {
            Some(folder) => folder.clone(),
            None => part_uri(&part_prefix, range),
        };

        let range_statement = statement
            .replace(FIRST_BLOCK_PLACEHOLDER, &first_block.to_string())
            .replace(LAST_BLOCK_PLACEHOLDER, &last_block.to_string())
            .replace(PART_URI_PLACEHOLDER, &quote_literal(&part_uri));

        // the range's copy renders the part uri as its destination like any other copy does,
        // and writing into the shared upload rather than into that part is what the upload
        // being installed for the uri turns it into
        if let Some(upload) = upload {
            install_shared_upload(part_uri, upload);
        }

        let nprocessed = unsafe {
            let nprocessed =
                range_memory_context.switch_to(|_| execute_copy_to_statement(&range_statement));

            range_memory_context.reset();

            nprocessed
        };

        // the files of a folder destination are finished by the range that wrote them, so
        // the rows of the range are counted here rather than when the destination is written
        if split_files {
            unsafe { pg_atomic_fetch_add_u64(&mut (*shared).nprocessed, nprocessed as i64) };
        }

        if let Some(upload) = upload {
            let metadata = take_range_metadata()
                .expect("the parallel COPY TO range wrote no parquet metadata");

            upload.end_range(range, metadata);

            uninstall_shared_upload();
        }
    }
}

// the report of a participant of a shared upload, as it travels over the queue:
//
//   u32 nparts,   nparts  x { u32 part, u32 content_id_len, content_id }
//   u32 nranges,  nranges x { u32 range, u32 nregions, nregions x Region,
//                             u32 metadata_len, metadata }
//   u32 tail_len, tail
//
// Region is written as it is laid out in memory, which the leader and the workers of one
// command agree on: they are the same build of the same library.
fn encode_participant_upload(upload: &ParticipantUpload) -> Vec<u8> {
    let mut report = Vec::new();

    push_u32(&mut report, upload.parts.len() as u32);

    for (part, part_id) in &upload.parts {
        push_u32(&mut report, *part);
        push_bytes(&mut report, part_id.content_id.as_bytes());
    }

    push_u32(&mut report, upload.ranges.len() as u32);

    for range in &upload.ranges {
        push_u32(&mut report, range.range);

        push_u32(&mut report, range.regions.len() as u32);
        report.extend_from_slice(regions_as_bytes(&range.regions));

        let metadata = encode_range_metadata(&range.metadata).unwrap_or_else(|e| {
            panic!("failed to write the metadata of a parallel COPY TO range: {e}")
        });

        push_bytes(&mut report, &metadata);
    }

    push_bytes(&mut report, &upload.tail);

    report
}

fn decode_participant_upload(mut report: &[u8]) -> ParticipantUpload {
    let nparts = take_u32(&mut report);

    let mut parts = Vec::with_capacity(nparts as usize);

    for _ in 0..nparts {
        let part = take_u32(&mut report);
        let content_id = String::from_utf8(take_bytes(&mut report).to_vec())
            .expect("a parallel COPY TO part id is not utf8");

        parts.push((part, PartId { content_id }));
    }

    let nranges = take_u32(&mut report);

    let mut ranges = Vec::with_capacity(nranges as usize);

    for _ in 0..nranges {
        let range = take_u32(&mut report);

        let nregions = take_u32(&mut report);
        let regions = take_regions(&mut report, nregions as usize);

        let encoded = Bytes::copy_from_slice(take_bytes(&mut report));

        let metadata = decode_range_metadata(encoded).unwrap_or_else(|e| {
            panic!("failed to read the metadata of a parallel COPY TO range: {e}")
        });

        ranges.push(RangeUpload {
            range,
            regions,
            metadata,
        });
    }

    let tail = take_bytes(&mut report).to_vec();

    assert!(
        report.is_empty(),
        "a parallel COPY TO report is longer than what it holds"
    );

    ParticipantUpload {
        parts,
        ranges,
        tail,
    }
}

fn push_u32(report: &mut Vec<u8>, value: u32) {
    report.extend_from_slice(&value.to_ne_bytes());
}

fn push_bytes(report: &mut Vec<u8>, bytes: &[u8]) {
    push_u32(report, bytes.len() as u32);
    report.extend_from_slice(bytes);
}

fn take_u32(report: &mut &[u8]) -> u32 {
    let (value, rest) = report.split_at(size_of::<u32>());
    *report = rest;

    u32::from_ne_bytes(
        value
            .try_into()
            .expect("a parallel COPY TO report is short"),
    )
}

fn take_bytes<'a>(report: &mut &'a [u8]) -> &'a [u8] {
    let len = take_u32(report) as usize;

    let (bytes, rest) = report.split_at(len);
    *report = rest;

    bytes
}

fn regions_as_bytes(regions: &[Region]) -> &[u8] {
    unsafe { std::slice::from_raw_parts(regions.as_ptr() as *const u8, size_of_val(regions)) }
}

fn take_regions(report: &mut &[u8], nregions: usize) -> Vec<Region> {
    let len = nregions * size_of::<Region>();

    let (bytes, rest) = report.split_at(len);
    *report = rest;

    let mut regions = Vec::with_capacity(nregions);

    // the bytes of the queue are not aligned the way a Region is, so they are copied into one
    // rather than read in place
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), regions.as_mut_ptr() as *mut u8, len);
        regions.set_len(nregions);
    }

    regions
}

// execute_copy_to_statement runs a rendered COPY TO parquet statement in this backend. It
// calls pg_parquet's COPY TO handler directly rather than going through ProcessUtility,
// since a parallel operation cannot open the subtransaction that SPI would want.
fn execute_copy_to_statement(statement: &str) -> u64 {
    let statement = CString::new(statement).expect("COPY TO statement has an interior nul byte");

    unsafe {
        let raw_stmts = PgList::<RawStmt>::from_pg(pg_sys::pg_parse_query(statement.as_ptr()));

        let raw_stmt = raw_stmts
            .get_ptr(0)
            .expect("COPY TO statement did not parse into a statement");

        let mut p_stmt = PgBox::<PlannedStmt>::alloc_node(NodeTag::T_PlannedStmt);
        p_stmt.commandType = CmdType::CMD_UTILITY;
        p_stmt.canSetTag = true;
        p_stmt.utilityStmt = (*raw_stmt).stmt;
        p_stmt.stmt_location = (*raw_stmt).stmt_location;
        p_stmt.stmt_len = (*raw_stmt).stmt_len;

        let p_stmt = PgBox::<PlannedStmt>::from_pg(p_stmt.into_pg());

        let params = PgBox::<pg_sys::ParamListInfoData>::from_pg(std::ptr::null_mut());
        let query_env = PgBox::<pg_sys::QueryEnvironment>::from_pg(std::ptr::null_mut());

        process_copy_to_parquet(&p_stmt, &statement, &params, &query_env)
    }
}

// render_range_copy_to_statement renders the COPY statement a single ctid range is copied
// with, leaving the range bounds and the part file as placeholders for the participant that
// claims the range. It returns None when an option cannot be rendered back into a statement,
// so that a command is never written with options other than the ones it was given.
fn render_range_copy_to_statement(
    p_stmt: &PgBox<PlannedStmt>,
    relation: &PgRelation,
    uri_info: &ParsedUriInfo,
) -> Option<String> {
    let options = render_copy_to_options(p_stmt, uri_info)?;

    let attributes = render_attribute_list(p_stmt);

    let relation_name = unsafe {
        let relation_name = quote_qualified_identifier(
            relation.namespace().as_pg_cstr(),
            relation.name().as_pg_cstr(),
        );

        cstr_to_string(relation_name)
    };

    // the placeholders are filled in by replacing their text in the rendered statement, so a
    // column, a relation, a schema or an option value whose name holds the text of one would
    // be replaced along with them and rewrite the statement into a command of its own. Such a
    // copy is left to the single process path.
    if [
        attributes.as_str(),
        relation_name.as_str(),
        options.as_str(),
    ]
    .iter()
    .any(|rendered| {
        PLACEHOLDERS
            .iter()
            .any(|placeholder| rendered.contains(placeholder))
    }) {
        return None;
    }

    // ONLY matches what the single process path does, which builds the SELECT without the
    // inheritance flag for a plain relation, so an inheritance child is not copied along
    Some(format!(
        "COPY (SELECT {attributes} FROM ONLY {relation_name} WHERE \
         ctid >= '({FIRST_BLOCK_PLACEHOLDER},0)'::tid AND \
         ctid < '({LAST_BLOCK_PLACEHOLDER},0)'::tid) \
         TO {PART_URI_PLACEHOLDER} ({options})"
    ))
}

// render_attribute_list renders the column list of the COPY TO command, or * when the
// command names no columns.
fn render_attribute_list(p_stmt: &PgBox<PlannedStmt>) -> String {
    let attribute_list = unsafe { PgList::<Node>::from_pg(copy_stmt_attribute_list(p_stmt)) };

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

// render_copy_to_options renders the options of the COPY TO command back into an option list.
// The format, the compression and the compression level are always rendered, from their
// resolved values, because they would otherwise be inferred from the extension of the part
// file rather than from the file the user named. file_size_bytes is rendered from its
// resolved value as well, since it is the one option given in units the renderer does not
// hand back, e.g. '1MB'.
fn render_copy_to_options(p_stmt: &PgBox<PlannedStmt>, uri_info: &ParsedUriInfo) -> Option<String> {
    let copy_stmt = unsafe { PgBox::<CopyStmt>::from_pg(p_stmt.utilityStmt as _) };

    let mut rendered_options = vec!["format 'parquet'".to_string()];

    for option in unsafe { PgList::<DefElem>::from_pg(copy_stmt.options) }.iter_ptr() {
        let option_name = unsafe { cstr_to_string((*option).defname) };

        // rendered from the resolved values below
        if [
            "format",
            "compression",
            "compression_level",
            "file_size_bytes",
        ]
        .contains(&option_name.as_str())
        {
            continue;
        }

        // an option without an argument is not something we can render back
        if unsafe { (*option).arg.is_null() } {
            return None;
        }

        let option_value = unsafe { cstr_to_string(defGetString(option)) };

        if NUMBER_VALUED_COPY_TO_OPTIONS.contains(&option_name.as_str()) {
            rendered_options.push(format!("{option_name} {option_value}"));
        } else if STRING_VALUED_COPY_TO_OPTIONS.contains(&option_name.as_str()) {
            rendered_options.push(format!("{option_name} {}", quote_literal(&option_value)));
        } else {
            return None;
        }
    }

    let file_size_bytes = copy_to_stmt_file_size_bytes(p_stmt);

    if file_size_bytes != INVALID_FILE_SIZE_BYTES {
        rendered_options.push(format!(
            "file_size_bytes {}",
            quote_literal(&file_size_bytes.to_string())
        ));
    }

    let compression = copy_to_stmt_compression(p_stmt, uri_info).to_string();
    rendered_options.push(format!("compression {}", quote_literal(&compression)));

    if let Some(compression_level) = copy_to_stmt_compression_level(p_stmt, uri_info) {
        rendered_options.push(format!("compression_level {compression_level}"));
    }

    Some(rendered_options.join(", "))
}

// part_uri is the uri the rows of the given ctid range are written into
fn part_uri(part_prefix: &str, range: u32) -> String {
    format!("{part_prefix}{PART_URI_INFIX}{range}.parquet")
}

fn quote_literal(value: &str) -> String {
    unsafe { cstr_to_string(quote_literal_cstr(value.as_pg_cstr())) }
}

unsafe fn cstr_to_string(cstr: *const c_char) -> String {
    CStr::from_ptr(cstr)
        .to_str()
        .expect("string is not valid utf8")
        .to_string()
}
