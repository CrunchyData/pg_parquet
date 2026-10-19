//! Where a statement's bytes come from or go to.
//!
//! A `COPY` names one of three things: a uri, the client connection (`TO STDOUT` / `FROM
//! STDIN`), or a program to pipe through. Only the first is somewhere a provider can be pointed
//! at — the other two are byte streams with no random access, and a format that reads a footer
//! or rewrites a header cannot be written straight into one.
//!
//! So both are served the way core serves a program: the bytes pass through a temporary file
//! Postgres owns, and the transfer at the far end is a byte copy. That copy is the same for
//! parquet, Arrow IPC and avro, which is why it is here rather than in each provider — and it is
//! the reason these three forms are not simply declined. Core can carry out `COPY t TO STDOUT`,
//! but not in a format core does not have.
//!
//! The protocol halves ([`send_to_client`], [`fill_from_client`]) are adapted from core's
//! `SendCopyBegin`, `ReceiveCopyBegin` and `CopyGetData`: a client cannot tell that these
//! messages did not come from core's own `COPY`, which is what makes `psql`'s `\copy` work.

use std::ffi::CStr;
use std::fs::File as StdFile;
use std::io::{Read, Write};
use std::os::fd::FromRawFd;

use pgrx::pg_sys::{
    get_role_oid, has_privs_of_role, makeStringInfo, pq_beginmessage, pq_copymsgbytes,
    pq_endmessage, pq_getmsgstring, pq_putemptymessage, pq_sendbyte, pq_sendbytes, pq_sendint16,
    superuser, wait_result_to_str, AsPgCStr, ClosePipeStream, DataDir, File, FileClose,
    FilePathName, GetUserId, OpenPipeStream, OpenTemporaryFile, QueryCancelHoldoffCount,
    StringInfo, PG_BINARY_R, PG_BINARY_W,
};
use pgrx::prelude::*;

use crate::object_store;

/// What the statement named.
#[derive(Clone, Debug)]
pub(crate) enum Endpoint {
    /// `COPY ... TO/FROM '<uri>'`: written and read in place, with no temporary file anywhere.
    Uri(String),
    /// `COPY ... TO STDOUT` / `COPY ... FROM STDIN`.
    Stdio,
    /// `COPY ... TO/FROM PROGRAM '<command>'`, the command as written.
    Program(String),
}

impl Endpoint {
    /// The uri's own path, for inferring a format from an extension. Empty for the other two:
    /// neither has a name to infer from, which is why both require an explicit format.
    pub(crate) fn path_for_inference(&self) -> &str {
        match self {
            Endpoint::Uri(uri) => uri,
            Endpoint::Stdio | Endpoint::Program(_) => "",
        }
    }

    /// Refuses the statement if the role may not reach this endpoint.
    ///
    /// A uri is tier 1's question. A program is core's — `pg_execute_server_program` — asked here
    /// because this hook runs before the `standard_ProcessUtility` that would otherwise ask it.
    /// `STDIN` and `STDOUT` need no privilege: the bytes go to the client that asked for them.
    pub(crate) fn check_privilege(&self, is_from: bool) {
        match self {
            Endpoint::Uri(uri) => {
                if !object_store::check_privilege(uri, is_from) {
                    let direction = if is_from { "read" } else { "write" };

                    ereport!(
                        PgLogLevel::ERROR,
                        PgSqlErrorCode::ERRCODE_INSUFFICIENT_PRIVILEGE,
                        format!("permission denied to {direction} {uri}"),
                        "Remote uris need object_store_read or object_store_write; a path on the \
                         server needs pg_read_server_files or pg_write_server_files."
                    );
                }
            }
            Endpoint::Program(_) => {
                if !is_member_of("pg_execute_server_program") {
                    ereport!(
                        PgLogLevel::ERROR,
                        PgSqlErrorCode::ERRCODE_INSUFFICIENT_PRIVILEGE,
                        "must be superuser or a member of the pg_execute_server_program role to \
                         COPY to or from an external program",
                        "Anyone can COPY to stdout or from stdin. psql's \\copy command also \
                         works for anyone."
                    );
                }
            }
            Endpoint::Stdio => {}
        }
    }

    /// Moves a finished file to wherever the statement said, after the provider has written it.
    ///
    /// A no-op for a uri: that one was written in place, and there is nothing to move.
    pub(crate) fn send(&self, path: &str, natts: i16) {
        match self {
            Endpoint::Uri(_) => {}
            Endpoint::Stdio => unsafe { send_to_client(path, natts) },
            Endpoint::Program(program) => send_to_program(program, path),
        }
    }

    /// Fills the temporary file before the provider reads it. A no-op for a uri.
    pub(crate) fn fill(&self, path: &str, natts: i16) {
        match self {
            Endpoint::Uri(_) => {}
            Endpoint::Stdio => unsafe { fill_from_client(path, natts) },
            Endpoint::Program(program) => fill_from_program(program, path),
        }
    }
}

/// The path a provider is pointed at, and the temporary file behind it if there is one.
///
/// The file has to outlive the statement, so the caller keeps it: dropping it closes it, and
/// Postgres removes a closed temporary file.
pub(crate) fn resolve(endpoint: &Endpoint) -> (String, Option<TempFile>) {
    match endpoint {
        Endpoint::Uri(uri) => (uri.clone(), None),
        Endpoint::Stdio | Endpoint::Program(_) => {
            let temp_file = TempFile::open();
            let path = temp_file.path().to_string();

            (path, Some(temp_file))
        }
    }
}

/// A temporary file in the cluster's own temp area, which Postgres removes when it is closed and
/// at the end of the transaction either way — including the one that ends in an error.
pub(crate) struct TempFile {
    fd: File,
    path: String,
}

impl TempFile {
    pub(crate) fn open() -> TempFile {
        let fd = unsafe { OpenTemporaryFile(false) };

        let path = unsafe {
            let data_dir = CStr::from_ptr(DataDir)
                .to_str()
                .expect("the data directory is valid utf-8");

            // FilePathName is already relative to the data directory, e.g.
            // "base/pgsql_tmp/pgsql_tmp<pid>.<n>", so that is the only prefix it needs. Joining a
            // temp tablespace path on top would name a file Postgres does not know about.
            let relative = CStr::from_ptr(FilePathName(fd))
                .to_str()
                .expect("a temporary file's path is valid utf-8");

            std::path::Path::new(data_dir)
                .join(relative)
                .to_string_lossy()
                .into_owned()
        };

        TempFile { fd, path }
    }

    pub(crate) fn path(&self) -> &str {
        &self.path
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        unsafe { FileClose(self.fd) };
    }
}

/// Whether the current role is a superuser or a member of a predefined role.
fn is_member_of(role: &str) -> bool {
    unsafe {
        superuser() || {
            let role_oid = get_role_oid(role.as_pg_cstr(), false);

            has_privs_of_role(GetUserId(), role_oid)
        }
    }
}

/// Reads and writes in 64kB chunks, as core's `COPY` does.
const TRANSFER_BUFFER_SIZE: usize = 65536;

/// The program's end of the pipe, as a file descriptor of our own.
///
/// Two descriptors for one pipe on purpose: `ClosePipeStream` closes the stream's, so handing a
/// `StdFile` that descriptor would close it twice. The dup is ours and the stream's is Postgres',
/// and the exit status only arrives when the stream is closed — which is why that is a method
/// rather than a drop.
struct ProgramPipe {
    stream: *mut libc::FILE,
    program: String,
}

impl ProgramPipe {
    fn open(program: &str, copy_from: bool) -> (ProgramPipe, StdFile) {
        let mode = if copy_from { PG_BINARY_R } else { PG_BINARY_W };

        let stream = unsafe { OpenPipeStream(program.as_pg_cstr(), mode.as_ptr()) };

        if stream.is_null() {
            ereport!(
                PgLogLevel::ERROR,
                PgSqlErrorCode::ERRCODE_IO_ERROR,
                format!("could not execute command \"{program}\"")
            );
        }

        let fd = unsafe { libc::dup(libc::fileno(stream as _)) };

        if fd < 0 {
            ereport!(
                PgLogLevel::ERROR,
                PgSqlErrorCode::ERRCODE_IO_ERROR,
                format!("could not duplicate the pipe to command \"{program}\"")
            );
        }

        let pipe = ProgramPipe {
            stream: stream as _,
            program: program.to_string(),
        };

        (pipe, unsafe { StdFile::from_raw_fd(fd) })
    }

    /// Closes the pipe and reports a program that failed, as core's `COPY` reports one.
    fn close(mut self) {
        let status = unsafe { ClosePipeStream(self.stream as _) };

        self.stream = std::ptr::null_mut();

        if status != 0 {
            let detail = unsafe { CStr::from_ptr(wait_result_to_str(status)) }
                .to_string_lossy()
                .into_owned();

            ereport!(
                PgLogLevel::ERROR,
                PgSqlErrorCode::ERRCODE_EXTERNAL_ROUTINE_EXCEPTION,
                format!("program \"{}\" failed", self.program),
                detail
            );
        }
    }
}

impl Drop for ProgramPipe {
    fn drop(&mut self) {
        // only on the error path: `close` takes ownership and nulls this first
        if !self.stream.is_null() {
            unsafe { ClosePipeStream(self.stream as _) };
        }
    }
}

/// Pipes a finished file into a program's standard input.
fn send_to_program(program: &str, path: &str) {
    let mut file =
        StdFile::open(path).unwrap_or_else(|e| error!("could not read back what was written: {e}"));

    let (pipe, mut stdin) = ProgramPipe::open(program, false);

    let copied = std::io::copy(&mut file, &mut stdin);

    // dropped before the stream is closed, so the program sees the end of its input and has a
    // chance to exit with a status worth reporting
    drop(stdin);

    if let Err(e) = copied {
        error!("could not write to command \"{program}\": {e}");
    }

    pipe.close();
}

/// Fills the temporary file from a program's standard output.
fn fill_from_program(program: &str, path: &str) {
    let (pipe, mut stdout) = ProgramPipe::open(program, true);

    let mut file = create(path);

    let copied = std::io::copy(&mut stdout, &mut file);

    drop(stdout);

    if let Err(e) = copied {
        error!("could not read from command \"{program}\": {e}");
    }

    pipe.close();
}

fn create(path: &str) -> StdFile {
    StdFile::create(path).unwrap_or_else(|e| error!("could not open a temporary file: {e}"))
}

/// Sends a finished file to the client as the result of a `COPY ... TO STDOUT`.
unsafe fn send_to_client(path: &str, natts: i16) {
    let mut file =
        StdFile::open(path).unwrap_or_else(|e| error!("could not read back what was written: {e}"));

    send_copy_out_begin(natts);

    let mut buffer = vec![0u8; TRANSFER_BUFFER_SIZE];

    loop {
        let read = file
            .read(&mut buffer)
            .unwrap_or_else(|e| error!("could not read back what was written: {e}"));

        if read == 0 {
            break;
        }

        send_copy_data(&buffer[..read]);
    }

    pq_putemptymessage(b'c' as _);
}

/// `CopyOutResponse`, declaring the binary format: what follows is a file and not rows of text.
unsafe fn send_copy_out_begin(natts: i16) {
    let buf = makeStringInfo();

    pq_beginmessage(buf, b'H' as _);

    pq_sendbyte(buf, BINARY_FORMAT as _);
    pq_sendint16(buf, natts as _);

    for _ in 0..natts {
        pq_sendint16(buf, BINARY_FORMAT as _);
    }

    pq_endmessage(buf);
}

unsafe fn send_copy_data(data: &[u8]) {
    let buf = makeStringInfo();

    pq_beginmessage(buf, b'd' as _);
    pq_sendbytes(buf, data.as_ptr() as _, data.len() as _);
    pq_endmessage(buf);
}

/// Fills the temporary file from the client, as the input of a `COPY ... FROM STDIN`.
unsafe fn fill_from_client(path: &str, natts: i16) {
    let mut state = CopyFromStdinState {
        fe_msgbuf: makeStringInfo(),
        reached_eof: false,
    };

    let mut file = create(path);

    send_copy_in_begin(natts);

    let mut buffer = vec![0u8; TRANSFER_BUFFER_SIZE];

    while !state.reached_eof {
        let read = receive_from_client(&mut state, &mut buffer);

        if read == 0 {
            break;
        }

        file.write_all(&buffer[..read])
            .unwrap_or_else(|e| error!("could not write to a temporary file: {e}"));
    }
}

/// `CopyInResponse`: the message a client waits for before it sends anything.
unsafe fn send_copy_in_begin(natts: i16) {
    let buf = makeStringInfo();

    pq_beginmessage(buf, b'G' as _);

    pq_sendbyte(buf, BINARY_FORMAT as _);
    pq_sendint16(buf, natts as _);

    for _ in 0..natts {
        pq_sendint16(buf, BINARY_FORMAT as _);
    }

    pq_endmessage(buf);

    ((*PqCommMethods).flush)();
}

/// A file is binary whatever the format is, and the declaration has to say so: a client that
/// was told `text` would be free to translate the bytes.
const BINARY_FORMAT: i8 = 1;

/// The part of core's `CopyFromStateData` the protocol loop needs, with core's names kept.
struct CopyFromStdinState {
    fe_msgbuf: StringInfo,
    reached_eof: bool,
}

const PQ_LARGE_MESSAGE_LIMIT: i32 = 1024 * 1024 * 1024 - 3;
const PQ_SMALL_MESSAGE_LIMIT: i32 = 10000;

/// Up to `buffer.len()` bytes of the client's `CopyData` stream, or 0 at its end. Lifted from
/// core's `CopyGetData` for the `COPY_FRONTEND` case.
unsafe fn receive_from_client(state: &mut CopyFromStdinState, buffer: &mut [u8]) -> usize {
    let mut out = buffer;

    let mut maxread = out.len();
    let mut bytesread = 0;

    while maxread > 0 && bytesread < 1 && !state.reached_eof {
        let mut flushed = false;

        while flushed || (*state.fe_msgbuf).cursor >= (*state.fe_msgbuf).len {
            // a cancel between the read and the message's end would leave the protocol stream
            // mid-message, so it is held off exactly as core holds it off
            QueryCancelHoldoffCount += 1;

            pq_startmsgread();

            let message_type = pq_getbyte();

            if message_type == -1 {
                error!("unexpected EOF on client connection with an open transaction");
            }

            let max_message_length = match message_type as u8 {
                b'd' => PQ_LARGE_MESSAGE_LIMIT,
                // CopyDone, CopyFail, Flush, Sync
                b'c' | b'f' | b'H' | b'S' => PQ_SMALL_MESSAGE_LIMIT,
                _ => error!("unexpected message type 0x{message_type:02X} during COPY from stdin"),
            };

            if pq_getmessage(state.fe_msgbuf, max_message_length) != 0 {
                error!("unexpected EOF on client connection with an open transaction");
            }

            QueryCancelHoldoffCount -= 1;

            match message_type as u8 {
                b'd' => break,
                b'c' => {
                    state.reached_eof = true;
                    return bytesread;
                }
                b'f' => {
                    let message = CStr::from_ptr(pq_getmsgstring(state.fe_msgbuf))
                        .to_string_lossy()
                        .into_owned();

                    error!("COPY from stdin failed: {message}");
                }
                b'H' | b'S' => {
                    flushed = true;
                    continue;
                }
                _ => error!("unexpected message type 0x{message_type:02X} during COPY from stdin"),
            }
        }

        let available = std::cmp::min(
            ((*state.fe_msgbuf).len - (*state.fe_msgbuf).cursor) as usize,
            maxread,
        );

        pq_copymsgbytes(state.fe_msgbuf, out.as_mut_ptr() as _, available as _);

        out = &mut out[available..];
        maxread -= available;
        bytesread += available;
    }

    bytesread
}

// not in pgrx's bindings, which stop at pqcomm.h: libpq-be.h is where the frontend half lives
#[repr(C)]
struct PQcommMethods {
    comm_reset: unsafe extern "C" fn(),
    flush: unsafe extern "C" fn() -> i32,
    flush_if_writable: unsafe extern "C" fn() -> i32,
    is_send_pending: unsafe extern "C" fn() -> bool,
    putmessage: unsafe extern "C" fn(msgtype: u32, s: *const std::ffi::c_char, len: usize) -> i32,
    putmessage_noblock: unsafe extern "C" fn(msgtype: u32, s: *const std::ffi::c_char, len: usize),
}

unsafe extern "C" {
    fn pq_startmsgread();
    fn pq_getmessage(s: StringInfo, maxlen: i32) -> i32;
    fn pq_getbyte() -> i32;

    static PqCommMethods: *mut PQcommMethods;
}
