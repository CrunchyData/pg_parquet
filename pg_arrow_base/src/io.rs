//! The two things a provider is handed instead of a whole file.
//!
//! [`FileSource`] and [`WriterSink`] are the base's side of provider ABI v2: a reader that
//! fetches the ranges a format asks for, and a writer that takes bytes as a format produces
//! them. Both are thin — the work is in [`crate::object_store`], and the runtime that does it
//! is in tier 1 — but they are where one rule is enforced that nothing else enforces.
//!
//! **Nothing in here reports.** A `get_ranges` or a `write` is called from inside a provider's
//! frames: parquet's decoder, arrow-ipc's writer, code in another `.so` that is holding a
//! buffer and expects a return value. An `ereport` there is a longjmp past that code's
//! cleanup — a leaked upload at best, a corrupt allocator state at worst. So every failure
//! leaves here as a `String`, travels back through the provider as a code, and becomes an
//! `ereport` at the top of the COPY path where there is nothing left to unwind through.
//!
//! That is also why `try_write` and `try_finish` exist on [`crate::object_store::Writer`] at
//! all: the reporting `write`/`finish` are for the base's own paths, and the `try_` forms are
//! for this one.

// `Upload` is the write half and is driven by the COPY TO path. `FileSource` is the read half,
// which the COPY FROM path will drive once it moves out of pg_parquet; it is here now because
// it is the base's half of provider ABI v2 and the ABI is not finished without it, and the
// tests are what keep it honest in the meantime.
#![allow(dead_code)]

use std::cell::{Cell, RefCell};
use std::fs::File as StdFile;
use std::io::{BufWriter, Write};
use std::os::unix::fs::FileExt;
use std::rc::Rc;

use bytes::Bytes;
use format_registry::provider::{ByteSink, ByteSource};
use format_registry::Result;

use crate::endpoint::Endpoint;
use crate::object_store::{self, Writer};

/// Which file the base is about to read or write.
///
/// The two differ in who is entitled to refuse them. An [`Place::Object`] is the uri the
/// statement named, and tier 1 checks the current user against it on every single operation.
/// [`Place::Staging`] is the temporary file the base made for itself to pass a `STDIN`/`STDOUT`
/// or `PROGRAM` statement's bytes through: it lives in the cluster's own temp area, nothing but
/// this backend knows its name, and whether the user may run the statement at all was already
/// settled by [`Endpoint::check_privilege`]. Handing that path to tier 1 would ask the uri
/// question about a file that is not a uri — and refuse, for a `pg_execute_server_program` user
/// who holds no file-access role, a statement they are entitled to run.
#[derive(Clone, Debug)]
pub(crate) enum Place {
    Object(String),
    Staging(String),
}

impl Place {
    /// The file a statement's bytes go through, which is its uri unless the base had to stage
    /// them.
    pub(crate) fn new(endpoint: &Endpoint, path: &str) -> Place {
        match endpoint {
            Endpoint::Uri(_) => Place::Object(path.to_string()),
            Endpoint::Stdio | Endpoint::Program(_) => Place::Staging(path.to_string()),
        }
    }

    pub(crate) fn object(uri: &str) -> Place {
        Place::Object(uri.to_string())
    }

    /// The uri or path itself, for a message or for the parts of a name a format may read.
    pub(crate) fn path(&self) -> &str {
        match self {
            Place::Object(path) | Place::Staging(path) => path,
        }
    }
}

/// The whole of a file, or `None` if there is none.
pub(crate) fn read_all(place: &Place) -> Option<Bytes> {
    match place {
        Place::Object(uri) => object_store::read_all(uri),
        Place::Staging(path) => std::fs::read(path).ok().map(Bytes::from),
    }
}

/// Writes a whole file, stopping the statement on failure.
pub(crate) fn write_all(place: &Place, data: &[u8]) {
    match place {
        Place::Object(uri) => object_store::write_all(uri, data),
        Place::Staging(path) => std::fs::write(path, data)
            .unwrap_or_else(|e| pgrx::error!("could not write the staging file {path}: {e}")),
    }
}

/// Random access to a file, for a format that reads a footer and then the splits it needs.
///
/// Holds a [`Place`] and the size the file had when the read started, not a client or an open
/// handle: the client lives in tier 1 and is shared with every other format in the backend.
/// Re-stating the size per call would be a round trip per `size()`, and a parquet reader asks
/// more than once.
#[derive(Debug)]
pub(crate) struct FileSource {
    place: Place,
    size: i64,
}

impl FileSource {
    /// Stats the file and fails if there is none, so that "no file" is reported before a
    /// provider has started decoding rather than as a short read in the middle.
    pub(crate) fn open(place: &Place) -> Result<FileSource> {
        let size = match place {
            Place::Object(uri) => object_store::size(uri),
            Place::Staging(path) => std::fs::metadata(path).ok().map(|data| data.len() as i64),
        };

        let size = size.ok_or_else(|| format!("there is no file at {}", place.path()))?;

        Ok(FileSource {
            place: place.clone(),
            size,
        })
    }

    /// The ranges of the staging file, read with positional reads so that concurrent calls on
    /// the one source cannot move each other's offset.
    fn staging_ranges(path: &str, ranges: &[(i64, i64)]) -> Result<Vec<Bytes>> {
        let file =
            StdFile::open(path).map_err(|e| format!("could not read the staging file: {e}"))?;

        ranges
            .iter()
            .map(|&(offset, length)| {
                let mut buffer = vec![0u8; length.max(0) as usize];

                file.read_exact_at(&mut buffer, offset as u64)
                    .map_err(|e| format!("could not read the staging file: {e}"))?;

                Ok(Bytes::from(buffer))
            })
            .collect()
    }
}

impl ByteSource for FileSource {
    fn size(&self) -> Result<i64> {
        Ok(self.size)
    }

    fn get_ranges(&self, ranges: &[(i64, i64)]) -> Result<Vec<Bytes>> {
        if ranges.is_empty() {
            return Ok(Vec::new());
        }

        match &self.place {
            // one call for all of them: tier 1 coalesces and fetches them concurrently, and a
            // loop over a singular form here would serialize a read upstream made parallel
            Place::Object(uri) => object_store::try_read_ranges(uri, ranges),
            Place::Staging(path) => FileSource::staging_ranges(path, ranges),
        }
    }
}

// `FileSource` is handed to a provider as an `Arc<dyn ByteSource>`, which `ByteSource` requires
// to be `Send + Sync` because an Arrow reader may be parked on another thread. A `String` and
// an `i64` satisfy that on their own — but note what the auto-traits do not say: `get_ranges`
// calls tier 1, whose client cache and runtime handle are `thread_local!`, so a call from a
// thread that is not this backend's resolves a second cache. Nothing in the COPY path does
// that; a provider that spawned readers would, which is why it is written down here.

/// An upload the base holds while a provider writes into it.
///
/// Split in two because ownership is split in two: the provider is given a `Box<dyn ByteSink>`
/// and must be able to write through it for as long as its writer lives, while only the base
/// may *complete* the object. So the tier 1 handle sits behind a shared cell, the provider gets
/// a [`WriterSink`] over it, and `finish` here is what makes the file visible — after the
/// provider's writer has returned without an error, which is the only moment at which the file
/// is known to be whole.
pub(crate) struct Upload {
    writer: Rc<RefCell<Option<Handle>>>,
    /// What has gone into tier 1 so far, for `pg_stat_progress_copy`. Counted here rather than
    /// asked of tier 1, which knows how much it has *uploaded* — a number that lags by up to
    /// the buffer and would make a small `COPY TO` look stalled at zero.
    written: Rc<Cell<i64>>,
    place: Place,
}

/// The open file behind an [`Upload`]: tier 1's writer for a uri, and a plain buffered file for
/// the base's own staging file. Only the first of the two has anything to *complete* — a local
/// file is visible as it is written — so `finish` on a staging one is just the last flush.
enum Handle {
    Object(Writer),
    Staging(BufWriter<StdFile>),
}

impl Handle {
    fn try_write(&mut self, data: &[u8]) -> Result<()> {
        match self {
            Handle::Object(writer) => writer.try_write(data),
            Handle::Staging(file) => file
                .write_all(data)
                .map_err(|e| format!("could not write the staging file: {e}")),
        }
    }

    fn try_finish(self) -> Result<()> {
        match self {
            Handle::Object(writer) => writer.try_finish(),
            Handle::Staging(mut file) => file
                .flush()
                .map_err(|e| format!("could not write the staging file: {e}")),
        }
    }
}

impl Upload {
    pub(crate) fn open(place: &Place) -> Upload {
        let handle = match place {
            Place::Object(uri) => Handle::Object(Writer::open(uri)),
            Place::Staging(path) => {
                let file = StdFile::create(path).unwrap_or_else(|e| {
                    pgrx::error!("could not open the staging file {path}: {e}")
                });

                Handle::Staging(BufWriter::new(file))
            }
        };

        Upload {
            writer: Rc::new(RefCell::new(Some(handle))),
            written: Rc::new(Cell::new(0)),
            place: place.clone(),
        }
    }

    pub(crate) fn bytes_written(&self) -> i64 {
        self.written.get()
    }

    /// The provider's end of it. More than one is harmless but pointless: they share the one
    /// handle, and a format writes one file.
    pub(crate) fn sink(&self) -> Box<dyn ByteSink> {
        Box::new(WriterSink {
            writer: Rc::clone(&self.writer),
            written: Rc::clone(&self.written),
        })
    }

    /// Completes the upload. Consuming, because there is nothing to do with it after.
    pub(crate) fn finish(self) -> Result<()> {
        let writer = self.writer.borrow_mut().take();

        match writer {
            Some(writer) => writer.try_finish(),
            // a write already failed, which discarded the upload inside tier 1 and reported
            // why; saying it again here would replace that message with a vaguer one
            None => Err(format!(
                "the upload to {} was already discarded by a failed write",
                self.place.path()
            )),
        }
    }
}

/// A streaming write, as a provider sees it: bytes in, nothing out, and no way to complete the
/// object. That last part is the point — the footer is the provider's last `write`, and whether
/// the file then becomes visible is the base's call.
pub(crate) struct WriterSink {
    writer: Rc<RefCell<Option<Handle>>>,
    written: Rc<Cell<i64>>,
}

impl ByteSink for WriterSink {
    fn write(&mut self, data: &[u8]) -> Result<()> {
        let mut writer = self.writer.borrow_mut();

        let Some(open) = writer.as_mut() else {
            return Err("the upload was already discarded by a failed write".to_string());
        };

        if let Err(message) = open.try_write(data) {
            // the upload is gone inside tier 1 either way; forgetting the handle here is what
            // makes a second write report the first failure instead of a closed-handle one
            *writer = None;
            return Err(message);
        }

        self.written.set(self.written.get() + data.len() as i64);

        Ok(())
    }
}
