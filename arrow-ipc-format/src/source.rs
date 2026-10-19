//! Tier 2's [`ByteSource`] as the `Read + Seek` arrow's IPC readers want.
//!
//! An IPC file is read back-to-front — the footer's length, the footer, then the blocks the
//! footer points at — which is exactly what a source of ranges serves well and what reading the
//! whole file to hand it over serves badly. Given this adapter, arrow's own `FileReader` does
//! all of it.
//!
//! The one decision here is how to serve a small read. Arrow asks for eight bytes of block
//! metadata before each batch body, and a round trip to an object store per eight bytes would
//! be unusable, so a read smaller than the window fetches a window and the reads that follow it
//! are served out of that. A read at least as large as the window is a batch body, and is
//! fetched as the one range it is.

use std::io::{Error, ErrorKind, Read, Result as IoResult, Seek, SeekFrom};
use std::sync::Arc;

use bytes::Bytes;
use format_registry::provider::ByteSource;

/// How much a small read fetches, and so the most that can be fetched and not used.
const WINDOW: u64 = 64 * 1024;

/// The most one fetch asks for, however much the caller wanted.
///
/// A range comes back in a palloc'd buffer, which cannot exceed 1GB, and a record batch can be
/// larger than that — a file of four 600MB values is one 2.4GB batch, and arrow asks for the
/// whole body in one `read`. A short read is what `Read` is allowed to do and what `read_exact`
/// loops on, so the batch arrives in pieces in arrow's own buffer.
const MAX_FETCH: u64 = 64 * 1024 * 1024;

/// A file the base holds, read in pieces.
pub(crate) struct SourceReader {
    source: Arc<dyn ByteSource>,
    size: u64,
    position: u64,
    window: Option<Window>,
}

/// A fetched span of the file.
struct Window {
    offset: u64,
    data: Bytes,
}

impl SourceReader {
    pub(crate) fn open(source: Arc<dyn ByteSource>) -> Result<Self, String> {
        let size = source.size()?;

        let size = u64::try_from(size).map_err(|_| format!("the file has a size of {size}"))?;

        Ok(Self {
            source,
            size,
            position: 0,
            window: None,
        })
    }

    /// Exactly the bytes of `[offset, offset + length)`, which the source is asked for as one
    /// `(offset, length)` range, clamped to the file: a store refuses a read that runs past the
    /// end rather than returning what it has.
    fn fetch(&self, offset: u64, length: u64) -> IoResult<Bytes> {
        let length = length.min(self.size.saturating_sub(offset));

        if length == 0 {
            return Ok(Bytes::new());
        }

        let mut ranges = self
            .source
            .get_ranges(&[(offset as i64, length as i64)])
            .map_err(|e| Error::other(e.to_string()))?;

        ranges
            .pop()
            .ok_or_else(|| Error::new(ErrorKind::UnexpectedEof, "the source served no range"))
    }
}

impl Read for SourceReader {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        let left = self.size.saturating_sub(self.position);

        if left == 0 || buf.is_empty() {
            return Ok(0);
        }

        let wanted = (buf.len() as u64).min(left);

        // a batch body: one range of its own, since a window would neither hold it nor help the
        // read after it
        if wanted >= WINDOW {
            let data = self.fetch(self.position, wanted.min(MAX_FETCH))?;
            buf[..data.len()].copy_from_slice(&data);
            self.position += data.len() as u64;

            return Ok(data.len());
        }

        let holds = |window: &Window| {
            window.offset <= self.position
                && self.position < window.offset + window.data.len() as u64
        };

        if !self.window.as_ref().is_some_and(holds) {
            self.window = Some(Window {
                offset: self.position,
                data: self.fetch(self.position, WINDOW)?,
            });
        }

        let window = self.window.as_ref().expect("the window was just fetched");

        let into = (self.position - window.offset) as usize;
        let len = (window.data.len() - into).min(wanted as usize);

        buf[..len].copy_from_slice(&window.data[into..into + len]);
        self.position += len as u64;

        Ok(len)
    }
}

impl Seek for SourceReader {
    fn seek(&mut self, to: SeekFrom) -> IoResult<u64> {
        // the window is kept: it is addressed by its offset in the file, so a seek back into one
        // that was already fetched — which is what reading a footer and then its blocks does —
        // is served without fetching it again
        let position = match to {
            SeekFrom::Start(offset) => Some(offset),
            SeekFrom::End(offset) => self.size.checked_add_signed(offset),
            SeekFrom::Current(offset) => self.position.checked_add_signed(offset),
        };

        self.position = position.ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidInput,
                "a seek to before the start of the file",
            )
        })?;

        Ok(self.position)
    }
}
