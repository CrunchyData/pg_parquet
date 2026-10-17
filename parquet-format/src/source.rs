//! Tier 2's [`ByteSource`] as parquet's [`ChunkReader`].
//!
//! This is the whole of what `open_ranged_reader` needs: given it, upstream's own reader does
//! the footer read, the row group selection and the page fetches, exactly as it does over a
//! `File`. Nothing here decides *what* to read.
//!
//! The one thing it does decide is how to serve a *sequential* read. Upstream asks for one of
//! those per page, to read a page header of a few dozen bytes, and then asks for the page's
//! data by exact range. A round trip to an object store per page header would be unusable, so a
//! sequential read fetches a window and the next header in the same column chunk is served out
//! of it.

use std::io::{Error, Read, Result as IoResult};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use format_registry::provider::ByteSource;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::reader::{ChunkReader, Length};

/// How much a sequential read fetches at a time, and so the most that can be fetched and not
/// used. Big enough that the page headers of a column chunk come in one or two round trips,
/// small enough not to pull a megabyte to read sixty bytes.
const WINDOW: usize = 64 * 1024;

/// A file the base holds, read in pieces.
pub struct SourceChunkReader {
    inner: Arc<Inner>,
}

struct Inner {
    source: Arc<dyn ByteSource>,
    size: u64,
    /// The last window a sequential read fetched. Shared so that the reads upstream makes
    /// through separate `get_read` calls still hit it; a `Mutex` because `ChunkReader` is
    /// `Sync` and nothing here may assume which thread is asking.
    window: Mutex<Option<Window>>,
}

/// A fetched span of the file.
struct Window {
    offset: u64,
    data: Bytes,
}

impl Window {
    /// The window from `offset` on, if it reaches that far.
    fn from(&self, offset: u64) -> Option<Bytes> {
        let into = offset.checked_sub(self.offset)? as usize;

        (into < self.data.len()).then(|| self.data.slice(into..))
    }

    /// Exactly `length` bytes at `offset`, if the window holds all of them.
    fn exactly(&self, offset: u64, length: usize) -> Option<Bytes> {
        let into = offset.checked_sub(self.offset)? as usize;
        let end = into.checked_add(length)?;

        (end <= self.data.len()).then(|| self.data.slice(into..end))
    }
}

impl SourceChunkReader {
    /// Asks the source for its size once, since every later call needs it and tier 1's answer
    /// cannot change under a read that has already started.
    pub fn new(source: Arc<dyn ByteSource>) -> Result<Self, String> {
        let size = source.size()?;

        let size =
            u64::try_from(size).map_err(|_| format!("the file has a negative size {size}"))?;

        Ok(Self {
            inner: Arc::new(Inner {
                source,
                size,
                window: Mutex::new(None),
            }),
        })
    }
}

impl Inner {
    fn locked_window(&self) -> std::sync::MutexGuard<'_, Option<Window>> {
        self.window
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// An exact range, from the window when it happens to be there and from tier 1 otherwise.
    fn exact(&self, offset: u64, length: usize) -> Result<Bytes, String> {
        if let Some(data) = self
            .locked_window()
            .as_ref()
            .and_then(|window| window.exactly(offset, length))
        {
            return Ok(data);
        }

        // deliberately not cached: this is a page's data, and keeping it would evict the window
        // of page headers that the next read wants
        fetch(self.source.as_ref(), offset, length, self.size)
    }

    /// Whatever is at `offset`, for a sequential read: the window if it reaches, otherwise a
    /// fresh window.
    fn window_at(&self, offset: u64) -> Result<Bytes, String> {
        let mut window = self.locked_window();

        if let Some(data) = window.as_ref().and_then(|window| window.from(offset)) {
            return Ok(data);
        }

        let length = WINDOW.min((self.size.saturating_sub(offset)) as usize);
        let data = fetch(self.source.as_ref(), offset, length, self.size)?;

        *window = Some(Window {
            offset,
            data: data.clone(),
        });

        Ok(data)
    }
}

impl Length for SourceChunkReader {
    fn len(&self) -> u64 {
        self.inner.size
    }
}

impl ChunkReader for SourceChunkReader {
    type T = ChunkRead;

    fn get_read(&self, start: u64) -> ParquetResult<Self::T> {
        Ok(ChunkRead {
            inner: self.inner.clone(),
            offset: start,
            buffered: Bytes::new(),
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        self.inner
            .exact(start, length)
            .map_err(|e| ParquetError::External(e.into()))
    }
}

/// A sequential read, served a window at a time.
pub struct ChunkRead {
    inner: Arc<Inner>,
    offset: u64,
    buffered: Bytes,
}

impl Read for ChunkRead {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        if self.buffered.is_empty() {
            if self.offset >= self.inner.size {
                return Ok(0);
            }

            self.buffered = self.inner.window_at(self.offset).map_err(Error::other)?;
            self.offset += self.buffered.len() as u64;
        }

        // `copy_to_slice` advances the buffer, so the remainder is what the next call reads
        let taken = buf.len().min(self.buffered.len());
        bytes::Buf::copy_to_slice(&mut self.buffered, &mut buf[..taken]);

        Ok(taken)
    }
}

/// One range, with the short-read and out-of-range cases reported rather than returned as
/// silently truncated bytes — a parquet reader given fewer bytes than it asked for fails with
/// something far less useful than this.
fn fetch(source: &dyn ByteSource, start: u64, length: usize, size: u64) -> Result<Bytes, String> {
    if start.saturating_add(length as u64) > size {
        return Err(format!(
            "a read of {length} bytes at offset {start} is past the end of a {size} byte file"
        ));
    }

    let offset = i64::try_from(start).map_err(|_| format!("offset {start} does not fit in i64"))?;
    let length =
        i64::try_from(length).map_err(|_| format!("length {length} does not fit in i64"))?;

    let mut ranges = source.get_ranges(&[(offset, length)])?;

    if ranges.len() != 1 {
        return Err(format!("asked for one range and got {}", ranges.len()));
    }

    let data = ranges.remove(0);

    if data.len() as i64 != length {
        return Err(format!(
            "asked for {length} bytes at offset {offset} and got {}",
            data.len()
        ));
    }

    Ok(data)
}
