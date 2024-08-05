use std::sync::Arc;

use bytes::Bytes;
use futures::future::BoxFuture;
use object_store::{buffered::BufWriter, path::Path, ObjectStore};
use parquet::{arrow::async_writer::AsyncFileWriter, errors::ParquetError};
use tokio::io::AsyncWriteExt;

// upstream already has ParquetObjectReader but no ParquetObjectWriter
// this file can be removed and replaced by upstream struct when the PR is merged
// https://github.com/apache/arrow-rs/pull/6013
#[derive(Debug)]
pub struct ParquetObjectWriter {
    w: BufWriter,
}

impl ParquetObjectWriter {
    /// Create a new [`ParquetObjectWriter`] that writes to the specified path in the given store.
    ///
    /// To configure the writer behavior, please build [`BufWriter`] and then use [`Self::from_raw`]
    pub fn new(store: Arc<dyn ObjectStore>, path: Path) -> Self {
        Self::from_raw(BufWriter::new(store, path))
    }

    /// Construct a new ParquetObjectWriter via a existing BufWriter.
    pub fn from_raw(w: BufWriter) -> Self {
        Self { w }
    }
}

impl AsyncFileWriter for ParquetObjectWriter {
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, Result<(), ParquetError>> {
        Box::pin(async {
            self.w
                .put(bs)
                .await
                .map_err(|err| ParquetError::External(Box::new(err)))
        })
    }

    fn complete(&mut self) -> BoxFuture<'_, Result<(), ParquetError>> {
        Box::pin(async {
            self.w
                .shutdown()
                .await
                .map_err(|err| ParquetError::External(Box::new(err)))
        })
    }
}
