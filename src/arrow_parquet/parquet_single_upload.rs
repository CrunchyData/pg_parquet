use std::sync::Arc;

use bytes::Bytes;
use futures::{future::BoxFuture, FutureExt};
use object_store::{
    buffered::BufWriter, path::Path as ObjectStorePath, Error as ObjectStoreError, ObjectStore,
};
use parquet::{
    arrow::async_writer::AsyncFileWriter,
    errors::{ParquetError, Result as ParquetResult},
};
use tokio::{io::AsyncWriteExt, sync::Mutex};

use crate::parquet_copy_hook::copy_cleanup::{
    finish_unfinished_write, give_up_unfinished_write, register_unfinished_write, CleanupId,
    UnfinishedWrite,
};

// SINGLE_UPLOAD_BUFFER_SIZE is how much of an object the writer holds in memory before it
// starts an upload for the rest of it. This is object_store's own default, named here so
// that a store which has no upload to start can report the size it is bounded by.
pub(crate) const SINGLE_UPLOAD_BUFFER_SIZE: usize = 10 * 1024 * 1024;

// single_upload_writer is the sink a copy that no other process takes part in writes into.
// object_store's writer puts the bytes of a copy that stays inside its buffer as a single
// object, and starts an upload of its own for a copy that runs over it. That upload is
// registered for cleanup, because the writer gives it up nowhere itself: a copy that does
// not finish it leaves the parts it uploaded in the bucket as storage nobody asked for, and
// a copy the backend is terminated in the middle of does not even unwind.
pub(crate) fn single_upload_writer(
    store: Arc<dyn ObjectStore>,
    path: ObjectStorePath,
) -> Box<dyn AsyncFileWriter> {
    let upload = SingleUpload {
        destination: path.to_string(),
        writer: Arc::new(Mutex::new(Some(BufWriter::with_capacity(
            store,
            path,
            SINGLE_UPLOAD_BUFFER_SIZE,
        )))),
    };

    let cleanup = register_unfinished_write(UnfinishedWrite::WriterUpload(upload.clone()));

    Box::new(SingleUploadWriter { upload, cleanup })
}

// SingleUpload is the writer of a single process copy as the cleanup of the copy holds it: a
// handle that gives the upload the writer started up, whether the copy is still writing into
// it or its writer is gone already.
#[derive(Clone)]
pub(crate) struct SingleUpload {
    destination: String,
    writer: Arc<Mutex<Option<BufWriter>>>,
}

impl SingleUpload {
    // what the copy writes, for the report of an upload that could not be given up
    pub(crate) fn destination(&self) -> &str {
        &self.destination
    }

    // abort gives the upload the writer started up, if it started one and the copy did not
    // finish it. The writer is taken out of the handle rather than borrowed, so that the
    // upload is given up once however many times this is called: the copy's own cleanup and
    // the sweep the backend runs at process exit both call it.
    pub(crate) async fn abort(&self) -> Result<(), ObjectStoreError> {
        let Some(mut writer) = self.take().await else {
            return Ok(());
        };

        // the writer gives nothing up while it is still creating the upload, which is the
        // state an error unwinding out of the middle of a write leaves it in: the request it
        // was waiting for is polled to the end first, so that the upload it created is the
        // writer's own to abort. A writer that created none flushes nothing, and one whose
        // request failed has nothing to give up either.
        let _ = writer.flush().await;

        writer.abort().await
    }

    async fn put(&self, bs: Bytes) -> ParquetResult<()> {
        let mut writer = self.writer.lock().await;

        let writer = writer.as_mut().ok_or_else(|| {
            ParquetError::General(format!(
                "the COPY TO into \"{}\" is finished already",
                self.destination
            ))
        })?;

        writer
            .put(bs)
            .await
            .map_err(|e| ParquetError::External(Box::new(e)))
    }

    async fn take(&self) -> Option<BufWriter> {
        self.writer.lock().await.take()
    }
}

// SingleUploadWriter is the sink the parquet writer of the copy writes into. The upload is
// given up when the writer is dropped without the copy having finished it, which is what the
// cleanup of a copy that failed or was cancelled runs through: the dest receiver that holds
// the writer is freed while the error of the copy unwinds.
struct SingleUploadWriter {
    upload: SingleUpload,
    cleanup: CleanupId,
}

impl AsyncFileWriter for SingleUploadWriter {
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, ParquetResult<()>> {
        async move { self.upload.put(bs).await }.boxed()
    }

    fn complete(&mut self) -> BoxFuture<'_, ParquetResult<()>> {
        async move {
            // the writer is taken out before it is shut down, so that nothing aborts a
            // writer that is in the middle of completing the upload. object_store does not
            // let go of an upload it failed to complete either, which is what makes taking
            // the writer out the whole of the cleanup a shutdown leaves to do.
            let Some(mut writer) = self.upload.take().await else {
                return Err(ParquetError::General(format!(
                    "the COPY TO into \"{}\" is finished already",
                    self.upload.destination()
                )));
            };

            writer
                .shutdown()
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))?;

            // what the copy wrote is the destination now, so there is no upload to give up
            finish_unfinished_write(self.cleanup);

            Ok(())
        }
        .boxed()
    }
}

impl Drop for SingleUploadWriter {
    fn drop(&mut self) {
        // a copy that finished its upload left nothing registered, so this is the cleanup of
        // a copy that did not: the error it failed with is unwinding, and the session that
        // asked for it is still there to be told about an upload that could not be given up
        give_up_unfinished_write(self.cleanup);
    }
}
