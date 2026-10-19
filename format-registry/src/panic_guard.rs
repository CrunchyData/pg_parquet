//! Keeping a panicking reader from taking the process down.
//!
//! A reader crosses from a provider to the base as an `FFI_ArrowArrayStream`, whose callbacks
//! are plain `extern "C"` — arrow's, not ours. A Rust panic cannot unwind out of one of those,
//! so it aborts the process instead, and in Postgres an aborted backend takes the whole cluster
//! into recovery. That matters because a panicking reader is not hypothetical: arrow's own IPC
//! reader panics on a corrupt buffer offset rather than returning an error, so a single flipped
//! byte in a file is enough.
//!
//! So a provider wraps its reader in this before handing it over, which turns a panic back into
//! the failed batch it should have been. The base then reports it as it reports any other read
//! failure.

use std::panic::{catch_unwind, AssertUnwindSafe};

use arrow::array::{RecordBatch, RecordBatchReader};
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;

/// Wraps a reader so that a panic while reading becomes an error on the batch it was reading.
///
/// The schema is taken here, while a panic can still unwind, because the stream's `get_schema`
/// callback has nowhere to report one.
pub fn panic_guarded(
    reader: Box<dyn RecordBatchReader + Send>,
) -> crate::Result<Box<dyn RecordBatchReader + Send>> {
    let schema = catch_unwind(AssertUnwindSafe(|| reader.schema()))
        .map_err(|payload| format!("reading the file's schema failed: {}", described(&payload)))?;

    Ok(Box::new(GuardedReader { reader, schema }))
}

struct GuardedReader {
    reader: Box<dyn RecordBatchReader + Send>,
    schema: SchemaRef,
}

impl Iterator for GuardedReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        match catch_unwind(AssertUnwindSafe(|| self.reader.next())) {
            Ok(batch) => batch,
            // the reader is left as the panic left it, which is why this is the last batch: the
            // error ends the read, and a caller that asked again would be polling a reader whose
            // own invariants no longer hold
            Err(payload) => Some(Err(ArrowError::InvalidArgumentError(format!(
                "the file is corrupt: {}",
                described(&payload)
            )))),
        }
    }
}

impl RecordBatchReader for GuardedReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// A panic payload as a message. Anything other than the usual `&str` or `String` is reported
/// as unknown rather than guessed at.
fn described(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        return (*message).to_string();
    }

    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }

    "the reader panicked".to_string()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int32Array, RecordBatch, RecordBatchReader};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::error::ArrowError;

    use super::panic_guarded;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    /// A reader that yields one batch and then panics, the way arrow's IPC reader panics on a
    /// corrupt buffer offset.
    struct PanicsOnTheSecondBatch {
        batches_left: usize,
    }

    impl Iterator for PanicsOnTheSecondBatch {
        type Item = Result<RecordBatch, ArrowError>;

        fn next(&mut self) -> Option<Self::Item> {
            if self.batches_left == 0 {
                panic!("slice offset=2986348800 length=4100 selflen=14520");
            }

            self.batches_left -= 1;

            Some(RecordBatch::try_new(
                schema(),
                vec![Arc::new(Int32Array::from(vec![1]))],
            ))
        }
    }

    impl RecordBatchReader for PanicsOnTheSecondBatch {
        fn schema(&self) -> SchemaRef {
            schema()
        }
    }

    #[test]
    fn test_a_panicking_read_becomes_a_failed_batch() {
        let mut reader = panic_guarded(Box::new(PanicsOnTheSecondBatch { batches_left: 1 }))
            .expect("the schema is readable");

        assert_eq!(
            reader.next().expect("a first batch").is_ok(),
            true,
            "the good batch"
        );

        let failed = reader
            .next()
            .expect("the panic is reported rather than ending the stream")
            .expect_err("the panicking batch");

        assert!(
            failed.to_string().contains("the file is corrupt")
                && failed.to_string().contains("slice offset=2986348800"),
            "the panic's own message is kept: {failed}"
        );
    }

    struct PanicsOnSchema;

    impl Iterator for PanicsOnSchema {
        type Item = Result<RecordBatch, ArrowError>;

        fn next(&mut self) -> Option<Self::Item> {
            None
        }
    }

    impl RecordBatchReader for PanicsOnSchema {
        fn schema(&self) -> SchemaRef {
            panic!("the footer's schema message is corrupt");
        }
    }

    #[test]
    fn test_a_panicking_schema_is_refused_before_the_stream_exists() {
        let Err(refused) = panic_guarded(Box::new(PanicsOnSchema)) else {
            panic!("a reader whose schema panics was accepted");
        };

        assert!(
            refused.contains("reading the file's schema failed")
                && refused.contains("the footer's schema message is corrupt"),
            "{refused}"
        );
    }
}
