//! The `DestReceiver` a `COPY TO` sends its rows to.
//!
//! Postgres does not know this extension exists: it runs the query the hook built and pushes
//! each result row at whatever `DestReceiver` it was given. What is here is that receiver — a
//! `#[repr(C)]` struct whose first field *is* a `DestReceiver`, so a pointer to it is a pointer
//! to one — plus the batching that turns a stream of rows into `RecordBatch`-sized groups.
//!
//! Two things decide when a batch is written:
//!
//! - a row count, so that a batch is small enough to convert without a spike, and
//! - the total size of a column so far, because an Arrow array is addressed by `i32` offsets
//!   and a batch that would push one past `i32::MAX` has to be written before this row joins
//!   it. A single wide `text` column reaches that long before the row count does.
//!
//! Everything a row touches is allocated in a per-batch memory context that is reset after each
//! write, which is what keeps the receiver's footprint flat across a billion rows.

use std::ffi::CString;

use pgrx::pg_sys::{
    datumCopy, get_typlenbyval, slot_getallattrs, toast_raw_datum_size,
    AllocSetContextCreateExtended, AsPgCStr, BlessTupleDesc, CommandDest, CurrentMemoryContext,
    Datum, DestReceiver, MemoryContext, MemoryContextAllocZero, MemoryContextDelete,
    MemoryContextReset, TupleDesc, TupleTableSlot, ALLOCSET_DEFAULT_INITSIZE,
    ALLOCSET_DEFAULT_MAXSIZE, ALLOCSET_DEFAULT_MINSIZE, VARHDRSZ,
};
use pgrx::{prelude::*, FromDatum, PgMemoryContexts, PgTupleDesc};

use crate::batch_writer::BatchWriter;
use crate::copy_plan::CopyToPlan;
use crate::io::Place;
use crate::{copy_progress, split};

/// Rows per batch.
///
/// Small on purpose: the conversion holds the whole batch as both tuples and Arrow arrays, and
/// a format that wants larger groups than this — parquet's row group is 122880 rows by
/// default — gets them by writing several batches into one group, which is the provider's
/// business and not the receiver's.
const ROWS_PER_BATCH: i64 = 1024;

/// An Arrow array is addressed with `i32` offsets, so no column of a batch may exceed this.
const MAX_ARROW_ARRAY_SIZE: i64 = i32::MAX as i64;

#[repr(C)]
pub(crate) struct ArrowCopyToDestReceiver {
    dest: DestReceiver,
    natts: usize,
    tupledesc: TupleDesc,
    /// The datums of the batch so far, staged column by column so that the conversion can read
    /// a whole column without going back through a heap tuple per row.
    collected_columns: *mut CollectedColumns,
    collected_tuple_count: i64,
    collected_tuple_column_sizes: *mut i64,
    /// Rows taken from the executor so far, for `pg_stat_progress_copy`. The receiver's own
    /// count rather than the portal's, because the portal's is only final at the end.
    tuples_written: i64,
    /// Everything the statement decided: the uri, the provider and its options. A `Box` the
    /// receiver owns, since a `DestReceiver` is a C struct and cannot hold a `String`.
    plan: *mut CopyToPlan,
    writer: *mut BatchWriter,
    batch_memory_context: MemoryContext,
    copy_memory_context: MemoryContext,
    /// Bytes of every file this statement has *finished*. The one being written is not in it:
    /// its size is the writer's to report until its footer is on.
    total_bytes_written: i64,
    /// How many files this statement has finished, which is also the number of the next one.
    /// Only ever more than one under `file_size_bytes`.
    files_written: i64,
}

/// The staged datums of the batch, one `Vec` per attribute, with the type information needed to
/// copy a datum into the batch's memory context. `typlen` and `typbyval` are looked up once per
/// `COPY` rather than once per datum.
pub(crate) struct CollectedColumns {
    columns: Vec<Vec<Option<Datum>>>,
    typlens: Vec<i16>,
    typbyvals: Vec<bool>,
}

impl CollectedColumns {
    fn new(tupledesc: &PgTupleDesc) -> Self {
        let natts = tupledesc.len();

        let mut typlens = Vec::with_capacity(natts);
        let mut typbyvals = Vec::with_capacity(natts);

        for idx in 0..natts {
            let typoid = tupledesc
                .get(idx)
                .expect("the descriptor has every attribute")
                .type_oid()
                .value();

            let mut typlen = -1_i16;
            let mut typbyval = false;
            unsafe { get_typlenbyval(typoid, &mut typlen, &mut typbyval) };

            typlens.push(typlen);
            typbyvals.push(typbyval);
        }

        Self {
            columns: vec![Vec::new(); natts],
            typlens,
            typbyvals,
        }
    }

    fn clear(&mut self) {
        for column in self.columns.iter_mut() {
            column.clear();
        }
    }
}

impl ArrowCopyToDestReceiver {
    fn collected_columns(&self) -> &CollectedColumns {
        unsafe {
            self.collected_columns
                .as_ref()
                .expect("the columns are staged from startup onwards")
        }
    }

    /// Stages one row's datums into the batch's memory context, column by column. A
    /// by-reference datum is copied, since the slot's memory is reused for the next row.
    fn collect_datums(&mut self, datums: &[Option<Datum>], column_sizes: Vec<i32>) {
        let collected_columns = unsafe {
            self.collected_columns
                .as_mut()
                .expect("the columns are staged from startup onwards")
        };

        for (idx, datum) in datums.iter().enumerate() {
            let datum = datum.map(|datum| {
                if collected_columns.typbyvals[idx] {
                    datum
                } else {
                    unsafe { datumCopy(datum, false, collected_columns.typlens[idx] as _) }
                }
            });

            collected_columns.columns[idx].push(datum);
        }

        let collected = unsafe {
            std::slice::from_raw_parts_mut(self.collected_tuple_column_sizes, self.natts)
        };

        for (collected, size) in collected.iter_mut().zip(column_sizes.iter()) {
            *collected += *size as i64;
        }

        self.collected_tuple_count += 1;

        // per row rather than per batch: a row that is collected has been processed, and core
        // reports its own COPY TO the same way
        self.tuples_written += 1;
        copy_progress::update_copy_to_tuples(self.tuples_written);
    }

    fn reset_collected_tuples(&mut self) {
        unsafe {
            self.collected_columns
                .as_mut()
                .expect("the columns are staged from startup onwards")
                .clear()
        };

        unsafe { MemoryContextReset(self.batch_memory_context) };

        self.collected_tuple_count = 0;
        self.collected_tuple_column_sizes = unsafe {
            MemoryContextAllocZero(
                self.batch_memory_context,
                std::mem::size_of::<i64>() * self.natts,
            ) as *mut i64
        };
    }

    /// Whether adding this row would push a column past what an Arrow array can address.
    fn would_exceed_max_array_size(&self, column_sizes: &[i32]) -> bool {
        let collected =
            unsafe { std::slice::from_raw_parts(self.collected_tuple_column_sizes, self.natts) };

        collected
            .iter()
            .zip(column_sizes)
            .any(|(collected, size)| *collected + *size as i64 > MAX_ARROW_ARRAY_SIZE)
    }

    fn writer(&mut self) -> &mut BatchWriter {
        unsafe {
            self.writer
                .as_mut()
                .expect("the writer is opened before the first row")
        }
    }

    fn plan(&self) -> &CopyToPlan {
        unsafe {
            self.plan
                .as_ref()
                .expect("the plan is set before the receiver is used")
        }
    }

    /// Opens the writer for the file that is to be written next: the statement's uri, or the next
    /// file under it when the statement is being split.
    fn open_writer(&mut self) {
        debug_assert!(self.writer.is_null());

        // read out of the pointer rather than through `plan()`, so that the borrow is the
        // plan's and not this receiver's: the closure below assigns to `self.writer`
        let plan = unsafe {
            self.plan
                .as_ref()
                .expect("the plan is set before the receiver is used")
        };

        // a split statement writes under a uri, which is the only end `file_size_bytes` is
        // allowed for, so a child is always an object
        let place = match plan.file_size_bytes {
            None => Place::new(&plan.endpoint, &plan.uri),
            // the id is claimed rather than counted, because a parallel COPY TO fills one
            // folder from several processes and two of them must not name the same file
            Some(_) => Place::object(&split::child_uri(
                &plan.uri,
                &plan.provider.file_extensions(),
                split::claim_file_id(self.files_written),
            )),
        };

        let tupledesc = unsafe { PgTupleDesc::from_pg_unchecked(self.tupledesc) };

        // the writer outlives this function, so it is allocated in the per-copy context rather
        // than in whatever context the executor happens to be in
        let mut copy_ctx = PgMemoryContexts::For(self.copy_memory_context);

        unsafe {
            copy_ctx.switch_to(|_| {
                let writer =
                    BatchWriter::open(plan.provider.clone(), &place, &plan.options, &tupledesc);

                self.writer = Box::into_raw(Box::new(writer));
            })
        };
    }

    /// Bytes this statement has written anywhere, for `pg_stat_progress_copy`.
    fn bytes_written(&mut self) -> i64 {
        let current = if self.writer.is_null() {
            0
        } else {
            self.writer().bytes_written()
        };

        self.total_bytes_written + current
    }

    /// Whether the file being written has reached the size the statement asked to split at.
    ///
    /// Asked only between batches, so a file ends up a little over the threshold rather than
    /// exactly at it: a batch is written whole or not at all.
    fn should_finish_current_file(&mut self) -> bool {
        let Some(limit) = self.plan().file_size_bytes else {
            return false;
        };

        self.writer().size_so_far() > limit
    }

    /// Completes the file being written. The next batch opens the next one, which is what keeps
    /// a split `COPY TO` from leaving an empty file behind at the end of the statement.
    fn finish_current_file(&mut self) {
        // consuming: the file is complete once this returns, and there is nothing left to write
        let writer = unsafe { Box::from_raw(self.writer) };
        self.writer = std::ptr::null_mut();

        self.total_bytes_written += writer.finish();
        self.files_written += 1;

        copy_progress::update_copy_to(self.tuples_written, self.total_bytes_written);
    }

    fn write_collected_tuples(&mut self) {
        debug_assert!(!self.tupledesc.is_null());

        // a split statement closed the last file as soon as it was large enough, so the file
        // this batch goes into may not be open yet
        if self.writer.is_null() {
            self.open_writer();
        }

        // read out of the pointer rather than through `collected_columns()`, so that the borrow
        // is the staging area's and not this receiver's: the writer is reached through `self`
        let columns = &unsafe {
            self.collected_columns
                .as_ref()
                .expect("the columns are staged from startup onwards")
        }
        .columns;

        self.writer().write_columns(columns);

        let bytes_written = self.bytes_written();

        copy_progress::update_copy_to(self.tuples_written, bytes_written);

        self.reset_collected_tuples();

        if self.should_finish_current_file() {
            self.finish_current_file();
        }
    }

    fn finish(&mut self) {
        if self.collected_tuple_count > 0 {
            self.write_collected_tuples();
        }

        // the batch that crossed a split threshold closed the file it went into, and a statement
        // whose rows ran out there has nothing left to finish
        if !self.writer.is_null() {
            self.finish_current_file();
        }

        // the file is whole only now, which is the earliest a `TO STDOUT` or a `TO PROGRAM` can
        // have its bytes: both are byte streams, and a format may still have been rewriting a
        // header until `finish` returned. A `TO '<uri>'` was written in place and this is a no-op.
        let plan = self.plan();

        plan.endpoint.send(&plan.uri, self.natts as i16);
    }

    /// Frees everything the receiver owns. Idempotent, because it runs both on the normal path
    /// and from the hook's cleanup after an error.
    pub(crate) fn cleanup(&mut self) {
        if !self.collected_columns.is_null() {
            drop(unsafe { Box::from_raw(self.collected_columns) });
            self.collected_columns = std::ptr::null_mut();
        }

        if !self.writer.is_null() {
            // a writer still here means the statement failed partway: dropping it discards the
            // upload rather than completing a truncated file
            drop(unsafe { Box::from_raw(self.writer) });
            self.writer = std::ptr::null_mut();
        }

        if !self.plan.is_null() {
            drop(unsafe { Box::from_raw(self.plan) });
            self.plan = std::ptr::null_mut();
        }

        if !self.batch_memory_context.is_null() {
            unsafe { MemoryContextDelete(self.batch_memory_context) };
            self.batch_memory_context = std::ptr::null_mut();
        }

        if !self.copy_memory_context.is_null() {
            unsafe { MemoryContextDelete(self.copy_memory_context) };
            self.copy_memory_context = std::ptr::null_mut();
        }
    }
}

#[pg_guard]
extern "C-unwind" fn copy_startup(dest: *mut DestReceiver, _operation: i32, tupledesc: TupleDesc) {
    let arrow_dest = unsafe {
        (dest as *mut ArrowCopyToDestReceiver)
            .as_mut()
            .expect("the dest receiver pointer is ours")
    };

    // a blessed tupledesc is what makes a row type looked up by typmod later; an unblessed one
    // fails for a composite column
    let tupledesc = unsafe { BlessTupleDesc(tupledesc) };

    // from_pg_unchecked, because this descriptor belongs to the executor and must not be
    // dropped when the wrapper goes out of scope
    let tupledesc = unsafe { PgTupleDesc::from_pg_unchecked(tupledesc) };

    arrow_dest.tupledesc = tupledesc.as_ptr();
    arrow_dest.natts = tupledesc.len();
    arrow_dest.collected_columns = Box::into_raw(Box::new(CollectedColumns::new(&tupledesc)));
    arrow_dest.collected_tuple_column_sizes = unsafe {
        MemoryContextAllocZero(
            arrow_dest.batch_memory_context,
            std::mem::size_of::<i64>() * tupledesc.len(),
        ) as *mut i64
    };

    // a statement that writes a single file opens it here rather than at the first row, so that
    // a `COPY TO` of no rows still writes a file: that file is the destination itself, and an
    // empty one is what tells a reader the statement ran. A statement that cuts its rows into
    // the files of a folder opens a file on the first row that goes into it instead, so that it
    // leaves no empty file behind
    if arrow_dest.plan().file_size_bytes.is_none() {
        arrow_dest.open_writer();
    }
}

#[pg_guard]
extern "C-unwind" fn copy_receive(slot: *mut TupleTableSlot, dest: *mut DestReceiver) -> bool {
    let arrow_dest = unsafe {
        (dest as *mut ArrowCopyToDestReceiver)
            .as_mut()
            .expect("the dest receiver pointer is ours")
    };

    let mut batch_ctx = PgMemoryContexts::For(arrow_dest.batch_memory_context);

    unsafe {
        batch_ctx.switch_to(|_| {
            // a slot from `SELECT * FROM t` may not have every attribute extracted yet
            slot_getallattrs(slot);

            let natts = arrow_dest.natts;

            let datums = std::slice::from_raw_parts((*slot).tts_values, natts);
            let nulls = std::slice::from_raw_parts((*slot).tts_isnull, natts);

            let datums: Vec<Option<Datum>> = datums
                .iter()
                .zip(nulls)
                .map(|(datum, is_null)| (!*is_null).then_some(*datum))
                .collect();

            let column_sizes = arrow_dest.tuple_column_sizes(&datums);

            // written before this row joins the batch, not after: the point is that the batch
            // never holds a column too large to convert
            if arrow_dest.would_exceed_max_array_size(&column_sizes) {
                arrow_dest.write_collected_tuples();
            }

            arrow_dest.collect_datums(&datums, column_sizes);

            if arrow_dest.collected_tuple_count >= ROWS_PER_BATCH {
                arrow_dest.write_collected_tuples();
            }
        })
    };

    true
}

#[pg_guard]
extern "C-unwind" fn copy_shutdown(dest: *mut DestReceiver) {
    let arrow_dest = unsafe {
        (dest as *mut ArrowCopyToDestReceiver)
            .as_mut()
            .expect("the dest receiver pointer is ours")
    };

    arrow_dest.finish();
}

#[pg_guard]
extern "C-unwind" fn copy_destroy(_dest: *mut DestReceiver) {}

impl ArrowCopyToDestReceiver {
    /// How many bytes each column of a row is, as Arrow will hold it.
    ///
    /// An approximation, and deliberately the cheap one: the detoasted size of a varlena, the
    /// length of a cstring, the fixed width of anything else. It is only used to decide when a
    /// batch is too wide to convert, and a type whose Arrow form is larger than its Postgres
    /// form is still bounded well below the `i32` limit by the row count.
    fn tuple_column_sizes(&self, datums: &[Option<Datum>]) -> Vec<i32> {
        let typlens = &self.collected_columns().typlens;

        datums
            .iter()
            .zip(typlens)
            .map(|(datum, typlen)| {
                let Some(datum) = datum else {
                    return 0;
                };

                match *typlen {
                    -1 => (unsafe { toast_raw_datum_size(*datum) }) as i32 - VARHDRSZ as i32,
                    -2 => {
                        let cstring = unsafe { CString::from_datum(*datum, false) }
                            .expect("a cstring datum is not null here");

                        cstring.as_bytes().len() as i32 + 1
                    }
                    fixed => fixed as i32,
                }
            })
            .collect()
    }
}

/// Builds a receiver for one `COPY TO`. The caller owns the result and must `cleanup` it.
pub(crate) fn create(plan: CopyToPlan) -> *mut ArrowCopyToDestReceiver {
    let batch_memory_context = unsafe {
        AllocSetContextCreateExtended(
            CurrentMemoryContext as _,
            "pg_arrow_base batch memory context".as_pg_cstr(),
            ALLOCSET_DEFAULT_MINSIZE as _,
            ALLOCSET_DEFAULT_INITSIZE as _,
            ALLOCSET_DEFAULT_MAXSIZE as _,
        )
    };

    let copy_memory_context = unsafe {
        AllocSetContextCreateExtended(
            CurrentMemoryContext as _,
            "pg_arrow_base copy memory context".as_pg_cstr(),
            ALLOCSET_DEFAULT_MINSIZE as _,
            ALLOCSET_DEFAULT_INITSIZE as _,
            ALLOCSET_DEFAULT_MAXSIZE as _,
        )
    };

    let mut arrow_dest = unsafe { PgBox::<ArrowCopyToDestReceiver, AllocatedByPostgres>::alloc0() };

    arrow_dest.dest.receiveSlot = Some(copy_receive);
    arrow_dest.dest.rStartup = Some(copy_startup);
    arrow_dest.dest.rShutdown = Some(copy_shutdown);
    arrow_dest.dest.rDestroy = Some(copy_destroy);
    arrow_dest.dest.mydest = CommandDest::DestCopyOut;

    arrow_dest.natts = 0;
    arrow_dest.tupledesc = std::ptr::null_mut();
    arrow_dest.collected_columns = std::ptr::null_mut();
    arrow_dest.collected_tuple_count = 0;
    arrow_dest.collected_tuple_column_sizes = std::ptr::null_mut();
    arrow_dest.tuples_written = 0;
    arrow_dest.plan = Box::into_raw(Box::new(plan));
    arrow_dest.writer = std::ptr::null_mut();
    arrow_dest.batch_memory_context = batch_memory_context;
    arrow_dest.copy_memory_context = copy_memory_context;
    arrow_dest.total_bytes_written = 0;
    arrow_dest.files_written = 0;

    arrow_dest.into_pg()
}
