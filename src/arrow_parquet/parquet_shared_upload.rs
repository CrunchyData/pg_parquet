use std::{
    cell::RefCell,
    io::{self, Write},
    panic::RefUnwindSafe,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use futures::{future::BoxFuture, FutureExt};
use object_store::{
    multipart::{MultipartStore, PartId},
    path::Path as ObjectStorePath,
    MultipartId,
};
use parquet::{
    arrow::async_writer::AsyncFileWriter,
    errors::{ParquetError, Result as ParquetResult},
    file::{
        metadata::{
            page_index::PageIndexBuilder, FileMetaData, PageIndexPolicy, ParquetMetaData,
            ParquetMetaDataBuilder, ParquetMetaDataReader, ParquetMetaDataWriter, RowGroupMetaData,
        },
        page_index::offset_index::{OffsetIndexMetaData, PageLocation},
        writer::TrackedWrite,
    },
};

// MIN_PART_SIZE is the smallest a part of a multipart upload may be. Every part but the last
// one of the upload has to reach it, so a participant that is left with less than this hands
// its remaining bytes to the leader instead of uploading them as a part of its own.
pub(crate) const MIN_PART_SIZE: usize = 5 * 1024 * 1024;

// PART_SIZE is the size a participant fills a part up to. A part is only closed at a row
// group boundary, so the part a participant uploads is this much plus the row group that ran
// over it. Parts of one upload are allowed to differ in size, which an object store that
// wants them all equal, e.g. R2, does not support.
pub(crate) const PART_SIZE: usize = 8 * 1024 * 1024;

// a part index is claimed out of the high bits of the same atomic the byte offset is claimed
// out of the low bits of, so that a participant takes both in a single uncontended
// instruction. 48 bits of offset reach 256 TiB, which is past what any of the stores takes
// as a single object.
pub(crate) const CLAIM_OFFSET_BITS: u32 = 48;
pub(crate) const CLAIM_PART_STEP: u64 = 1 << CLAIM_OFFSET_BITS;
pub(crate) const CLAIM_OFFSET_MASK: u64 = CLAIM_PART_STEP - 1;

// PartClaim hands out the next part of the shared upload together with the byte offset of
// the destination that part covers. The two have to be taken together, since the parts of an
// upload are numbered by the order their bytes have in the finished object.
// RefUnwindSafe is what lets the claim be held across the error handler the copy runs under:
// a claim is a single atomic instruction, so an error cannot leave it half taken.
pub(crate) trait PartClaim: Send + Sync + RefUnwindSafe {
    fn claim(&self, len: usize) -> (u32, u64);
}

// Region is a stretch of one range's parquet file that ended up at a known offset of the
// destination. An offset inside the stretch is turned into an offset of the destination by
// adding delta to it, which is all the rebasing a row group that lies inside the stretch
// needs: the bytes of the range's file are uploaded as they were written.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Region {
    // the stretch, as offsets of the range's own parquet file
    pub(crate) file_start: u64,
    pub(crate) file_end: u64,
    // what an offset inside the stretch is shifted by. For a tail region this is still
    // missing where the leader writes the tail, see place_tail.
    pub(crate) delta: i64,
    // whether the stretch is still in the participant's hands rather than uploaded, in which
    // case the leader writes it and finishes delta itself
    pub(crate) tail: bool,
}

impl Region {
    // place_tail finishes the delta of a tail region, once the leader knows the offset it
    // writes the participant's tail bytes at
    fn place_tail(&mut self, tail_offset: u64) {
        if self.tail {
            self.delta += tail_offset as i64;
            self.tail = false;
        }
    }
}

// RangeUpload is what one ctid range contributed to the shared upload: where the bytes of its
// parquet file ended up, and the metadata of the file that describes them.
pub(crate) struct RangeUpload {
    pub(crate) range: u32,
    pub(crate) regions: Vec<Region>,
    pub(crate) metadata: ParquetMetaData,
}

// ParticipantUpload is everything one participant of the copy contributed to the shared
// upload. The leader collects one of these per participant, writes the tails and the footer,
// and completes the upload with them.
pub(crate) struct ParticipantUpload {
    pub(crate) parts: Vec<(u32, PartId)>,
    pub(crate) ranges: Vec<RangeUpload>,
    // the bytes of the participant's last part, which did not reach MIN_PART_SIZE
    pub(crate) tail: Vec<u8>,
}

// SharedUpload is one participant's end of the destination's multipart upload. It is given
// to the parquet writer of every range the participant claims, so that the rows of a range
// are uploaded into the destination itself rather than staged as a part object of their own.
pub(crate) struct SharedUpload {
    inner: Arc<Mutex<SharedUploadInner>>,
}

struct SharedUploadInner {
    store: Arc<dyn MultipartStore>,
    path: ObjectStorePath,
    upload_id: MultipartId,
    claimer: Arc<dyn PartClaim>,

    // what the parquet writers handed over and is not uploaded yet
    buffer: Vec<u8>,
    // how much the parquet writers handed over in total, and where the buffer starts in it
    stream_len: u64,
    buffer_start: u64,

    // the stretches of the stream that are uploaded, in stream order
    regions: Vec<StreamRegion>,
    parts: Vec<(u32, PartId)>,

    // where the range that is being written started in the stream, and the ranges that are
    // written already
    file_start: u64,
    files: Vec<FileUpload>,
}

// StreamRegion is a stretch of the participant's stream that went into a single part
struct StreamRegion {
    stream_start: u64,
    stream_end: u64,
    base: u64,
}

struct FileUpload {
    range: u32,
    stream_start: u64,
    stream_end: u64,
    metadata: ParquetMetaData,
}

// PendingPart is a part that is claimed and is about to be uploaded
struct PendingPart {
    part: u32,
    data: Vec<u8>,
}

impl SharedUpload {
    pub(crate) fn new(
        store: Arc<dyn MultipartStore>,
        path: ObjectStorePath,
        upload_id: MultipartId,
        claimer: Arc<dyn PartClaim>,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SharedUploadInner {
                store,
                path,
                upload_id,
                claimer,
                buffer: Vec::with_capacity(PART_SIZE),
                stream_len: 0,
                buffer_start: 0,
                regions: Vec::new(),
                parts: Vec::new(),
                file_start: 0,
                files: Vec::new(),
            })),
        }
    }

    // writer is the sink the parquet writer of a single range writes into
    pub(crate) fn writer(&self) -> Box<dyn AsyncFileWriter> {
        Box::new(SharedUploadWriter {
            inner: self.inner.clone(),
        })
    }

    // end_range closes the parquet file of a range, with the metadata the file's own footer
    // holds, so that the leader can say where the range's row groups ended up
    pub(crate) fn end_range(&self, range: u32, metadata: ParquetMetaData) {
        let mut inner = self.lock();

        let stream_start = inner.file_start;
        let stream_end = inner.stream_len;

        inner.files.push(FileUpload {
            range,
            stream_start,
            stream_end,
            metadata,
        });

        inner.file_start = stream_end;
    }

    // finish ends the participant's share of the upload: the buffer becomes a last part of
    // its own when it is large enough to be one, and the tail of the upload otherwise.
    pub(crate) async fn finish(self) -> ParquetResult<ParticipantUpload> {
        self.upload_pending_part(MIN_PART_SIZE).await?;

        let mut inner = self.lock();

        let tail = std::mem::take(&mut inner.buffer);
        let tail_start = inner.buffer_start;
        let stream_len = inner.stream_len;

        let files = std::mem::take(&mut inner.files);

        let ranges = files
            .into_iter()
            .map(|file| RangeUpload {
                range: file.range,
                regions: file_regions(&inner.regions, &file, tail_start, stream_len),
                metadata: file.metadata,
            })
            .collect();

        Ok(ParticipantUpload {
            parts: std::mem::take(&mut inner.parts),
            ranges,
            tail,
        })
    }

    // upload_pending_part uploads the buffer as the next part of the upload, once it reached
    // the given size. The part is claimed and recorded before it is uploaded, since the
    // claim is what orders the parts of the upload, and a participant uploads its parts one
    // after the other.
    async fn upload_pending_part(&self, min_size: usize) -> ParquetResult<()> {
        let pending = {
            let mut inner = self.lock();

            match inner.take_pending_part(min_size) {
                Some(pending) => pending,
                None => return Ok(()),
            }
        };

        let (store, path, upload_id) = {
            let inner = self.lock();

            (
                inner.store.clone(),
                inner.path.clone(),
                inner.upload_id.clone(),
            )
        };

        let part_id = store
            .put_part(
                &path,
                &upload_id,
                pending.part as usize,
                pending.data.into(),
            )
            .await
            .map_err(|e| ParquetError::External(Box::new(e)))?;

        self.lock().parts.push((pending.part, part_id));

        Ok(())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, SharedUploadInner> {
        self.inner
            .lock()
            .expect("shared upload of the parallel COPY TO is poisoned")
    }
}

impl SharedUploadInner {
    // take_pending_part claims the destination's next part for the buffer and hands the
    // buffer out to be uploaded, once the buffer reached the given size
    fn take_pending_part(&mut self, min_size: usize) -> Option<PendingPart> {
        if self.buffer.is_empty() || self.buffer.len() < min_size {
            return None;
        }

        let data = std::mem::replace(&mut self.buffer, Vec::with_capacity(PART_SIZE));

        let (part, base) = self.claimer.claim(data.len());

        self.regions.push(StreamRegion {
            stream_start: self.buffer_start,
            stream_end: self.buffer_start + data.len() as u64,
            base,
        });

        self.buffer_start += data.len() as u64;

        Some(PendingPart { part, data })
    }
}

// SharedUploadWriter is the sink a single range's parquet writer writes into. Every write it
// receives ends at a row group boundary, since the parquet writer only drains its own buffer
// once it closed a row group, which is what lets a part end wherever the part size is
// reached: no row group is ever split between two parts.
struct SharedUploadWriter {
    inner: Arc<Mutex<SharedUploadInner>>,
}

impl AsyncFileWriter for SharedUploadWriter {
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, ParquetResult<()>> {
        let upload = SharedUpload {
            inner: self.inner.clone(),
        };

        async move {
            {
                let mut inner = upload.lock();

                inner.buffer.extend_from_slice(&bs);
                inner.stream_len += bs.len() as u64;
            }

            upload.upload_pending_part(PART_SIZE).await
        }
        .boxed()
    }

    // the sink outlives the parquet writer that completes it, since the ranges a participant
    // claims share it, so there is nothing to finish here. What is left of the last range is
    // uploaded by SharedUpload::finish.
    fn complete(&mut self) -> BoxFuture<'_, ParquetResult<()>> {
        async move { Ok(()) }.boxed()
    }
}

// file_regions is where the bytes of a single range's parquet file ended up. The stretches of
// the stream the file covers are the parts the participant uploaded while it was writing the
// file, plus the tail it did not upload.
fn file_regions(
    regions: &[StreamRegion],
    file: &FileUpload,
    tail_start: u64,
    stream_len: u64,
) -> Vec<Region> {
    let mut file_regions = Vec::new();

    for region in regions {
        if let Some(region) = intersect(
            file,
            region.stream_start,
            region.stream_end,
            region.base as i64,
            false,
        ) {
            file_regions.push(region);
        }
    }

    // the tail is not uploaded yet, so its offset of the destination is still missing, which
    // the leader adds once it knows where it writes the tail
    if let Some(region) = intersect(file, tail_start, stream_len, 0, true) {
        file_regions.push(region);
    }

    file_regions
}

// intersect is the part of the given stretch of the stream that holds bytes of the given
// file, as offsets of the file itself
fn intersect(
    file: &FileUpload,
    stream_start: u64,
    stream_end: u64,
    base: i64,
    tail: bool,
) -> Option<Region> {
    let start = stream_start.max(file.stream_start);
    let end = stream_end.min(file.stream_end);

    if start >= end {
        return None;
    }

    Some(Region {
        file_start: start - file.stream_start,
        file_end: end - file.stream_start,
        // an offset of the file sits at file.stream_start + offset of the stream, which is
        // base + (file.stream_start + offset - stream_start) of the destination
        delta: base + file.stream_start as i64 - stream_start as i64,
        tail,
    })
}

// complete_shared_upload finishes the destination's upload: the tails the participants could
// not upload themselves and the footer of the whole file become its last part, and the parts
// every participant uploaded are what the upload is completed with. It returns how many rows
// the finished file holds. The claim of the last part is handed in rather than taken here,
// since what hands the claims out is gone by the time the upload is completed.
pub(crate) async fn complete_shared_upload(
    store: Arc<dyn MultipartStore>,
    path: ObjectStorePath,
    upload_id: MultipartId,
    last_claim: (u32, u64),
    participants: Vec<ParticipantUpload>,
    nranges: u32,
) -> ParquetResult<i64> {
    let mut parts = participants
        .iter()
        .flat_map(|participant| participant.parts.iter().cloned())
        .collect::<Vec<_>>();

    // the last part holds the tails and the footer, so it is claimed after every other part
    // of the upload is, which every participant being done with its ranges is what makes
    // possible. Its length is not known before the footer is written, and does not have to
    // be: nothing of the file follows it.
    let (part, base) = last_claim;

    let (last_part, nrows) = last_part(participants, nranges, base)?;

    let part_id = store
        .put_part(&path, &upload_id, part as usize, last_part.into())
        .await
        .map_err(|e| ParquetError::External(Box::new(e)))?;

    parts.push((part, part_id));

    // the parts of an upload are completed in the order their bytes have in the object, which
    // is the order they were claimed in
    parts.sort_by_key(|(part, _)| *part);

    for (at, (part, _)) in parts.iter().enumerate() {
        assert_eq!(
            at, *part as usize,
            "the parts of the parallel COPY TO upload are not consecutive"
        );
    }

    let parts = parts.into_iter().map(|(_, part_id)| part_id).collect();

    store
        .complete_multipart(&path, &upload_id, parts)
        .await
        .map_err(|e| ParquetError::External(Box::new(e)))?;

    Ok(nrows)
}

// last_part builds the last part of the upload, which is written at the given offset of the
// destination: the tails of the participants, and the footer that says where every row group
// of every range ended up. It returns the part and how many rows the file holds.
fn last_part(
    participants: Vec<ParticipantUpload>,
    nranges: u32,
    base: u64,
) -> ParquetResult<(Vec<u8>, i64)> {
    let mut last_part = Vec::new();

    let mut ranges = Vec::with_capacity(nranges as usize);

    for mut participant in participants {
        // the participant's tail goes into the last part, which is where the regions that
        // are still in its hands end up
        let tail_offset = base + last_part.len() as u64;
        last_part.extend_from_slice(&participant.tail);

        for range in participant.ranges.iter_mut() {
            for region in range.regions.iter_mut() {
                region.place_tail(tail_offset);
            }
        }

        ranges.append(&mut participant.ranges);
    }

    // the rows of the file are in the order the ranges cover the table in, which is the order
    // the single process path reads them in
    ranges.sort_by_key(|range| range.range);

    assert_eq!(
        ranges.len(),
        nranges as usize,
        "the parallel COPY TO did not report every range of the table"
    );

    let mut row_groups = Vec::new();

    // the page indexes of the ranges are rewritten into the footer rather than read where
    // they were uploaded, since the offsets of the pages they hold are the ones the pages
    // had in the parquet file of the range rather than the ones they have here
    let nrow_groups = ranges
        .iter()
        .map(|range| range.metadata.row_groups().len())
        .sum();

    let ncolumns = range_metadata(&ranges)?.schema_descr().num_columns();

    let mut page_index = PageIndexBuilder::new(nrow_groups, ncolumns);

    for (at, range) in ranges.iter().enumerate() {
        assert_eq!(
            at, range.range as usize,
            "the parallel COPY TO reported a range twice"
        );

        for (in_range, row_group) in range.metadata.row_groups().iter().enumerate() {
            row_groups.push(rebase_row_group(row_group, &range.regions)?);

            let indexes = range.metadata.page_index_for_row_group(in_range);
            let in_file = row_groups.len() - 1;

            for column in 0..row_group.columns().len() {
                if let Some(column_index) = indexes.column_index(column) {
                    page_index.put_column_index(column_index.clone(), in_file, column);
                }

                if let Some(offset_index) = indexes.offset_index(column) {
                    let offset_index = rebase_offset_index(offset_index, &range.regions)?;

                    page_index.put_offset_index(offset_index, in_file, column);
                }
            }
        }
    }

    let nrows = row_groups
        .iter()
        .map(|row_group| row_group.num_rows())
        .sum();

    // every range wrote the rows of the same table with the same options, so the schema and
    // the key value metadata of the file are the ones every range's own file has
    let range_metadata = range_metadata(&ranges)?;

    let file_metadata = FileMetaData::new(
        range_metadata.version(),
        nrows,
        range_metadata.created_by().map(str::to_string),
        range_metadata.key_value_metadata().cloned(),
        range_metadata.schema_descr_ptr(),
        range_metadata.column_orders().cloned(),
    );

    let metadata = ParquetMetaDataBuilder::new(file_metadata)
        .set_row_groups(row_groups)
        .set_page_index(Some(Arc::new(page_index.build())))
        .build();

    // the page indexes of the footer are written at the offsets they get in the destination,
    // which the part the footer goes into is not the start of
    let footer_start = base + last_part.len() as u64;

    let mut footer = Vec::new();

    let tracked = skipped_write(&mut footer, footer_start)?;

    ParquetMetaDataWriter::new_with_tracked(tracked, &metadata).finish()?;

    last_part.extend_from_slice(&footer);

    Ok((last_part, nrows))
}

// range_metadata is the metadata of a range's own parquet file, which every range of the same
// copy shares everything but its row groups with
fn range_metadata(ranges: &[RangeUpload]) -> ParquetResult<&FileMetaData> {
    ranges
        .first()
        .map(|range| range.metadata.file_metadata())
        .ok_or_else(|| {
            ParquetError::General("the parallel COPY TO has no range to take the schema of".into())
        })
}

// SkipWrite swallows the bytes written into it until the given offset is reached. It is what
// lets a parquet footer be written with the offsets it has in the destination while it is
// written into a buffer that holds the end of the destination only.
struct SkipWrite<W: Write> {
    skip: usize,
    inner: W,
}

impl<W: Write> Write for SkipWrite<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.skip >= buf.len() {
            self.skip -= buf.len();

            return Ok(buf.len());
        }

        let skipped = self.skip;
        self.skip = 0;

        self.inner
            .write(&buf[skipped..])
            .map(|written| written + skipped)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

// skipped_write is a writer whose bytes land at the given offset of the destination, and which
// counts them as if everything before that offset went through it as well. The bytes before it
// are written nowhere, which is what makes them cost nothing but the calls that write them.
fn skipped_write<W: Write>(inner: W, at: u64) -> ParquetResult<TrackedWrite<SkipWrite<W>>> {
    static SKIPPED: [u8; 64 * 1024] = [0; 64 * 1024];

    let skip = usize::try_from(at).map_err(|_| {
        ParquetError::General(format!(
            "the parallel COPY TO cannot write a footer at offset {at} of the destination"
        ))
    })?;

    let mut write = TrackedWrite::new(SkipWrite { skip, inner });

    let mut left = skip;

    while left > 0 {
        let at_once = left.min(SKIPPED.len());

        write.write_all(&SKIPPED[..at_once])?;

        left -= at_once;
    }

    Ok(write)
}

// rebase_row_group turns the offsets a row group has in the parquet file of its own range
// into the offsets it has in the destination. Each offset is rebased on its own: the pages of
// a row group sit where the row group was written, but the page index and the bloom filter of
// the same row group are written past the last row group of the range, so they are uploaded
// in a region of their own.
fn rebase_row_group(
    row_group: &RowGroupMetaData,
    regions: &[Region],
) -> ParquetResult<RowGroupMetaData> {
    let columns = row_group
        .columns()
        .iter()
        .map(|column| {
            column
                .clone()
                .into_builder()
                .set_data_page_offset(rebase_offset(column.data_page_offset(), regions)?)
                .set_dictionary_page_offset(rebase_optional_offset(
                    column.dictionary_page_offset(),
                    regions,
                )?)
                .set_index_page_offset(rebase_optional_offset(column.index_page_offset(), regions)?)
                .set_bloom_filter_offset(rebase_optional_offset(
                    column.bloom_filter_offset(),
                    regions,
                )?)
                // the page index of the range was uploaded with the rows it belongs to,
                // but it is the rebased one the footer carries that is read, which the
                // writer of the footer is what points the column chunks at
                .set_column_index_offset(None)
                .set_column_index_length(None)
                .set_offset_index_offset(None)
                .set_offset_index_length(None)
                .build()
        })
        .collect::<ParquetResult<Vec<_>>>()?;

    row_group
        .clone()
        .into_builder()
        .set_file_offset(rebase_offset(row_group_offset(row_group), regions)?)
        .set_column_metadata(columns)
        .build()
}

// rebase_offset is where the given offset of a range's own parquet file ended up in the
// destination
fn rebase_offset(offset: i64, regions: &[Region]) -> ParquetResult<i64> {
    let at = offset as u64;

    regions
        .iter()
        .find(|region| region.file_start <= at && at < region.file_end)
        .map(|region| offset + region.delta)
        .ok_or_else(|| {
            ParquetError::General(format!(
                "the parallel COPY TO did not report where offset {offset} of its range was \
                 uploaded"
            ))
        })
}

// rebase_offset_index turns the offsets the pages of a column chunk have in the parquet file
// of its own range into the offsets they have in the destination
fn rebase_offset_index(
    offset_index: &OffsetIndexMetaData,
    regions: &[Region],
) -> ParquetResult<OffsetIndexMetaData> {
    let page_locations = offset_index
        .page_locations()
        .iter()
        .map(|page| {
            Ok(PageLocation {
                offset: rebase_offset(page.offset, regions)?,
                compressed_page_size: page.compressed_page_size,
                first_row_index: page.first_row_index,
            })
        })
        .collect::<ParquetResult<Vec<_>>>()?;

    Ok(OffsetIndexMetaData {
        page_locations,
        unencoded_byte_array_data_bytes: offset_index.unencoded_byte_array_data_bytes().cloned(),
    })
}

fn rebase_optional_offset(offset: Option<i64>, regions: &[Region]) -> ParquetResult<Option<i64>> {
    offset
        .map(|offset| rebase_offset(offset, regions))
        .transpose()
}

// row_group_offset is where the data of a row group starts in the file it was written into.
// The file offset the row group itself carries is not written by every writer, so it is taken
// from the first column chunk, which is where the row group's data starts.
fn row_group_offset(row_group: &RowGroupMetaData) -> i64 {
    row_group
        .columns()
        .iter()
        .map(|column| {
            column
                .dictionary_page_offset()
                .unwrap_or_else(|| column.data_page_offset())
        })
        .min()
        .unwrap_or(0)
}

// encode_range_metadata serializes the metadata of a range's own parquet file, which is how it
// travels from the participant that wrote the range to the leader that writes the footer
pub(crate) fn encode_range_metadata(metadata: &ParquetMetaData) -> ParquetResult<Vec<u8>> {
    let mut encoded = Vec::new();

    ParquetMetaDataWriter::new(&mut encoded, metadata).finish()?;

    Ok(encoded)
}

// decode_range_metadata reads back what encode_range_metadata wrote. What it wrote is a
// parquet footer, page indexes and all, so it is read the way the footer of a file is, which
// is also what makes the offsets the indexes carry in it come out right.
pub(crate) fn decode_range_metadata(encoded: Bytes) -> ParquetResult<ParquetMetaData> {
    ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .parse_and_finish(&encoded)
}

// the shared upload the ranges a participant of a parallel COPY TO claims write into. The
// destination of a range is a rendered COPY statement, which cannot carry anything but the
// uri the range is written to, so the upload the uri stands for is installed here for as
// long as the participant is writing ranges.
thread_local! {
    static INSTALLED_UPLOAD: RefCell<Option<InstalledUpload>> = const { RefCell::new(None) };
}

struct InstalledUpload {
    // the uri the range that is being written renders as its destination, which is what the
    // parquet writer of another copy running in the same backend is told apart by
    range_uri: String,
    upload: SharedUpload,
    // the metadata of the range's own parquet file, which its writer leaves behind when it
    // writes the file's footer
    range_metadata: Option<ParquetMetaData>,
}

impl SharedUpload {
    fn clone_handle(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

// install_shared_upload makes the given upload the destination of the copy that writes the
// given uri, until uninstall_shared_upload is called
pub(crate) fn install_shared_upload(range_uri: String, upload: &SharedUpload) {
    INSTALLED_UPLOAD.with_borrow_mut(|installed| {
        *installed = Some(InstalledUpload {
            range_uri,
            upload: upload.clone_handle(),
            range_metadata: None,
        })
    });
}

pub(crate) fn uninstall_shared_upload() {
    INSTALLED_UPLOAD.with_borrow_mut(|installed| *installed = None);
}

// shared_upload_writer is the sink a copy into the given uri writes into, when the uri is the
// one a shared upload is installed for
pub(crate) fn shared_upload_writer(uri: &str) -> Option<Box<dyn AsyncFileWriter>> {
    INSTALLED_UPLOAD.with_borrow(|installed| {
        let installed = installed.as_ref()?;

        (installed.range_uri == uri).then(|| installed.upload.writer())
    })
}

// stash_range_metadata keeps the metadata of the range's own parquet file, which is what says
// where the row groups the range wrote sit in the bytes it uploaded
pub(crate) fn stash_range_metadata(metadata: &ParquetMetaData) {
    INSTALLED_UPLOAD.with_borrow_mut(|installed| {
        if let Some(installed) = installed.as_mut() {
            installed.range_metadata = Some(metadata.clone());
        }
    });
}

pub(crate) fn take_range_metadata() -> Option<ParquetMetaData> {
    INSTALLED_UPLOAD.with_borrow_mut(|installed| installed.as_mut()?.range_metadata.take())
}
