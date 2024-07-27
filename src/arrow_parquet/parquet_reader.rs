use arrow::array::RecordBatch;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use pgrx::{
    pg_sys::{
        fmgr_info, getTypeBinaryOutputInfo, varlena, Datum, FmgrInfo, InvalidOid, SendFunctionCall,
        TupleDesc,
    },
    vardata_any, varsize_any_exhdr, void_mut_ptr, AllocatedByPostgres, PgBox, PgTupleDesc,
};

use crate::{arrow_parquet::arrow_to_pg::as_pg_datum, pgrx_utils::collect_valid_attributes};

pub(crate) struct ParquetReaderContext {
    buffer: Vec<u8>,
    offset: usize,
    batch_size: i64,
    started: bool,
    finished: bool,
    parquet_reader: ParquetRecordBatchReader,
    tupledesc: TupleDesc,
    binary_out_funcs: Vec<PgBox<FmgrInfo>>,
}

impl ParquetReaderContext {
    pub(crate) fn new(filename: String, batch_size: i64, tupledesc: TupleDesc) -> Self {
        let parquet_file = std::fs::OpenOptions::new()
            .read(true)
            .open(&filename)
            .unwrap();

        let builder = ParquetRecordBatchReaderBuilder::try_new(parquet_file).unwrap();
        pgrx::debug2!("Converted arrow schema is: {}", builder.schema());

        let parquet_reader = builder.with_batch_size(1).build().unwrap();

        let binary_out_funcs = Self::collect_binary_out_funcs(tupledesc);

        ParquetReaderContext {
            buffer: Vec::new(),
            offset: 0,
            batch_size,
            tupledesc,
            parquet_reader,
            binary_out_funcs,
            started: false,
            finished: false,
        }
    }

    fn collect_binary_out_funcs(tupledesc: TupleDesc) -> Vec<PgBox<FmgrInfo, AllocatedByPostgres>> {
        unsafe {
            let tupledesc = PgTupleDesc::from_pg(tupledesc);

            let mut binary_out_funcs = vec![];

            let include_generated_columns = false;
            let attributes = collect_valid_attributes(&tupledesc, include_generated_columns);

            for att in attributes.iter() {
                let typoid = att.type_oid();

                let mut send_func_oid = InvalidOid;
                let mut is_varlena = false;
                getTypeBinaryOutputInfo(typoid.value(), &mut send_func_oid, &mut is_varlena);

                let arg_fninfo = PgBox::<FmgrInfo>::alloc0().into_pg_boxed();
                fmgr_info(send_func_oid, arg_fninfo.as_ptr());

                binary_out_funcs.push(arg_fninfo);
            }

            let _ = tupledesc.into_pg();

            binary_out_funcs
        }
    }

    pub(crate) fn bytes_in_buffer(&self) -> usize {
        self.buffer.len() - self.offset
    }

    pub(crate) fn reset_buffer(&mut self) {
        self.buffer.clear();
        self.offset = 0;
    }

    fn copy_start(&mut self) {
        self.started = true;

        /* Binary signature */
        let signature_bytes = b"\x50\x47\x43\x4f\x50\x59\x0a\xff\x0d\x0a\x00";
        self.buffer.extend_from_slice(signature_bytes);

        /* Flags field */
        let flags = 0_i32;
        let flags_bytes = flags.to_be_bytes();
        self.buffer.extend_from_slice(&flags_bytes);

        /* No header extension */
        let header_ext_len = 0_i32;
        let header_ext_len_bytes = header_ext_len.to_be_bytes();
        self.buffer.extend_from_slice(&header_ext_len_bytes);
    }

    fn copy_finish(&mut self) {
        self.finished = true;

        /* trailer */
        let trailer_len = -1_i16;
        let trailer_len_bytes = trailer_len.to_be_bytes();
        self.buffer.extend_from_slice(&trailer_len_bytes);
    }

    fn row_to_tuple_datums(
        record_batch: RecordBatch,
        tupledesc: &PgTupleDesc,
    ) -> Vec<Option<Datum>> {
        let mut datums = vec![];

        let include_generated_columns = false;
        let attributes = collect_valid_attributes(&tupledesc, include_generated_columns);

        for attribute in attributes {
            let name = attribute.name();
            let typoid = attribute.type_oid().value();
            let typmod = attribute.type_mod();

            let column = record_batch.column_by_name(name).unwrap();

            let datum = as_pg_datum(column.to_data(), typoid, typmod);
            datums.push(datum);
        }

        datums
    }

    pub(crate) fn read_parquet(&mut self) -> bool {
        if self.finished {
            return true;
        }

        if !self.started {
            self.copy_start();
        }

        let tupledesc = unsafe { PgTupleDesc::from_pg(self.tupledesc) };
        let include_generated_columns = false;
        let attributes = collect_valid_attributes(&tupledesc, include_generated_columns);
        let natts = attributes.len() as i16;

        for _ in 0..self.batch_size {
            pgrx::pg_sys::check_for_interrupts!();

            let record_batch = self.parquet_reader.next();

            if let Some(batch_result) = record_batch {
                let record_batch = batch_result.unwrap();
                // pgrx::warning!("record batch: {:?}", record_batch);

                /* 2 bytes: per-tuple header */
                let attnum_len_bytes = natts.to_be_bytes();
                self.buffer.extend_from_slice(&attnum_len_bytes);

                let tuple_datums = Self::row_to_tuple_datums(record_batch, &tupledesc);

                for (datum, out_func) in tuple_datums.into_iter().zip(self.binary_out_funcs.iter())
                {
                    if let Some(datum) = datum {
                        unsafe {
                            let datum_bytes: *mut varlena =
                                SendFunctionCall(out_func.as_ptr(), datum);

                            /* 4 bytes: attribute's data size */
                            let data_size = varsize_any_exhdr(datum_bytes);
                            let data_size_bytes = (data_size as i32).to_be_bytes();
                            self.buffer.extend_from_slice(&data_size_bytes);

                            /* variable bytes: attribute's data */
                            let data = vardata_any(datum_bytes) as *const u8;
                            let data_bytes = std::slice::from_raw_parts(data, data_size);
                            self.buffer.extend_from_slice(&data_bytes);
                        };
                    } else {
                        /* 4 bytes: null */
                        let null_value = -1_i32;
                        let null_value_bytes = null_value.to_be_bytes();
                        self.buffer.extend_from_slice(&null_value_bytes);
                    }
                }
            } else {
                self.copy_finish();
                break;
            }
        }

        let _ = tupledesc.into_pg();

        true
    }

    pub(crate) fn copy_to_outbuf(&mut self, len: usize, outbuf: void_mut_ptr) {
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.buffer.as_ptr().add(self.offset),
                outbuf as *mut u8,
                len,
            );

            self.offset += len;
        };
    }
}
