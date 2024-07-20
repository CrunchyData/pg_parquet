use arrow::array::{
    Array, ArrayData, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, IntervalMonthDayNanoArray, ListArray, StringArray,
    StructArray, Time64MicrosecondArray, TimestampMicrosecondArray,
};
use pgrx::{
    pg_sys::{
        Datum, Oid, BOOLARRAYOID, BOOLOID, CHARARRAYOID, CHAROID, DATEARRAYOID, DATEOID,
        FLOAT4ARRAYOID, FLOAT4OID, FLOAT8ARRAYOID, FLOAT8OID, INT2ARRAYOID, INT2OID, INT4ARRAYOID,
        INT4OID, INT8ARRAYOID, INT8OID, INTERVALARRAYOID, INTERVALOID, NUMERICARRAYOID, NUMERICOID,
        TEXTARRAYOID, TEXTOID, TIMEARRAYOID, TIMEOID, TIMESTAMPARRAYOID, TIMESTAMPOID,
        TIMESTAMPTZARRAYOID, TIMESTAMPTZOID, TIMETZARRAYOID, TIMETZOID, VARCHARARRAYOID,
        VARCHAROID,
    },
    prelude::PgHeapTuple,
    AllocatedByRust, IntoDatum, PgTupleDesc,
};

use crate::pgrx_utils::{
    array_element_typoid, collect_valid_attributes, is_array_type, is_composite_type, tuple_desc,
};

pub(crate) mod bool;
pub(crate) mod date;
pub(crate) mod float4;
pub(crate) mod float8;
pub(crate) mod int2;
pub(crate) mod int4;
pub(crate) mod int8;
pub(crate) mod interval;
pub(crate) mod numeric;
pub(crate) mod record;
pub(crate) mod text;
pub(crate) mod time;
pub(crate) mod timestamp;

pub(crate) trait ArrowArrayToPgType<T: From<ArrayData>> {
    fn as_pg_datum(self, typoid: Oid, typmod: i32) -> Option<Datum>;
}

pub(crate) fn as_pg_datum(row: ArrayData, typoid: Oid, typmod: i32) -> Option<Datum> {
    let is_array = is_array_type(typoid);
    let is_composite = is_composite_type(typoid);

    if is_array {
        as_pg_array_datum(row, typoid, typmod)
    } else if is_composite {
        let tupledesc = tuple_desc(typoid, typmod);
        as_pg_tuple(row, tupledesc).into_datum()
    } else {
        as_pg_primitive_datum(row, typoid)
    }
}

fn as_pg_primitive_datum(primitive_array: ArrayData, typoid: Oid) -> Option<Datum> {
    match typoid {
        FLOAT4OID => {
            let arrow_array: Float32Array = primitive_array.into();
            ArrowArrayToPgType::as_pg_datum(arrow_array, typoid, -1)
        }
        FLOAT8OID => {
            let arrow_array: Float64Array = primitive_array.into();
            ArrowArrayToPgType::as_pg_datum(arrow_array, typoid, -1)
        }
        INT2OID => {
            let arrow_array: Int16Array = primitive_array.into();
            ArrowArrayToPgType::as_pg_datum(arrow_array, typoid, -1)
        }
        INT4OID => {
            let arrow_array: Int32Array = primitive_array.into();
            ArrowArrayToPgType::as_pg_datum(arrow_array, typoid, -1)
        }
        INT8OID => {
            let arrow_array: Int64Array = primitive_array.into();
            ArrowArrayToPgType::as_pg_datum(arrow_array, typoid, -1)
        }
        BOOLOID => {
            let arrow_array: BooleanArray = primitive_array.into();
            ArrowArrayToPgType::as_pg_datum(arrow_array, typoid, -1)
        }
        CHAROID | TEXTOID | VARCHAROID => {
            let arrow_array: StringArray = primitive_array.into();
            ArrowArrayToPgType::as_pg_datum(arrow_array, typoid, -1)
        }
        NUMERICOID => {
            let arrow_array: Decimal128Array = primitive_array.into();
            ArrowArrayToPgType::as_pg_datum(arrow_array, typoid, -1)
        }
        DATEOID => {
            let arrow_array: Date32Array = primitive_array.into();
            ArrowArrayToPgType::as_pg_datum(arrow_array, typoid, -1)
        }
        TIMEOID | TIMETZOID => {
            let arrow_array: Time64MicrosecondArray = primitive_array.into();
            ArrowArrayToPgType::as_pg_datum(arrow_array, typoid, -1)
        }
        TIMESTAMPOID | TIMESTAMPTZOID => {
            let arrow_array: TimestampMicrosecondArray = primitive_array.into();
            ArrowArrayToPgType::as_pg_datum(arrow_array, typoid, -1)
        }
        INTERVALOID => {
            let arrow_array: IntervalMonthDayNanoArray = primitive_array.into();
            ArrowArrayToPgType::as_pg_datum(arrow_array, typoid, -1)
        }
        _ => {
            panic!("unsupported primitive type {:?}", primitive_array)
        }
    }
}

fn as_pg_array_datum(list_array: ArrayData, typoid: Oid, typmod: i32) -> Option<Datum> {
    let list_array: ListArray = list_array.into();

    if list_array.is_null(0) {
        return None;
    }

    let list_array = list_array.value(0).to_data();

    match typoid {
        FLOAT4ARRAYOID => {
            let arrow_array: Float32Array = list_array.into();
            ArrowArrayToPgType::as_pg_datum(arrow_array, typoid, typmod)
        }
        FLOAT8ARRAYOID => {
            let arrow_array: Float64Array = list_array.into();
            ArrowArrayToPgType::as_pg_datum(arrow_array, typoid, typmod)
        }
        INT2ARRAYOID => {
            let arrow_array: Int16Array = list_array.into();
            ArrowArrayToPgType::as_pg_datum(arrow_array, typoid, typmod)
        }
        INT4ARRAYOID => {
            let arrow_array: Int32Array = list_array.into();
            ArrowArrayToPgType::as_pg_datum(arrow_array, typoid, typmod)
        }
        INT8ARRAYOID => {
            let arrow_array: Int64Array = list_array.into();
            ArrowArrayToPgType::as_pg_datum(arrow_array, typoid, typmod)
        }
        BOOLARRAYOID => {
            let arrow_array: BooleanArray = list_array.into();
            ArrowArrayToPgType::as_pg_datum(arrow_array, typoid, typmod)
        }
        CHARARRAYOID | TEXTARRAYOID | VARCHARARRAYOID => {
            let arrow_array: StringArray = list_array.into();
            ArrowArrayToPgType::as_pg_datum(arrow_array, typoid, typmod)
        }
        NUMERICARRAYOID => {
            let arrow_array: Decimal128Array = list_array.into();
            ArrowArrayToPgType::as_pg_datum(arrow_array, typoid, typmod)
        }
        DATEARRAYOID => {
            let arrow_array: Date32Array = list_array.into();
            ArrowArrayToPgType::as_pg_datum(arrow_array, typoid, typmod)
        }
        TIMEARRAYOID | TIMETZARRAYOID => {
            let arrow_array: Time64MicrosecondArray = list_array.into();
            ArrowArrayToPgType::as_pg_datum(arrow_array, typoid, typmod)
        }
        TIMESTAMPARRAYOID | TIMESTAMPTZARRAYOID => {
            let arrow_array: TimestampMicrosecondArray = list_array.into();
            ArrowArrayToPgType::as_pg_datum(arrow_array, typoid, typmod)
        }
        INTERVALARRAYOID => {
            let arrow_array: IntervalMonthDayNanoArray = list_array.into();
            ArrowArrayToPgType::as_pg_datum(arrow_array, typoid, typmod)
        }
        _ => {
            let element_typoid = array_element_typoid(typoid);

            if is_composite_type(element_typoid) {
                as_pg_tuple_array(list_array, element_typoid, typmod).into_datum()
            } else {
                panic!("unsupported array type {:?}", typoid)
            }
        }
    }
}

fn as_pg_tuple(tuple: ArrayData, tupledesc: PgTupleDesc) -> Option<PgHeapTuple<AllocatedByRust>> {
    let arrow_struct_array: StructArray = tuple.into();

    if arrow_struct_array.is_null(0) {
        return None;
    }

    let mut datums = vec![];

    let attributes = collect_valid_attributes(&tupledesc);

    for attribute in attributes {
        let name = attribute.name();
        let typoid = attribute.type_oid().value();
        let typmod = attribute.type_mod();

        let column_data = arrow_struct_array.column_by_name(name).unwrap();

        let datum = as_pg_datum(column_data.into_data(), typoid, typmod);
        datums.push(datum);
    }

    Some(unsafe { PgHeapTuple::from_datums(tupledesc, datums) }.unwrap())
}

fn as_pg_tuple_array(
    tuples: ArrayData,
    typoid: Oid,
    typmod: i32,
) -> Option<Vec<Option<PgHeapTuple<'static, AllocatedByRust>>>> {
    let struct_array: StructArray = tuples.into();

    let len = struct_array.len();
    let mut values = Vec::with_capacity(len);

    let tupledesc = tuple_desc(typoid, typmod);

    for i in 0..len {
        let tuple = struct_array.slice(i, 1);
        let tuple = as_pg_tuple(tuple.to_data(), tupledesc.clone());
        values.push(tuple);
    }

    Some(values)
}
