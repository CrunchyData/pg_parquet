use arrow::{array::ArrayRef, datatypes::FieldRef};
use pgrx::{pg_sys::Oid, FromDatum, IntoDatum};

pub(crate) mod bool;
pub(crate) mod bpchar;
pub(crate) mod bytea;
pub(crate) mod char;
pub(crate) mod date;
pub(crate) mod float4;
pub(crate) mod float8;
pub(crate) mod int2;
pub(crate) mod int4;
pub(crate) mod int8;
pub(crate) mod interval;
pub(crate) mod json;
pub(crate) mod jsonb;
pub(crate) mod numeric;
pub(crate) mod oid;
pub(crate) mod record;
pub(crate) mod text;
pub(crate) mod time;
pub(crate) mod timestamp;
pub(crate) mod timestamptz;
pub(crate) mod timetz;
pub(crate) mod uuid;
pub(crate) mod varchar;

pub(crate) trait PgTypeToArrowArray<T: IntoDatum + FromDatum> {
    fn as_arrow_array(self, name: &str, typoid: Oid, typmod: i32) -> (FieldRef, ArrayRef);
}
