use arrow::{array::ArrayRef, datatypes::FieldRef};
use pgrx::{pg_sys::Oid, FromDatum, IntoDatum};

pub(crate) trait PgTypeToArrowArray<T: IntoDatum + FromDatum> {
    fn as_arrow_array(self, name: &str, _typoid: Oid, _typmod: i32) -> (FieldRef, ArrayRef);
}
