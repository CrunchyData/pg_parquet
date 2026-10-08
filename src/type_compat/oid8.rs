use pgrx::{
    datum::UnboxDatum,
    pg_sys::{Datum, Oid, OID8OID},
    FromDatum, IntoDatum,
};

// Oid8 is a thin wrapper for PostgreSQL 19's 8 byte object identifier. pgrx has no datum type
// for it, and we want it in the parquet file as UInt64 rather than as text, so the datum
// conversions it needs are written here. oid8 is passed by value, so the datum is the value.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) struct Oid8(pub(crate) u64);

impl IntoDatum for Oid8 {
    fn into_datum(self) -> Option<Datum> {
        Some(Datum::from(self.0))
    }

    fn type_oid() -> Oid {
        OID8OID
    }
}

impl FromDatum for Oid8 {
    unsafe fn from_polymorphic_datum(datum: Datum, is_null: bool, _typoid: Oid) -> Option<Self>
    where
        Self: Sized,
    {
        if is_null {
            None
        } else {
            Some(Self(datum.value() as u64))
        }
    }
}

unsafe impl UnboxDatum for Oid8 {
    type As<'src> = Oid8;

    unsafe fn unbox<'src>(datum: pgrx::datum::Datum<'src>) -> Self::As<'src>
    where
        Self: 'src,
    {
        Self(datum.sans_lifetime().value() as u64)
    }
}
