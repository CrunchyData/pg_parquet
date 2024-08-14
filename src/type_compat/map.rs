use pgrx::{
    pg_sys::{
        self, Anum_pg_type_oid, AsPgCStr, GetSysCacheOid, InvalidOid, Oid,
        SysCacheIdentifier_TYPEOID,
    },
    prelude::PgHeapTuple,
    AllocatedByRust, FromDatum, IntoDatum,
};

use crate::pgrx_utils::is_domain_of_array_type;

// we need to set this just before converting map to Datum
// and vice versa since IntoDatum's type_oid() is a static method and
// we cannot pass it the typoid of the current map instance.
static mut MAP_TYPOID: pg_sys::Oid = pg_sys::InvalidOid;

pub(crate) fn set_crunchy_map_typoid(typoid: pg_sys::Oid) {
    unsafe {
        MAP_TYPOID = typoid;
    }
}

pub(crate) fn find_crunchy_map_type(typoid: Oid) -> Oid {
    let crunchy_map_extoid = unsafe { get_extension_oid("crunchy_map".as_pg_cstr(), true) };

    if crunchy_map_extoid == InvalidOid {
        return InvalidOid;
    }

    let crunchy_map_schema_oid = unsafe { get_extension_schema(crunchy_map_extoid) };

    let found_typoid = unsafe {
        GetSysCacheOid(
            SysCacheIdentifier_TYPEOID as _,
            Anum_pg_type_oid as _,
            typoid.into_datum().unwrap(),
            crunchy_map_schema_oid.into_datum().unwrap(),
            pg_sys::Datum::from(0),
            pg_sys::Datum::from(0),
        )
    };

    if found_typoid == InvalidOid {
        return InvalidOid;
    }

    debug_assert!(found_typoid == typoid);

    found_typoid
}

pub(crate) fn is_crunchy_map_type(typoid: Oid) -> bool {
    if !is_domain_of_array_type(typoid) {
        return false;
    }

    let crunchy_map_type_oid = find_crunchy_map_type(typoid);
    crunchy_map_type_oid != InvalidOid
}

#[allow(improper_ctypes)]
extern "C" {
    fn get_extension_oid(name: *const i8, missing_ok: bool) -> Oid;
    fn get_extension_schema(ext_oid: Oid) -> Oid;
}

// crunchy_map is a domain type over array of key-value pairs
pub(crate) struct PGMap<'a> {
    pub(crate) entries: Vec<PgHeapTuple<'a, AllocatedByRust>>,
}

impl IntoDatum for PGMap<'_> {
    fn into_datum(self) -> Option<pg_sys::Datum> {
        self.entries.into_datum()
    }

    fn type_oid() -> pg_sys::Oid {
        unsafe { MAP_TYPOID }
    }
}

impl FromDatum for PGMap<'_> {
    unsafe fn from_polymorphic_datum(
        datum: pg_sys::Datum,
        is_null: bool,
        _typoid: pg_sys::Oid,
    ) -> Option<Self>
    where
        Self: Sized,
    {
        if is_null {
            None
        } else {
            let entries = Vec::<PgHeapTuple<AllocatedByRust>>::from_datum(datum, is_null).unwrap();
            Some(PGMap { entries })
        }
    }
}
