use std::{ffi::CString, ops::Deref};

use pgrx::{
    pg_sys::{
        self, makeString, Anum_pg_type_oid, AsPgCStr, GetSysCacheOid, InvalidOid, LookupFuncName,
        Oid, OidFunctionCall1Coll, SysCacheIdentifier_TYPENAMENSP, BYTEAOID,
    },
    FromDatum, IntoDatum, PgList,
};

// we need to set this just before converting Geometry to Datum
// and vice versa since IntoDatum's type_oid() is a static method and
// we cannot pass it the typoid of the current Geometry instance.
static mut GEOMETRY_TYPOID: pg_sys::Oid = pg_sys::InvalidOid;

pub(crate) fn set_geometry_typoid(typoid: pg_sys::Oid) {
    unsafe {
        GEOMETRY_TYPOID = typoid;
    }
}

pub(crate) fn find_postgis_geometry_type() -> Oid {
    let postgis_extoid = unsafe { get_extension_oid("postgis".as_pg_cstr(), true) };

    if postgis_extoid == InvalidOid {
        return InvalidOid;
    }

    let postgis_schema_oid = unsafe { get_extension_schema(postgis_extoid) };

    let postgis_geometry_type_name = CString::new("geometry").unwrap();

    let postgis_geometry_typoid = unsafe {
        GetSysCacheOid(
            SysCacheIdentifier_TYPENAMENSP as _,
            Anum_pg_type_oid as _,
            postgis_geometry_type_name.into_datum().unwrap(),
            postgis_schema_oid.into_datum().unwrap(),
            pg_sys::Datum::from(0),
            pg_sys::Datum::from(0),
        )
    };

    if postgis_geometry_typoid == InvalidOid {
        return InvalidOid;
    }

    postgis_geometry_typoid
}

pub(crate) fn is_postgis_geometry_type(typoid: Oid) -> bool {
    let postgis_geometry_typoid = find_postgis_geometry_type();
    typoid == postgis_geometry_typoid
}

#[allow(improper_ctypes)]
extern "C" {
    fn get_extension_oid(name: *const i8, missing_ok: bool) -> Oid;
    fn get_extension_schema(ext_oid: Oid) -> Oid;
}

fn st_asbinary_funcoid() -> Oid {
    unsafe {
        debug_assert!(is_postgis_geometry_type(GEOMETRY_TYPOID));

        let funcname = makeString("st_asbinary".as_pg_cstr());
        let mut funcnamelist = PgList::new();
        funcnamelist.push(funcname);

        let mut arg_argtypes = vec![GEOMETRY_TYPOID];

        LookupFuncName(funcnamelist.as_ptr(), 1, arg_argtypes.as_mut_ptr(), false)
    }
}

fn st_geomfromwkb_funcoid() -> Oid {
    unsafe {
        debug_assert!(is_postgis_geometry_type(GEOMETRY_TYPOID));

        let funcname = makeString("st_geomfromwkb".as_pg_cstr());
        let mut funcnamelist = PgList::new();
        funcnamelist.push(funcname);

        let mut arg_argtypes = vec![BYTEAOID];

        LookupFuncName(funcnamelist.as_ptr(), 1, arg_argtypes.as_mut_ptr(), false)
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct Geometry(pub(crate) Vec<u8>);

impl Deref for Geometry {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Vec<u8>> for Geometry {
    fn from(bytes: Vec<u8>) -> Geometry {
        Self(bytes)
    }
}

impl IntoDatum for Geometry {
    fn into_datum(self) -> Option<pg_sys::Datum> {
        let func_oid = st_geomfromwkb_funcoid();

        let wkb_datum = self.0.into_datum().unwrap();

        Some(unsafe { OidFunctionCall1Coll(func_oid, InvalidOid, wkb_datum) })
    }

    fn type_oid() -> pg_sys::Oid {
        unsafe { GEOMETRY_TYPOID }
    }
}

impl FromDatum for Geometry {
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
            let func_oid = st_asbinary_funcoid();
            let geom_datum = datum;

            let wkb_datum = unsafe { OidFunctionCall1Coll(func_oid, InvalidOid, geom_datum) };

            let wkb = Vec::<u8>::from_datum(wkb_datum, false).unwrap();
            Some(Self(wkb))
        }
    }
}
