use once_cell::sync::OnceCell;
use pgrx::{
    datum::UnboxDatum,
    pg_sys::{
        self, Anum_pg_type_oid, AsPgCStr, GetSysCacheOid, InvalidOid, Oid,
        SysCacheIdentifier::TYPEOID,
    },
    prelude::PgHeapTuple,
    AllocatedByRust, FromDatum, IntoDatum,
};

use crate::{
    pgrx_missing_declerations::{get_extension_oid, get_extension_schema},
    pgrx_utils::is_domain_of_array_type,
};

// we need to reset the crunchy_map context at each copy start
static mut CRUNCHY_MAP_CONTEXT: OnceCell<CrunchyMapContext> = OnceCell::new();

fn get_crunchy_map_context() -> &'static mut CrunchyMapContext {
    unsafe {
        CRUNCHY_MAP_CONTEXT
            .get_mut()
            .expect("crunchy_map context is not initialized")
    }
}

pub(crate) fn reset_crunchy_map_context() {
    unsafe { CRUNCHY_MAP_CONTEXT.take() };

    unsafe {
        CRUNCHY_MAP_CONTEXT
            .set(CrunchyMapContext::new())
            .expect("failed to reset crunchy_map context")
    };
}

pub(crate) fn is_crunchy_map_typoid(typoid: Oid) -> bool {
    let crunchy_map_context = get_crunchy_map_context();

    if crunchy_map_context.crunchy_map_ext_schema_oid.is_none() {
        return false;
    }

    // crunchy map is a domain type over array of key-value pairs
    if !is_domain_of_array_type(typoid) {
        return false;
    }

    let crunchy_map_ext_schema_oid = crunchy_map_context
        .crunchy_map_ext_schema_oid
        .expect("expected crunchy_map is created");

    let found_typoid = unsafe {
        GetSysCacheOid(
            TYPEOID as _,
            Anum_pg_type_oid as _,
            typoid.into_datum().unwrap(),
            crunchy_map_ext_schema_oid.into_datum().unwrap(),
            pg_sys::Datum::from(0),
            pg_sys::Datum::from(0),
        )
    };

    let is_crunchy_map = found_typoid != InvalidOid;

    if is_crunchy_map {
        crunchy_map_context
            .per_crunchy_map_context
            .set_current_crunchy_map_typoid(typoid);
    }

    is_crunchy_map
}

#[derive(Debug, PartialEq, Clone)]
struct CrunchyMapPerTypeContext {
    current_crunchy_map_typoid: Option<Oid>,
}

impl CrunchyMapPerTypeContext {
    fn set_current_crunchy_map_typoid(&mut self, typoid: Oid) {
        self.current_crunchy_map_typoid = Some(typoid);
    }
}

#[derive(Debug, PartialEq, Clone)]
struct CrunchyMapContext {
    crunchy_map_ext_oid: Option<Oid>,
    crunchy_map_ext_schema_oid: Option<Oid>,
    per_crunchy_map_context: CrunchyMapPerTypeContext,
}

impl CrunchyMapContext {
    fn new() -> Self {
        let crunchy_map_ext_oid = unsafe { get_extension_oid("crunchy_map".as_pg_cstr(), true) };
        let crunchy_map_ext_oid = if crunchy_map_ext_oid == InvalidOid {
            None
        } else {
            Some(crunchy_map_ext_oid)
        };

        let crunchy_map_ext_schema_oid = crunchy_map_ext_oid
            .map(|crunchy_map_ext_oid| unsafe { get_extension_schema(crunchy_map_ext_oid) });

        Self {
            crunchy_map_ext_oid,
            crunchy_map_ext_schema_oid,
            per_crunchy_map_context: CrunchyMapPerTypeContext {
                current_crunchy_map_typoid: None,
            },
        }
    }
}

// crunchy_map is a domain type over array of key-value pairs
pub(crate) struct CrunchyMap<'a> {
    pub(crate) entries: pgrx::Array<'a, PgHeapTuple<'a, AllocatedByRust>>,
}

impl IntoDatum for CrunchyMap<'_> {
    fn into_datum(self) -> Option<pg_sys::Datum> {
        self.entries.into_datum()
    }

    fn type_oid() -> pg_sys::Oid {
        get_crunchy_map_context()
            .per_crunchy_map_context
            .current_crunchy_map_typoid
            .expect("crunchy_map type context is not initialized")
    }
}

impl FromDatum for CrunchyMap<'_> {
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
            let entries = pgrx::Array::<PgHeapTuple<AllocatedByRust>>::from_datum(datum, false)
                .expect("cannot convert datum to crunchy_map entries");

            Some(CrunchyMap { entries })
        }
    }
}

unsafe impl<'a> UnboxDatum for CrunchyMap<'a> {
    type As<'src> = Self
    where
        Self: 'src;

    unsafe fn unbox<'src>(datum: pgrx::datum::Datum<'src>) -> Self::As<'src>
    where
        Self: 'src,
    {
        let entries =
            pgrx::Array::<PgHeapTuple<AllocatedByRust>>::from_datum(datum.sans_lifetime(), false)
                .expect("cannot convert datum to crunchy_map entries");

        CrunchyMap { entries }
    }
}
