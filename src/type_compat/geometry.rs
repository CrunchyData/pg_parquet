use std::{ffi::CString, ops::Deref};

use once_cell::sync::OnceCell;
use parquet_geospatial::{WkbEdges, WkbMetadata, WkbType};
use pgrx::{
    datum::UnboxDatum,
    pg_sys::{
        get_extension_oid, makeString, Anum_pg_type_oid, AsPgCStr, Datum, GetSysCacheOid,
        InvalidOid, LookupFuncName, Oid, OidFunctionCall1Coll, SysCacheIdentifier::TYPENAMENSP,
        BYTEAOID,
    },
    spi::quote_identifier,
    FromDatum, IntoDatum, PgList, Spi,
};

// postgis uses 0 for the geometries whose coordinate reference system is not known
const UNKNOWN_SRID: i32 = 0;

// postgis geographies are in WGS 84 unless their type modifier says otherwise
const DEFAULT_GEOGRAPHY_SRID: i32 = 4326;

// we need to reset the postgis context at each copy start
static mut POSTGIS_CONTEXT: OnceCell<PostgisContext> = OnceCell::new();

fn get_postgis_context() -> &'static PostgisContext {
    #[allow(static_mut_refs)]
    unsafe {
        POSTGIS_CONTEXT
            .get()
            .expect("postgis context is not initialized")
    }
}

pub(crate) fn reset_postgis_context() {
    #[allow(static_mut_refs)]
    unsafe {
        POSTGIS_CONTEXT.take()
    };

    #[allow(static_mut_refs)]
    unsafe {
        POSTGIS_CONTEXT
            .set(PostgisContext::new())
            .expect("failed to reset postgis context")
    };
}

pub(crate) fn is_postgis_geometry_type(typoid: Oid) -> bool {
    if let Some(geometry_typoid) = get_postgis_context().geometry_typoid {
        return typoid == geometry_typoid;
    }

    false
}

pub(crate) fn is_postgis_geography_type(typoid: Oid) -> bool {
    if let Some(geography_typoid) = get_postgis_context().geography_typoid {
        return typoid == geography_typoid;
    }

    false
}

// geometry_wkb_type returns the Arrow extension type for a postgis geometry column. Parquet
// writes the column with its GEOMETRY logical type, whose edges are planar like postgis's
// geometry type, and computes the bounding box and the geometry types of the column itself.
pub(crate) fn geometry_wkb_type(typmod: i32) -> WkbType {
    let crs = crs_for_srid(srid_from_typmod(typmod));

    // planar edges
    let edges = None;

    WkbType::new(Some(WkbMetadata::new(crs.as_deref(), edges)))
}

// geography_wkb_type returns the Arrow extension type for a postgis geography column. Parquet
// writes the column with its GEOGRAPHY logical type, whose spherical edges match postgis's
// geography type.
pub(crate) fn geography_wkb_type(typmod: i32) -> WkbType {
    let srid = match srid_from_typmod(typmod) {
        UNKNOWN_SRID => DEFAULT_GEOGRAPHY_SRID,
        srid => srid,
    };

    let crs = crs_for_srid(srid);

    WkbType::new(Some(WkbMetadata::new(
        crs.as_deref(),
        Some(WkbEdges::Spherical),
    )))
}

// srid_from_typmod extracts the srid that a geometry or geography type modifier encodes. see
// postgis: https://github.com/postgis/postgis/blob/stable-3.5/liblwgeom/liblwgeom.h.in
fn srid_from_typmod(typmod: i32) -> i32 {
    if typmod < 0 {
        // the column has no type modifier, so its srid is not known
        return UNKNOWN_SRID;
    }

    ((typmod & 0x0FFFFF00) - (typmod & 0x10000000)) >> 8
}

// crs_for_srid returns the coordinate reference system of the srid as an "authority:code"
// string, e.g. "EPSG:4326", which is one of the forms that Parquet allows for a crs. It returns
// None for the srids that postgis does not know, which Parquet represents as an unset crs.
fn crs_for_srid(srid: i32) -> Option<String> {
    if srid == UNKNOWN_SRID {
        return None;
    }

    // spatial_ref_sys lives in the schema of the postgis extension, which is not necessarily in
    // the search path
    let schema_name = get_postgis_context().ext_schema_name.as_ref()?;

    // the scalar subquery makes the query return a single null row, rather than no row at all,
    // for an srid that spatial_ref_sys does not have
    let query = format!(
        "select (select auth_name || ':' || auth_srid
                 from {schema_name}.spatial_ref_sys
                 where srid = {srid} and auth_name is not null and auth_srid is not null)"
    );

    Spi::get_one::<String>(&query)
        .unwrap_or_else(|e| panic!("failed to get the crs of srid {srid}: {e}"))
}

#[derive(Debug, PartialEq, Clone)]
struct PostgisContext {
    ext_schema_name: Option<String>,
    geometry_typoid: Option<Oid>,
    geography_typoid: Option<Oid>,
    geometry_to_wkb_funcoid: Option<Oid>,
    geography_to_wkb_funcoid: Option<Oid>,
    geometry_from_wkb_funcoid: Option<Oid>,
    geography_from_wkb_funcoid: Option<Oid>,
}

impl PostgisContext {
    fn new() -> Self {
        let postgis_ext_oid = unsafe { get_extension_oid("postgis".as_pg_cstr(), true) };
        let postgis_ext_oid = if postgis_ext_oid == InvalidOid {
            None
        } else {
            Some(postgis_ext_oid)
        };

        let postgis_ext_schema_oid = postgis_ext_oid.map(|_| Self::extension_schema_oid());

        let ext_schema_name = postgis_ext_schema_oid.map(Self::schema_name);

        let geometry_typoid = postgis_ext_oid.map(|postgis_ext_oid| {
            Self::postgis_typoid(
                postgis_ext_oid,
                postgis_ext_schema_oid.expect("expected postgis is created"),
                "geometry",
            )
        });

        let geography_typoid = postgis_ext_oid.map(|postgis_ext_oid| {
            Self::postgis_typoid(
                postgis_ext_oid,
                postgis_ext_schema_oid.expect("expected postgis is created"),
                "geography",
            )
        });

        let geometry_to_wkb_funcoid = geometry_typoid.map(Self::to_wkb_funcoid);

        let geography_to_wkb_funcoid = geography_typoid.map(Self::to_wkb_funcoid);

        let geometry_from_wkb_funcoid =
            postgis_ext_oid.map(|_| Self::from_wkb_funcoid("st_geomfromwkb"));

        let geography_from_wkb_funcoid =
            postgis_ext_oid.map(|_| Self::from_wkb_funcoid("st_geogfromwkb"));

        Self {
            ext_schema_name,
            geometry_typoid,
            geography_typoid,
            geometry_to_wkb_funcoid,
            geography_to_wkb_funcoid,
            geometry_from_wkb_funcoid,
            geography_from_wkb_funcoid,
        }
    }

    fn extension_schema_oid() -> Oid {
        Spi::get_one("SELECT extnamespace FROM pg_extension WHERE extname = 'postgis'")
            .expect("failed to get postgis extension schema")
            .expect("postgis extension schema not found")
    }

    fn schema_name(schema_oid: Oid) -> String {
        let query = format!("SELECT nspname::text FROM pg_namespace WHERE oid = {schema_oid}");

        let schema_name = Spi::get_one::<String>(&query)
            .expect("failed to get the name of the postgis extension schema")
            .expect("postgis extension schema not found");

        quote_identifier(schema_name)
    }

    fn to_wkb_funcoid(postgis_typoid: Oid) -> Oid {
        unsafe {
            let function_name = makeString("st_asbinary".as_pg_cstr());
            let mut function_name_list = PgList::new();
            function_name_list.push(function_name);

            let mut arg_types = vec![postgis_typoid];

            LookupFuncName(
                function_name_list.as_ptr(),
                1,
                arg_types.as_mut_ptr(),
                false,
            )
        }
    }

    fn from_wkb_funcoid(from_wkb_func_name: &str) -> Oid {
        unsafe {
            let function_name = makeString(from_wkb_func_name.as_pg_cstr());
            let mut function_name_list = PgList::new();
            function_name_list.push(function_name);

            let mut arg_types = vec![BYTEAOID];

            LookupFuncName(
                function_name_list.as_ptr(),
                1,
                arg_types.as_mut_ptr(),
                false,
            )
        }
    }

    fn postgis_typoid(
        postgis_ext_oid: Oid,
        postgis_ext_schema_oid: Oid,
        postgis_type_name: &str,
    ) -> Oid {
        if postgis_ext_oid == InvalidOid {
            return InvalidOid;
        }

        let postgis_type_name = CString::new(postgis_type_name).expect("CString::new failed");

        unsafe {
            GetSysCacheOid(
                TYPENAMENSP as _,
                Anum_pg_type_oid as _,
                postgis_type_name.into_datum().unwrap(),
                postgis_ext_schema_oid.into_datum().unwrap(),
                Datum::from(0), // not used key
                Datum::from(0), // not used key
            )
        }
    }
}

// Geometry is a wrapper around a byte vector that represents a postgis geometry in WKB format.
#[derive(Debug, PartialEq)]
pub(crate) struct Geometry(pub(crate) Vec<u8>);

// we store Geometry as a WKB byte vector, and we allow it to be dereferenced as such
impl Deref for Geometry {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Vec<u8>> for Geometry {
    fn from(wkb: Vec<u8>) -> Self {
        Self(wkb)
    }
}

impl IntoDatum for Geometry {
    fn into_datum(self) -> Option<Datum> {
        let geometry_from_wkb_funcoid = get_postgis_context()
            .geometry_from_wkb_funcoid
            .expect("geometry_from_wkb_funcoid");

        let wkb_datum = self.0.into_datum().expect("cannot convert wkb to datum");

        Some(unsafe { OidFunctionCall1Coll(geometry_from_wkb_funcoid, InvalidOid, wkb_datum) })
    }

    fn type_oid() -> Oid {
        get_postgis_context()
            .geometry_typoid
            .expect("postgis context not initialized")
    }
}

impl FromDatum for Geometry {
    unsafe fn from_polymorphic_datum(datum: Datum, is_null: bool, _typoid: Oid) -> Option<Self>
    where
        Self: Sized,
    {
        if is_null {
            None
        } else {
            let geometry_to_wkb_funcoid = get_postgis_context()
                .geometry_to_wkb_funcoid
                .expect("geometry_to_wkb_funcoid");

            let wkb_datum =
                unsafe { OidFunctionCall1Coll(geometry_to_wkb_funcoid, InvalidOid, datum) };

            let is_null = false;
            let wkb =
                Vec::<u8>::from_datum(wkb_datum, is_null).expect("cannot convert datum to wkb");
            Some(Self(wkb))
        }
    }
}

unsafe impl UnboxDatum for Geometry {
    type As<'src> = Geometry;

    unsafe fn unbox<'src>(datum: pgrx::datum::Datum<'src>) -> Self::As<'src>
    where
        Self: 'src,
    {
        let geometry_to_wkb_funcoid = get_postgis_context()
            .geometry_to_wkb_funcoid
            .expect("geometry_to_wkb_funcoid");

        let wkb_datum =
            OidFunctionCall1Coll(geometry_to_wkb_funcoid, InvalidOid, datum.sans_lifetime());

        let is_null = false;
        let wkb = Vec::<u8>::from_datum(wkb_datum, is_null).expect("cannot convert datum to wkb");
        Geometry(wkb)
    }
}

// Geography is a wrapper around a byte vector that represents a postgis geography in WKB format.
#[derive(Debug, PartialEq)]
pub(crate) struct Geography(pub(crate) Vec<u8>);

// we store Geography as a WKB byte vector, and we allow it to be dereferenced as such
impl Deref for Geography {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Vec<u8>> for Geography {
    fn from(wkb: Vec<u8>) -> Self {
        Self(wkb)
    }
}

impl IntoDatum for Geography {
    fn into_datum(self) -> Option<Datum> {
        let geography_from_wkb_funcoid = get_postgis_context()
            .geography_from_wkb_funcoid
            .expect("geography_from_wkb_funcoid");

        let wkb_datum = self.0.into_datum().expect("cannot convert wkb to datum");

        Some(unsafe { OidFunctionCall1Coll(geography_from_wkb_funcoid, InvalidOid, wkb_datum) })
    }

    fn type_oid() -> Oid {
        get_postgis_context()
            .geography_typoid
            .expect("postgis context not initialized")
    }
}

impl FromDatum for Geography {
    unsafe fn from_polymorphic_datum(datum: Datum, is_null: bool, _typoid: Oid) -> Option<Self>
    where
        Self: Sized,
    {
        if is_null {
            None
        } else {
            let geography_to_wkb_funcoid = get_postgis_context()
                .geography_to_wkb_funcoid
                .expect("geography_to_wkb_funcoid");

            let wkb_datum =
                unsafe { OidFunctionCall1Coll(geography_to_wkb_funcoid, InvalidOid, datum) };

            let is_null = false;
            let wkb =
                Vec::<u8>::from_datum(wkb_datum, is_null).expect("cannot convert datum to wkb");
            Some(Self(wkb))
        }
    }
}

unsafe impl UnboxDatum for Geography {
    type As<'src> = Geography;

    unsafe fn unbox<'src>(datum: pgrx::datum::Datum<'src>) -> Self::As<'src>
    where
        Self: 'src,
    {
        let geography_to_wkb_funcoid = get_postgis_context()
            .geography_to_wkb_funcoid
            .expect("geography_to_wkb_funcoid");

        let wkb_datum =
            OidFunctionCall1Coll(geography_to_wkb_funcoid, InvalidOid, datum.sans_lifetime());

        let is_null = false;
        let wkb = Vec::<u8>::from_datum(wkb_datum, is_null).expect("cannot convert datum to wkb");
        Geography(wkb)
    }
}
