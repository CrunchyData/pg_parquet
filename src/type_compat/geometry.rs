use std::{ffi::CString, ops::Deref};

use arrow_schema::{extension::ExtensionType, Field};
use once_cell::sync::OnceCell;
use parquet_geospatial::{WkbEdges, WkbMetadata, WkbType};
use pgrx::{
    datum::UnboxDatum,
    pg_sys::{
        get_extension_oid, makeString, Anum_pg_type_oid, AsPgCStr, Datum, GetSysCacheOid,
        InvalidOid, LookupFuncName, Oid, OidFunctionCall1Coll, OidFunctionCall2Coll,
        SysCacheIdentifier::TYPENAMENSP, BYTEAOID, INT4OID,
    },
    spi::{quote_identifier, quote_literal},
    FromDatum, IntoDatum, PgList, Spi,
};
use serde_json::Value;

// postgis uses 0 for the geometries whose coordinate reference system is not known
const UNKNOWN_SRID: i32 = 0;

// postgis stores the lon/lat WGS 84 coordinate reference system as srid 4326, which is also the
// srid of a geography whose type modifier does not say otherwise
const WGS84_SRID: i32 = 4326;

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
        UNKNOWN_SRID => WGS84_SRID,
        srid => srid,
    };

    let crs = crs_for_srid(srid);

    WkbType::new(Some(WkbMetadata::new(
        crs.as_deref(),
        Some(WkbEdges::Spherical),
    )))
}

// srid_for_typmod returns the srid that a geometry or geography column's type modifier says its
// values have, which is the srid the read path falls back to when the file has no crs
pub(crate) fn srid_for_typmod(typmod: i32) -> Option<i32> {
    Some(srid_from_typmod(typmod)).filter(|srid| *srid != UNKNOWN_SRID)
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
    let schema_name = quote_identifier(get_postgis_context().ext_schema_name.as_deref()?);

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

// srid_for_arrow_field maps the crs that Parquet reports for a geometry or geography column back
// to a postgis srid, which the read path restores on the geometries it creates. It returns None
// for a crs that spatial_ref_sys cannot resolve, which leaves the geometries with the unknown
// srid.
pub(crate) fn srid_for_arrow_field(field: &Field) -> Option<i32> {
    if field.extension_type_name()? != WkbType::NAME {
        return None;
    }

    let wkb_type = field.try_extension_type::<WkbType>().ok()?;

    let metadata = wkb_type.metadata();

    // Parquet omits the crs of a lon/lat column and reports it back as "OGC:CRS84"
    if metadata.crs_is_lon_lat() {
        return Some(WGS84_SRID);
    }

    srid_for_crs(metadata.crs.as_ref()?)
}

fn srid_for_crs(crs: &Value) -> Option<i32> {
    if let Some(crs) = crs.as_str() {
        // "srid:<code>" is the engine specific form, which is a postgis srid in our own files
        if let Some(srid) = crs.strip_prefix("srid:") {
            return srid
                .parse::<i32>()
                .ok()
                .filter(|srid| *srid != UNKNOWN_SRID);
        }

        // the "authority:code" form, e.g. "EPSG:4269"
        let (auth_name, auth_srid) = crs.split_once(':')?;

        return srid_for_authority(auth_name, auth_srid.parse().ok()?);
    }

    // PROJJSON, which identifies the crs by its authority under its "id" key
    let crs_id = crs.get("id")?;

    let auth_name = crs_id.get("authority")?.as_str()?;

    let auth_srid = match crs_id.get("code")? {
        Value::Number(code) => code.as_i64()?.try_into().ok()?,
        Value::String(code) => code.parse().ok()?,
        _ => return None,
    };

    srid_for_authority(auth_name, auth_srid)
}

// srid_for_authority is the reverse of crs_for_srid
fn srid_for_authority(auth_name: &str, auth_srid: i32) -> Option<i32> {
    // spatial_ref_sys lives in the schema of the postgis extension, which is not necessarily in
    // the search path
    let schema_name = quote_identifier(get_postgis_context().ext_schema_name.as_deref()?);

    let quoted_auth_name = quote_literal(auth_name);

    // the scalar subquery makes the query return a single null row, rather than no row at all,
    // for a crs that spatial_ref_sys does not have
    let query = format!(
        "select (select srid
                 from {schema_name}.spatial_ref_sys
                 where auth_name = {quoted_auth_name} and auth_srid = {auth_srid}
                 order by srid
                 limit 1)"
    );

    Spi::get_one::<i32>(&query)
        .unwrap_or_else(|e| panic!("failed to get the srid of crs {auth_name}:{auth_srid}: {e}"))
        .filter(|srid| *srid != UNKNOWN_SRID)
}

#[derive(Debug, PartialEq, Clone)]
struct PostgisContext {
    ext_schema_name: Option<String>,
    geometry_typoid: Option<Oid>,
    geography_typoid: Option<Oid>,
    geometry_to_wkb_funcoid: Option<Oid>,
    geography_to_wkb_funcoid: Option<Oid>,
    geometry_from_wkb_funcoid: Option<Oid>,
    geometry_from_wkb_srid_funcoid: Option<Oid>,
    geography_from_wkb_funcoid: Option<Oid>,
    geography_from_geometry_funcoid: Option<Oid>,
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

        let geometry_to_wkb_funcoid = geometry_typoid.map(|geometry_typoid| {
            Self::to_wkb_funcoid(
                Self::expect_ext_schema_name(&ext_schema_name),
                geometry_typoid,
            )
        });

        let geography_to_wkb_funcoid = geography_typoid.map(|geography_typoid| {
            Self::to_wkb_funcoid(
                Self::expect_ext_schema_name(&ext_schema_name),
                geography_typoid,
            )
        });

        let geometry_from_wkb_funcoid = postgis_ext_oid.map(|_| {
            Self::from_wkb_funcoid(
                Self::expect_ext_schema_name(&ext_schema_name),
                "st_geomfromwkb",
            )
        });

        let geometry_from_wkb_srid_funcoid = postgis_ext_oid
            .map(|_| Self::from_wkb_srid_funcoid(Self::expect_ext_schema_name(&ext_schema_name)));

        let geography_from_wkb_funcoid = postgis_ext_oid.map(|_| {
            Self::from_wkb_funcoid(
                Self::expect_ext_schema_name(&ext_schema_name),
                "st_geogfromwkb",
            )
        });

        let geography_from_geometry_funcoid = geometry_typoid.map(|geometry_typoid| {
            Self::geography_from_geometry_funcoid(
                Self::expect_ext_schema_name(&ext_schema_name),
                geometry_typoid,
            )
        });

        Self {
            ext_schema_name,
            geometry_typoid,
            geography_typoid,
            geometry_to_wkb_funcoid,
            geography_to_wkb_funcoid,
            geometry_from_wkb_funcoid,
            geometry_from_wkb_srid_funcoid,
            geography_from_wkb_funcoid,
            geography_from_geometry_funcoid,
        }
    }

    fn expect_ext_schema_name(ext_schema_name: &Option<String>) -> &str {
        ext_schema_name
            .as_deref()
            .expect("expected postgis is created")
    }

    fn extension_schema_oid() -> Oid {
        Spi::get_one("SELECT extnamespace FROM pg_extension WHERE extname = 'postgis'")
            .expect("failed to get postgis extension schema")
            .expect("postgis extension schema not found")
    }

    fn schema_name(schema_oid: Oid) -> String {
        let query = format!("SELECT nspname::text FROM pg_namespace WHERE oid = {schema_oid}");

        Spi::get_one::<String>(&query)
            .expect("failed to get the name of the postgis extension schema")
            .expect("postgis extension schema not found")
    }

    fn to_wkb_funcoid(schema_name: &str, postgis_typoid: Oid) -> Oid {
        Self::funcoid(schema_name, "st_asbinary", &[postgis_typoid])
    }

    fn from_wkb_funcoid(schema_name: &str, from_wkb_func_name: &str) -> Oid {
        Self::funcoid(schema_name, from_wkb_func_name, &[BYTEAOID])
    }

    // the two argument form of st_geomfromwkb gives the geometry the srid we pass
    fn from_wkb_srid_funcoid(schema_name: &str) -> Oid {
        Self::funcoid(schema_name, "st_geomfromwkb", &[BYTEAOID, INT4OID])
    }

    // the geography(geometry) cast, which is how a geography gets an srid other than the lon/lat
    // one that st_geogfromwkb assumes
    fn geography_from_geometry_funcoid(schema_name: &str, geometry_typoid: Oid) -> Oid {
        Self::funcoid(schema_name, "geography", &[geometry_typoid])
    }

    // the function names are qualified with the extension schema since postgis is not
    // necessarily on the search path
    fn funcoid(schema_name: &str, func_name: &str, arg_typoids: &[Oid]) -> Oid {
        unsafe {
            let mut function_name_list = PgList::new();
            function_name_list.push(makeString(schema_name.as_pg_cstr()));
            function_name_list.push(makeString(func_name.as_pg_cstr()));

            let mut arg_types = arg_typoids.to_vec();

            LookupFuncName(
                function_name_list.as_ptr(),
                arg_types.len() as _,
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

// Geometry is a wrapper around a byte vector that represents a postgis geometry in WKB format,
// together with the srid that the geometry gets when it is converted back to a postgis datum.
#[derive(Debug, PartialEq)]
pub(crate) struct Geometry {
    wkb: Vec<u8>,
    srid: Option<i32>,
}

impl Geometry {
    pub(crate) fn new(wkb: Vec<u8>, srid: Option<i32>) -> Self {
        Self { wkb, srid }
    }
}

// we store Geometry as a WKB byte vector, and we allow it to be dereferenced as such
impl Deref for Geometry {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.wkb
    }
}

impl IntoDatum for Geometry {
    fn into_datum(self) -> Option<Datum> {
        let wkb_datum = self.wkb.into_datum().expect("cannot convert wkb to datum");

        // WKB carries no srid, so we restore the one that the file's crs resolved to
        if let Some(srid) = self.srid {
            let geometry_from_wkb_srid_funcoid = get_postgis_context()
                .geometry_from_wkb_srid_funcoid
                .expect("geometry_from_wkb_srid_funcoid");

            let srid_datum = srid.into_datum().expect("cannot convert srid to datum");

            return Some(unsafe {
                OidFunctionCall2Coll(
                    geometry_from_wkb_srid_funcoid,
                    InvalidOid,
                    wkb_datum,
                    srid_datum,
                )
            });
        }

        let geometry_from_wkb_funcoid = get_postgis_context()
            .geometry_from_wkb_funcoid
            .expect("geometry_from_wkb_funcoid");

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

            // the write path only ever needs the WKB bytes
            Some(Self::new(wkb, None))
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

        // the write path only ever needs the WKB bytes
        Geometry::new(wkb, None)
    }
}

// Geography is a wrapper around a byte vector that represents a postgis geography in WKB format,
// together with the srid that the geography gets when it is converted back to a postgis datum.
#[derive(Debug, PartialEq)]
pub(crate) struct Geography {
    wkb: Vec<u8>,
    srid: Option<i32>,
}

impl Geography {
    pub(crate) fn new(wkb: Vec<u8>, srid: Option<i32>) -> Self {
        Self { wkb, srid }
    }
}

// we store Geography as a WKB byte vector, and we allow it to be dereferenced as such
impl Deref for Geography {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.wkb
    }
}

impl IntoDatum for Geography {
    fn into_datum(self) -> Option<Datum> {
        let context = get_postgis_context();

        let wkb_datum = self.wkb.into_datum().expect("cannot convert wkb to datum");

        // st_geogfromwkb reads the WKB as lon/lat, so a geography in any other crs has to go
        // through a geometry, which can take the srid, and be cast back
        if let Some(srid) = self.srid.filter(|srid| *srid != WGS84_SRID) {
            let geometry_from_wkb_srid_funcoid = context
                .geometry_from_wkb_srid_funcoid
                .expect("geometry_from_wkb_srid_funcoid");

            let geography_from_geometry_funcoid = context
                .geography_from_geometry_funcoid
                .expect("geography_from_geometry_funcoid");

            let srid_datum = srid.into_datum().expect("cannot convert srid to datum");

            let geometry_datum = unsafe {
                OidFunctionCall2Coll(
                    geometry_from_wkb_srid_funcoid,
                    InvalidOid,
                    wkb_datum,
                    srid_datum,
                )
            };

            return Some(unsafe {
                OidFunctionCall1Coll(geography_from_geometry_funcoid, InvalidOid, geometry_datum)
            });
        }

        let geography_from_wkb_funcoid = context
            .geography_from_wkb_funcoid
            .expect("geography_from_wkb_funcoid");

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

            // the write path only ever needs the WKB bytes
            Some(Self::new(wkb, None))
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

        // the write path only ever needs the WKB bytes
        Geography::new(wkb, None)
    }
}
