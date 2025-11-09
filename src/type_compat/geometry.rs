use std::{
    collections::{HashMap, HashSet},
    ffi::CString,
    ops::Deref,
    str::FromStr,
};

use once_cell::sync::OnceCell;
use pgrx::{
    datum::{DatumWithOid, UnboxDatum},
    pg_sys::{
        get_extension_oid, makeString, Anum_pg_type_oid, AsPgCStr, Datum, GetSysCacheOid,
        InvalidOid, LookupFuncName, Oid, OidFunctionCall1Coll, SysCacheIdentifier::TYPENAMENSP,
        BYTEAOID,
    },
    FromDatum, IntoDatum, PgList, Spi,
};
use serde::{Deserialize, Serialize};

use crate::pgrx_utils::array_typoid;

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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) enum GeometryType {
    Point,
    LineString,
    Polygon,
    MultiPoint,
    MultiLineString,
    MultiPolygon,
    GeometryCollection,
}

impl FromStr for GeometryType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "point" => Ok(Self::Point),
            "linestring" => Ok(Self::LineString),
            "polygon" => Ok(Self::Polygon),
            "multipoint" => Ok(Self::MultiPoint),
            "multilinestring" => Ok(Self::MultiLineString),
            "multipolygon" => Ok(Self::MultiPolygon),
            "geometrycollection" => Ok(Self::GeometryCollection),
            _ => Err(format!("Invalid GeometryType: {s}")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) enum GeometryEncoding {
    // only WKB is supported for now
    #[allow(clippy::upper_case_acronyms)]
    WKB,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) enum GeometryOrientation {
    #[serde(rename = "counterclockwise")]
    CounterClockwise,
}

impl FromStr for GeometryOrientation {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "counterclockwise" => Ok(Self::CounterClockwise),
            _ => Err(format!("Invalid GeometryOrientation: {s}")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) enum GeometryEdgeType {
    #[serde(rename = "planar")]
    Planar,
    #[serde(rename = "spherical")]
    Spherical,
}

impl FromStr for GeometryEdgeType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "planar" => Ok(Self::Planar),
            "spherical" => Ok(Self::Spherical),
            _ => Err(format!("Invalid GeometryEdgeType: {s}")),
        }
    }
}

// (min_x, min_y, min_z, max_x, max_y, max_z) if 3d
// (min_x, min_y, max_x, max_y) if 2d
// (min_x, max_x) if 1d
pub(crate) type GeometryBbox = Vec<f64>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct GeometryColumn {
    pub(crate) encoding: GeometryEncoding,
    pub(crate) geometry_types: HashSet<GeometryType>,
    pub(crate) crs: Option<serde_json::Value>, // e.g. "EPSG:4326" in pproj format
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) orientation: Option<GeometryOrientation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) edges: Option<GeometryEdgeType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) bbox: Option<GeometryBbox>,
}

impl GeometryColumn {
    fn merge(&mut self, other: Self) {
        if self.crs != other.crs {
            // have different crs, set it to null
            self.crs = None;
        }

        if self.orientation != other.orientation {
            // have different orientation, set it to null
            self.orientation = None;
        }

        if self.edges != other.edges {
            // have different edges, set it to null
            self.edges = None;
        }

        // update geometry types by merging the two sets
        self.geometry_types.extend(other.geometry_types);

        // update bbox
        if let Some(old_bbox) = &self.bbox {
            if let Some(new_bbox) = &other.bbox {
                if new_bbox.len() == 2 && old_bbox.len() == 2 {
                    self.bbox = Some(vec![
                        old_bbox[0].min(new_bbox[0]),
                        old_bbox[1].max(new_bbox[1]),
                    ]);
                } else if new_bbox.len() == 2 && old_bbox.len() == 4 {
                    self.bbox = Some(vec![
                        old_bbox[0].min(new_bbox[0]),
                        old_bbox[1],
                        old_bbox[2].max(new_bbox[1]),
                        old_bbox[3],
                    ]);
                } else if new_bbox.len() == 2 && old_bbox.len() == 6 {
                    self.bbox = Some(vec![
                        old_bbox[0].min(new_bbox[0]),
                        old_bbox[1],
                        old_bbox[2],
                        old_bbox[3].max(new_bbox[1]),
                        old_bbox[4],
                        old_bbox[5],
                    ]);
                } else if new_bbox.len() == 4 && old_bbox.len() == 2 {
                    self.bbox = Some(vec![
                        new_bbox[0].min(old_bbox[0]),
                        new_bbox[1],
                        new_bbox[2].max(old_bbox[1]),
                        new_bbox[3],
                    ]);
                } else if new_bbox.len() == 4 && old_bbox.len() == 4 {
                    self.bbox = Some(vec![
                        new_bbox[0].min(old_bbox[0]),
                        new_bbox[1].min(old_bbox[1]),
                        new_bbox[2].max(old_bbox[2]),
                        new_bbox[3].max(old_bbox[3]),
                    ]);
                } else if new_bbox.len() == 4 && old_bbox.len() == 6 {
                    self.bbox = Some(vec![
                        old_bbox[0].min(new_bbox[0]),
                        old_bbox[1].min(new_bbox[1]),
                        old_bbox[2],
                        old_bbox[3].max(new_bbox[2]),
                        old_bbox[4].max(new_bbox[3]),
                        old_bbox[5],
                    ]);
                } else if new_bbox.len() == 6 && old_bbox.len() == 2 {
                    self.bbox = Some(vec![
                        new_bbox[0].min(old_bbox[0]),
                        new_bbox[1],
                        new_bbox[2],
                        new_bbox[3].max(old_bbox[1]),
                        new_bbox[4],
                        new_bbox[5],
                    ]);
                } else if new_bbox.len() == 6 && old_bbox.len() == 4 {
                    self.bbox = Some(vec![
                        new_bbox[0].min(old_bbox[0]),
                        new_bbox[1].min(old_bbox[1]),
                        new_bbox[2],
                        new_bbox[3].max(old_bbox[2]),
                        new_bbox[4].max(old_bbox[3]),
                        new_bbox[5],
                    ]);
                } else if new_bbox.len() == 6 && old_bbox.len() == 6 {
                    self.bbox = Some(vec![
                        new_bbox[0].min(old_bbox[0]),
                        new_bbox[1].min(old_bbox[1]),
                        new_bbox[2].min(old_bbox[2]),
                        new_bbox[3].max(old_bbox[3]),
                        new_bbox[4].max(old_bbox[4]),
                        new_bbox[5].max(old_bbox[5]),
                    ]);
                }
            } else {
                // new bbox is None, keep the old one
                self.bbox = Some(old_bbox.clone());
            }
        } else {
            // no old bbox, set the new one
            self.bbox = other.bbox;
        }
    }
}

impl From<DatumWithOid<'_>> for GeometryColumn {
    fn from(datum_with_oid: DatumWithOid<'_>) -> Self {
        debug_assert!(
            {
                if !crate::pgrx_utils::is_array_type(datum_with_oid.oid()) {
                    false
                } else {
                    let element_oid = crate::pgrx_utils::array_element_typoid(datum_with_oid.oid());
                    is_postgis_geometry_type(element_oid) || is_postgis_geography_type(element_oid)
                }
            },
            "expected geometry or geography type"
        );

        let query = if get_postgis_context().postgis_sfcgal_ext_exists {
            "select
                -- geometry type
                array_agg(distinct regexp_replace(st_geometrytype(g::geometry), '^ST_', '')) as geom_types,

                -- srid_name
                case
                    when count(distinct st_srid(g::geometry)) > 1 then null
                    else (
                        select s.auth_name || ':' || s.auth_srid
                        FROM spatial_ref_sys s
                        where s.srid = any_value(st_srid(g::geometry))
                        LIMIT 1
                    )
                end :: text as srid_name,

                -- orientation
                case
                    when count(distinct case
                            when regexp_replace(st_geometrytype(g::geometry), '^ST_', '') = 'Polygon' then cg_orientation(g::geometry)
                            else -1000
                        end) = 1 and any_value(case
                            when regexp_replace(st_geometrytype(g::geometry), '^ST_', '') = 'Polygon' then cg_orientation(g::geometry)
                            else -1000
                        end) = -1 then 'counterclockwise'
                    else null
                end as orientation,

                -- edges
                case
                    when count(distinct case
                            when regexp_replace(st_geometrytype(g::geometry), '^ST_', '') = 'Polygon' then
                                case
                                    when CG_IsPlanar(g::geometry) = true then 'planar'
                                    when CG_IsPlanar(g::geometry) = false then 'spherical'
                                    else 'none'
                                end
                            else 'none'
                        end) = 1 then
                        any_value(case
                            when regexp_replace(st_geometrytype(g::geometry), '^ST_', '') = 'Polygon' then
                                case
                                    when CG_IsPlanar(g::geometry) = true then 'planar'
                                    when CG_IsPlanar(g::geometry) = false then 'spherical'
                                    else null
                                end
                            else null
                        end)
                    else null
                end as edges,

                -- bbox
                case
                    when count(distinct st_ndims(g::geometry)) = 1 and any_value(st_ndims(g::geometry)) = 1 then
                        array[
                            min(ST_XMin(g::geometry)),
                            max(ST_XMax(g::geometry))
                        ]
                    when count(distinct st_ndims(g::geometry)) = 1 and any_value(st_ndims(g::geometry)) = 2 then
                        array[
                            min(ST_XMin(g::geometry)),
                            min(ST_YMin(g::geometry)),
                            max(ST_XMax(g::geometry)),
                            max(ST_YMax(g::geometry))
                        ]
                    when count(distinct st_ndims(g::geometry)) = 1 and any_value(st_ndims(g::geometry)) = 3 then
                        array[
                            min(ST_XMin(g::geometry)),
                            min(ST_YMin(g::geometry)),
                            min(ST_ZMin(g::geometry)),
                            max(ST_XMax(g::geometry)),
                            max(ST_YMax(g::geometry)),
                            max(ST_ZMax(g::geometry))
                        ]
                    else array[]::float8[]
                end as bbox
            from unnest($1::geometry[]) as g;"
        } else {
            "select
                -- geometry type
                array_agg(distinct regexp_replace(st_geometrytype(g::geometry), '^ST_', '')) as geom_types,

                -- srid_name
                case
                    when count(distinct st_srid(g::geometry)) > 1 then null
                    else (
                        select s.auth_name || ':' || s.auth_srid
                        FROM spatial_ref_sys s
                        where s.srid = any_value(st_srid(g::geometry))
                        LIMIT 1
                    )
                end :: text as srid_name,

                -- orientation (needs postgis_sfcgal extension)
                null as orientation,

                -- edges (needs postgis_sfcgal extension)
                null as edges,

                -- bbox
                case
                    when count(distinct st_ndims(g::geometry)) = 1 and any_value(st_ndims(g::geometry)) = 1 then
                        array[
                            min(ST_XMin(g::geometry)),
                            max(ST_XMax(g::geometry))
                        ]
                    when count(distinct st_ndims(g::geometry)) = 1 and any_value(st_ndims(g::geometry)) = 2 then
                        array[
                            min(ST_XMin(g::geometry)),
                            min(ST_YMin(g::geometry)),
                            max(ST_XMax(g::geometry)),
                            max(ST_YMax(g::geometry))
                        ]
                    when count(distinct st_ndims(g::geometry)) = 1 and any_value(st_ndims(g::geometry)) = 3 then
                        array[
                            min(ST_XMin(g::geometry)),
                            min(ST_YMin(g::geometry)),
                            min(ST_ZMin(g::geometry)),
                            max(ST_XMax(g::geometry)),
                            max(ST_YMax(g::geometry)),
                            max(ST_ZMax(g::geometry))
                        ]
                    else array[]::float8[]
                end as bbox
            from unnest($1::geometry[]) as g;"
        };

        Spi::connect(|client| {
            let tup_table = client.select(query, None, &[datum_with_oid]).unwrap();

            debug_assert!(
                tup_table.len() == 1,
                "expected exactly one row in the result"
            );

            let result = tup_table.into_iter().next().unwrap();

            let geometry_types_strs: Vec<String> = result["geom_types"]
                .value::<Vec<String>>()
                .unwrap()
                .unwrap();

            let geometry_types: HashSet<GeometryType> = geometry_types_strs
                .into_iter()
                .filter_map(|s| GeometryType::from_str(&s).ok())
                .collect();

            let crs = result["srid_name"]
                .value::<String>()
                .unwrap()
                .and_then(|srid_name| {
                    // we only support EPSG:4326 for now
                    if srid_name == "EPSG:4326" {
                        Some(serde_json::from_str(EPSG_4326_PROJJSON).unwrap())
                    } else {
                        None
                    }
                });

            let orientation = result["orientation"]
                .value::<String>()
                .unwrap()
                .and_then(|s| GeometryOrientation::from_str(&s).ok());

            let edges = result["edges"]
                .value::<String>()
                .unwrap()
                .and_then(|s| GeometryEdgeType::from_str(&s).ok());

            let bbox = result["bbox"].value::<Vec<f64>>().unwrap();

            GeometryColumn {
                encoding: GeometryEncoding::WKB,
                geometry_types,
                crs,
                orientation,
                edges,
                bbox,
            }
        })
    }
}

impl From<Vec<Geometry>> for GeometryColumn {
    fn from(geometries: Vec<Geometry>) -> Self {
        if geometries.is_empty() {
            return GeometryColumn {
                encoding: GeometryEncoding::WKB,
                geometry_types: HashSet::new(),
                crs: None,
                orientation: None,
                edges: None,
                bbox: None,
            };
        }

        let geom_array_datum = geometries
            .into_datum()
            .expect("cannot convert geometry vec to datum");

        let typoid = array_typoid(Geometry::type_oid());

        let datum_with_oid = unsafe { DatumWithOid::new(geom_array_datum, typoid) };

        GeometryColumn::from(datum_with_oid)
    }
}

impl From<Vec<Geography>> for GeometryColumn {
    fn from(geographies: Vec<Geography>) -> Self {
        if geographies.is_empty() {
            return GeometryColumn {
                encoding: GeometryEncoding::WKB,
                geometry_types: HashSet::new(),
                crs: None,
                orientation: None,
                edges: None,
                bbox: None,
            };
        }

        let geog_array_datum = geographies
            .into_datum()
            .expect("cannot convert geography vec to datum");

        let typoid = array_typoid(Geography::type_oid());

        let datum_with_oid = unsafe { DatumWithOid::new(geog_array_datum, typoid) };

        GeometryColumn::from(datum_with_oid)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct GeoparquetMetadata {
    pub(crate) version: String,
    pub(crate) primary_column: String,
    pub(crate) columns: HashMap<String, GeometryColumn>,
}

impl GeoparquetMetadata {
    pub(crate) fn new() -> Self {
        Self {
            version: "1.1.0".into(),
            primary_column: String::new(),
            columns: HashMap::new(),
        }
    }

    pub(crate) fn update(&mut self, column_name: &str, geometry_column: GeometryColumn) {
        if geometry_column.geometry_types.is_empty() {
            // do not update for empty geometry columns
            return;
        }

        // we use the first geometry column as the primary column
        if self.primary_column.is_empty() {
            self.primary_column = column_name.to_string();
        }

        if let Some(old_geometry_column) = self.columns.get_mut(column_name) {
            old_geometry_column.merge(geometry_column);
        } else {
            self.columns
                .insert(column_name.to_string(), geometry_column);
        }
    }
}

// geoparquet_metadata_json_from_tupledesc returns metadata for geometry columns in json format.
// in a format specified by https://geoparquet.org/releases/v1.1.0
// e.g. "{\"version\":\"1.1.0\",
//        \"primary_column\":\"a\",
//        \"columns\":{\"a\":{\"encoding\":\"WKB\", \"geometry_types\":[\"Point\"]},
//                     \"b\":{\"encoding\":\"WKB\", \"geometry_types\":[\"LineString\"]}}}"
pub(crate) fn geoparquet_metadata_to_json(geoparquet_metadata: &GeoparquetMetadata) -> String {
    serde_json::to_string(geoparquet_metadata).unwrap_or_else(|_| {
        panic!("failed to serialize geometry columns metadata {geoparquet_metadata:?}")
    })
}

#[derive(Debug, PartialEq, Clone)]
struct PostgisContext {
    postgis_sfcgal_ext_exists: bool,
    geometry_typoid: Option<Oid>,
    geography_typoid: Option<Oid>,
    geom_towkb_funcoid: Option<Oid>,
    geog_towkb_funcoid: Option<Oid>,
    geom_fromwkb_funcoid: Option<Oid>,
    geog_fromwkb_funcoid: Option<Oid>,
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

        let postgis_sfcgal_ext_exists = postgis_ext_oid
            .map(|_| {
                let postgis_sfcgal_ext_oid =
                    unsafe { get_extension_oid("postgis_sfcgal".as_pg_cstr(), true) };
                postgis_sfcgal_ext_oid != InvalidOid
            })
            .unwrap_or(false);

        let geom_towkb_funcoid = postgis_ext_oid.map(|postgis_ext_oid| {
            Self::towkb_funcoid(
                postgis_ext_oid,
                postgis_ext_schema_oid.expect("expected postgis is created"),
                "geometry",
            )
        });

        let geog_towkb_funcoid = postgis_ext_oid.map(|postgis_ext_oid| {
            Self::towkb_funcoid(
                postgis_ext_oid,
                postgis_ext_schema_oid.expect("expected postgis is created"),
                "geography",
            )
        });

        let geom_fromwkb_funcoid =
            postgis_ext_oid.map(|_| Self::from_wkb_funcoid("st_geomfromwkb"));

        let geog_fromwkb_funcoid =
            postgis_ext_oid.map(|_| Self::from_wkb_funcoid("st_geogfromwkb"));

        let geometry_typoid = postgis_ext_oid.map(|_| {
            Self::postgis_typoid(
                postgis_ext_oid.expect("expected postgis is created"),
                postgis_ext_schema_oid.expect("expected postgis is created"),
                "geometry",
            )
        });

        let geography_typoid = postgis_ext_oid.map(|_| {
            Self::postgis_typoid(
                postgis_ext_oid.expect("expected postgis is created"),
                postgis_ext_schema_oid.expect("expected postgis is created"),
                "geography",
            )
        });

        Self {
            postgis_sfcgal_ext_exists,
            geometry_typoid,
            geography_typoid,
            geom_towkb_funcoid,
            geog_towkb_funcoid,
            geom_fromwkb_funcoid,
            geog_fromwkb_funcoid,
        }
    }

    fn extension_schema_oid() -> Oid {
        Spi::get_one("SELECT extnamespace FROM pg_extension WHERE extname = 'postgis'")
            .expect("failed to get postgis extension schema")
            .expect("postgis extension schema not found")
    }

    fn towkb_funcoid(
        postgis_ext_oid: Oid,
        postgis_ext_schema_oid: Oid,
        postgis_typname: &str,
    ) -> Oid {
        unsafe {
            let postgis_typoid =
                Self::postgis_typoid(postgis_ext_oid, postgis_ext_schema_oid, postgis_typname);

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

    fn from_wkb_funcoid(from_wkb_funcname: &str) -> Oid {
        unsafe {
            let function_name = makeString(from_wkb_funcname.as_pg_cstr());
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
        postgis_typename: &str,
    ) -> Oid {
        if postgis_ext_oid == InvalidOid {
            return InvalidOid;
        }

        let postgis_type_name = CString::new(postgis_typename).expect("CString::new failed");

        let postgis_typoid = unsafe {
            GetSysCacheOid(
                TYPENAMENSP as _,
                Anum_pg_type_oid as _,
                postgis_type_name.into_datum().unwrap(),
                postgis_ext_schema_oid.into_datum().unwrap(),
                Datum::from(0), // not used key
                Datum::from(0), // not used key
            )
        };

        if postgis_typoid == InvalidOid {
            return InvalidOid;
        }

        postgis_typoid
    }
}

// Geometry is a wrapper around a byte vector that represents a PostGIS geometry in WKB format.
#[derive(Debug, Clone, PartialEq)]
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
        let geom_fromwkb_funcoid = get_postgis_context()
            .geom_fromwkb_funcoid
            .expect("geom_fromwkb_funcoid");

        let wkb_datum = self.0.into_datum().expect("cannot convert wkb to datum");

        Some(unsafe { OidFunctionCall1Coll(geom_fromwkb_funcoid, InvalidOid, wkb_datum) })
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
            let st_asbinary_func_oid = get_postgis_context()
                .geom_towkb_funcoid
                .expect("geom_towkb_funcoid");

            let geom_datum = datum;

            let wkb_datum =
                unsafe { OidFunctionCall1Coll(st_asbinary_func_oid, InvalidOid, geom_datum) };

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
        let st_asbinary_func_oid = get_postgis_context()
            .geom_towkb_funcoid
            .expect("geom_towkb_funcoid");

        let geom_datum = datum.sans_lifetime();

        let wkb_datum = OidFunctionCall1Coll(st_asbinary_func_oid, InvalidOid, geom_datum);

        let is_null = false;
        let wkb = Vec::<u8>::from_datum(wkb_datum, is_null).expect("cannot convert datum to wkb");
        Geometry(wkb)
    }
}

// Geography is a wrapper around a byte vector that represents a PostGIS geography in WKB format.
#[derive(Debug, Clone, PartialEq)]
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
        let geog_fromwkb_funcoid = get_postgis_context()
            .geog_fromwkb_funcoid
            .expect("geog_fromwkb_funcoid");

        let wkb_datum = self.0.into_datum().expect("cannot convert wkb to datum");

        Some(unsafe { OidFunctionCall1Coll(geog_fromwkb_funcoid, InvalidOid, wkb_datum) })
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
            let st_asbinary_func_oid = get_postgis_context()
                .geog_towkb_funcoid
                .expect("geog_towkb_funcoid");

            let geog_datum = datum;

            let wkb_datum =
                unsafe { OidFunctionCall1Coll(st_asbinary_func_oid, InvalidOid, geog_datum) };

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
        let st_asbinary_func_oid = get_postgis_context()
            .geog_towkb_funcoid
            .expect("geog_towkb_funcoid");

        let geog_datum = datum.sans_lifetime();

        let wkb_datum = OidFunctionCall1Coll(st_asbinary_func_oid, InvalidOid, geog_datum);

        let is_null = false;
        let wkb = Vec::<u8>::from_datum(wkb_datum, is_null).expect("cannot convert datum to wkb");
        Geography(wkb)
    }
}

pub(crate) static EPSG_4326_PROJJSON: &str = r#"{
  "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
  "type": "GeographicCRS",
  "name": "WGS 84",
  "datum_ensemble": {
    "name": "World Geodetic System 1984 ensemble",
    "members": [
      {
        "name": "World Geodetic System 1984 (Transit)",
        "id": {
          "authority": "EPSG",
          "code": 1166
        }
      },
      {
        "name": "World Geodetic System 1984 (G730)",
        "id": {
          "authority": "EPSG",
          "code": 1152
        }
      },
      {
        "name": "World Geodetic System 1984 (G873)",
        "id": {
          "authority": "EPSG",
          "code": 1153
        }
      },
      {
        "name": "World Geodetic System 1984 (G1150)",
        "id": {
          "authority": "EPSG",
          "code": 1154
        }
      },
      {
        "name": "World Geodetic System 1984 (G1674)",
        "id": {
          "authority": "EPSG",
          "code": 1155
        }
      },
      {
        "name": "World Geodetic System 1984 (G1762)",
        "id": {
          "authority": "EPSG",
          "code": 1156
        }
      },
      {
        "name": "World Geodetic System 1984 (G2139)",
        "id": {
          "authority": "EPSG",
          "code": 1309
        }
      },
      {
        "name": "World Geodetic System 1984 (G2296)",
        "id": {
          "authority": "EPSG",
          "code": 1383
        }
      }
    ],
    "ellipsoid": {
      "name": "WGS 84",
      "semi_major_axis": 6378137,
      "inverse_flattening": 298.257223563
    },
    "accuracy": "2.0",
    "id": {
      "authority": "EPSG",
      "code": 6326
    }
  },
  "coordinate_system": {
    "subtype": "ellipsoidal",
    "axis": [
      {
        "name": "Geodetic latitude",
        "abbreviation": "Lat",
        "direction": "north",
        "unit": "degree"
      },
      {
        "name": "Geodetic longitude",
        "abbreviation": "Lon",
        "direction": "east",
        "unit": "degree"
      }
    ]
  },
  "scope": "Horizontal component of 3D system.",
  "area": "World.",
  "bbox": {
    "south_latitude": -90,
    "west_longitude": -180,
    "north_latitude": 90,
    "east_longitude": 180
  },
  "id": {
    "authority": "EPSG",
    "code": 4326
  }
}"#;
