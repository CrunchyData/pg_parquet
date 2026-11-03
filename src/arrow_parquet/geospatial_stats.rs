use std::sync::Arc;

use parquet::basic::LogicalType;
use parquet::geospatial::accumulator::{
    init_geo_stats_accumulator_factory, GeoStatsAccumulator, GeoStatsAccumulatorFactory,
    ParquetGeoStatsAccumulator, VoidGeoStatsAccumulator,
};
use parquet::geospatial::bounding_box::BoundingBox;
use parquet::geospatial::statistics::GeospatialStatistics;
use parquet::schema::types::ColumnDescPtr;
use parquet_geospatial::bounding::GeometryBounder;
use parquet_geospatial::interval::IntervalTrait;

// installs the accumulator factory that computes the geospatial statistics of the geometry and
// geography columns. it must be called before any parquet writer is created.
pub(crate) fn init_geospatial_stats_accumulator_factory() {
    init_geo_stats_accumulator_factory(Arc::new(BoundableWkbGeoStatsAccumulatorFactory {}))
        .expect("failed to install the geospatial statistics accumulator factory");
}

// parquet can only compute the bounding box of the geometry types that the parquet geospatial
// specification defines. postgis also has curved, triangulated and polyhedral types, whose wkb
// the bounder misreads instead of rejecting, so we check the wkb ourselves and drop the
// statistics of the columns that contain such a geometry.
#[derive(Debug)]
struct BoundableWkbGeoStatsAccumulatorFactory {}

impl GeoStatsAccumulatorFactory for BoundableWkbGeoStatsAccumulatorFactory {
    fn new_accumulator(&self, descr: &ColumnDescPtr) -> Box<dyn GeoStatsAccumulator> {
        match descr.logical_type_ref() {
            Some(LogicalType::Geometry(_)) => Box::new(BoundableWkbGeoStatsAccumulator::default()),
            // parquet implements no bounding for the geography logical type, so we do it ourselves
            Some(LogicalType::Geography(_)) => Box::new(GeographyGeoStatsAccumulator::default()),
            _ => Box::new(VoidGeoStatsAccumulator::default()),
        }
    }
}

#[derive(Debug, Default)]
struct BoundableWkbGeoStatsAccumulator {
    inner: ParquetGeoStatsAccumulator,
    unboundable: bool,
    non_finite: bool,
}

impl GeoStatsAccumulator for BoundableWkbGeoStatsAccumulator {
    fn is_valid(&self) -> bool {
        !self.unboundable && self.inner.is_valid()
    }

    fn update_wkb(&mut self, wkb: &[u8]) {
        let (boundable, finite) = wkb_is_boundable_and_finite(wkb);

        if boundable {
            self.inner.update_wkb(wkb);
        } else {
            self.unboundable = true;
        }

        self.non_finite |= !finite;
    }

    fn finish(&mut self) -> Option<Box<GeospatialStatistics>> {
        // the inner accumulator is always finished to reset its state
        let statistics = self.inner.finish();

        let unboundable = std::mem::take(&mut self.unboundable);
        let non_finite = std::mem::take(&mut self.non_finite);

        if unboundable {
            return None;
        }

        if non_finite {
            // the geometry types are exact either way, so only the box goes
            return Some(Box::new(GeospatialStatistics::new(
                None,
                statistics
                    .as_ref()
                    .and_then(|statistics| statistics.geospatial_types())
                    .cloned(),
            )));
        }

        statistics
    }
}

// the geospatial statistics of a geography column. parquet accumulates none, since bounding a
// geography means bounding geodesic edges, which can reach beyond the box that the coordinates of
// their endpoints give. the geometry types are exact either way, so they are always written, while
// the bounding box is only written for a column whose geometries carry no edges at all.
#[derive(Debug)]
struct GeographyGeoStatsAccumulator {
    bounder: GeometryBounder,
    invalid: bool,
    has_edges: bool,
    non_finite: bool,
}

impl Default for GeographyGeoStatsAccumulator {
    fn default() -> Self {
        Self {
            bounder: new_geography_bounder(),
            invalid: false,
            has_edges: false,
            non_finite: false,
        }
    }
}

// the bounder is left without a wraparound hint, so a column that straddles the antimeridian gets
// the plain box of its coordinates rather than the tighter wraparound form of the specification.
// parquet-geospatial's hint only produces a box when the column has coordinates on both sides of
// the hint's midpoint, and returns an empty one otherwise, which is most columns.
fn new_geography_bounder() -> GeometryBounder {
    GeometryBounder::empty()
}

impl GeoStatsAccumulator for GeographyGeoStatsAccumulator {
    fn is_valid(&self) -> bool {
        !self.invalid
    }

    fn update_wkb(&mut self, wkb: &[u8]) {
        let (boundable, finite) = wkb_is_boundable_and_finite(wkb);

        if !boundable || self.bounder.update_wkb(wkb).is_err() {
            self.invalid = true;
            return;
        }

        self.has_edges |= wkb_has_edges(wkb);
        self.non_finite |= !finite;
    }

    fn finish(&mut self) -> Option<Box<GeospatialStatistics>> {
        // the bounder is always replaced to reset its state
        let bounder = std::mem::replace(&mut self.bounder, new_geography_bounder());
        let invalid = std::mem::take(&mut self.invalid);
        let has_edges = std::mem::take(&mut self.has_edges);
        let non_finite = std::mem::take(&mut self.non_finite);

        if invalid {
            return None;
        }

        let bbox = if has_edges || non_finite {
            None
        } else {
            bounding_box_of(&bounder)
        };

        let geospatial_types = Some(bounder.geometry_types()).filter(|types| !types.is_empty());

        Some(Box::new(GeospatialStatistics::new(bbox, geospatial_types)))
    }
}

// the bounding box of the accumulated geometries, in the shape that parquet's own geometry
// accumulator gives it: absent unless both x and y have a value, and z and m only when present
fn bounding_box_of(bounder: &GeometryBounder) -> Option<BoundingBox> {
    let (x, y) = (bounder.x(), bounder.y());

    if x.is_empty() || y.is_empty() {
        return None;
    }

    let mut bbox = BoundingBox::new(x.lo(), x.hi(), y.lo(), y.hi());

    if !bounder.z().is_empty() {
        bbox = bbox.with_zrange(bounder.z().lo(), bounder.z().hi());
    }

    if !bounder.m().is_empty() {
        bbox = bbox.with_mrange(bounder.m().lo(), bounder.m().hi());
    }

    Some(bbox)
}

// returns true unless the geometry is a point or a multipoint, whose coordinates are the whole
// geometry. a collection of points counts as having edges, which only costs it its bounding box.
fn wkb_has_edges(wkb: &[u8]) -> bool {
    !matches!(wkb_geometry_type(wkb), Some(1 | 4))
}

// the iso wkb geometry type of the payload, without the dimensions that its type code also carries
fn wkb_geometry_type(wkb: &[u8]) -> Option<u32> {
    let (byte_order, rest) = wkb.split_first()?;

    let little_endian = match byte_order {
        0 => false,
        1 => true,
        _ => return None,
    };

    let (type_code, _) = consume_u32(rest, little_endian)?;

    Some(type_code % 1000)
}

// the deepest geometry collection nesting whose bounding box we compute. The bounder recurses
// once per level and runs out of the backend's stack a few thousand levels in, while postgis
// parses and round-trips that many happily, so a payload nested deeper than this is left
// unbounded rather than handed to it.
const MAX_WKB_NESTING_DEPTH: u32 = 64;

// returns whether the whole wkb payload consists of the geometry types that parquet can bound,
// and whether every coordinate in it is a finite number. A NaN or an infinity bounds nothing: a
// box that carries one reads as a box a query does not intersect, so a reader that skips a row
// group by it skips rows that are in there.
fn wkb_is_boundable_and_finite(wkb: &[u8]) -> (bool, bool) {
    let mut finite = true;

    let boundable = matches!(consume_geometry(wkb, 0, &mut finite), Some(rest) if rest.is_empty());

    (boundable, finite)
}

// the iso type code of the first geometry in the payload whose type the parquet geospatial
// specification does not define, or None if every type in it is one the specification defines.
//
// Every geometry in a payload is a header followed by its own coordinates, and a collection
// holds its parts as the geometries that follow it, so reading one header after another to the
// end of the bytes reaches all of them without following the nesting and without a depth to
// give up at. A type the specification leaves out is therefore found wherever it is, including
// behind a sibling nested deeper than the bounding walk goes, and no geometry is refused for
// being nested deeply, since the nesting is never what this stops on.
pub(crate) fn wkb_type_outside_the_specification(wkb: &[u8]) -> Option<u32> {
    // the bounding box is what a non-finite coordinate costs a geometry, and that is answered by
    // the walk below rather than here
    let mut finiteness_is_not_our_question = true;
    let mut rest = wkb;

    while !rest.is_empty() {
        let WkbHeader {
            little_endian,
            type_code,
            coords_per_point,
            rest: after_header,
        } = consume_header(rest)?;

        rest = match type_code % 1000 {
            // point
            1 => consume_points(
                after_header,
                little_endian,
                coords_per_point,
                1,
                &mut finiteness_is_not_our_question,
            )?,
            // linestring
            2 => consume_coord_sequence(
                after_header,
                little_endian,
                coords_per_point,
                &mut finiteness_is_not_our_question,
            )?,
            // polygon
            3 => consume_rings(
                after_header,
                little_endian,
                coords_per_point,
                &mut finiteness_is_not_our_question,
            )?,
            // multipoint, multilinestring, multipolygon and geometrycollection, which are
            // nothing here but the count of the geometries that follow them
            4..=7 => consume_u32(after_header, little_endian)?.1,
            // the curved types, and tin, polyhedral surface and triangle
            _ => return Some(type_code),
        };
    }

    None
}

// consumes one wkb geometry and returns the bytes that follow it, or None if the geometry is not
// one that parquet can bound
fn consume_geometry<'a>(wkb: &'a [u8], depth: u32, finite: &mut bool) -> Option<&'a [u8]> {
    if depth > MAX_WKB_NESTING_DEPTH {
        return None;
    }

    let WkbHeader {
        little_endian,
        type_code,
        coords_per_point,
        rest,
    } = consume_header(wkb)?;

    // see https://github.com/apache/parquet-format/blob/master/Geospatial.md for the type codes
    match type_code % 1000 {
        // point
        1 => consume_points(rest, little_endian, coords_per_point, 1, finite),
        // linestring
        2 => consume_coord_sequence(rest, little_endian, coords_per_point, finite),
        // polygon
        3 => consume_rings(rest, little_endian, coords_per_point, finite),
        // multipoint, multilinestring, multipolygon and geometrycollection
        4..=7 => {
            let (part_count, mut rest) = consume_u32(rest, little_endian)?;

            for _ in 0..part_count {
                rest = consume_geometry(rest, depth + 1, finite)?;
            }

            Some(rest)
        }
        _ => None,
    }
}

// the head of one wkb geometry, and the bytes of it that follow. Reading it is the only part of
// a walk that can meet bytes which are not iso wkb at all, so both walks read it from here.
//
// Each of the ways that can fail is unreachable from a geometry postgis gave us, which is the
// only kind either walk is handed: postgis normalises what it is given, so a big endian payload
// is a little endian one by the time it reaches us, and it rejects a type code and a length it
// cannot read before we see the bytes at all. They are read rather than assumed anyway, because
// what makes them unreachable is postgis and not the specification.
struct WkbHeader<'a> {
    little_endian: bool,
    type_code: u32,
    coords_per_point: usize,
    rest: &'a [u8],
}

fn consume_header(wkb: &[u8]) -> Option<WkbHeader<'_>> {
    let (byte_order, rest) = wkb.split_first()?;

    let little_endian = match byte_order {
        0 => false,
        1 => true,
        _ => return None,
    };

    let (type_code, rest) = consume_u32(rest, little_endian)?;

    // an iso wkb type code is the geometry type plus 1000 per extra dimension. anything else,
    // e.g. the extended wkb flags that postgis writes for its own format, is rejected.
    let coords_per_point = match type_code / 1000 {
        0 => 2,
        1 | 2 => 3,
        3 => 4,
        _ => return None,
    };

    Some(WkbHeader {
        little_endian,
        type_code,
        coords_per_point,
        rest,
    })
}

fn consume_rings<'a>(
    wkb: &'a [u8],
    little_endian: bool,
    coords_per_point: usize,
    finite: &mut bool,
) -> Option<&'a [u8]> {
    let (ring_count, mut rest) = consume_u32(wkb, little_endian)?;

    for _ in 0..ring_count {
        rest = consume_coord_sequence(rest, little_endian, coords_per_point, finite)?;
    }

    Some(rest)
}

fn consume_coord_sequence<'a>(
    wkb: &'a [u8],
    little_endian: bool,
    coords_per_point: usize,
    finite: &mut bool,
) -> Option<&'a [u8]> {
    let (point_count, rest) = consume_u32(wkb, little_endian)?;

    consume_points(
        rest,
        little_endian,
        coords_per_point,
        point_count as usize,
        finite,
    )
}

// consumes the coordinates of the given number of points, and clears `finite` for a coordinate
// that is not a finite number
fn consume_points<'a>(
    wkb: &'a [u8],
    little_endian: bool,
    coords_per_point: usize,
    point_count: usize,
    finite: &mut bool,
) -> Option<&'a [u8]> {
    let len = coords_per_point
        .checked_mul(size_of::<f64>())?
        .checked_mul(point_count)?;

    let (coords, rest) = wkb.split_at_checked(len)?;

    let (coords, _) = coords.as_chunks::<{ size_of::<f64>() }>();

    for coord in coords {
        let coord = *coord;

        let coord = if little_endian {
            f64::from_le_bytes(coord)
        } else {
            f64::from_be_bytes(coord)
        };

        *finite &= coord.is_finite();
    }

    Some(rest)
}

fn consume_u32(wkb: &[u8], little_endian: bool) -> Option<(u32, &[u8])> {
    let (bytes, rest) = wkb.split_at_checked(size_of::<u32>())?;

    let bytes = bytes.try_into().ok()?;

    let value = if little_endian {
        u32::from_le_bytes(bytes)
    } else {
        u32::from_be_bytes(bytes)
    };

    Some((value, rest))
}
