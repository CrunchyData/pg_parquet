use std::sync::Arc;

use parquet::basic::LogicalType;
use parquet::geospatial::accumulator::{
    init_geo_stats_accumulator_factory, GeoStatsAccumulator, GeoStatsAccumulatorFactory,
    ParquetGeoStatsAccumulator, VoidGeoStatsAccumulator,
};
use parquet::geospatial::statistics::GeospatialStatistics;
use parquet::schema::types::ColumnDescPtr;

// the deepest geometry collection nesting that we walk while validating a wkb payload
const MAX_WKB_NESTING_DEPTH: u32 = 64;

// installs the accumulator factory that computes the geospatial statistics of the geometry
// columns. it must be called before any parquet writer is created.
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
        // parquet does not implement bounding for the geography logical type
        if matches!(descr.logical_type_ref(), Some(LogicalType::Geometry(_))) {
            Box::new(BoundableWkbGeoStatsAccumulator::default())
        } else {
            Box::new(VoidGeoStatsAccumulator::default())
        }
    }
}

#[derive(Debug, Default)]
struct BoundableWkbGeoStatsAccumulator {
    inner: ParquetGeoStatsAccumulator,
    unboundable: bool,
}

impl GeoStatsAccumulator for BoundableWkbGeoStatsAccumulator {
    fn is_valid(&self) -> bool {
        !self.unboundable && self.inner.is_valid()
    }

    fn update_wkb(&mut self, wkb: &[u8]) {
        if wkb_is_boundable(wkb) {
            self.inner.update_wkb(wkb);
        } else {
            self.unboundable = true;
        }
    }

    fn finish(&mut self) -> Option<Box<GeospatialStatistics>> {
        // the inner accumulator is always finished to reset its state
        let statistics = self.inner.finish();

        if std::mem::take(&mut self.unboundable) {
            None
        } else {
            statistics
        }
    }
}

// returns true if the whole wkb payload consists of the geometry types that parquet can bound
fn wkb_is_boundable(wkb: &[u8]) -> bool {
    matches!(consume_geometry(wkb, 0), Some(rest) if rest.is_empty())
}

// consumes one wkb geometry and returns the bytes that follow it, or None if the geometry is not
// one that parquet can bound
fn consume_geometry(wkb: &[u8], depth: u32) -> Option<&[u8]> {
    if depth > MAX_WKB_NESTING_DEPTH {
        return None;
    }

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

    let point_len = coords_per_point * size_of::<f64>();

    // see https://github.com/apache/parquet-format/blob/master/Geospatial.md for the type codes
    match type_code % 1000 {
        // point
        1 => consume_bytes(rest, point_len),
        // linestring
        2 => consume_coord_sequence(rest, little_endian, point_len),
        // polygon
        3 => {
            let (ring_count, mut rest) = consume_u32(rest, little_endian)?;

            for _ in 0..ring_count {
                rest = consume_coord_sequence(rest, little_endian, point_len)?;
            }

            Some(rest)
        }
        // multipoint, multilinestring, multipolygon and geometrycollection
        4..=7 => {
            let (part_count, mut rest) = consume_u32(rest, little_endian)?;

            for _ in 0..part_count {
                rest = consume_geometry(rest, depth + 1)?;
            }

            Some(rest)
        }
        _ => None,
    }
}

fn consume_coord_sequence(wkb: &[u8], little_endian: bool, point_len: usize) -> Option<&[u8]> {
    let (point_count, rest) = consume_u32(wkb, little_endian)?;

    consume_bytes(rest, point_len.checked_mul(point_count as usize)?)
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

fn consume_bytes(wkb: &[u8], len: usize) -> Option<&[u8]> {
    let (_, rest) = wkb.split_at_checked(len)?;

    Some(rest)
}
