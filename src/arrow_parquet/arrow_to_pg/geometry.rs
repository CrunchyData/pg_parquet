use arrow::array::{Array, BinaryArray};

use crate::type_compat::geometry::Geometry;

use super::{ArrowArrayToPgType, ArrowToPgContext};

// Geometry
impl ArrowArrayToPgType<'_, BinaryArray, Geometry> for Geometry {
    fn to_pg_type(arr: BinaryArray, _context: ArrowToPgContext<'_>) -> Option<Geometry> {
        if arr.is_null(0) {
            None
        } else {
            Some(arr.value(0).to_vec().into())
        }
    }
}

// Geometry[]
impl ArrowArrayToPgType<'_, BinaryArray, Vec<Option<Geometry>>> for Vec<Option<Geometry>> {
    fn to_pg_type(
        arr: BinaryArray,
        _context: ArrowToPgContext<'_>,
    ) -> Option<Vec<Option<Geometry>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            if let Some(val) = val {
                vals.push(Some(val.to_vec().into()));
            } else {
                vals.push(None);
            }
        }

        Some(vals)
    }
}
