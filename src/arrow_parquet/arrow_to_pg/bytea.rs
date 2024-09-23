use arrow::array::{Array, BinaryArray};

use super::{ArrowArrayToPgType, ArrowToPgAttributeContext};

// Bytea
impl ArrowArrayToPgType<BinaryArray, Vec<u8>> for Vec<u8> {
    fn to_pg_type(arr: BinaryArray, _context: &ArrowToPgAttributeContext) -> Option<Vec<u8>> {
        if arr.is_null(0) {
            None
        } else {
            Some(arr.value(0).to_vec())
        }
    }
}

// Bytea[]
impl ArrowArrayToPgType<BinaryArray, Vec<Option<Vec<u8>>>> for Vec<Option<Vec<u8>>> {
    fn to_pg_type(
        arr: BinaryArray,
        _context: &ArrowToPgAttributeContext,
    ) -> Option<Vec<Option<Vec<u8>>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            if let Some(val) = val {
                vals.push(Some(val.to_vec()));
            } else {
                vals.push(None);
            }
        }

        Some(vals)
    }
}
