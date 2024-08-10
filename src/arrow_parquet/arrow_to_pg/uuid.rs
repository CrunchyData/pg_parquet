use arrow::array::{Array, FixedSizeBinaryArray};
use pgrx::{PgTupleDesc, Uuid};

use super::ArrowArrayToPgType;

// Uuid
impl<'a> ArrowArrayToPgType<'_, FixedSizeBinaryArray, Uuid> for Uuid {
    fn as_pg(arr: FixedSizeBinaryArray, _tupledesc: Option<PgTupleDesc>) -> Option<Uuid> {
        if arr.is_null(0) {
            None
        } else {
            let val = arr.value(0);
            let val = Uuid::from_slice(val).unwrap();
            Some(val)
        }
    }
}

// Uuid[]
impl<'a> ArrowArrayToPgType<'_, FixedSizeBinaryArray, Vec<Option<Uuid>>> for Vec<Option<Uuid>> {
    fn as_pg(
        arr: FixedSizeBinaryArray,
        _tupledesc: Option<PgTupleDesc>,
    ) -> Option<Vec<Option<Uuid>>> {
        let mut vals = vec![];
        for val in arr.iter() {
            let val = val.map(|val| Uuid::from_slice(val).unwrap());
            vals.push(val);
        }
        Some(vals)
    }
}
