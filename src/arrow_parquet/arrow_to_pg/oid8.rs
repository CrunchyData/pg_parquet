use arrow::array::{Array, UInt64Array};

use crate::type_compat::oid8::Oid8;

use super::{ArrowArrayToPgType, ArrowToPgAttributeContext};

// Oid8
impl ArrowArrayToPgType<Oid8> for UInt64Array {
    fn to_pg_type(self, _context: &ArrowToPgAttributeContext) -> Option<Oid8> {
        if self.is_null(0) {
            None
        } else {
            Some(Oid8(self.value(0)))
        }
    }
}

// Oid8[]
impl ArrowArrayToPgType<Vec<Option<Oid8>>> for UInt64Array {
    fn to_pg_type(self, _context: &ArrowToPgAttributeContext) -> Option<Vec<Option<Oid8>>> {
        let mut vals = vec![];
        for val in self.iter() {
            vals.push(val.map(Oid8));
        }
        Some(vals)
    }
}
