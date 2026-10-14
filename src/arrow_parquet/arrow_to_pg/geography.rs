use arrow::array::{Array, BinaryArray};

use crate::type_compat::geometry::Geography;

use super::{ArrowArrayToPgType, ArrowToPgAttributeContext};

// Geography
impl ArrowArrayToPgType<Geography> for BinaryArray {
    fn to_pg_type(self, context: &ArrowToPgAttributeContext) -> Option<Geography> {
        if self.is_null(0) {
            None
        } else {
            Some(Geography::new(self.value(0).to_vec(), context.srid()))
        }
    }
}

// Geography[]
impl ArrowArrayToPgType<Vec<Option<Geography>>> for BinaryArray {
    fn to_pg_type(self, context: &ArrowToPgAttributeContext) -> Option<Vec<Option<Geography>>> {
        let mut vals = vec![];
        for val in self.iter() {
            if let Some(val) = val {
                vals.push(Some(Geography::new(val.to_vec(), context.srid())));
            } else {
                vals.push(None);
            }
        }

        Some(vals)
    }
}
