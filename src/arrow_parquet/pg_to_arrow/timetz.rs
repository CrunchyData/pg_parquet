use std::sync::Arc;

use arrow::array::{ArrayRef, ListArray, Time64MicrosecondArray};

use crate::{
    arrow_parquet::{arrow_utils::arrow_array_offsets, pg_to_arrow::PgTypeToArrowArray},
    type_compat::end_of_day_time::EndOfDayTimeTz,
};

use super::PgToArrowAttributeContext;

// TimeTz
impl PgTypeToArrowArray<EndOfDayTimeTz> for Vec<Option<EndOfDayTimeTz>> {
    fn to_arrow_array(self, _context: &PgToArrowAttributeContext) -> ArrayRef {
        let timetzs = self
            .into_iter()
            .map(|timetz| timetz.map(|timetz| timetz.0))
            .collect::<Vec<_>>();
        let timetz_array = Time64MicrosecondArray::from(timetzs);
        Arc::new(timetz_array)
    }
}

// TimeTz[]
impl PgTypeToArrowArray<EndOfDayTimeTz> for Vec<Option<Vec<Option<EndOfDayTimeTz>>>> {
    fn to_arrow_array(self, element_context: &PgToArrowAttributeContext) -> ArrayRef {
        let (offsets, nulls) = arrow_array_offsets(&self);

        // gets rid of the first level of Option, then flattens the inner Vec<Option<bool>>.
        let pg_array = self
            .into_iter()
            .flatten()
            .flatten()
            .map(|timetz| timetz.map(|timetz| timetz.0))
            .collect::<Vec<_>>();

        let timetz_array = Time64MicrosecondArray::from(pg_array);

        let list_array = ListArray::new(
            element_context.field(),
            offsets,
            Arc::new(timetz_array),
            Some(nulls),
        );

        Arc::new(list_array)
    }
}
