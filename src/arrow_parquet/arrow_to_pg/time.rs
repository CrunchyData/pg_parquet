use arrow::array::{Array, Time64MicrosecondArray};

use crate::type_compat::{end_of_day_time::EndOfDayTime, pg_arrow_type_conversions::i64_to_time};

use super::{ArrowArrayToPgType, ArrowToPgAttributeContext};

// Time
//
// the microseconds are turned into a datum here rather than into pgrx's Time, so that the end
// of the day survives the read the way it survives the write. i64_to_time is still what refuses
// a value that is no time of day, since it gives the error such a value is refused with.
impl ArrowArrayToPgType<EndOfDayTime> for Time64MicrosecondArray {
    fn to_pg_type(self, _context: &ArrowToPgAttributeContext) -> Option<EndOfDayTime> {
        if self.is_null(0) {
            None
        } else {
            Some(validated_time(self.value(0)))
        }
    }
}

fn validated_time(microseconds: i64) -> EndOfDayTime {
    i64_to_time(microseconds);
    EndOfDayTime(microseconds)
}

// Time[]
impl ArrowArrayToPgType<Vec<Option<EndOfDayTime>>> for Time64MicrosecondArray {
    fn to_pg_type(self, _context: &ArrowToPgAttributeContext) -> Option<Vec<Option<EndOfDayTime>>> {
        let mut vals = vec![];
        for val in self.iter() {
            let val = val.map(validated_time);
            vals.push(val);
        }
        Some(vals)
    }
}
