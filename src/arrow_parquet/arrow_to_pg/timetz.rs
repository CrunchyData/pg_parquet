use arrow::array::{Array, Time64MicrosecondArray};

use crate::type_compat::{
    end_of_day_time::EndOfDayTimeTz, pg_arrow_type_conversions::i64_to_timetz,
};

use super::{ArrowArrayToPgType, ArrowToPgAttributeContext};

// Timetz
//
// the microseconds are turned into a datum here rather than into pgrx's TimeWithTimeZone,
// which cannot hold the end of the day: "24:00:00" is a value postgres stores and a value
// TimeWithTimeZone::try_from refuses, so a file that holds one is read through the datum.
impl ArrowArrayToPgType<EndOfDayTimeTz> for Time64MicrosecondArray {
    fn to_pg_type(self, _context: &ArrowToPgAttributeContext) -> Option<EndOfDayTimeTz> {
        if self.is_null(0) {
            None
        } else {
            Some(EndOfDayTimeTz(validated_timetz_microseconds(self.value(0))))
        }
    }
}

// a value past the end of the day is no time of day, and the error it is refused with is the
// one i64_to_timetz gives, so the check is made by asking it about the value it can answer for
fn validated_timetz_microseconds(microseconds: i64) -> i64 {
    const MICROSECONDS_PER_DAY: i64 = 24 * 60 * 60 * 1_000_000;

    if microseconds != MICROSECONDS_PER_DAY {
        i64_to_timetz(microseconds);
    }

    microseconds
}

// Timetz[]
impl ArrowArrayToPgType<Vec<Option<EndOfDayTimeTz>>> for Time64MicrosecondArray {
    fn to_pg_type(
        self,
        _context: &ArrowToPgAttributeContext,
    ) -> Option<Vec<Option<EndOfDayTimeTz>>> {
        let mut vals = vec![];
        for val in self.iter() {
            let val = val.map(|val| EndOfDayTimeTz(validated_timetz_microseconds(val)));
            vals.push(val);
        }
        Some(vals)
    }
}
