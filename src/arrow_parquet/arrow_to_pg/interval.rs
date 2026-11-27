use arrow::array::{
    Array, DurationMicrosecondArray, DurationMillisecondArray, DurationNanosecondArray,
    DurationSecondArray,
};
use arrow_schema::TimeUnit;
use pgrx::datum::Interval;

use crate::type_compat::interval::duration_to_interval;

use super::{ArrowArrayToPgType, ArrowToPgAttributeContext};

// a duration is a count of one unit of time, which is the microseconds of an interval that has
// no months and no days
macro_rules! duration_to_pg_interval {
    ($array_type:ty, $unit:expr) => {
        impl ArrowArrayToPgType<Interval> for $array_type {
            fn to_pg_type(self, _context: &ArrowToPgAttributeContext) -> Option<Interval> {
                if self.is_null(0) {
                    None
                } else {
                    Some(duration_to_interval(self.value(0), &$unit))
                }
            }
        }

        impl ArrowArrayToPgType<Vec<Option<Interval>>> for $array_type {
            fn to_pg_type(
                self,
                _context: &ArrowToPgAttributeContext,
            ) -> Option<Vec<Option<Interval>>> {
                let mut vals = vec![];
                for val in self.iter() {
                    vals.push(val.map(|val| duration_to_interval(val, &$unit)));
                }
                Some(vals)
            }
        }
    };
}

duration_to_pg_interval!(DurationSecondArray, TimeUnit::Second);
duration_to_pg_interval!(DurationMillisecondArray, TimeUnit::Millisecond);
duration_to_pg_interval!(DurationMicrosecondArray, TimeUnit::Microsecond);
duration_to_pg_interval!(DurationNanosecondArray, TimeUnit::Nanosecond);
