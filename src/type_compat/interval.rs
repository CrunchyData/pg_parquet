use arrow_schema::TimeUnit;
use pgrx::datum::Interval;

// duration_to_interval converts an arrow Duration, which is a count of one unit of time and
// carries no months, into an interval. Seconds, milliseconds and microseconds convert exactly
// into the microseconds postgres keeps; nanoseconds are rounded to the microsecond.
pub(crate) fn duration_to_interval(value: i64, unit: &TimeUnit) -> Interval {
    let microseconds = match unit {
        TimeUnit::Second => value.checked_mul(1_000_000),
        TimeUnit::Millisecond => value.checked_mul(1_000),
        TimeUnit::Microsecond => Some(value),
        TimeUnit::Nanosecond => Some(value / 1_000),
    };

    fn out_of_range(value: i64, unit: &TimeUnit) -> ! {
        panic!(
            "duration of {value} {unit:?} in the parquet file is out of range for PostgreSQL \"interval\" type"
        )
    }

    let Some(microseconds) = microseconds else {
        out_of_range(value, unit)
    };

    let no_months = 0;
    let no_days = 0;

    match Interval::new(no_months, no_days, microseconds) {
        Ok(interval) => interval,
        Err(_) => out_of_range(value, unit),
    }
}
