//! Rendering a value out of a parquet footer as Postgres text.
//!
//! `parquet.column_stats` and `parquet.metadata` report a column's minimum and maximum, and
//! what the footer holds for a date is an `i32` and for a timestamp an `i64`. Turning those
//! into something a user can read is the only conversion left in this extension, and it is a
//! narrow one: a statistic is a single scalar from the file's own metadata, never a column of
//! data. The data conversion is tier 2's and happens through `pg_arrow_base` — nothing here is
//! reachable from a `COPY`.
//!
//! The epoch offsets are the parquet specification's own, which is why they are written down
//! next to the code that reads a parquet footer.

use std::ffi::CStr;

use arrow::datatypes::{Decimal128Type, DecimalType};
use pgrx::datum::{Date, Time, TimeWithTimeZone, Timestamp, TimestampWithTimeZone};
use pgrx::pg_sys::AsPgCStr;
use pgrx::{direct_function_call, ereport, pg_sys, AnyNumeric, IntoDatum};

/// Postgres stores -infinity as the minimum of the underlying integer, but DuckDB reserves that
/// value for its NULL. A file written here holds one above the minimum, and both read back as
/// -infinity.
const NEG_INFINITY_DATE: i32 = i32::MIN + 1;
const NEG_INFINITY_TIMESTAMP: i64 = i64::MIN + 1;

/// The PG epoch (2000-01-01) is 10957 days after the Unix epoch (1970-01-01) parquet counts
/// from.
const DAYS_BETWEEN_UNIX_AND_PG_EPOCH: i32 = 10957;
const MICROS_BETWEEN_UNIX_AND_PG_EPOCH: i64 = 10957 * 24 * 60 * 60 * 1_000_000;

/// Reports a statistic Postgres cannot represent. Infinity is not one of those: Postgres stores
/// it as the limits of the underlying integer, which are handled first.
fn error_out_of_range(pg_type: &str, value: i64) -> ! {
    ereport!(
        pgrx::PgLogLevel::ERROR,
        pgrx::PgSqlErrorCode::ERRCODE_DATETIME_FIELD_OVERFLOW,
        format!("the file holds a {pg_type} statistic Postgres cannot represent: {value}"),
        "The value is outside the range of the Postgres type the column was read as."
    );

    unreachable!("ereport at ERROR does not return");
}

pub(crate) fn i32_to_date(i32_date: i32) -> Date {
    if i32_date == i32::MAX {
        return Date::positive_infinity();
    } else if i32_date == i32::MIN || i32_date == NEG_INFINITY_DATE {
        return Date::negative_infinity();
    }

    let pg_epoch_days = i32_date
        .checked_sub(DAYS_BETWEEN_UNIX_AND_PG_EPOCH)
        .unwrap_or_else(|| error_out_of_range("date", i32_date as i64));

    unsafe { Date::from_pg_epoch_days(pg_epoch_days) }
}

pub(crate) fn i64_to_timestamp(i64_timestamp: i64) -> Timestamp {
    if i64_timestamp == i64::MAX {
        return Timestamp::positive_infinity();
    } else if i64_timestamp == i64::MIN || i64_timestamp == NEG_INFINITY_TIMESTAMP {
        return Timestamp::negative_infinity();
    }

    let pg_epoch_micros = i64_timestamp
        .checked_sub(MICROS_BETWEEN_UNIX_AND_PG_EPOCH)
        .unwrap_or_else(|| error_out_of_range("timestamp", i64_timestamp));

    pg_epoch_micros
        .try_into()
        .unwrap_or_else(|_| error_out_of_range("timestamp", i64_timestamp))
}

pub(crate) fn i64_to_timestamptz(i64_timestamptz: i64, timezone: &str) -> TimestampWithTimeZone {
    if i64_timestamptz == i64::MAX {
        return TimestampWithTimeZone::positive_infinity();
    } else if i64_timestamptz == i64::MIN || i64_timestamptz == NEG_INFINITY_TIMESTAMP {
        return TimestampWithTimeZone::negative_infinity();
    }

    let pg_epoch_micros = i64_timestamptz
        .checked_sub(MICROS_BETWEEN_UNIX_AND_PG_EPOCH)
        .unwrap_or_else(|| error_out_of_range("timestamptz", i64_timestamptz));

    let timestamp: Timestamp = pg_epoch_micros
        .try_into()
        .unwrap_or_else(|_| error_out_of_range("timestamptz", i64_timestamptz));

    (timestamp, timezone)
        .try_into()
        .unwrap_or_else(|_| error_out_of_range("timestamptz", i64_timestamptz))
}

pub(crate) fn i64_to_time(i64_time: i64) -> Time {
    i64_time
        .try_into()
        .unwrap_or_else(|_| error_out_of_range("time", i64_time))
}

pub(crate) fn i64_to_timetz(i64_timetz: i64) -> TimeWithTimeZone {
    let utc_tz = 0;

    (i64_timetz, utc_tz)
        .try_into()
        .unwrap_or_else(|_| error_out_of_range("timetz", i64_timetz))
}

/// A decimal statistic as a `numeric`. Formatted through arrow rather than by hand, since
/// arrow's rendering of a `Decimal128` already agrees with Postgres' own.
pub(crate) fn i128_to_numeric(
    decimal: i128,
    precision: u32,
    scale: u32,
    typmod: i32,
) -> AnyNumeric {
    let numeric_str = Decimal128Type::format_decimal(decimal, precision as _, scale as _);

    unsafe {
        let numeric_cstring = CStr::from_ptr(numeric_str.as_pg_cstr());

        direct_function_call(
            pg_sys::numeric_in,
            &[
                numeric_cstring.into_datum(),
                0.into_datum(),
                typmod.into_datum(),
            ],
        )
        .expect("cannot convert string to numeric")
    }
}

pub(crate) fn make_numeric_typmod(precision: i32, scale: i32) -> i32 {
    ((precision << 16) | (scale & 0x7ff)) + pg_sys::VARHDRSZ as i32
}
