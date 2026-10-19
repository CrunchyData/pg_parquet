use core::panic;
use std::ffi::CStr;

use arrow::datatypes::{Decimal128Type, DecimalType};
use pgrx::{
    datum::{Date, Interval, Time, TimeWithTimeZone, Timestamp, TimestampWithTimeZone},
    direct_function_call, ereport,
    pg_sys::{self, AsPgCStr},
    AnyNumeric, IntoDatum, Numeric,
};

// Postgres stores -infinity as the minimum of the underlying integer, but DuckDB reserves
// that value for its NULL and fails to read it back. We write one above the minimum, which is
// what DuckDB writes for -infinity, and we read both of them back as -infinity.
pub const NEG_INFINITY_DATE: i32 = i32::MIN + 1;
pub const NEG_INFINITY_TIMESTAMP: i64 = i64::MIN + 1;

// PG epoch (2000-01-01) is 10957 days after the Unix epoch (1970-01-01) Arrow counts from
const MICROS_BETWEEN_UNIX_AND_PG_EPOCH: i64 = 10957 * 24 * 60 * 60 * 1_000_000;

pub fn date_to_i32(date: Date) -> i32 {
    if date.is_infinity() {
        return i32::MAX;
    } else if date.is_neg_infinity() {
        return NEG_INFINITY_DATE;
    }

    // PG epoch is (2000-01-01). Convert it to Unix epoch (1970-01-01). +10957 days. The day
    // count is shifted rather than the date itself, because PG date arithmetic cannot
    // represent a date within 10957 days of its own maximum, while the day count of every
    // PG date leaves room for the shift in an i32.
    date.to_unix_epoch_days()
}

pub fn i32_to_date(i32_date: i32) -> Date {
    // an infinite date in the file is one of the limits of the underlying i32
    if i32_date == i32::MAX {
        return Date::positive_infinity();
    } else if i32_date == i32::MIN || i32_date == NEG_INFINITY_DATE {
        return Date::negative_infinity();
    }

    // Parquet uses Unix epoch (1970-01-01). Convert it to PG epoch (2000-01-01). -10957 days
    let pg_epoch_days = i32_date
        .checked_sub(10957)
        .unwrap_or_else(|| error_out_of_range_temporal("date", i32_date as i64));

    unsafe { Date::from_pg_epoch_days(pg_epoch_days) }
}

// Reports a temporal value read from a file that Postgres cannot represent. Postgres stores
// infinity as the limits of the underlying integer, which are handled separately.
fn error_out_of_range_temporal(pg_type: &str, value: i64) -> ! {
    ereport!(
        pgrx::PgLogLevel::ERROR,
        pgrx::PgSqlErrorCode::ERRCODE_DATETIME_FIELD_OVERFLOW,
        format!(
            "{} value {} in the file is out of range for PostgreSQL \"{}\" type",
            pg_type, value, pg_type
        ),
        "Consider excluding the column from the COPY FROM statement.",
    );

    unreachable!("ereport with ERROR level does not return")
}

// Shifts a Postgres microsecond count to the Unix epoch Arrow counts from. i64::MAX is
// reserved for an infinite timestamp, so the one finite value that lands on it is out of
// range too.
fn pg_epoch_micros_to_unix_micros(pg_type: &str, pg_epoch_micros: i64) -> i64 {
    match pg_epoch_micros.checked_add(MICROS_BETWEEN_UNIX_AND_PG_EPOCH) {
        Some(unix_micros) if unix_micros != i64::MAX => unix_micros,
        _ => error_out_of_range_for_arrow(pg_type, pg_epoch_micros),
    }
}

// Reports a Postgres temporal value that Arrow cannot represent. Arrow counts microseconds
// from the Unix epoch in an i64, which runs out about 30 years before the latest timestamp
// Postgres itself can hold.
fn error_out_of_range_for_arrow(pg_type: &str, pg_epoch_micros: i64) -> ! {
    ereport!(
        pgrx::PgLogLevel::ERROR,
        pgrx::PgSqlErrorCode::ERRCODE_DATETIME_FIELD_OVERFLOW,
        format!(
            "{} value {} microseconds after the PostgreSQL epoch is out of range for an Arrow \"{}\" column",
            pg_type, pg_epoch_micros, pg_type
        ),
        "Consider excluding the column from the COPY TO statement.",
    );

    unreachable!("ereport with ERROR level does not return")
}

pub fn timestamp_to_i64(timestamp: Timestamp) -> i64 {
    if timestamp.is_infinity() {
        return i64::MAX;
    } else if timestamp.is_neg_infinity() {
        return NEG_INFINITY_TIMESTAMP;
    }

    let timestamp_as_bytes: Vec<u8> = unsafe {
        direct_function_call(pg_sys::time_send, &[timestamp.into_datum()])
            .expect("cannot convert timestamp to bytes")
    };

    let pg_epoch_micros = i64::from_be_bytes(
        timestamp_as_bytes[0..8]
            .try_into()
            .unwrap_or_else(|e| panic!("{}", e)),
    );

    // Convert PG epoch to the Unix epoch Arrow counts from. The microseconds are shifted
    // rather than the timestamp itself, because PG timestamp arithmetic cannot represent a
    // timestamp within 10957 days of its own maximum.
    pg_epoch_micros_to_unix_micros("timestamp", pg_epoch_micros)
}

pub fn i64_to_timestamp(i64_timestamp: i64) -> Timestamp {
    // an infinite timestamp in the file is one of the limits of the underlying i64
    if i64_timestamp == i64::MAX {
        return Timestamp::positive_infinity();
    } else if i64_timestamp == i64::MIN || i64_timestamp == NEG_INFINITY_TIMESTAMP {
        return Timestamp::negative_infinity();
    }

    // Parquet uses Unix epoch (1970-01-01). Convert it to PG epoch (2000-01-01).
    let pg_epoch_micros = i64_timestamp
        .checked_sub(MICROS_BETWEEN_UNIX_AND_PG_EPOCH)
        .unwrap_or_else(|| error_out_of_range_temporal("timestamp", i64_timestamp));

    pg_epoch_micros
        .try_into()
        .unwrap_or_else(|_| error_out_of_range_temporal("timestamp", i64_timestamp))
}

pub fn timestamptz_to_i64(timestamptz: TimestampWithTimeZone) -> i64 {
    if timestamptz.is_infinity() {
        return i64::MAX;
    } else if timestamptz.is_neg_infinity() {
        return NEG_INFINITY_TIMESTAMP;
    }

    // timestamptz is already adjusted to utc internally by Postgres. Postgres uses
    // local session timezone to display timestamptz values.
    let timestamptz_as_bytes: Vec<u8> = unsafe {
        direct_function_call(pg_sys::timestamptz_send, &[timestamptz.into_datum()])
            .expect("cannot convert timestamptz to bytes")
    };

    let pg_epoch_micros = i64::from_be_bytes(
        timestamptz_as_bytes[0..8]
            .try_into()
            .unwrap_or_else(|e| panic!("{}", e)),
    );

    // Convert PG epoch to the Unix epoch Arrow counts from. The microseconds are shifted
    // rather than the timestamptz itself, because adding days to a timestamptz is a local
    // session timezone operation: it adds a day of local wall clock time, which is 23 or 25
    // hours across a DST boundary. That would move the instant the file records.
    pg_epoch_micros_to_unix_micros("timestamptz", pg_epoch_micros)
}

pub fn i64_to_timestamptz(i64_timestamptz: i64, timezone: &str) -> TimestampWithTimeZone {
    // an infinite timestamptz in the file is one of the limits of the underlying i64
    if i64_timestamptz == i64::MAX {
        return TimestampWithTimeZone::positive_infinity();
    } else if i64_timestamptz == i64::MIN || i64_timestamptz == NEG_INFINITY_TIMESTAMP {
        return TimestampWithTimeZone::negative_infinity();
    }

    // Parquet uses Unix epoch (1970-01-01). Convert it to PG epoch (2000-01-01).
    let pg_epoch_micros = i64_timestamptz
        .checked_sub(MICROS_BETWEEN_UNIX_AND_PG_EPOCH)
        .unwrap_or_else(|| error_out_of_range_temporal("timestamptz", i64_timestamptz));

    let timestamp: Timestamp = pg_epoch_micros
        .try_into()
        .unwrap_or_else(|_| error_out_of_range_temporal("timestamptz", i64_timestamptz));

    (timestamp, timezone)
        .try_into()
        .unwrap_or_else(|_| error_out_of_range_temporal("timestamptz", i64_timestamptz))
}

pub fn time_to_i64(time: Time) -> i64 {
    let time_as_bytes: Vec<u8> = unsafe {
        direct_function_call(pg_sys::time_send, &[time.into_datum()])
            .expect("cannot convert time to bytes")
    };
    i64::from_be_bytes(
        time_as_bytes[0..8]
            .try_into()
            .unwrap_or_else(|e| panic!("{}", e)),
    )
}

pub fn i64_to_time(i64_time: i64) -> Time {
    i64_time
        .try_into()
        .unwrap_or_else(|_| error_out_of_range_temporal("time", i64_time))
}

pub fn timetz_to_i64(timetz: TimeWithTimeZone) -> i64 {
    let timezone_as_secs: AnyNumeric = unsafe {
        direct_function_call(
            pg_sys::extract_timetz,
            &["timezone".into_datum(), timetz.into_datum()],
        )
    }
    .expect("cannot extract timezone from timetz");

    let timezone_as_secs: f64 = timezone_as_secs
        .try_into()
        .unwrap_or_else(|e| panic!("{}", e));

    let timezone_as_interval = Interval::from_seconds(timezone_as_secs);
    let adjusted_timetz: TimeWithTimeZone = unsafe {
        direct_function_call(
            pg_sys::timetz_mi_interval,
            &[timetz.into_datum(), timezone_as_interval.into_datum()],
        )
        .expect("cannot adjust PG timetz to Unix timetz")
    };

    let adjusted_timetz_as_bytes: Vec<u8> = unsafe {
        direct_function_call(pg_sys::timetz_send, &[adjusted_timetz.into_datum()])
            .expect("cannot convert timetz to bytes")
    };
    i64::from_be_bytes(
        adjusted_timetz_as_bytes[0..8]
            .try_into()
            .unwrap_or_else(|e| panic!("{}", e)),
    )
}

pub fn i64_to_timetz(i64_timetz: i64) -> TimeWithTimeZone {
    let utc_tz = 0;
    (i64_timetz, utc_tz)
        .try_into()
        .unwrap_or_else(|_| error_out_of_range_temporal("timetz", i64_timetz))
}

fn error_if_special_numeric(numeric: AnyNumeric) {
    if ["NaN", "Infinity", "-Infinity"]
        .iter()
        .any(|&s| format!("{numeric}") == s)
    {
        ereport!(
        pgrx::PgLogLevel::ERROR,
        pgrx::PgSqlErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
        "Special numeric values like NaN, Inf, -Inf are not allowed for numeric type during copy to a file",
        "Use float types instead.",
    );
    }
}

/// Decodes a numeric datum into the `i128` an Arrow decimal column holds, reading Postgres'
/// base-10000 digit array in place.
///
/// `None` means the value needs [`numeric_to_i128`] instead: `NaN` and `±Inf`, the long header
/// format, a scale that would lose digits, or an `i128` overflow.
pub fn numeric_datum_to_i128(datum: pg_sys::Datum, scale: u32) -> Option<i128> {
    const NUMERIC_SIGN_MASK: u16 = 0xC000;
    const NUMERIC_SPECIAL: u16 = 0xC000;
    const NUMERIC_SHORT: u16 = 0x8000;
    const NUMERIC_SHORT_SIGN_MASK: u16 = 0x2000;
    const NUMERIC_SHORT_WEIGHT_SIGN_MASK: u16 = 0x0040;
    const NUMERIC_SHORT_WEIGHT_MASK: u16 = 0x003F;

    unsafe {
        let varlena = pg_sys::pg_detoast_datum_packed(datum.cast_mut_ptr());

        let data = pgrx::varlena::vardata_any(varlena).cast::<u8>();
        let len = pgrx::varlena::varsize_any_exhdr(varlena);

        if len < 2 {
            return None;
        }

        let header = (data as *const u16).read_unaligned();

        if header & NUMERIC_SIGN_MASK == NUMERIC_SPECIAL {
            return None;
        }

        // the long header format holds a weight or a display scale that the short one cannot,
        // which takes a value wider than the 38 digits of a decimal128 column, so it is left
        // to the general path rather than decoded here
        if header & NUMERIC_SHORT == 0 {
            return None;
        }

        let is_negative = header & NUMERIC_SHORT_SIGN_MASK != 0;

        let mut weight = (header & NUMERIC_SHORT_WEIGHT_MASK) as i32;

        if header & NUMERIC_SHORT_WEIGHT_SIGN_MASK != 0 {
            weight |= !(NUMERIC_SHORT_WEIGHT_MASK as i32);
        }

        let digits_offset = 2;

        let ndigits = (len - digits_offset) / 2;

        let mut acc: i128 = 0;

        for i in 0..ndigits {
            let digit = (data.add(digits_offset + i * 2) as *const i16).read_unaligned();
            acc = acc.checked_mul(10_000)?.checked_add(digit as i128)?;
        }

        // acc holds the digits shifted left by (ndigits - 1 - weight) base-10000 places,
        // so the decimal exponent still owed to reach 10^scale is:
        let exponent = scale as i32 - 4 * (ndigits as i32 - 1 - weight);

        let scaled = if exponent >= 0 {
            acc.checked_mul(10_i128.checked_pow(exponent as u32)?)?
        } else {
            let divisor = 10_i128.checked_pow((-exponent) as u32)?;

            // the general path would round here, so leave it to numeric_out
            if acc % divisor != 0 {
                return None;
            }

            acc / divisor
        };

        Some(if is_negative { -scaled } else { scaled })
    }
}

pub fn numeric_to_i128(numeric: AnyNumeric, typmod: i32, col_name: &str) -> i128 {
    error_if_special_numeric(numeric.clone());

    let numeric_str = if is_unbounded_numeric_typmod(typmod) {
        let rescaled_unbounded_numeric = rescale_unbounded_numeric_or_error(numeric, col_name);

        format!("{rescaled_unbounded_numeric}")
    } else {
        // format returns a string representation of the numeric value based on numeric_out
        format!("{numeric}")
    };

    let normalized_numeric_str = numeric_str.replace('.', "");

    normalized_numeric_str
        .parse::<i128>()
        .expect("invalid decimal")
}

pub fn i128_to_numeric(decimal: i128, precision: u32, scale: u32, typmod: i32) -> AnyNumeric {
    // format decimal via arrow since it is consistent with PG's numeric formatting
    let numeric_str = Decimal128Type::format_decimal(decimal, precision as _, scale as _);

    // compute numeric from string representation
    let numeric: AnyNumeric = unsafe {
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
    };

    numeric
}

// unbounded_numeric_value_digits returns the number of integral and decimal digits in an unbounded numeric value.
fn unbounded_numeric_value_digits(numeric_str: &str) -> (usize, usize) {
    let numeric_str = numeric_str.replace(['-', '+'], "");

    let has_decimal_point = numeric_str.contains('.');

    if has_decimal_point {
        let parts = numeric_str.split('.').collect::<Vec<_>>();
        (parts[0].len(), parts[1].len())
    } else {
        (numeric_str.as_str().len(), 0)
    }
}

fn rescale_unbounded_numeric_or_error(
    unbounded_numeric: AnyNumeric,
    col_name: &str,
) -> Numeric<DEFAULT_UNBOUNDED_NUMERIC_PRECISION, DEFAULT_UNBOUNDED_NUMERIC_SCALE> {
    let unbounded_numeric_str = format!("{unbounded_numeric}");

    let (n_integral_digits, n_scale_digits) =
        unbounded_numeric_value_digits(&unbounded_numeric_str);

    // we need to do error checks before rescaling since rescaling to a lower scale
    // silently truncates the value
    if n_integral_digits > DEFAULT_UNBOUNDED_NUMERIC_MAX_INTEGRAL_DIGITS as _ {
        ereport!(
            pgrx::PgLogLevel::ERROR,
            pgrx::PgSqlErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
            format!(
                "numeric value contains {} digits before decimal point, which exceeds max allowed integral digits {} during copy to a file",
                n_integral_digits, DEFAULT_UNBOUNDED_NUMERIC_MAX_INTEGRAL_DIGITS
            ),
            format!(
                "Consider specifying precision and scale for column \"{}\". Replace type \"numeric\" to \"numeric(P,S)\".",
                col_name
            ),
        );
    } else if n_scale_digits > DEFAULT_UNBOUNDED_NUMERIC_SCALE as _ {
        ereport!(
            pgrx::PgLogLevel::ERROR,
            pgrx::PgSqlErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
            format!(
                "numeric value contains {} digits after decimal point, which exceeds max allowed decimal digits {} during copy to a file",
                n_scale_digits, DEFAULT_UNBOUNDED_NUMERIC_SCALE
            ),
            format!(
                "Consider specifying precision and scale for column \"{}\". Replace type \"numeric\" to \"numeric(P,S)\".",
                col_name
            ),
        );
    }

    unbounded_numeric
        .rescale::<DEFAULT_UNBOUNDED_NUMERIC_PRECISION, DEFAULT_UNBOUNDED_NUMERIC_SCALE>()
        .unwrap_or_else(|e| panic!("{}", e))
}

pub const MAX_NUMERIC_PRECISION: u32 = 38;
pub const DEFAULT_UNBOUNDED_NUMERIC_PRECISION: u32 = MAX_NUMERIC_PRECISION;
pub const DEFAULT_UNBOUNDED_NUMERIC_SCALE: u32 = 9;
pub const DEFAULT_UNBOUNDED_NUMERIC_MAX_INTEGRAL_DIGITS: u32 =
    DEFAULT_UNBOUNDED_NUMERIC_PRECISION - DEFAULT_UNBOUNDED_NUMERIC_SCALE;

// should_write_numeric_as_text determines whether a numeric datum should be written as text.
// It is written as text when precision is greater than MAX_NUMERIC_PRECISION e.g. "numeric(50, 10)"
pub fn should_write_numeric_as_text(precision: u32) -> bool {
    precision > MAX_NUMERIC_PRECISION
}

// extract_precision_and_scale_from_numeric_typmod extracts precision and scale from numeric typmod
// with the following rules:
// - If typmod is -1, it means unbounded numeric, so we use default precision and scale.
// - Even if PG allows negative scale, arrow does not. We adjust precision by adding abs(scale) to it,
//   and set scale to 0.
//
// It always returns non-negative precision and scale due to the above rule.
pub fn extract_precision_and_scale_from_numeric_typmod(typmod: i32) -> (u32, u32) {
    // if typmod is -1, it means unbounded numeric, so we use default precision and scale
    if is_unbounded_numeric_typmod(typmod) {
        return (
            DEFAULT_UNBOUNDED_NUMERIC_PRECISION,
            DEFAULT_UNBOUNDED_NUMERIC_SCALE,
        );
    }

    let mut precision = extract_precision_from_numeric_typmod(typmod);
    let mut scale = extract_scale_from_numeric_typmod(typmod);

    // Even if PG allows negative scale, arrow does not.
    // We adjust precision by adding scale to it and set scale to 0.
    // e.g. "123.34::numeric(3,-2)" becomes "100::numeric(5,0)"
    if scale < 0 {
        precision += scale.abs();
        scale = 0;
    }

    // Even if PG allows scale to be greater than precision, arrow does not.
    // We set precision to the same value as scale.
    // e.g. "0.0023::numeric(2,4)" becomes "0.0023::numeric(4,4)"
    if scale > precision {
        precision = scale;
    }

    debug_assert!(precision >= 0);
    debug_assert!(scale >= 0);

    (precision as _, scale as _)
}

#[inline]
fn extract_precision_from_numeric_typmod(typmod: i32) -> i32 {
    // taken from PG's numeric.c
    (((typmod - pg_sys::VARHDRSZ as i32) >> 16) & 0xffff) as _
}

#[inline]
fn extract_scale_from_numeric_typmod(typmod: i32) -> i32 {
    // taken from PG's numeric.c
    (((typmod - pg_sys::VARHDRSZ as i32) & 0x7ff) ^ 1024) - 1024
}

#[inline]
pub fn make_numeric_typmod(precision: i32, scale: i32) -> i32 {
    ((precision << 16) | (scale & 0x7ff)) + pg_sys::VARHDRSZ as i32
}

pub fn is_unbounded_numeric_typmod(typmod: i32) -> bool {
    typmod == -1
}
