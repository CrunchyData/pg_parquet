use std::ffi::CStr;

use pgrx::{
    datum::{Date, Interval, Time, TimeWithTimeZone, Timestamp, TimestampWithTimeZone},
    direct_function_call, pg_sys, AnyNumeric, IntoDatum,
};

pub(crate) const MAX_DECIMAL_PRECISION: usize = 38;

pub(crate) fn date_to_i32(date: Date) -> i32 {
    // PG epoch is (2000-01-01). Convert it to Unix epoch (1970-01-01). +10957 days
    let adjusted_date: Date = unsafe {
        direct_function_call(pg_sys::date_pli, &[date.into_datum(), 10957.into_datum()]).unwrap()
    };

    let adjusted_date_as_bytes: Vec<u8> =
        unsafe { direct_function_call(pg_sys::date_send, &[adjusted_date.into_datum()]).unwrap() };
    i32::from_be_bytes(adjusted_date_as_bytes[0..4].try_into().unwrap())
}

pub(crate) fn i32_to_date(i32_date: i32) -> Option<Date> {
    // Duckdb epoch is Unix epoch (1970-01-01). Convert it to PG epoch (2000-01-01). -10957 days
    let adjusted_date = unsafe { Date::from_pg_epoch_days(i32_date - 10957) };
    Some(adjusted_date)
}

pub(crate) fn timestamp_to_i64(timestamp: Timestamp) -> i64 {
    // PG epoch is (2000-01-01). Convert it to Unix epoch (1970-01-01). +10957 days
    let adjustment_interval = Interval::from_days(10957);
    let adjusted_timestamp: Timestamp = unsafe {
        direct_function_call(
            pg_sys::timestamp_pl_interval,
            &[timestamp.into_datum(), adjustment_interval.into_datum()],
        )
        .unwrap()
    };

    let adjusted_timestamp_as_bytes: Vec<u8> = unsafe {
        direct_function_call(pg_sys::time_send, &[adjusted_timestamp.into_datum()]).unwrap()
    };
    i64::from_be_bytes(adjusted_timestamp_as_bytes[0..8].try_into().unwrap())
}

pub(crate) fn i64_to_timestamp(i64_timestamp: i64) -> Option<Timestamp> {
    let timestamp: Timestamp = i64_timestamp.try_into().unwrap();

    // Duckdb epoch is Unix epoch (1970-01-01). Convert it to PG epoch (2000-01-01). -10957 days
    let adjustment_interval = Interval::from_days(10957);
    let adjusted_timestamp: Timestamp = unsafe {
        direct_function_call(
            pg_sys::timestamp_mi_interval,
            &[timestamp.into_datum(), adjustment_interval.into_datum()],
        )
        .unwrap()
    };

    Some(adjusted_timestamp)
}

pub(crate) fn timestamptz_to_i64(timestamptz: TimestampWithTimeZone) -> i64 {
    // PG epoch is (2000-01-01). Convert it to Unix epoch (1970-01-01). +10957 days
    let adjustment_interval = Interval::from_days(10957);
    let adjusted_timestamptz: TimestampWithTimeZone = unsafe {
        direct_function_call(
            pg_sys::timestamptz_pl_interval,
            &[timestamptz.into_datum(), adjustment_interval.into_datum()],
        )
        .unwrap()
    };

    let adjusted_timestamptz_as_bytes: Vec<u8> = unsafe {
        direct_function_call(
            pg_sys::timestamptz_send,
            &[adjusted_timestamptz.into_datum()],
        )
        .unwrap()
    };
    i64::from_be_bytes(adjusted_timestamptz_as_bytes[0..8].try_into().unwrap())
}

pub(crate) fn i64_to_timestamptz(i64_timestamptz: i64) -> Option<TimestampWithTimeZone> {
    let timestamptz: TimestampWithTimeZone = i64_timestamptz.try_into().unwrap();

    // Duckdb epoch is Unix epoch (1970-01-01). Convert it to PG epoch (2000-01-01). -10957 days
    let adjustment_interval = Interval::from_days(10957);
    let adjusted_timestamptz: TimestampWithTimeZone = unsafe {
        direct_function_call(
            pg_sys::timestamptz_mi_interval,
            &[timestamptz.into_datum(), adjustment_interval.into_datum()],
        )
        .unwrap()
    };

    Some(adjusted_timestamptz)
}

pub(crate) fn time_to_i64(time: Time) -> i64 {
    let time_as_bytes: Vec<u8> =
        unsafe { direct_function_call(pg_sys::time_send, &[time.into_datum()]).unwrap() };
    i64::from_be_bytes(time_as_bytes[0..8].try_into().unwrap())
}

pub(crate) fn i64_to_time(i64_time: i64) -> Option<Time> {
    Some(i64_time.try_into().unwrap())
}

pub(crate) fn timetz_to_i64(timetz: TimeWithTimeZone) -> i64 {
    let timezone_as_secs: AnyNumeric = unsafe {
        direct_function_call(
            pg_sys::extract_timetz,
            &["timezone".into_datum(), timetz.into_datum()],
        )
    }
    .unwrap();

    let timezone_as_secs: f64 = timezone_as_secs.try_into().unwrap();
    let timezone_as_interval = Interval::from_seconds(timezone_as_secs);
    let adjusted_timetz: TimeWithTimeZone = unsafe {
        direct_function_call(
            pg_sys::timetz_mi_interval,
            &[timetz.into_datum(), timezone_as_interval.into_datum()],
        )
        .unwrap()
    };

    let adjusted_timetz_as_bytes: Vec<u8> = unsafe {
        direct_function_call(pg_sys::timetz_send, &[adjusted_timetz.into_datum()]).unwrap()
    };
    i64::from_be_bytes(adjusted_timetz_as_bytes[0..8].try_into().unwrap())
}

pub(crate) fn i64_to_timetz(i64_timetz: i64) -> Option<TimeWithTimeZone> {
    i64_to_time(i64_timetz).map(|time| time.into())
}

pub(crate) fn numeric_to_i128(numeric: AnyNumeric) -> i128 {
    // obtain numeric's string representation
    let numeric_str: &CStr =
        unsafe { direct_function_call(pg_sys::numeric_out, &[numeric.into_datum()]).unwrap() };
    let numeric_str = numeric_str.to_str().unwrap();

    let sign = if numeric_str.starts_with('-') { -1 } else { 1 };

    let numeric_str = numeric_str.replace(['-', '+', '.'], "");

    let numeric_digits = numeric_str.chars().map(|c| c.to_digit(10).unwrap() as i8);

    // convert digits into arrow decimal
    let mut decimal: i128 = 0;
    for digit in numeric_digits.into_iter() {
        decimal = decimal * 10 + digit as i128;
    }
    decimal *= sign;

    decimal
}

pub(crate) fn i128_to_numeric(i128_decimal: i128, scale: usize) -> Option<AnyNumeric> {
    let sign = if i128_decimal < 0 { "-" } else { "" };
    let i128_decimal = i128_decimal.abs();

    // calculate decimal digits
    let mut decimal_digits = vec![];
    let mut decimal = i128_decimal;
    while decimal > 0 {
        let digit = (decimal % 10) as i8;
        decimal_digits.push(digit);
        decimal /= 10;
    }

    // get fraction as string
    let fraction = decimal_digits
        .iter()
        .take(scale)
        .map(|v| v.to_string())
        .rev()
        .reduce(|acc, v| acc + &v)
        .unwrap_or_default();

    // get integral as string
    let integral = decimal_digits
        .iter()
        .skip(scale)
        .map(|v| v.to_string())
        .rev()
        .reduce(|acc, v| acc + &v)
        .unwrap_or_default();

    // create numeric string representation
    let numeric_str = if integral.is_empty() && fraction.is_empty() {
        "0".into()
    } else {
        format!("{}{}.{}", sign, integral, fraction)
    };
    let numeric_str = std::ffi::CString::new(numeric_str).unwrap();

    // compute numeric from string representation
    let numeric: AnyNumeric = unsafe {
        direct_function_call(
            pg_sys::numeric_in,
            &[numeric_str.into_datum(), 0.into_datum(), 0.into_datum()],
        )
        .unwrap()
    };

    Some(numeric)
}

// taken from PG's numeric.c
#[inline]
pub(crate) fn extract_precision_from_numeric_typmod(typmod: i32) -> usize {
    (((typmod - pg_sys::VARHDRSZ as i32) >> 16) & 0xffff) as usize
}

// taken from PG's numeric.c
#[inline]
pub(crate) fn extract_scale_from_numeric_typmod(typmod: i32) -> usize {
    ((((typmod - pg_sys::VARHDRSZ as i32) & 0x7ff) ^ 1024) - 1024) as usize
}
