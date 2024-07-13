use pgrx::{
    direct_function_call, pg_sys, AnyNumeric, Date, Interval, IntoDatum, Time, TimeWithTimeZone,
    Timestamp, TimestampWithTimeZone,
};

pub(crate) fn date_to_i32(date: Date) -> Option<i32> {
    // PG epoch is (2000-01-01). Convert it to Unix epoch (1970-01-01). +10957 days
    let adjusted_date: Date = unsafe {
        direct_function_call(pg_sys::date_pli, &[date.into_datum(), 10957.into_datum()]).unwrap()
    };

    let adjusted_date_as_bytes: Vec<u8> =
        unsafe { direct_function_call(pg_sys::date_send, &[adjusted_date.into_datum()]).unwrap() };
    Some(i32::from_be_bytes(
        adjusted_date_as_bytes[0..4].try_into().unwrap(),
    ))
}

pub(crate) fn timestamp_to_i64(timestamp: Timestamp) -> Option<i64> {
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
    Some(i64::from_be_bytes(
        adjusted_timestamp_as_bytes[0..8].try_into().unwrap(),
    ))
}

pub(crate) fn timestamptz_to_i64(timestamptz: TimestampWithTimeZone) -> Option<i64> {
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
    Some(i64::from_be_bytes(
        adjusted_timestamptz_as_bytes[0..8].try_into().unwrap(),
    ))
}

pub(crate) fn time_to_i64(time: Time) -> Option<i64> {
    let time_as_bytes: Vec<u8> =
        unsafe { direct_function_call(pg_sys::time_send, &[time.into_datum()]).unwrap() };
    Some(i64::from_be_bytes(time_as_bytes[0..8].try_into().unwrap()))
}

pub(crate) fn timetz_to_i64(timetz: TimeWithTimeZone) -> Option<i64> {
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
    Some(i64::from_be_bytes(
        adjusted_timetz_as_bytes[0..8].try_into().unwrap(),
    ))
}