use parquet::data_type::{ByteArray, FixedLenByteArray};
use pgrx::{
    direct_function_call, pg_sys, AnyNumeric, Date, Interval, IntoDatum, Json, Time,
    TimeWithTimeZone, Timestamp, TimestampWithTimeZone, Uuid,
};

pub(crate) fn date_to_i32(date: Date) -> i32 {
    // PG epoch is (2000-01-01). Convert it to Unix epoch (1970-01-01). +10957 days
    let adjusted_date: Date = unsafe {
        direct_function_call(pg_sys::date_pli, &[date.into_datum(), 10957.into_datum()]).unwrap()
    };

    let adjusted_date_as_bytes: Vec<u8> =
        unsafe { direct_function_call(pg_sys::date_send, &[adjusted_date.into_datum()]).unwrap() };
    i32::from_be_bytes(adjusted_date_as_bytes[0..4].try_into().unwrap())
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

pub(crate) fn time_to_i64(time: Time) -> i64 {
    let time_as_bytes: Vec<u8> =
        unsafe { direct_function_call(pg_sys::time_send, &[time.into_datum()]).unwrap() };
    i64::from_be_bytes(time_as_bytes[0..8].try_into().unwrap())
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
            pg_sys::timetz_pl_interval,
            &[timetz.into_datum(), timezone_as_interval.into_datum()],
        )
        .unwrap()
    };

    let adjusted_timetz_as_bytes: Vec<u8> = unsafe {
        direct_function_call(pg_sys::timetz_send, &[adjusted_timetz.into_datum()]).unwrap()
    };
    i64::from_be_bytes(adjusted_timetz_as_bytes[0..8].try_into().unwrap())
}

pub(crate) fn interval_to_fixed_byte_array(interval: Interval) -> FixedLenByteArray {
    let interval_as_bytes: Vec<u8> =
        unsafe { direct_function_call(pg_sys::interval_send, &[interval.into_datum()]).unwrap() };

    let time_in_microsec = i64::from_be_bytes(interval_as_bytes[0..8].try_into().unwrap());
    let day = i32::from_be_bytes(interval_as_bytes[8..12].try_into().unwrap());
    let month = i32::from_be_bytes(interval_as_bytes[12..16].try_into().unwrap());

    // Postgres interval has microsecond resolution, parquet only milliseconds
    // plus postgres doesn't overflow the seconds into the day field
    let ms_per_day = 1000 * 60 * 60 * 24;
    let millis_total = time_in_microsec / 1000;
    let days = millis_total / ms_per_day;
    let millis = millis_total % ms_per_day;
    let mut adjusted_interval_bytes = vec![0u8; 12];
    adjusted_interval_bytes[0..4].copy_from_slice(&i32::to_le_bytes(month));
    adjusted_interval_bytes[4..8].copy_from_slice(&i32::to_le_bytes(day + days as i32));
    adjusted_interval_bytes[8..12].copy_from_slice(&i32::to_le_bytes(millis as i32));

    FixedLenByteArray::from(adjusted_interval_bytes)
}

pub(crate) fn uuid_to_fixed_byte_array(uuid: Uuid) -> FixedLenByteArray {
    let uuid_as_bytes: Vec<u8> =
        unsafe { direct_function_call(pg_sys::uuid_send, &[uuid.into_datum()]).unwrap() };
    FixedLenByteArray::from(uuid_as_bytes)
}

pub(crate) fn json_to_byte_array(json: Json) -> ByteArray {
    let json_as_bytes: Vec<u8> =
        unsafe { direct_function_call(pg_sys::json_send, &[json.into_datum()]).unwrap() };
    ByteArray::from(json_as_bytes)
}
