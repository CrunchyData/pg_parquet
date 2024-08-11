use std::ffi::CStr;

use arrow::datatypes::IntervalMonthDayNano;
use pgrx::{
    direct_function_call,
    pg_sys::{
        self, InvalidOid, TimeTzADT, BITOID, BPCHAROID, NAMEOID, TIME_UTC, VARBITOID, VARCHAROID,
    },
    AnyNumeric, Date, FromDatum, Interval, IntoDatum, Time, TimeWithTimeZone, Timestamp,
    TimestampWithTimeZone,
};

use crate::pgrx_utils::lookup_type_name;

pub(crate) const DECIMAL_PRECISION: u8 = 38;
pub(crate) const DECIMAL_SCALE: i8 = 8;

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

pub(crate) fn i32_to_date(i32_date: i32) -> Option<Date> {
    // Duckdb epoch is Unix epoch (1970-01-01). Convert it to PG epoch (2000-01-01). -10957 days
    let adjusted_date = unsafe { Date::from_pg_epoch_days(i32_date - 10957) };
    Some(adjusted_date)
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

pub(crate) fn i64_to_timestamp(i64_timestamp: i64) -> Option<Timestamp> {
    let timestamp: Timestamp = i64_timestamp.into();

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

pub(crate) fn i64_to_time(i64_time: i64) -> Option<Time> {
    let time: Time = i64_time.into();
    Some(time)
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

pub(crate) fn i64_to_timetz(i64_timetz: i64) -> Option<TimeWithTimeZone> {
    let timetz = TimeTzADT {
        time: i64_timetz,
        zone: TIME_UTC as _,
    };
    let timetz: TimeWithTimeZone = timetz.into();

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

    Some(adjusted_timetz)
}

pub(crate) fn interval_to_nano(interval: Interval) -> Option<IntervalMonthDayNano> {
    let months = interval.months();
    let days = interval.days();
    let microseconds = interval.micros();
    Some(IntervalMonthDayNano::new(months, days, microseconds))
}

pub(crate) fn nano_to_interval(nano: IntervalMonthDayNano) -> Option<Interval> {
    let months = nano.months;
    let days = nano.days;
    let microseconds = nano.nanoseconds;
    Some(Interval::new(months, days, microseconds).unwrap())
}

pub(crate) fn numeric_to_i128(numeric: AnyNumeric) -> Option<i128> {
    let numeric_str: &CStr =
        unsafe { direct_function_call(pg_sys::numeric_out, &[numeric.into_datum()]).unwrap() };
    let numeric_str = numeric_str.to_str().unwrap();

    let sign = if numeric_str.starts_with('-') { -1 } else { 1 };
    let numeric_str = numeric_str.trim_start_matches('-');

    let integral = numeric_str.split('.').nth(0).unwrap();
    let mut integral = i128::from_str_radix(integral, 10).unwrap();
    let fraction = if let Some(fraction) = numeric_str.split('.').nth(1) {
        fraction
    } else {
        "0"
    };
    let fraction_len = fraction.len();
    let mut fraction = i128::from_str_radix(fraction, 10).unwrap();
    let zeros_needed = if fraction == 0 {
        DECIMAL_SCALE as usize
    } else {
        DECIMAL_SCALE as usize - fraction_len
    };

    let mut integral_digits = vec![];
    while integral > 0 {
        let digit = integral % 10;
        integral_digits.push(digit);
        integral /= 10;
    }

    let mut fraction_digits = vec![];
    if zeros_needed > 0 {
        for _ in 0..zeros_needed {
            fraction_digits.push(0);
        }
    }
    while fraction > 0 {
        let digit = fraction % 10;
        fraction_digits.push(digit);
        fraction /= 10;
    }

    let digits_ordered_and_merged = integral_digits
        .into_iter()
        .rev()
        .chain(fraction_digits.into_iter().rev())
        .collect::<Vec<_>>();

    let mut decimal: i128 = 0;
    for (i, digit) in digits_ordered_and_merged.iter().rev().enumerate() {
        decimal += digit * 10_i128.pow(i as u32);
    }
    decimal *= sign;

    Some(decimal)
}

pub(crate) fn i128_to_numeric(i128_decimal: i128) -> Option<AnyNumeric> {
    let sign = if i128_decimal < 0 { "-" } else { "" };
    let i128_decimal = i128_decimal.abs();

    let mut decimal_digits = vec![];
    let mut decimal = i128_decimal;
    while decimal > 0 {
        let digit = decimal % 10;
        decimal_digits.push(digit);
        decimal /= 10;
    }

    let mut integral = vec![];
    let mut fraction = vec![];
    let mut is_integral = false;
    for digit in decimal_digits.into_iter().rev() {
        if is_integral {
            integral.push(digit);
        } else {
            fraction.push(digit);
        }
        if fraction.len() == DECIMAL_SCALE as usize {
            is_integral = true;
        }
    }

    let integral = integral
        .into_iter()
        .rev()
        .map(|d| d.to_string())
        .collect::<String>();
    let fraction = fraction
        .into_iter()
        .map(|d| d.to_string())
        .collect::<String>();

    let numeric_str = format!("{}{}.{}", sign, integral, fraction);
    let numeric_str = std::ffi::CString::new(numeric_str).unwrap();

    let numeric: AnyNumeric = unsafe {
        direct_function_call(
            pg_sys::numeric_in,
            &[numeric_str.into_datum(), 0.into_datum(), 0.into_datum()],
        )
        .unwrap()
    };

    Some(numeric)
}

#[derive(Debug, PartialEq)]
pub(crate) struct Varchar(pub(crate) String);

impl IntoDatum for Varchar {
    fn into_datum(self) -> Option<pg_sys::Datum> {
        self.0.into_datum()
    }

    fn type_oid() -> pg_sys::Oid {
        VARCHAROID
    }
}

impl FromDatum for Varchar {
    unsafe fn from_polymorphic_datum(
        datum: pg_sys::Datum,
        is_null: bool,
        typoid: pg_sys::Oid,
    ) -> Option<Self>
    where
        Self: Sized,
    {
        let val = String::from_polymorphic_datum(datum, is_null, typoid);
        val.and_then(|val| Some(Self(val)))
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct Bpchar(pub(crate) String);

impl IntoDatum for Bpchar {
    fn into_datum(self) -> Option<pg_sys::Datum> {
        self.0.into_datum()
    }

    fn type_oid() -> pg_sys::Oid {
        BPCHAROID
    }
}

impl FromDatum for Bpchar {
    unsafe fn from_polymorphic_datum(
        datum: pg_sys::Datum,
        is_null: bool,
        typoid: pg_sys::Oid,
    ) -> Option<Self>
    where
        Self: Sized,
    {
        let val = String::from_polymorphic_datum(datum, is_null, typoid);
        val.and_then(|val| Some(Self(val)))
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct Bit(pub(crate) String);

impl IntoDatum for Bit {
    fn into_datum(self) -> Option<pg_sys::Datum> {
        let bits_len = self.0.len();
        let bytes_len = (bits_len + 7) / 8;

        let mut bit_bytes = Vec::with_capacity(4 + bytes_len);

        bit_bytes.extend_from_slice(&(bits_len as i32).to_le_bytes());

        let mut byte = 0;
        let mut bit = 0;
        for c in self.0.chars() {
            if c == '1' {
                byte |= 1 << (7 - bit);
            }
            bit += 1;

            if bit == 8 {
                bit_bytes.push(byte);
                byte = 0;
                bit = 0;
            }
        }

        // handle last byte
        if bit > 0 {
            bit_bytes.push(byte);
        }

        let varlena_len: usize = pg_sys::VARHDRSZ + 4 + bytes_len;

        unsafe {
            let varlena = pg_sys::palloc(varlena_len) as *mut pg_sys::varlena;

            let varattrib_4b = varlena
                .cast::<pg_sys::varattrib_4b>()
                .as_mut()
                .unwrap_unchecked()
                .va_4byte
                .as_mut();

            varattrib_4b.va_header = <usize as TryInto<u32>>::try_into(varlena_len)
                .expect("Rust string too large for a Postgres varlena datum")
                << 2u32;

            std::ptr::copy_nonoverlapping(
                bit_bytes.as_ptr().cast(),
                varattrib_4b.va_data.as_mut_ptr(),
                bit_bytes.len(),
            );

            Some(pgrx::pg_sys::Datum::from(varlena))
        }
    }

    fn type_oid() -> pg_sys::Oid {
        BITOID
    }
}

impl FromDatum for Bit {
    unsafe fn from_polymorphic_datum(
        datum: pg_sys::Datum,
        is_null: bool,
        _typoid: pg_sys::Oid,
    ) -> Option<Self>
    where
        Self: Sized,
    {
        if is_null || datum.is_null() {
            None
        } else {
            let varlena = pg_sys::pg_detoast_datum_packed(datum.cast_mut_ptr());

            let varlena_len = pgrx::varsize_any_exhdr(varlena);
            let varlena_data = pgrx::vardata_any(varlena);
            let varlena_data = std::slice::from_raw_parts(varlena_data.cast::<u8>(), varlena_len);

            let bits_len = i32::from_le_bytes(varlena_data[0..4].try_into().unwrap()) as usize;
            let bytes_len = (bits_len + 7) / 8;

            let mut bit_string = String::new();

            for i in 0..bytes_len - 1 {
                let byte = varlena_data[4 + i];
                for j in 0..8 {
                    let bit = (byte >> (7 - j)) & 1;
                    bit_string.push_str(&bit.to_string());
                }
            }

            // handle last byte separately
            let last_byte = varlena_data[4 + bytes_len - 1];
            let last_byte_bits = bits_len % 8;
            let last_byte_bits = if last_byte_bits == 0 {
                8
            } else {
                last_byte_bits
            };
            for j in 0..last_byte_bits {
                let bit = (last_byte >> (7 - j)) & 1;
                bit_string.push_str(&bit.to_string());
            }

            Some(Self(bit_string))
        }
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct VarBit(pub(crate) String);

impl IntoDatum for VarBit {
    fn into_datum(self) -> Option<pg_sys::Datum> {
        let bits_len = self.0.len();
        let bytes_len = (bits_len + 7) / 8;

        let mut varbit_bytes = Vec::with_capacity(4 + bytes_len);

        varbit_bytes.extend_from_slice(&(bits_len as i32).to_le_bytes());

        let mut byte = 0;
        let mut bit = 0;
        for c in self.0.chars() {
            if c == '1' {
                byte |= 1 << (7 - bit);
            }
            bit += 1;

            if bit == 8 {
                varbit_bytes.push(byte);
                byte = 0;
                bit = 0;
            }
        }

        // handle last byte
        if bit > 0 {
            varbit_bytes.push(byte);
        }

        let varlena_len: usize = pg_sys::VARHDRSZ + 4 + bytes_len;

        unsafe {
            let varlena = pg_sys::palloc(varlena_len) as *mut pg_sys::varlena;

            let varattrib_4b = varlena
                .cast::<pg_sys::varattrib_4b>()
                .as_mut()
                .unwrap_unchecked()
                .va_4byte
                .as_mut();

            varattrib_4b.va_header = <usize as TryInto<u32>>::try_into(varlena_len)
                .expect("Rust string too large for a Postgres varlena datum")
                << 2u32;

            std::ptr::copy_nonoverlapping(
                varbit_bytes.as_ptr().cast(),
                varattrib_4b.va_data.as_mut_ptr(),
                varbit_bytes.len(),
            );

            Some(pgrx::pg_sys::Datum::from(varlena))
        }
    }

    fn type_oid() -> pg_sys::Oid {
        VARBITOID
    }
}

impl FromDatum for VarBit {
    unsafe fn from_polymorphic_datum(
        datum: pg_sys::Datum,
        is_null: bool,
        _typoid: pg_sys::Oid,
    ) -> Option<Self>
    where
        Self: Sized,
    {
        if is_null || datum.is_null() {
            None
        } else {
            let varlena = pg_sys::pg_detoast_datum_packed(datum.cast_mut_ptr());

            let varlena_len = pgrx::varsize_any_exhdr(varlena);
            let varlena_data = pgrx::vardata_any(varlena);
            let varlena_data = std::slice::from_raw_parts(varlena_data.cast::<u8>(), varlena_len);

            let bits_len = i32::from_le_bytes(varlena_data[0..4].try_into().unwrap()) as usize;
            let bytes_len = (bits_len + 7) / 8;

            let mut varbit_string = String::new();

            for i in 0..bytes_len - 1 {
                let byte = varlena_data[4 + i];
                for j in 0..8 {
                    let bit = (byte >> (7 - j)) & 1;
                    varbit_string.push_str(&bit.to_string());
                }
            }

            // handle last byte separately
            let last_byte = varlena_data[4 + bytes_len - 1];
            let last_byte_bits = bits_len % 8;
            let last_byte_bits = if last_byte_bits == 0 {
                8
            } else {
                last_byte_bits
            };
            for j in 0..last_byte_bits {
                let bit = (last_byte >> (7 - j)) & 1;
                varbit_string.push_str(&bit.to_string());
            }

            Some(Self(varbit_string))
        }
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct Name(pub(crate) String);

impl IntoDatum for Name {
    fn into_datum(self) -> Option<pg_sys::Datum> {
        let val = std::ffi::CString::new(self.0).unwrap();
        let val = val.as_c_str();
        val.into_datum()
    }

    fn type_oid() -> pg_sys::Oid {
        NAMEOID
    }
}

impl FromDatum for Name {
    unsafe fn from_polymorphic_datum(
        datum: pg_sys::Datum,
        is_null: bool,
        typoid: pg_sys::Oid,
    ) -> Option<Self>
    where
        Self: Sized,
    {
        let val: Option<&core::ffi::CStr> =
            FromDatum::from_polymorphic_datum(datum, is_null, typoid);

        val.and_then(|val| {
            let val = val.to_str().unwrap();
            Some(Self(val.to_string()))
        })
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct Enum(String);

// we need to store the enum type oid in a static variable since
// pgrx IntoDatum's associated function type_oid() is a static function
static mut ENUM_TYPE_OID: pg_sys::Oid = InvalidOid;

impl Enum {
    pub(crate) fn new(label: String, enum_oid: pg_sys::Oid) -> Self {
        unsafe { ENUM_TYPE_OID = enum_oid };
        Self(label)
    }

    pub(crate) fn label(&self) -> &str {
        &self.0
    }
}

impl IntoDatum for Enum {
    fn into_datum(self) -> Option<pg_sys::Datum> {
        let label = self.0;
        let enum_name = lookup_type_name(unsafe { ENUM_TYPE_OID }, -1);
        let enum_datum = ::pgrx::enum_helper::lookup_enum_by_label(&enum_name, &label);
        Some(enum_datum)
    }

    fn type_oid() -> pg_sys::Oid {
        unsafe { ENUM_TYPE_OID }
    }
}

impl FromDatum for Enum {
    unsafe fn from_polymorphic_datum(
        datum: pg_sys::Datum,
        is_null: bool,
        _typoid: pg_sys::Oid,
    ) -> Option<Self>
    where
        Self: Sized,
    {
        if is_null {
            None
        } else {
            let label_oid = ::pgrx::pg_sys::Oid::from_datum(datum, is_null).unwrap();
            let (label, _, _) = ::pgrx::enum_helper::lookup_enum_by_oid(label_oid);
            Some(Self(label))
        }
    }
}
