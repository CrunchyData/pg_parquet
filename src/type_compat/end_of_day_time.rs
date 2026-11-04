use pgrx::{
    datum::UnboxDatum,
    pg_sys::{Datum, Oid, TimeTzADT, TIMEOID, TIMETZOID},
    FromDatum, IntoDatum, PgMemoryContexts,
};

// the microseconds a day is long, which is also the time of day postgres writes as "24:00:00"
const MICROSECONDS_PER_DAY: i64 = 24 * 60 * 60 * 1_000_000;

// postgres counts a time of day in microseconds and lets it reach the end of the day, so
// "24:00:00" is a day of microseconds and a legal value of time and timetz. pgrx reads those
// types through its own Time and TimeWithTimeZone, which cannot carry the end of the day:
// FromDatum takes the microseconds modulo a day, and TimeWithTimeZone's conversion to the
// microseconds at UTC goes through postgres's own timetz operators, which land on the start of
// the day again. These two read the datum as postgres stores it instead, so a copy writes the
// microseconds the row holds and reads back the value it wrote. Both directions of the copy
// and both the scalar and the array of either type go through these, so there is one way the
// microseconds of a time of day are carried.

// EndOfDayTime is a time of day in microseconds, as the time datum holds it.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) struct EndOfDayTime(pub(crate) i64);

impl FromDatum for EndOfDayTime {
    unsafe fn from_polymorphic_datum(datum: Datum, is_null: bool, _typoid: Oid) -> Option<Self>
    where
        Self: Sized,
    {
        if is_null {
            None
        } else {
            // time is passed by value, so the datum is the microseconds
            Some(Self(datum.value() as i64))
        }
    }
}

impl IntoDatum for EndOfDayTime {
    fn into_datum(self) -> Option<Datum> {
        Some(Datum::from(self.0))
    }

    fn type_oid() -> Oid {
        TIMEOID
    }
}

unsafe impl UnboxDatum for EndOfDayTime {
    type As<'src> = EndOfDayTime;

    unsafe fn unbox<'src>(datum: pgrx::datum::Datum<'src>) -> Self::As<'src>
    where
        Self: 'src,
    {
        Self(datum.sans_lifetime().value() as i64)
    }
}

// EndOfDayTimeTz is a time of day in microseconds at UTC, which is what the parquet logical type
// holds: the zone the row carries is taken off the time here, the way the conversion of a timetz
// array has postgres's own operators do it.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) struct EndOfDayTimeTz(pub(crate) i64);

// microseconds_at_utc reads the time and the zone the timetz datum points at. The zone postgres
// stores is the seconds west of UTC rather than the offset, so it is added rather than taken off.
unsafe fn microseconds_at_utc(datum: Datum) -> i64 {
    // timetz is larger than a datum, so the datum is a pointer to the time and its zone
    let timetz = unsafe { *(datum.cast_mut_ptr::<TimeTzADT>()) };

    let microseconds_at_utc = timetz.time + (timetz.zone as i64) * 1_000_000;

    // a time can fall on the day before or the day after at UTC, and the day a time belongs to
    // is no part of a time of day, so it is read as that time of the same day. A zone is under
    // 16 hours from UTC, so one day in either direction is as far as a time can move. The end of
    // the day is left where it is, which is the whole of why the datum is read here.
    if microseconds_at_utc < 0 {
        microseconds_at_utc + MICROSECONDS_PER_DAY
    } else if microseconds_at_utc > MICROSECONDS_PER_DAY {
        microseconds_at_utc - MICROSECONDS_PER_DAY
    } else {
        microseconds_at_utc
    }
}

impl FromDatum for EndOfDayTimeTz {
    unsafe fn from_polymorphic_datum(datum: Datum, is_null: bool, _typoid: Oid) -> Option<Self>
    where
        Self: Sized,
    {
        if is_null {
            None
        } else {
            Some(Self(unsafe { microseconds_at_utc(datum) }))
        }
    }
}

impl IntoDatum for EndOfDayTimeTz {
    fn into_datum(self) -> Option<Datum> {
        // timetz is larger than a datum, so the value is copied into the memory of the copy
        // and the datum is the pointer to it, the way pgrx's own TimeWithTimeZone does it. The
        // microseconds are already at UTC, which is the zone they are given.
        let mut timetz = TimeTzADT {
            time: self.0,
            zone: 0,
        };

        let timetz = unsafe {
            PgMemoryContexts::CurrentMemoryContext
                .copy_ptr_into(&mut timetz as *mut _, core::mem::size_of::<TimeTzADT>())
        };

        Some(Datum::from(timetz))
    }

    fn type_oid() -> Oid {
        TIMETZOID
    }
}

unsafe impl UnboxDatum for EndOfDayTimeTz {
    type As<'src> = EndOfDayTimeTz;

    unsafe fn unbox<'src>(datum: pgrx::datum::Datum<'src>) -> Self::As<'src>
    where
        Self: 'src,
    {
        Self(unsafe { microseconds_at_utc(datum.sans_lifetime()) })
    }
}
