use pgrx::{
    pg_sys::{
        self, fmgr_info, getTypeInputInfo, getTypeOutputInfo, AsPgCStr, FmgrInfo,
        InputFunctionCall, InvalidOid, Oid, OutputFunctionCall,
    },
    FromDatum, IntoDatum, PgBox,
};

// we need to set this just before converting FallbackToText to Datum
// and vice versa since IntoDatum's type_oid() is a static method and
// we cannot pass it the typoid of the current FallbackToText instance.
static mut CURRENT_FALLBACK_TYPOID: pg_sys::Oid = pg_sys::InvalidOid;
pub(crate) fn set_fallback_typoid(typoid: pg_sys::Oid) {
    unsafe {
        CURRENT_FALLBACK_TYPOID = typoid;
    }
}

fn get_input_function_for_typoid(typoid: Oid) -> (PgBox<FmgrInfo>, Oid) {
    let mut input_func_oid = InvalidOid;
    let mut typio_param = InvalidOid;

    unsafe { getTypeInputInfo(typoid, &mut input_func_oid, &mut typio_param) };

    let input_func = unsafe { PgBox::<FmgrInfo>::alloc0().into_pg_boxed() };
    unsafe { fmgr_info(input_func_oid, input_func.as_ptr()) };

    (input_func, typio_param)
}

fn get_output_function_for_typoid(typoid: Oid) -> PgBox<FmgrInfo> {
    let mut out_func_oid = InvalidOid;
    let mut is_varlena = false;

    unsafe { getTypeOutputInfo(typoid, &mut out_func_oid, &mut is_varlena) };

    let out_func = unsafe { PgBox::<FmgrInfo>::alloc0().into_pg_boxed() };
    unsafe { fmgr_info(out_func_oid, out_func.as_ptr()) };

    out_func
}

#[derive(Debug, PartialEq)]
pub(crate) struct FallbackToText {
    text_repr: String,
    _typoid: Oid,
    typmod: i32,
}

impl From<FallbackToText> for String {
    fn from(fallback: FallbackToText) -> String {
        fallback.text_repr
    }
}

impl FallbackToText {
    pub(crate) fn new(text_repr: String, _typoid: Oid, typmod: i32) -> Self {
        Self {
            text_repr,
            _typoid,
            typmod,
        }
    }
}

impl IntoDatum for FallbackToText {
    fn into_datum(self) -> Option<pg_sys::Datum> {
        let (input_func, typio_param) = get_input_function_for_typoid(Self::type_oid());

        let datum = unsafe {
            InputFunctionCall(
                input_func.as_ptr(),
                self.text_repr.as_pg_cstr(),
                typio_param,
                self.typmod,
            )
        };

        Some(datum)
    }

    fn type_oid() -> pg_sys::Oid {
        unsafe { CURRENT_FALLBACK_TYPOID }
    }
}

impl FromDatum for FallbackToText {
    unsafe fn from_polymorphic_datum(
        datum: pg_sys::Datum,
        is_null: bool,
        typoid: pg_sys::Oid,
    ) -> Option<Self>
    where
        Self: Sized,
    {
        if is_null {
            None
        } else {
            let output_func = get_output_function_for_typoid(typoid);

            let att_cstr = OutputFunctionCall(output_func.as_ptr(), datum);
            let att_val = std::ffi::CStr::from_ptr(att_cstr)
                .to_str()
                .unwrap()
                .to_owned();

            Some(Self::new(att_val, typoid, -1))
        }
    }
}
