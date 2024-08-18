use std::borrow::BorrowMut;

use once_cell::sync::OnceCell;
use pgrx::{
    pg_sys::{
        self, fmgr_info, getTypeInputInfo, getTypeOutputInfo, AsPgCStr, FmgrInfo,
        InputFunctionCall, InvalidOid, Oid, OutputFunctionCall,
    },
    FromDatum, IntoDatum, PgBox,
};

// we need to reset the FallbackToTextContext for each type which fallbacks to text
static mut FALLBACK_TO_TEXT_CONTEXT: OnceCell<FallbackToTextContext> = OnceCell::new();

fn get_fallback_to_text_context() -> &'static mut FallbackToTextContext {
    unsafe {
        FALLBACK_TO_TEXT_CONTEXT
            .get_mut()
            .expect("fallback_to_text context is not initialized")
    }
}

pub(crate) fn reset_fallback_to_text_context(typoid: Oid, typmod: i32) {
    unsafe { FALLBACK_TO_TEXT_CONTEXT.take() };

    unsafe {
        FALLBACK_TO_TEXT_CONTEXT
            .set(FallbackToTextContext::new(typoid, typmod))
            .expect("failed to reset fallback_to_text context")
    };
}

#[derive(Debug)]
struct FallbackToTextContext {
    typoid: Oid,
    typmod: i32,
    input_func: FmgrInfo,
    input_ioparam: Oid,
    output_func: FmgrInfo,
}

impl FallbackToTextContext {
    fn new(typoid: Oid, typmod: i32) -> Self {
        let (input_func, input_ioparam) = Self::get_input_function_for_typoid(typoid);

        let output_func = Self::get_output_function_for_typoid(typoid);

        Self {
            typoid,
            typmod,
            input_func,
            input_ioparam,
            output_func,
        }
    }

    fn get_input_function_for_typoid(typoid: Oid) -> (FmgrInfo, Oid) {
        let mut input_func_oid = InvalidOid;
        let mut typio_param = InvalidOid;

        unsafe { getTypeInputInfo(typoid, &mut input_func_oid, &mut typio_param) };

        let mut input_func = unsafe { PgBox::<FmgrInfo>::alloc0().to_owned() };
        unsafe { fmgr_info(input_func_oid, input_func.borrow_mut()) };

        (input_func, typio_param)
    }

    fn get_output_function_for_typoid(typoid: Oid) -> FmgrInfo {
        let mut out_func_oid = InvalidOid;
        let mut is_varlena = false;

        unsafe { getTypeOutputInfo(typoid, &mut out_func_oid, &mut is_varlena) };

        let mut out_func = unsafe { PgBox::<FmgrInfo>::alloc0().to_owned() };
        unsafe { fmgr_info(out_func_oid, out_func.borrow_mut()) };

        out_func
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct FallbackToText(pub(crate) String);

impl From<FallbackToText> for String {
    fn from(fallback: FallbackToText) -> String {
        fallback.0
    }
}

impl IntoDatum for FallbackToText {
    fn into_datum(self) -> Option<pg_sys::Datum> {
        let fallback_to_text_context = get_fallback_to_text_context();

        let datum = unsafe {
            InputFunctionCall(
                fallback_to_text_context.input_func.borrow_mut(),
                self.0.as_pg_cstr(),
                fallback_to_text_context.input_ioparam,
                fallback_to_text_context.typmod,
            )
        };

        Some(datum)
    }

    fn type_oid() -> pg_sys::Oid {
        get_fallback_to_text_context().typoid
    }
}

impl FromDatum for FallbackToText {
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
            let att_cstr = OutputFunctionCall(
                get_fallback_to_text_context().output_func.borrow_mut(),
                datum,
            );
            let att_val = std::ffi::CStr::from_ptr(att_cstr)
                .to_str()
                .unwrap()
                .to_owned();

            Some(Self(att_val))
        }
    }
}
