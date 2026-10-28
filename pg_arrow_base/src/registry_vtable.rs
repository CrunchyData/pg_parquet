//! The side of the registry a provider in another `.so` sees.
//!
//! Publishing half of the mirror image of [`crate::object_store`]: tier 2 consumes tier 1's
//! vtable and publishes its own for tier 3. A provider resolves
//! `pg_arrow_base.registry.v1`, calls `register_provider` with a pointer to its own static
//! descriptor, and is from then on indistinguishable from a statically linked one everywhere
//! above the registry.
//!
//! The rules are the same as tier 1's, for the same reasons: nothing unwinds across the edge,
//! nothing ereports, and returned memory is palloc'd in the caller's context.

use std::ffi::{c_char, c_void, CStr};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use format_registry::abi::{
    CProvider, PgArrowProviderV1, PgArrowRegistryApiV1, PGARROW_ERROR, PGARROW_OK,
    PG_ARROW_REGISTRY_MAGIC, PG_ARROW_REGISTRY_RENDEZVOUS_KEY, PG_ARROW_REGISTRY_VERSION,
};
use format_registry::registry;
use pgrx::pg_sys::{find_rendezvous_variable, palloc};

static PG_ARROW_REGISTRY_API: PgArrowRegistryApiV1 = PgArrowRegistryApiV1 {
    magic: PG_ARROW_REGISTRY_MAGIC,
    version: PG_ARROW_REGISTRY_VERSION,
    register_provider: pgarrow_register_provider,
    unregister_provider: pgarrow_unregister_provider,
    reserved: [std::ptr::null(); 4],
};

/// Publishes the registry. Called from `_PG_init`, which is before any provider can look for
/// it — and a provider that loads first and finds the slot empty is expected to `load_file`
/// the base, which runs this.
pub(crate) fn publish() {
    let key = std::ffi::CString::new(PG_ARROW_REGISTRY_RENDEZVOUS_KEY)
        .expect("the rendezvous key has no interior nul");

    unsafe {
        let slot = find_rendezvous_variable(key.as_ptr());

        // a non-null slot means a second copy of the base is loaded in this backend, from a
        // different $libdir. Leaving the first in place keeps every provider in one registry.
        if (*slot).is_null() {
            *slot = &PG_ARROW_REGISTRY_API as *const PgArrowRegistryApiV1 as *mut c_void;
        }
    }
}

fn palloc_cstring(message: &str) -> *mut c_char {
    let bytes = message.as_bytes();

    unsafe {
        let buffer = palloc(bytes.len() + 1) as *mut u8;
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), buffer, bytes.len());
        *buffer.add(bytes.len()) = 0;
        buffer as *mut c_char
    }
}

unsafe extern "C-unwind" fn pgarrow_register_provider(
    provider: *const PgArrowProviderV1,
    error: *mut *mut c_char,
) -> i32 {
    // a provider registers from its own `_PG_init`, where an `ereport` would leave the library
    // half loaded, so a failure here comes back as a code and a message
    let result = catch_unwind(AssertUnwindSafe(|| {
        let provider = CProvider::new(provider)?;

        registry::register(Arc::new(provider))
    }))
    .unwrap_or_else(|_| Err("panicked while registering a format provider".to_string()));

    match result {
        Ok(()) => PGARROW_OK,
        Err(message) => {
            if !error.is_null() {
                *error = palloc_cstring(&message);
            }

            PGARROW_ERROR
        }
    }
}

unsafe extern "C-unwind" fn pgarrow_unregister_provider(name: *const c_char) -> bool {
    if name.is_null() {
        return false;
    }

    let Ok(name) = CStr::from_ptr(name).to_str() else {
        return false;
    };

    registry::unregister(name)
}
