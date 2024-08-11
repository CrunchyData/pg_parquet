use std::collections::HashSet;

use pgrx::{
    pg_sys::{self, lookup_rowtype_tupdesc, FormData_pg_attribute, Oid},
    PgTupleDesc,
};

pub(crate) fn collect_valid_attributes<'a>(
    tupdesc: &'a PgTupleDesc,
    include_generated_columns: bool,
) -> Vec<&'a FormData_pg_attribute> {
    let mut attributes = vec![];
    let mut attributes_set = HashSet::<&str>::new();

    for i in 0..tupdesc.len() {
        let attribute = tupdesc.get(i).unwrap();
        if attribute.is_dropped() {
            continue;
        }

        if !include_generated_columns && attribute.attgenerated != 0 {
            continue;
        }

        let name = attribute.name();

        if attributes_set.contains(name) {
            panic!(
                "duplicate attribute {} is not allowed in parquet schema",
                name
            );
        }
        attributes_set.insert(name);

        attributes.push(attribute);
    }

    attributes
}

pub(crate) fn tuple_desc(typoid: Oid, typmod: i32) -> PgTupleDesc<'static> {
    let tupledesc = unsafe { lookup_rowtype_tupdesc(typoid, typmod) };
    unsafe { PgTupleDesc::from_pg(tupledesc) }
}

pub(crate) fn lookup_type_name(oid: pg_sys::Oid, typmod: i32) -> String {
    unsafe {
        let cstr_name = pg_sys::format_type_extended(oid, typmod, 0);
        let cstr = std::ffi::CStr::from_ptr(cstr_name);
        let typname = cstr.to_string_lossy().to_string();
        pg_sys::pfree(cstr_name as _); // don't leak the palloc'd cstr_name
        typname
    }
}

pub(crate) fn is_composite_type(typoid: Oid) -> bool {
    unsafe { pg_sys::type_is_rowtype(typoid) }
}

pub(crate) fn is_array_type(typoid: Oid) -> bool {
    unsafe { pg_sys::type_is_array(typoid) }
}

pub(crate) fn array_element_typoid(array_typoid: Oid) -> Oid {
    assert!(is_array_type(array_typoid));
    unsafe { pg_sys::get_element_type(array_typoid) }
}

pub(crate) fn is_enum_typoid(typoid: Oid) -> bool {
    unsafe { pg_sys::type_is_enum(typoid) }
}
