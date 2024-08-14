use std::collections::HashSet;

use pgrx::{
    pg_sys::{self, lookup_rowtype_tupdesc, FormData_pg_attribute, InvalidOid, Oid},
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

pub(crate) fn is_composite_type(typoid: Oid) -> bool {
    unsafe { pg_sys::type_is_rowtype(typoid) }
}

pub(crate) fn is_array_type(typoid: Oid) -> bool {
    unsafe { pg_sys::type_is_array(typoid) }
}

pub(crate) fn is_domain_of_array_type(typoid: Oid) -> bool {
    if is_array_type(typoid) {
        return false;
    }

    let base_typoid = unsafe { pg_sys::getBaseType(typoid) };

    if base_typoid == InvalidOid {
        return false;
    }

    is_array_type(base_typoid)
}

pub(crate) fn array_element_typoid(array_typoid: Oid) -> Oid {
    debug_assert!(is_array_type(array_typoid));
    unsafe { pg_sys::get_element_type(array_typoid) }
}

pub(crate) fn domain_array_base_elem_typoid(domain_typoid: Oid) -> Oid {
    debug_assert!(is_domain_of_array_type(domain_typoid));

    let base_array_typoid = unsafe { pg_sys::getBaseType(domain_typoid) };
    debug_assert!(is_array_type(base_array_typoid));

    array_element_typoid(base_array_typoid)
}
