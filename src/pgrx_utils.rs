use std::collections::HashSet;

use pgrx::{
    pg_sys::{
        getBaseType, get_element_type, lookup_rowtype_tupdesc, type_is_array, type_is_rowtype,
        FormData_pg_attribute, InvalidOid, Oid,
    },
    PgTupleDesc,
};

#[cfg(feature = "pg13")]
use pgrx::pg_sys::FirstNormalObjectId;

// collect_valid_attributes collects not-dropped attributes from the tuple descriptor.
// If include_generated_columns is false, it will skip generated columns.
pub(crate) fn collect_valid_attributes(
    tupdesc: &PgTupleDesc,
    include_generated_columns: bool,
) -> Vec<FormData_pg_attribute> {
    let mut attributes = vec![];
    let mut attributes_set = HashSet::<&str>::new();

    for i in 0..tupdesc.len() {
        let attribute = tupdesc.get(i).expect("failed to get attribute");
        if attribute.is_dropped() {
            continue;
        }

        if !include_generated_columns && attribute.attgenerated != 0 {
            continue;
        }

        let name = attribute.name();

        if attributes_set.contains(name) {
            panic!(
                "duplicate attribute \"{}\" is not allowed in parquet schema",
                name
            );
        }
        attributes_set.insert(name);

        attributes.push(attribute.to_owned());
    }

    attributes
}

pub(crate) fn tuple_desc(typoid: Oid, typmod: i32) -> PgTupleDesc<'static> {
    let tupledesc = unsafe { lookup_rowtype_tupdesc(typoid, typmod) };
    unsafe { PgTupleDesc::from_pg(tupledesc) }
}

pub(crate) fn is_composite_type(typoid: Oid) -> bool {
    unsafe { type_is_rowtype(typoid) }
}

pub(crate) fn is_array_type(typoid: Oid) -> bool {
    unsafe { type_is_array(typoid) }
}

pub(crate) fn is_supported_array_element_type(_array_element_id: Oid) -> bool {
    #[cfg(feature = "pg13")]
    if u32::from(_array_element_id) >= FirstNormalObjectId {
        // we don't support arrays of user-defined composite types in pg 13
        return !is_composite_type(_array_element_id);
    }

    true
}

pub(crate) fn is_supported_composite_type(_composite_id: Oid) -> bool {
    #[cfg(feature = "pg13")]
    return !u32::from(_composite_id) >= FirstNormalObjectId;
    #[cfg(not(feature = "pg13"))]
    true
}

pub(crate) fn is_domain_of_array_type(typoid: Oid) -> bool {
    if is_array_type(typoid) {
        return false;
    }

    let base_typoid = unsafe { getBaseType(typoid) };

    if base_typoid == InvalidOid {
        return false;
    }

    is_array_type(base_typoid)
}

pub(crate) fn array_element_typoid(array_typoid: Oid) -> Oid {
    debug_assert!(is_array_type(array_typoid));
    unsafe { get_element_type(array_typoid) }
}

pub(crate) fn domain_array_base_elem_typoid(domain_typoid: Oid) -> Oid {
    debug_assert!(is_domain_of_array_type(domain_typoid));

    let base_array_typoid = unsafe { getBaseType(domain_typoid) };
    debug_assert!(is_array_type(base_array_typoid));

    array_element_typoid(base_array_typoid)
}
