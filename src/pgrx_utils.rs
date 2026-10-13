use std::{collections::HashSet, ffi::CStr};

use pgrx::{
    pg_sys::{
        format_type_extended, format_type_with_typemod, getBaseType, getBaseTypeAndTypmod,
        get_array_type, get_element_type, get_extension_oid, get_rel_relkind, get_typ_typrelid,
        lookup_rowtype_tupdesc, makeString, makeTypeNameFromNameList, truncate_identifier,
        type_is_array, type_is_rowtype, typenameTypeIdAndMod, AsPgCStr, FormData_pg_attribute,
        InvalidOid, LookupTypeNameOid, Oid, FORMAT_TYPE_FORCE_QUALIFY, FORMAT_TYPE_TYPEMOD_GIVEN,
        RELKIND_COMPOSITE_TYPE,
    },
    PgList, PgTupleDesc,
};

#[derive(Debug, Clone, Copy)]
pub(crate) enum CollectAttributesFor {
    CopyFrom,
    CopyTo,
    Other,
}

// collect_attributes_for collects not-dropped attributes from the tuple descriptor.
// If copy_operation is CopyTo, it also collects generated columns. Otherwise, it does not.
pub(crate) fn collect_attributes_for(
    copy_operation: CollectAttributesFor,
    tupdesc: &PgTupleDesc,
) -> Vec<FormData_pg_attribute> {
    let include_generated_columns = match copy_operation {
        CollectAttributesFor::CopyFrom => false,
        CollectAttributesFor::CopyTo | CollectAttributesFor::Other => true,
    };

    let mut attributes = vec![];
    let mut attributes_set = HashSet::<&str>::new();

    for i in 0..tupdesc.len() {
        let attribute = tupdesc.get(i).expect("failed to get attribute");
        if attribute.is_dropped() {
            continue;
        }

        if !include_generated_columns && is_generated_attribute(attribute) {
            continue;
        }

        let name = attribute.name();

        if attributes_set.contains(name) {
            panic!("duplicate attribute \"{name}\" is not allowed in parquet schema");
        }
        attributes_set.insert(name);

        attributes.push(attribute.to_owned());
    }

    attributes
}

pub(crate) fn is_generated_attribute(attribute: &FormData_pg_attribute) -> bool {
    attribute.attgenerated != 0
}

pub(crate) fn tuple_desc(typoid: Oid, typmod: i32) -> PgTupleDesc<'static> {
    let tupledesc = unsafe { PgTupleDesc::from_pg(lookup_rowtype_tupdesc(typoid, typmod)) };

    // return an owned copy of the tupledesc (needs pfree but not release) That prevents a bunch of
    // errors during cleanup.
    tupledesc.clone()
}

pub(crate) fn is_composite_type(typoid: Oid) -> bool {
    unsafe { type_is_rowtype(typoid) }
}

// is_standalone_composite_type returns true for a composite type that was created by
// CREATE TYPE. Every relation has a composite type as well, but that one belongs to the
// relation: its owner changes it with ALTER TABLE and drops it with the relation.
pub(crate) fn is_standalone_composite_type(typoid: Oid) -> bool {
    if !is_composite_type(typoid) {
        return false;
    }

    let relid = unsafe { get_typ_typrelid(typoid) };

    relid != InvalidOid
        && unsafe { get_rel_relkind(relid) } == RELKIND_COMPOSITE_TYPE as std::ffi::c_char
}

pub(crate) fn is_array_type(typoid: Oid) -> bool {
    unsafe { type_is_array(typoid) }
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

pub(crate) fn array_typoid(element_typoid: Oid) -> Oid {
    unsafe { get_array_type(element_typoid) }
}

pub(crate) fn domain_array_base_elem_type(domain_typoid: Oid) -> (Oid, i32) {
    debug_assert!(is_domain_of_array_type(domain_typoid));

    let mut base_array_typmod = -1;
    let base_array_typoid = unsafe { getBaseTypeAndTypmod(domain_typoid, &mut base_array_typmod) };
    debug_assert!(is_array_type(base_array_typoid));

    (array_element_typoid(base_array_typoid), base_array_typmod)
}

pub(crate) fn extension_exists(extension_name: &str) -> bool {
    let extension_name = extension_name.as_pg_cstr();
    let extension_oid = unsafe { get_extension_oid(extension_name, true) };
    extension_oid != InvalidOid
}

// get_type_name_with_typemod returns the formatted type name, including its
// type modifier e.g. "numeric(10,2)".
pub(crate) fn get_type_name_with_typemod(typoid: Oid, typmod: i32) -> String {
    let typename = unsafe { format_type_with_typemod(typoid, typmod) };
    unsafe {
        CStr::from_ptr(typename)
            .to_str()
            .expect("invalid CString for type name")
            .to_string()
    }
}

// get_qualified_type_name_with_typemod is get_type_name_with_typemod with the schema name
// always included e.g. "pg_catalog.numeric(10,2)". The unqualified form omits the schema of
// a type that the search_path makes visible, so only this one is the same in every session.
pub(crate) fn get_qualified_type_name_with_typemod(typoid: Oid, typmod: i32) -> String {
    let flags = FORMAT_TYPE_TYPEMOD_GIVEN | FORMAT_TYPE_FORCE_QUALIFY;

    let typename = unsafe { format_type_extended(typoid, typmod, flags as _) };

    unsafe {
        CStr::from_ptr(typename)
            .to_str()
            .expect("invalid CString for type name")
            .to_string()
    }
}

pub(crate) fn type_info_from_name(schema_name: &str, type_name: &str) -> (Oid, i32) {
    let mut typoid = InvalidOid;
    let mut typmod = -1;

    let mut name_list = PgList::new();
    name_list.push(unsafe { makeString(schema_name.as_pg_cstr()) });
    name_list.push(unsafe { makeString(type_name.as_pg_cstr()) });

    let typename = unsafe { makeTypeNameFromNameList(name_list.into_pg()) };

    let missing_ok = true;

    if unsafe { LookupTypeNameOid(std::ptr::null_mut(), typename, missing_ok) } != InvalidOid {
        unsafe { typenameTypeIdAndMod(std::ptr::null_mut(), typename, &mut typoid, &mut typmod) };
    }

    (typoid, typmod)
}

// truncate_identifier_like_postgres truncates a name to the identifier length limit the same way
// the Postgres parser does, i.e. at a character boundary. A plain byte truncation could cut a
// multi-byte character in half. "warn" makes it report the truncation, as the parser does.
pub(crate) fn truncate_identifier_like_postgres(name: &str, warn: bool) -> String {
    let ident = name.as_pg_cstr();

    unsafe { truncate_identifier(ident, name.len() as _, warn) };

    unsafe { CStr::from_ptr(ident) }
        .to_str()
        .expect("truncated identifier is not valid UTF-8")
        .to_string()
}
