use std::collections::HashSet;

use pgrx::{
    pg_sys::{lookup_rowtype_tupdesc, FormData_pg_attribute, Oid},
    PgTupleDesc,
};

pub(crate) fn collect_attributes<'a>(tupdesc: &'a PgTupleDesc) -> Vec<&'a FormData_pg_attribute> {
    let mut attributes = vec![];
    let mut attributes_set = HashSet::<&str>::new();

    for i in 0..tupdesc.len() {
        let attribute = tupdesc.get(i).unwrap();
        if attribute.is_dropped() {
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
