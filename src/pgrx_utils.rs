use std::collections::HashSet;

use pgrx::{
    heap_tuple::PgHeapTuple,
    pg_sys::{self, FormData_pg_attribute, Oid},
    AllocatedByRust, FromDatum, IntoDatum, PgTupleDesc,
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

pub(crate) fn tupledesc_for_typeoid(typoid: Oid) -> Option<PgTupleDesc<'static>> {
    let is_composite_type = unsafe { pg_sys::type_is_rowtype(typoid) };
    if !is_composite_type {
        return None;
    }

    PgTupleDesc::for_composite_type_by_oid(typoid)
}

pub(crate) fn tupledesc_for_tuples(
    tuples: Vec<PgHeapTuple<'_, AllocatedByRust>>,
) -> (Vec<PgHeapTuple<'_, AllocatedByRust>>, PgTupleDesc) {
    unsafe {
        let tuples = tuples
            .into_iter()
            .map(|x| x.into_datum().unwrap())
            .collect::<Vec<_>>();

        let tuple_datum = tuples.first().unwrap();
        let htup_header =
            pg_sys::pg_detoast_datum(tuple_datum.cast_mut_ptr()) as pg_sys::HeapTupleHeader;
        let tup_type = htup_header.as_ref().unwrap().t_choice.t_datum.datum_typeid;
        let tup_typmod = htup_header.as_ref().unwrap().t_choice.t_datum.datum_typmod;
        let tup_desc = pg_sys::lookup_rowtype_tupdesc(tup_type, tup_typmod);

        let tuples = tuples
            .into_iter()
            .map(|x| PgHeapTuple::from_datum(x, false).unwrap())
            .collect::<Vec<_>>();
        (tuples, PgTupleDesc::from_pg(tup_desc))
    }
}
