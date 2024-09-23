use pgrx::{
    pg_sys::{self, Alias, Oid, ParseNamespaceItem, ParseState, Relation, TupleTableSlot},
    PgBox,
    prelude::*,
};

#[allow(improper_ctypes)]
#[pg_guard]
extern "C" {
    pub(crate) fn get_extension_oid(name: *const i8, missing_ok: bool) -> Oid;

    pub(crate) fn get_extension_schema(ext_oid: Oid) -> Oid;

    pub(crate) fn addRangeTableEntryForRelation(
        pstate: *mut ParseState,
        rel: Relation,
        lockmode: i32,
        alias: *mut Alias,
        inh: bool,
        inFromCl: bool,
    ) -> *mut ParseNamespaceItem;

    pub(crate) fn addNSItemToQuery(
        pstate: *mut ParseState,
        nsitem: *mut ParseNamespaceItem,
        add_to_join_list: bool,
        add_to_rel_namespace: bool,
        add_to_var_namespace: bool,
    );

    pub(crate) fn assign_expr_collations(
        pstate: *mut ParseState,
        expr: *mut pg_sys::Node,
    ) -> *mut pg_sys::Node;
}

/*
 * slot_getallattrs
 *		This function forces all the entries of the slot's Datum/isnull
 *		arrays to be valid.  The caller may then extract data directly
 *		from those arrays instead of using slot_getattr.
 */
pub(crate) fn slot_getallattrs(slot: *mut TupleTableSlot) {
    // copied from Postgres since this method was inlined in the original code
    // (not found in pg_sys)
    // handles select * from table
    unsafe {
        let slot = PgBox::from_pg(slot);
        let tts_tupledesc = PgBox::from_pg(slot.tts_tupleDescriptor);
        if (slot.tts_nvalid as i32) < tts_tupledesc.natts {
            pg_sys::slot_getsomeattrs_int(slot.as_ptr(), tts_tupledesc.natts);
        }
    };
}
