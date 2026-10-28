//! Who is allowed to reach a uri.
//!
//! This is the one check that every consumer of object storage has to make, which is why it is
//! in the vtable rather than left to each of them: an extension that forgot to call it would
//! be a read-any-bucket primitive handed to any user who can run a function.
//!
//! The rule mirrors Postgres' own for local paths. A superuser is unrestricted; everyone else
//! needs the privileges of a role, chosen by what is being reached and in which direction:
//!
//! | target | reading | writing |
//! |---|---|---|
//! | a path on the server | `pg_read_server_files` | `pg_write_server_files` |
//! | a remote uri | `object_store_read` | `object_store_write` |
//!
//! `COPY ... FROM PROGRAM` and `COPY ... TO STDOUT` are deliberately not handled here. They
//! are not object store access at all — they are COPY features that happen to share a code
//! path in `pg_parquet` today — so the check for them stays with whoever implements COPY.

use object_store_core::uri::ParsedUri;
use pgrx::pg_sys::{get_role_oid, has_privs_of_role, superuser, AsPgCStr, GetUserId};
use pgrx::{ereport, PgLogLevel, PgSqlErrorCode};

/// Grants the privileges of this role to let a non-superuser read a remote uri.
pub const OBJECT_STORE_READ_ROLE: &str = "object_store_read";

/// Grants the privileges of this role to let a non-superuser write a remote uri.
pub const OBJECT_STORE_WRITE_ROLE: &str = "object_store_write";

/// The role that governs reaching this uri in this direction.
pub fn required_role_name(uri: &ParsedUri, copy_from: bool) -> &'static str {
    match (uri.is_local(), copy_from) {
        (true, true) => "pg_read_server_files",
        (true, false) => "pg_write_server_files",
        (false, true) => OBJECT_STORE_READ_ROLE,
        (false, false) => OBJECT_STORE_WRITE_ROLE,
    }
}

/// Whether the current user may reach the uri. Prefer [`ensure_access_privilege_to_uri`] in a
/// path that should fail; this exists for a caller that has something better to do than error,
/// such as deciding whether to offer a uri at all.
pub fn has_access_privilege_to_uri(uri: &ParsedUri, copy_from: bool) -> bool {
    if unsafe { superuser() } {
        return true;
    }

    let user_id = unsafe { GetUserId() };

    let role_name = required_role_name(uri, copy_from);

    // missing_ok = false: the roles are created by this extension's bootstrap script, so one
    // being absent means the catalog no longer matches the installed extension
    let role_id = unsafe { get_role_oid(role_name.to_string().as_pg_cstr(), false) };

    unsafe { has_privs_of_role(user_id, role_id) }
}

/// Errors unless the current user may reach the uri.
pub fn ensure_access_privilege_to_uri(uri: &ParsedUri, copy_from: bool) {
    if has_access_privilege_to_uri(uri, copy_from) {
        return;
    }

    let role_name = required_role_name(uri, copy_from);
    let direction = if copy_from { "read" } else { "write" };
    let object_type = if uri.is_local() { "file" } else { "remote uri" };

    ereport!(
        PgLogLevel::ERROR,
        PgSqlErrorCode::ERRCODE_INSUFFICIENT_PRIVILEGE,
        format!("permission denied to {direction} a {object_type}"),
        format!(
            "Only roles with privileges of the \"{role_name}\" role may {direction} a \
             {object_type}."
        ),
    );
}
