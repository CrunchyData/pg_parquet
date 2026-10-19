//! The second table: the multi-process upload and the delete.
//!
//! No multipart upload is actually completed here, because the only stores that have one are the
//! cloud providers — `object_store` implements `MultipartStore` for S3, Azure, GCS and
//! `InMemory`, and for neither the local filesystem nor `http`. What is provable without a
//! provider is everything a caller has to get right before the first part goes out: that the
//! table is published, that a store with no multipart upload says so in the documented way
//! rather than failing somewhere later, and that a bad identifier is a message.

#[pgrx::pg_schema]
mod tests {
    use std::ffi::{c_char, CStr, CString};

    use pgrx::prelude::*;

    use crate::multipart::{
        PgObjectStoreExtV1, PG_OBJECT_STORE_EXT_MAGIC, PG_OBJECT_STORE_EXT_VERSION,
    };
    use crate::vtable::{
        PgObjectStoreApiV1, PGOS_ERROR, PGOS_NOT_FOUND, PGOS_OK, PG_OBJECT_STORE_RENDEZVOUS_KEY,
    };

    /// The extension table, resolved the way a consumer resolves it: through the rendezvous
    /// variable and then through the main table's `ext` slot.
    fn ext() -> &'static PgObjectStoreExtV1 {
        let key = CString::new(PG_OBJECT_STORE_RENDEZVOUS_KEY).unwrap();

        unsafe {
            let slot = pgrx::pg_sys::find_rendezvous_variable(key.as_ptr());
            let api = &*(*slot as *const PgObjectStoreApiV1);

            assert!(!api.ext.is_null(), "the extension table was not published");

            &*(api.ext as *const PgObjectStoreExtV1)
        }
    }

    fn message_of(error: *mut c_char) -> String {
        assert!(!error.is_null(), "an error code comes with a message");

        unsafe { CStr::from_ptr(error) }
            .to_str()
            .unwrap()
            .to_string()
    }

    #[pg_test]
    fn test_the_extension_table_is_published_with_a_matching_magic() {
        let ext = ext();

        assert_eq!(ext.magic, PG_OBJECT_STORE_EXT_MAGIC);
        assert_eq!(ext.version, PG_OBJECT_STORE_EXT_VERSION);
        assert!(
            ext.reserved.iter().all(|slot| slot.is_null()),
            "the spare slots are null until something lands in them"
        );
    }

    #[pg_test]
    fn test_a_store_without_multipart_is_a_null_upload_id_rather_than_an_error() {
        let uri = CString::new("/tmp/pg_object_store_no_multipart.bin").unwrap();

        let ext = ext();

        let mut error: *mut c_char = std::ptr::null_mut();
        // deliberately not null to start with, so the assertion is about the call clearing it
        let mut sentinel = 0u8;
        let mut upload_id = &mut sentinel as *mut u8 as *mut c_char;

        let code = unsafe { (ext.multipart_begin)(uri.as_ptr(), &mut upload_id, &mut error) };

        assert_eq!(
            code, PGOS_OK,
            "a store with no multipart upload is a plan to make, not a failure"
        );
        assert!(upload_id.is_null(), "{}", message_of(error));
        assert!(
            error.is_null(),
            "nothing went wrong, so nothing is reported"
        );
    }

    #[pg_test]
    fn test_uploading_into_a_store_without_multipart_says_what_is_missing() {
        let uri = CString::new("/tmp/pg_object_store_no_multipart.bin").unwrap();
        let upload_id = CString::new("made-up").unwrap();

        let ext = ext();

        let mut error: *mut c_char = std::ptr::null_mut();
        let mut part_id: *mut c_char = std::ptr::null_mut();

        let payload = b"a part";

        let code = unsafe {
            (ext.multipart_put_part)(
                uri.as_ptr(),
                upload_id.as_ptr(),
                0,
                payload.as_ptr(),
                payload.len() as i64,
                &mut part_id,
                &mut error,
            )
        };

        assert_eq!(code, PGOS_ERROR);
        assert!(part_id.is_null(), "a failed part hands back no id");

        let message = message_of(error);
        assert!(message.contains("no multipart upload"), "{message}");
    }

    #[pg_test]
    fn test_a_missing_identifier_is_a_message() {
        let uri = CString::new("/tmp/pg_object_store_no_multipart.bin").unwrap();

        let ext = ext();

        let mut error: *mut c_char = std::ptr::null_mut();
        let mut part_id: *mut c_char = std::ptr::null_mut();

        // a null upload id is caught before anything is reached, so this says so rather than
        // reporting what the local filesystem cannot do
        let code = unsafe {
            (ext.multipart_put_part)(
                uri.as_ptr(),
                std::ptr::null(),
                0,
                b"x".as_ptr(),
                1,
                &mut part_id,
                &mut error,
            )
        };

        assert_eq!(code, PGOS_ERROR);

        let message = message_of(error);
        assert!(message.contains("upload id must not be null"), "{message}");

        error = std::ptr::null_mut();

        let upload_id = CString::new("made-up").unwrap();

        let code = unsafe {
            (ext.multipart_put_part)(
                uri.as_ptr(),
                upload_id.as_ptr(),
                -1,
                b"x".as_ptr(),
                1,
                &mut part_id,
                &mut error,
            )
        };

        assert_eq!(code, PGOS_ERROR);

        let message = message_of(error);
        assert!(message.contains("must not be negative"), "{message}");
    }

    #[pg_test]
    fn test_the_vtable_deletes_a_local_file() {
        let path = "/tmp/pg_object_store_deleted.bin";
        let uri = CString::new(path).unwrap();

        std::fs::write(path, b"to be removed").unwrap();

        let ext = ext();

        let mut error: *mut c_char = std::ptr::null_mut();

        assert_eq!(
            unsafe { (ext.delete)(uri.as_ptr(), &mut error) },
            PGOS_OK,
            "delete failed"
        );
        assert!(!std::path::Path::new(path).exists(), "the file is gone");

        // and again: a cleanup path that runs twice is not an error the second time
        assert_eq!(
            unsafe { (ext.delete)(uri.as_ptr(), &mut error) },
            PGOS_NOT_FOUND
        );
        assert!(error.is_null(), "an absent object sets no message");
    }

    #[pg_test]
    fn test_deleting_what_cannot_be_reached_is_a_message() {
        let uri = CString::new("ftp://host/key").unwrap();

        let ext = ext();

        let mut error: *mut c_char = std::ptr::null_mut();

        assert_eq!(
            unsafe { (ext.delete)(uri.as_ptr(), &mut error) },
            PGOS_ERROR
        );

        let message = message_of(error);
        assert!(message.contains("ftp"), "{message}");
    }

    #[pg_test]
    fn test_the_second_table_checks_the_write_privilege_too() {
        let uri = CString::new("s3://pgos-bucket/key").unwrap();
        let upload_id = CString::new("made-up").unwrap();

        let ext = ext();

        // a role in neither object store role, so the check fails before anything is reached.
        // Every entry point here is a write, which is the only direction there is to check.
        Spi::run("CREATE ROLE pgos_deleter; SET ROLE pgos_deleter;").unwrap();

        // each call is made inside the loop rather than collected into an array first, so that
        // every one of them sees an error slot the iteration before it cleared
        for what in ["begin", "put_part", "complete", "abort", "delete"] {
            let mut error: *mut c_char = std::ptr::null_mut();
            let mut out: *mut c_char = std::ptr::null_mut();

            let code = unsafe {
                match what {
                    "begin" => (ext.multipart_begin)(uri.as_ptr(), &mut out, &mut error),
                    "put_part" => (ext.multipart_put_part)(
                        uri.as_ptr(),
                        upload_id.as_ptr(),
                        0,
                        b"x".as_ptr(),
                        1,
                        &mut out,
                        &mut error,
                    ),
                    "complete" => (ext.multipart_complete)(
                        uri.as_ptr(),
                        upload_id.as_ptr(),
                        0,
                        std::ptr::null(),
                        &mut error,
                    ),
                    "abort" => (ext.multipart_abort)(uri.as_ptr(), upload_id.as_ptr(), &mut error),
                    _ => (ext.delete)(uri.as_ptr(), &mut error),
                }
            };

            assert_eq!(code, PGOS_ERROR, "{what} skipped the privilege check");

            let message = message_of(error);
            assert!(
                message.contains("permission denied to write"),
                "{what}: {message}"
            );
        }

        Spi::run("RESET ROLE; DROP ROLE pgos_deleter;").unwrap();
    }

    #[pg_test]
    fn test_a_shallow_list_reports_one_level_and_walks_no_further() {
        let dir = "/tmp/pg_object_store_shallow";
        let unreadable = format!("{dir}/locked");

        std::fs::remove_dir_all(dir).ok();
        std::fs::create_dir_all(&unreadable).expect("failed to create the directories");
        std::fs::write(format!("{dir}/top.bin"), b"at the top").expect("failed to write");
        std::fs::write(format!("{unreadable}/deep.bin"), b"below").expect("failed to write");

        // the owner's own bits deny the owner too, so a recursive walk of the prefix fails here
        // while a one-level list of it never looks inside
        std::fs::set_permissions(
            &unreadable,
            std::os::unix::fs::PermissionsExt::from_mode(0o000),
        )
        .expect("failed to lock the directory");

        let prefix = CString::new(format!("{dir}/")).unwrap();

        let mut error: *mut c_char = std::ptr::null_mut();
        let mut count = 0i32;
        let mut locations: *mut *mut c_char = std::ptr::null_mut();
        let mut sizes: *mut i64 = std::ptr::null_mut();

        let list_shallow = ext()
            .list_shallow
            .expect("the shallow list should be published");

        let code = unsafe {
            list_shallow(
                prefix.as_ptr(),
                &mut count,
                &mut locations,
                &mut sizes,
                &mut error,
            )
        };

        std::fs::set_permissions(
            &unreadable,
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .expect("failed to unlock the directory");

        if code != PGOS_OK {
            panic!("the shallow list failed: {}", message_of(error));
        }

        assert_eq!(count, 1, "only the object at the top level is reported");

        let location = unsafe { CStr::from_ptr(*locations) }.to_str().unwrap();

        assert!(location.ends_with("/top.bin"), "{location}");
        assert_eq!(unsafe { *sizes }, "at the top".len() as i64);

        std::fs::remove_dir_all(dir).ok();
    }
}
