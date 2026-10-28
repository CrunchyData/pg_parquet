//! The C entry points another extension links against. These call the vtable the way a
//! consumer does — through the rendezvous variable, with an out parameter for the message —
//! because a mistake in that contract is not visible from SQL.

#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_the_vtable_is_published_with_a_matching_magic() {
        use crate::vtable::{
            PG_OBJECT_STORE_API_MAGIC, PG_OBJECT_STORE_API_VERSION, PG_OBJECT_STORE_RENDEZVOUS_KEY,
        };

        let key = std::ffi::CString::new(PG_OBJECT_STORE_RENDEZVOUS_KEY).unwrap();

        let api = unsafe {
            let slot = pgrx::pg_sys::find_rendezvous_variable(key.as_ptr());
            assert!(!(*slot).is_null(), "the vtable was not published");
            &*(*slot as *const crate::vtable::PgObjectStoreApiV1)
        };

        assert_eq!(api.magic, PG_OBJECT_STORE_API_MAGIC);
        assert_eq!(api.version, PG_OBJECT_STORE_API_VERSION);
        assert!(
            !api.ext.is_null(),
            "the extension table is published with the main one"
        );
        assert!(
            api.reserved.iter().all(|slot| slot.is_null()),
            "the spare slot is null until something lands in it"
        );
    }

    #[pg_test]
    fn test_the_vtable_round_trips_a_local_file() {
        use std::ffi::{c_char, CString};

        let uri = CString::new("/tmp/pg_object_store_vtable.bin").unwrap();
        let payload = b"through the vtable";

        let key = CString::new(crate::vtable::PG_OBJECT_STORE_RENDEZVOUS_KEY).unwrap();

        let api = unsafe {
            let slot = pgrx::pg_sys::find_rendezvous_variable(key.as_ptr());
            &*(*slot as *const crate::vtable::PgObjectStoreApiV1)
        };

        let mut error: *mut c_char = std::ptr::null_mut();

        let put = unsafe {
            (api.put)(
                uri.as_ptr(),
                payload.as_ptr(),
                payload.len() as i64,
                &mut error,
            )
        };

        assert_eq!(put, crate::vtable::PGOS_OK, "put failed");

        let mut size = 0i64;
        let head = unsafe { (api.head)(uri.as_ptr(), &mut size, &mut error) };

        assert_eq!(head, crate::vtable::PGOS_OK, "head failed");
        assert_eq!(size, payload.len() as i64);

        let mut data: *mut u8 = std::ptr::null_mut();
        let mut len = 0i64;

        let got = unsafe { (api.get_range)(uri.as_ptr(), 8, 3, &mut data, &mut len, &mut error) };

        assert_eq!(got, crate::vtable::PGOS_OK, "get_range failed");
        assert_eq!(len, 3);

        let fetched = unsafe { std::slice::from_raw_parts(data, len as usize) };
        assert_eq!(fetched, b"the");

        std::fs::remove_file("/tmp/pg_object_store_vtable.bin").ok();
    }

    #[pg_test]
    fn test_the_vtable_reports_a_missing_object_rather_than_erroring() {
        use std::ffi::{c_char, CString};

        let uri = CString::new("/tmp/pg_object_store_vtable_absent.bin").unwrap();
        std::fs::remove_file("/tmp/pg_object_store_vtable_absent.bin").ok();

        let key = CString::new(crate::vtable::PG_OBJECT_STORE_RENDEZVOUS_KEY).unwrap();

        let api = unsafe {
            let slot = pgrx::pg_sys::find_rendezvous_variable(key.as_ptr());
            &*(*slot as *const crate::vtable::PgObjectStoreApiV1)
        };

        let mut error: *mut c_char = std::ptr::null_mut();
        let mut size = 0i64;

        let head = unsafe { (api.head)(uri.as_ptr(), &mut size, &mut error) };

        assert_eq!(
            head,
            crate::vtable::PGOS_NOT_FOUND,
            "an absent object is a code, not an error"
        );
        assert!(error.is_null(), "no message is set for a missing object");
    }

    #[pg_test]
    fn test_the_vtable_returns_a_message_instead_of_ereporting() {
        use std::ffi::{c_char, CStr, CString};

        let uri = CString::new("ftp://host/key").unwrap();

        let key = CString::new(crate::vtable::PG_OBJECT_STORE_RENDEZVOUS_KEY).unwrap();

        let api = unsafe {
            let slot = pgrx::pg_sys::find_rendezvous_variable(key.as_ptr());
            &*(*slot as *const crate::vtable::PgObjectStoreApiV1)
        };

        let mut error: *mut c_char = std::ptr::null_mut();
        let mut allowed = false;

        let code = unsafe { (api.check_privilege)(uri.as_ptr(), true, &mut allowed, &mut error) };

        assert_eq!(code, crate::vtable::PGOS_ERROR);
        assert!(!error.is_null(), "an error code comes with a message");

        let message = unsafe { CStr::from_ptr(error) }.to_str().unwrap();
        assert!(message.contains("ftp"), "{message}");
    }

    /// The vtable, plus an error slot, for the streaming tests below. Resolving it is the same
    /// four lines in each one.
    fn api() -> &'static crate::vtable::PgObjectStoreApiV1 {
        let key = std::ffi::CString::new(crate::vtable::PG_OBJECT_STORE_RENDEZVOUS_KEY).unwrap();

        unsafe {
            let slot = pgrx::pg_sys::find_rendezvous_variable(key.as_ptr());
            &*(*slot as *const crate::vtable::PgObjectStoreApiV1)
        }
    }

    #[pg_test]
    fn test_the_vtable_streams_a_local_file() {
        use std::ffi::{c_char, c_void, CString};

        let path = "/tmp/pg_object_store_streamed.bin";
        let uri = CString::new(path).unwrap();
        std::fs::remove_file(path).ok();

        let api = api();

        let mut error: *mut c_char = std::ptr::null_mut();
        let mut handle: *mut c_void = std::ptr::null_mut();

        let code = unsafe { (api.put_begin.unwrap())(uri.as_ptr(), &mut handle, &mut error) };

        assert_eq!(code, crate::vtable::PGOS_OK, "put_begin failed");
        assert!(!handle.is_null(), "a successful begin hands back a handle");

        // three parts, because the point of the entry point is that the caller never holds the
        // whole object
        for part in ["one ", "two ", "three"] {
            let code = unsafe {
                (api.put_part.unwrap())(handle, part.as_ptr(), part.len() as i64, &mut error)
            };

            assert_eq!(code, crate::vtable::PGOS_OK, "put_part failed");
        }

        // nothing is visible until finish: a local write goes to a temp file in the same
        // directory and is renamed over the path there and nowhere earlier
        assert!(
            !std::path::Path::new(path).exists(),
            "an unfinished upload must not be readable"
        );

        let code = unsafe { (api.put_finish.unwrap())(handle, &mut error) };
        assert_eq!(code, crate::vtable::PGOS_OK, "put_finish failed");

        assert_eq!(std::fs::read(path).unwrap(), b"one two three");
        assert_eq!(
            crate::writer::open_writer_count(),
            0,
            "finish closes the handle"
        );

        std::fs::remove_file(path).ok();
    }

    #[pg_test]
    fn test_an_aborted_stream_writes_nothing() {
        use std::ffi::{c_char, c_void, CString};

        let path = "/tmp/pg_object_store_aborted.bin";
        let uri = CString::new(path).unwrap();
        std::fs::remove_file(path).ok();

        let api = api();

        let mut error: *mut c_char = std::ptr::null_mut();
        let mut handle: *mut c_void = std::ptr::null_mut();

        unsafe {
            assert_eq!(
                (api.put_begin.unwrap())(uri.as_ptr(), &mut handle, &mut error),
                crate::vtable::PGOS_OK
            );

            let payload = b"discarded";
            assert_eq!(
                (api.put_part.unwrap())(handle, payload.as_ptr(), payload.len() as i64, &mut error),
                crate::vtable::PGOS_OK
            );

            assert_eq!(
                (api.put_abort.unwrap())(handle, &mut error),
                crate::vtable::PGOS_OK,
                "put_abort failed"
            );
        }

        // nothing appears, on a cloud store or a local path: what an aborted upload would
        // have written was never renamed over the destination
        assert!(
            !std::path::Path::new(path).exists(),
            "an aborted upload writes nothing"
        );
        assert_eq!(
            crate::writer::open_writer_count(),
            0,
            "abort closes the handle"
        );
    }

    #[pg_test]
    fn test_aborting_a_stream_past_the_buffer_capacity_leaves_no_parts() {
        use std::ffi::{c_char, c_void, CString};

        let path = "/tmp/pg_object_store_aborted_multipart.bin";
        let uri = CString::new(path).unwrap();
        std::fs::remove_file(path).ok();

        let api = api();

        let mut error: *mut c_char = std::ptr::null_mut();
        let mut handle: *mut c_void = std::ptr::null_mut();

        // the other abort test stays inside the buffer, where there is no upload to give up.
        // 12 MiB against the 10 MiB default capacity is the path where there is one.
        let part = vec![b'z'; 1024 * 1024];

        unsafe {
            assert_eq!(
                (api.put_begin.unwrap())(uri.as_ptr(), &mut handle, &mut error),
                crate::vtable::PGOS_OK
            );

            for _ in 0..12 {
                assert_eq!(
                    (api.put_part.unwrap())(handle, part.as_ptr(), part.len() as i64, &mut error),
                    crate::vtable::PGOS_OK
                );
            }

            assert_eq!(
                (api.put_abort.unwrap())(handle, &mut error),
                crate::vtable::PGOS_OK,
                "put_abort failed"
            );
        }

        assert!(
            !std::path::Path::new(path).exists(),
            "an aborted upload makes none of its parts the object"
        );

        // the local store stages the parts of an upload in siblings of the target it names
        // after it, so a leftover one is an upload that was not given up
        let staged = std::fs::read_dir("/tmp")
            .unwrap()
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| {
                name.starts_with("pg_object_store_aborted_multipart.bin")
                    && name != "pg_object_store_aborted_multipart.bin"
            })
            .collect::<Vec<_>>();

        assert!(
            staged.is_empty(),
            "the parts of the aborted upload are still there: {staged:?}"
        );

        std::fs::remove_file(path).ok();
    }

    #[pg_test]
    fn test_a_closed_handle_is_reported_rather_than_reused() {
        use std::ffi::{c_char, c_void, CStr, CString};

        let path = "/tmp/pg_object_store_closed_handle.bin";
        let uri = CString::new(path).unwrap();
        std::fs::remove_file(path).ok();

        let api = api();

        let mut error: *mut c_char = std::ptr::null_mut();
        let mut handle: *mut c_void = std::ptr::null_mut();

        unsafe {
            assert_eq!(
                (api.put_begin.unwrap())(uri.as_ptr(), &mut handle, &mut error),
                crate::vtable::PGOS_OK
            );

            assert_eq!(
                (api.put_finish.unwrap())(handle, &mut error),
                crate::vtable::PGOS_OK
            );

            // the same handle a second time: a stale one has to be a message, not a jump into
            // freed memory
            assert_eq!(
                (api.put_finish.unwrap())(handle, &mut error),
                crate::vtable::PGOS_ERROR
            );

            assert!(!error.is_null());
            let message = CStr::from_ptr(error).to_str().unwrap();
            assert!(message.contains("is not open"), "{message}");

            // and so does one that was never handed out
            error = std::ptr::null_mut();
            assert_eq!(
                (api.put_part.unwrap())(9999 as *mut c_void, b"x".as_ptr(), 1, &mut error),
                crate::vtable::PGOS_ERROR
            );
            assert!(!error.is_null());

            error = std::ptr::null_mut();
            assert_eq!(
                (api.put_finish.unwrap())(std::ptr::null_mut(), &mut error),
                crate::vtable::PGOS_ERROR
            );

            let message = CStr::from_ptr(error).to_str().unwrap();
            assert!(message.contains("must not be null"), "{message}");
        }

        std::fs::remove_file(path).ok();
    }

    #[pg_test]
    fn test_a_stream_past_the_buffer_capacity_still_lands_whole() {
        use std::ffi::{c_char, c_void, CString};

        let path = "/tmp/pg_object_store_multipart.bin";
        let uri = CString::new(path).unwrap();
        std::fs::remove_file(path).ok();

        let api = api();

        let mut error: *mut c_char = std::ptr::null_mut();
        let mut handle: *mut c_void = std::ptr::null_mut();

        // BufWriter buffers below its capacity and switches to a multipart upload above it, so
        // this is the test that covers the second of those two paths. 12 MiB against a 10 MiB
        // default capacity.
        let part = vec![b'z'; 1024 * 1024];
        let parts = 12;

        unsafe {
            assert_eq!(
                (api.put_begin.unwrap())(uri.as_ptr(), &mut handle, &mut error),
                crate::vtable::PGOS_OK
            );

            for _ in 0..parts {
                assert_eq!(
                    (api.put_part.unwrap())(handle, part.as_ptr(), part.len() as i64, &mut error),
                    crate::vtable::PGOS_OK
                );
            }

            assert_eq!(
                (api.put_finish.unwrap())(handle, &mut error),
                crate::vtable::PGOS_OK,
                "put_finish failed"
            );
        }

        let written = std::fs::metadata(path).unwrap().len();
        assert_eq!(written, (parts * part.len()) as u64);

        std::fs::remove_file(path).ok();
    }

    #[pg_test]
    fn test_a_write_to_an_unreachable_scheme_is_a_message() {
        use std::ffi::{c_char, c_void, CStr, CString};

        let uri = CString::new("ftp://host/key").unwrap();

        let api = api();

        let mut error: *mut c_char = std::ptr::null_mut();
        // deliberately not null to start with, so that the assertion below is about
        // put_begin clearing it rather than about it never having been set
        let mut sentinel = 0u8;
        let mut handle: *mut c_void = &mut sentinel as *mut u8 as *mut c_void;

        let code = unsafe { (api.put_begin.unwrap())(uri.as_ptr(), &mut handle, &mut error) };

        assert_eq!(code, crate::vtable::PGOS_ERROR);
        assert!(
            handle.is_null(),
            "a failed begin leaves nothing callable behind"
        );

        let message = unsafe { CStr::from_ptr(error) }.to_str().unwrap();
        assert!(message.contains("ftp"), "{message}");
    }
    #[pg_test]
    fn test_the_plural_form_answers_an_empty_range_without_a_request() {
        use std::ffi::{c_char, CString};

        let uri = CString::new("/tmp/pg_object_store_vtable_empty_range.bin").unwrap();
        std::fs::write(
            "/tmp/pg_object_store_vtable_empty_range.bin",
            b"through the vtable",
        )
        .expect("failed to write the file");

        let key = CString::new(crate::vtable::PG_OBJECT_STORE_RENDEZVOUS_KEY).unwrap();

        let api = unsafe {
            let slot = pgrx::pg_sys::find_rendezvous_variable(key.as_ptr());
            &*(*slot as *const crate::vtable::PgObjectStoreApiV1)
        };

        let mut error: *mut c_char = std::ptr::null_mut();

        // an empty range beside two real ones: a store rejects the empty one as invalid, which
        // would fail the whole batch
        let offsets = [8i64, 0, 4];
        let lengths = [0i64, 7, 3];

        let mut data = [std::ptr::null_mut::<u8>(); 3];
        let mut lens = [0i64; 3];

        let got = unsafe {
            (api.get_ranges)(
                uri.as_ptr(),
                3,
                offsets.as_ptr(),
                lengths.as_ptr(),
                data.as_mut_ptr(),
                lens.as_mut_ptr(),
                &mut error,
            )
        };

        assert_eq!(got, crate::vtable::PGOS_OK, "get_ranges failed");
        assert_eq!(lens, [0, 7, 3]);

        let fetched = |i: usize| unsafe { std::slice::from_raw_parts(data[i], lens[i] as usize) };

        assert_eq!(fetched(0), b"");
        assert_eq!(fetched(1), b"through");
        assert_eq!(fetched(2), b"ugh");

        std::fs::remove_file("/tmp/pg_object_store_vtable_empty_range.bin").ok();
    }

    #[pg_test]
    fn test_a_range_past_the_palloc_cap_is_a_message_rather_than_an_allocation_failure() {
        use std::ffi::{c_char, CStr, CString};

        let uri = CString::new("/tmp/pg_object_store_vtable_over_cap.bin").unwrap();

        let api = api();

        // a palloc of more than 1GB is an allocation failure raised from inside the fetch,
        // which a consumer cannot tell from the store being unreachable
        for singular in [true, false] {
            let mut error: *mut c_char = std::ptr::null_mut();
            let mut data: *mut u8 = std::ptr::null_mut();
            let mut len = 0i64;

            let over = 2_000_000_000i64;

            let code = unsafe {
                if singular {
                    (api.get_range)(uri.as_ptr(), 0, over, &mut data, &mut len, &mut error)
                } else {
                    (api.get_ranges)(
                        uri.as_ptr(),
                        1,
                        [0i64].as_ptr(),
                        [over].as_ptr(),
                        &mut data,
                        &mut len,
                        &mut error,
                    )
                }
            };

            assert_eq!(code, crate::vtable::PGOS_ERROR);

            let message = unsafe { CStr::from_ptr(error) }.to_str().unwrap();

            assert!(
                message.contains("more than the 1073741823 a single read returns"),
                "{message}"
            );
        }
    }

    #[pg_test]
    fn test_an_abandoned_write_leaves_the_destination_as_it_was() {
        use std::ffi::{c_char, c_void, CString};

        let path = "/tmp/pg_object_store_kept.bin";
        let uri = CString::new(path).unwrap();

        std::fs::write(path, b"what was already there").expect("failed to write the file");

        let api = api();

        let mut error: *mut c_char = std::ptr::null_mut();
        let mut handle: *mut c_void = std::ptr::null_mut();

        unsafe {
            assert_eq!(
                (api.put_begin.unwrap())(uri.as_ptr(), &mut handle, &mut error),
                crate::vtable::PGOS_OK
            );

            let payload = b"a write that is given up";
            assert_eq!(
                (api.put_part.unwrap())(handle, payload.as_ptr(), payload.len() as i64, &mut error),
                crate::vtable::PGOS_OK
            );

            assert_eq!(
                (api.put_abort.unwrap())(handle, &mut error),
                crate::vtable::PGOS_OK
            );
        }

        // opening a write once truncated the destination before a byte of it existed, so a
        // statement that then failed -- an error, a cancellation, a terminated backend -- had
        // destroyed the file it was replacing
        assert_eq!(
            std::fs::read(path).unwrap(),
            b"what was already there",
            "a write that was never finished must not have changed the file"
        );

        std::fs::remove_file(path).ok();
    }

    #[pg_test]
    fn test_a_destination_that_cannot_be_written_is_reported_at_the_open() {
        use std::ffi::{c_char, c_void, CStr, CString};

        let path = "/tmp/pg_object_store_readonly.bin";
        let uri = CString::new(path).unwrap();

        std::fs::write(path, b"read only").expect("failed to write the file");
        std::fs::set_permissions(path, std::os::unix::fs::PermissionsExt::from_mode(0o400))
            .expect("failed to make the file read only");

        let api = api();

        let mut error: *mut c_char = std::ptr::null_mut();
        let mut handle: *mut c_void = std::ptr::null_mut();

        let code = unsafe { (api.put_begin.unwrap())(uri.as_ptr(), &mut handle, &mut error) };

        std::fs::set_permissions(path, std::os::unix::fs::PermissionsExt::from_mode(0o600)).ok();

        assert_eq!(
            code,
            crate::vtable::PGOS_ERROR,
            "an unwritable destination is not reported at the first flush"
        );

        let message = unsafe { CStr::from_ptr(error) }.to_str().unwrap();

        assert!(message.contains("cannot be written"), "{message}");

        std::fs::remove_file(path).ok();
    }

    #[pg_test]
    fn test_a_directory_that_cannot_be_written_is_reported_at_the_open() {
        use std::ffi::{c_char, c_void, CStr, CString};

        let dir = "/tmp/pg_object_store_readonly_dir";
        let path = format!("{dir}/inside.bin");
        let uri = CString::new(path.clone()).unwrap();

        std::fs::remove_dir_all(dir).ok();
        std::fs::create_dir_all(dir).expect("failed to create the directory");
        std::fs::set_permissions(dir, std::os::unix::fs::PermissionsExt::from_mode(0o500))
            .expect("failed to make the directory read only");

        let api = api();

        let mut error: *mut c_char = std::ptr::null_mut();
        let mut handle: *mut c_void = std::ptr::null_mut();

        let code = unsafe { (api.put_begin.unwrap())(uri.as_ptr(), &mut handle, &mut error) };

        std::fs::set_permissions(dir, std::os::unix::fs::PermissionsExt::from_mode(0o700)).ok();

        assert_eq!(
            code,
            crate::vtable::PGOS_ERROR,
            "a destination whose directory cannot be written is not reported part way through"
        );

        let message = unsafe { CStr::from_ptr(error) }.to_str().unwrap();

        assert!(message.contains(&path), "{message}");
        assert!(message.contains("cannot be written"), "{message}");

        // the probe the check writes is its own and is not left behind
        assert_eq!(
            std::fs::read_dir(dir).unwrap().count(),
            0,
            "the writability check left something in the directory"
        );

        std::fs::remove_dir_all(dir).ok();
    }
}
