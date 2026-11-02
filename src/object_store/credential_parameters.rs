use std::ffi::{c_char, c_void, CStr, CString};

use object_store::ObjectStoreScheme;
use pgrx::{pg_guard, pg_sys, GucSetting};

// ALLOW_CREDENTIALS_IN_PARAMETERS opens the parameters below, which a session puts the
// credentials of an object store themselves into rather than naming a profile they are kept
// under. A credential set that way reaches the places a statement reaches: the server log,
// pg_stat_activity, pg_stat_statements and the error reports of the session. It is off by
// default and only a superuser turns it on, since whether the credentials of the sessions of
// a server may be written into its log is the server's decision and not a session's.
pub(crate) static ALLOW_CREDENTIALS_IN_PARAMETERS: GucSetting<bool> =
    GucSetting::<bool>::new(false);

pub(crate) static AWS_ACCESS_KEY_ID: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
pub(crate) static AWS_SECRET_ACCESS_KEY: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
pub(crate) static AWS_SESSION_TOKEN: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
pub(crate) static AWS_REGION: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
pub(crate) static AWS_ENDPOINT_URL: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

pub(crate) static AZURE_STORAGE_ACCOUNT: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
pub(crate) static AZURE_STORAGE_KEY: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
pub(crate) static AZURE_STORAGE_SAS_TOKEN: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
pub(crate) static AZURE_STORAGE_CONNECTION_STRING: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
pub(crate) static AZURE_STORAGE_ENDPOINT: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

pub(crate) static GOOGLE_SERVICE_ACCOUNT_KEY: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

// parameter is the value a session put in one of the parameters above, and None for a
// parameter it left alone. It is None for every one of them while the server does not allow
// credentials in parameters, so taking the permission away stops the next copy of a session
// that set them rather than only the next session.
pub(crate) fn parameter(setting: &GucSetting<Option<CString>>) -> Option<String> {
    if !ALLOW_CREDENTIALS_IN_PARAMETERS.get() {
        return None;
    }

    let value = setting.get()?;

    let value = value
        .to_str()
        .unwrap_or_else(|e| panic!("a pg_parquet credential parameter is not valid utf-8: {e}"));

    if value.is_empty() {
        return None;
    }

    Some(value.to_string())
}

// credentials_are_in_parameters tells whether the session put the credentials of the store a
// uri names into the parameters. Such a store is built from them rather than from the profile
// the session named, and it is not cached: the parameters are the session's to change, so a
// store built from them answers the copy that was made with them and nothing after it.
pub(crate) fn credentials_are_in_parameters(scheme: &ObjectStoreScheme) -> bool {
    match scheme {
        ObjectStoreScheme::AmazonS3 => parameter(&AWS_ACCESS_KEY_ID).is_some(),
        ObjectStoreScheme::MicrosoftAzure => {
            parameter(&AZURE_STORAGE_CONNECTION_STRING).is_some()
                || parameter(&AZURE_STORAGE_ACCOUNT).is_some()
        }
        ObjectStoreScheme::GoogleCloudStorage => parameter(&GOOGLE_SERVICE_ACCOUNT_KEY).is_some(),
        _ => false,
    }
}

// check_credential_parameter refuses a credential the session would leave behind it. The
// parameters are the session's own, so they are refused from ALTER ROLE and ALTER DATABASE,
// whose settings are kept in pg_db_role_setting, which every role of the cluster may read and
// pg_dumpall writes out. GUC_DISALLOW_IN_FILE and GUC_DISALLOW_IN_AUTO_FILE refuse
// postgresql.conf and ALTER SYSTEM for the same reason, and GUC_NOT_WHILE_SEC_REST refuses a
// definer's rights function setting one on a caller's behalf.
//
// It also refuses the credential outright while the server does not allow credentials in
// parameters, so a session is told why rather than finding that what it set is ignored.
#[pg_guard]
unsafe extern "C-unwind" fn check_credential_parameter(
    newval: *mut *mut c_char,
    _extra: *mut *mut c_void,
    source: pg_sys::GucSource::Type,
) -> bool {
    // a reset carries no credential, so there is nothing to refuse
    let value = unsafe { *newval };
    if value.is_null() || unsafe { CStr::from_ptr(value) }.is_empty() {
        return true;
    }

    if !ALLOW_CREDENTIALS_IN_PARAMETERS.get() {
        unsafe {
            refuse_with(
                c"The server does not allow credentials in parameters.",
                c"Set pg_parquet.allow_credentials_in_parameters to on, which only a superuser \
                  may do, or name a profile with pg_parquet.profile_name instead.",
            );
        }

        return false;
    }

    // the settings of a role and of a database are kept in a catalog every role may read.
    // PGC_S_TEST is the source ALTER ROLE and ALTER DATABASE validate a value under, and it
    // is the one that has to be refused to keep the credential out of the catalog: the three
    // below are only reached when a setting that is already in there is applied to a session,
    // which is too late, the credential having been written by then.
    let is_kept_in_a_catalog = matches!(
        source,
        pg_sys::GucSource::PGC_S_TEST
            | pg_sys::GucSource::PGC_S_DATABASE
            | pg_sys::GucSource::PGC_S_USER
            | pg_sys::GucSource::PGC_S_DATABASE_USER
    );

    if is_kept_in_a_catalog {
        unsafe {
            refuse_with(
                c"A credential of a role or of a database is kept in pg_db_role_setting, which \
                  every role of the cluster may read.",
                c"Set it in the session that makes the requests instead.",
            );
        }

        return false;
    }

    true
}

// refuse_with says why a parameter was refused. guc.c reports and then frees the two strings,
// so each is a copy of its own in the memory context the check is running in.
unsafe fn refuse_with(detail: &CStr, hint: &CStr) {
    unsafe {
        pg_sys::GUC_check_errdetail_string = pg_sys::pstrdup(detail.as_ptr());
        pg_sys::GUC_check_errhint_string = pg_sys::pstrdup(hint.as_ptr());
    }
}

// CREDENTIAL_PARAMETERS is every parameter a credential goes in, with the name it is defined
// under and what it holds.
#[allow(clippy::type_complexity)]
pub(crate) const CREDENTIAL_PARAMETERS: &[(&CStr, &CStr, &GucSetting<Option<CString>>, bool)] = &[
    (
        c"pg_parquet.aws_access_key_id",
        c"Access key id the session makes its s3 requests with",
        &AWS_ACCESS_KEY_ID,
        true,
    ),
    (
        c"pg_parquet.aws_secret_access_key",
        c"Secret access key the session makes its s3 requests with",
        &AWS_SECRET_ACCESS_KEY,
        true,
    ),
    (
        c"pg_parquet.aws_session_token",
        c"Session token the session makes its s3 requests with",
        &AWS_SESSION_TOKEN,
        true,
    ),
    (
        c"pg_parquet.aws_region",
        c"Region the session makes its s3 requests in",
        &AWS_REGION,
        false,
    ),
    (
        c"pg_parquet.aws_endpoint_url",
        c"Endpoint the session makes its s3 requests to",
        &AWS_ENDPOINT_URL,
        false,
    ),
    (
        c"pg_parquet.azure_storage_account",
        c"Storage account the session makes its azure requests to",
        &AZURE_STORAGE_ACCOUNT,
        false,
    ),
    (
        c"pg_parquet.azure_storage_key",
        c"Storage key the session makes its azure requests with",
        &AZURE_STORAGE_KEY,
        true,
    ),
    (
        c"pg_parquet.azure_storage_sas_token",
        c"Sas token the session makes its azure requests with",
        &AZURE_STORAGE_SAS_TOKEN,
        true,
    ),
    (
        c"pg_parquet.azure_storage_connection_string",
        c"Connection string the session makes its azure requests with",
        &AZURE_STORAGE_CONNECTION_STRING,
        true,
    ),
    (
        c"pg_parquet.azure_storage_endpoint",
        c"Endpoint the session makes its azure requests to",
        &AZURE_STORAGE_ENDPOINT,
        false,
    ),
    (
        c"pg_parquet.gcs_service_account_key",
        c"Service account key the session makes its gcs requests with",
        &GOOGLE_SERVICE_ACCOUNT_KEY,
        true,
    ),
];

// define_credential_parameters defines every parameter above. The ones holding a credential
// are hidden from SHOW ALL and pg_settings, refused in postgresql.conf, in ALTER SYSTEM and in
// a security restricted operation, and checked by the hook above; note that SHOW <name> and
// current_setting() read a hidden parameter all the same, so hiding it is not keeping it.
pub(crate) fn define_credential_parameters() {
    for (name, description, setting, holds_a_credential) in CREDENTIAL_PARAMETERS {
        let (flags, check_hook) = if *holds_a_credential {
            (
                pgrx::GucFlags::NO_SHOW_ALL
                    | pgrx::GucFlags::DISALLOW_IN_FILE
                    | pgrx::GucFlags::DISALLOW_IN_AUTO_FILE
                    | pgrx::GucFlags::NOT_WHILE_SEC_REST,
                Some(check_credential_parameter as _),
            )
        } else {
            (pgrx::GucFlags::default(), None)
        };

        unsafe {
            pgrx::GucRegistry::define_string_guc_with_hooks(
                name,
                description,
                description,
                setting,
                pgrx::GucContext::Userset,
                flags,
                check_hook,
                None,
                None,
            );
        }
    }
}
