use std::ffi::CString;

use object_store::ObjectStoreScheme;
use pgrx::{ereport, pg_sys, pg_sys::superuser, GucSetting, PgLogLevel, PgSqlErrorCode, Spi};

// PROFILE_NAME names the credentials the session makes its object store requests with. It
// is unset by default, which reads the environment of the server and the config files of
// the cloud sdks, i.e. the one set of credentials every session of the server shares.
//
// Any role may set it on purpose. The name of a profile is not a secret, so a session is
// free to say which one it wants, and parquet.user_profile_mapping, which only
// parquet_profile_manager may write, is what says which ones it may have.
pub(crate) static PROFILE_NAME: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

// GCS_CREDENTIAL_DIRECTORY is the directory a profile's gcs service account key is read
// from, as <directory>/<profile>.json. s3 and azure each have a config file with a section
// per profile to read instead, which gcs has none of, so a gcs profile names a file.
pub(crate) static GCS_CREDENTIAL_DIRECTORY: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

// a profile name becomes a part of a file name and of a config file's section name, so it
// is as long as a Postgres identifier and made of what cannot be read as a path
const MAX_PROFILE_NAME_LEN: usize = 63;

// current_profile is the profile the session selected for the scheme of a uri, after
// checking that the session is mapped to it. It is None for a session that selected none,
// which is the credentials of the server's environment, and None for a store that has no
// credentials for a profile to name, which a local path and an http(s) endpoint have not.
pub(crate) fn current_profile(scheme: &ObjectStoreScheme) -> Option<String> {
    let profile = guc_profile_name()?;

    ensure_profile_name_is_a_name(&profile);

    // a local path and an http(s) endpoint are read with no credentials at all, so there
    // is no identity for a profile to pick between and nothing to refuse
    let scheme_name = scheme_name(scheme)?;

    ensure_session_is_mapped_to_profile(scheme_name, &profile);

    Some(profile)
}

// guc_profile_name is the profile the session named, where unset and empty are both no
// profile: a GUC that is reset reads as the empty string rather than as unset once it has
// been set in the session.
fn guc_profile_name() -> Option<String> {
    let profile = PROFILE_NAME.get()?;

    let profile = profile
        .to_str()
        .unwrap_or_else(|e| panic!("pg_parquet.profile_name is not valid utf-8: {e}"));

    if profile.is_empty() {
        return None;
    }

    Some(profile.to_string())
}

// scheme_name is the name the profiles of a scheme are mapped under, for a scheme whose
// store is reached with credentials that a profile can name.
fn scheme_name(scheme: &ObjectStoreScheme) -> Option<&'static str> {
    match scheme {
        ObjectStoreScheme::AmazonS3 => Some("s3"),
        ObjectStoreScheme::MicrosoftAzure => Some("az"),
        ObjectStoreScheme::GoogleCloudStorage => Some("gs"),
        _ => None,
    }
}

// ensure_profile_name_is_a_name refuses a profile name that is not one. The name becomes a
// part of the path of a gcs service account key and of the name of an azure config file's
// section, so a name carrying a separator or a parent directory would name a file of the
// server that the mapping never mentioned.
fn ensure_profile_name_is_a_name(profile: &str) {
    let is_a_name = profile.len() <= MAX_PROFILE_NAME_LEN
        && profile
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
        && !profile.starts_with('.')
        && !profile.contains("..");

    if !is_a_name {
        ereport!(
            PgLogLevel::ERROR,
            PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
            format!("pg_parquet.profile_name \"{profile}\" is not a profile name"),
            format!(
                "A profile name is at most {MAX_PROFILE_NAME_LEN} ascii letters, digits, \
                 underscores, dashes and dots, and it neither begins with a dot nor carries \
                 two of them in a row, since the name is read as a part of a path."
            ),
        );
    }
}

// ensure_session_is_mapped_to_profile refuses a profile the session is not mapped to, which
// is what makes a GUC that any role may set the way to select one. The mapping is read on
// every copy rather than when the GUC was set: a mapping that is taken away then stops the
// next copy, and a GUC a role carries in pg_db_role_setting grants nothing by itself.
fn ensure_session_is_mapped_to_profile(scheme_name: &str, profile: &str) {
    // a superuser reads the credentials of every profile from the files of the server
    if unsafe { superuser() } {
        return;
    }

    // the workers of a parallel copy open stores of their own, with the profile and the
    // user of the leader that already read the mapping for both, so the mapping is read
    // once per copy rather than once per worker
    if unsafe { pg_sys::ParallelWorkerNumber } >= 0 {
        return;
    }

    let mapped = Spi::connect(|client| {
        client
            .select(
                "SELECT parquet.profile_is_allowed($1, $2)",
                Some(1),
                &[scheme_name.into(), profile.into()],
            )?
            .first()
            .get_one::<bool>()
    })
    .unwrap_or_else(|e| panic!("failed to read the profile mapping: {e}"))
    .unwrap_or(false);

    if !mapped {
        ereport!(
            PgLogLevel::ERROR,
            PgSqlErrorCode::ERRCODE_INSUFFICIENT_PRIVILEGE,
            format!("permission denied for profile \"{profile}\""),
            format!(
                "parquet.user_profile_mapping does not map the session to the profile for a \
                 {scheme_name} store. Only roles with privileges of the \
                 \"parquet_profile_manager\" role map a profile to a role."
            ),
        );
    }
}

// gcs_service_account_path is the file a profile's gcs service account key is read from.
// The profile is a name rather than a path, so the directory the keys are kept in is the
// one a superuser set, which is what keeps a session to the keys that were put there.
pub(crate) fn gcs_service_account_path(profile: &str) -> String {
    let directory = GCS_CREDENTIAL_DIRECTORY
        .get()
        .map(|directory| {
            directory
                .to_str()
                .unwrap_or_else(|e| {
                    panic!("pg_parquet.gcs_credential_directory is not valid utf-8: {e}")
                })
                .to_string()
        })
        .filter(|directory| !directory.is_empty());

    match directory {
        Some(directory) => format!("{}/{profile}.json", directory.trim_end_matches('/')),
        None => {
            ereport!(
                PgLogLevel::ERROR,
                PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
                format!(
                    "pg_parquet.gcs_credential_directory is not set, so the profile \
                     \"{profile}\" names no service account key"
                ),
                "A gcs profile is read from <pg_parquet.gcs_credential_directory>/<profile>\
                 .json, so a superuser sets the directory the keys of the profiles are kept \
                 in before a session may name one."
            );

            unreachable!("a gcs profile was resolved without a credential directory");
        }
    }
}
