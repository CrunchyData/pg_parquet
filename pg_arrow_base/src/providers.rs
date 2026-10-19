//! Which format a statement means, and getting that provider loaded.

use std::ffi::CStr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use format_registry::provider::FormatProvider;
use format_registry::registry;
use pgrx::pg_sys::{load_file, my_exec_path, pkglib_path, AsPgCStr, MAXPGPATH};
use pgrx::prelude::*;

/// The library a format is expected to live in. A provider is free to be called anything, but
/// a predictable name is what lets the base load one on demand instead of requiring every
/// format to be preloaded.
fn library_for(format: &str) -> String {
    format!("$libdir/pg_{format}")
}

/// Whether a format's library is installed, asked of the filesystem rather than by loading it.
///
/// Recognition has to decide whether a statement is this stack's before anything is loaded, and
/// being wrong either way costs something: declining a `.parquet` uri hands it to core `COPY`,
/// which writes text under that name, and claiming every unknown extension would dlopen a
/// guessed library for somebody else's `COPY ... FROM '/tmp/x.csv'`.
pub(crate) fn library_exists(format: &str) -> bool {
    if format.contains('/') || format.contains('$') || format.is_empty() {
        return false;
    }

    library_file_exists(&library_for(format))
}

/// Whether a `$libdir/name` library is installed. Asked before every load: `load_file` writes
/// its failure to the server log before raising it, so a guess that is wrong would leave a
/// "could not access file" behind for a statement that is nobody's problem.
fn library_file_exists(library: &str) -> bool {
    let Some(name) = library.strip_prefix("$libdir/") else {
        return false;
    };

    if name.contains('/') || name.is_empty() {
        return false;
    }

    // the global is a flexible array, so it is read through a raw pointer rather than borrowed
    let dir = unsafe { CStr::from_ptr((&raw const pkglib_path) as *const std::ffi::c_char) };

    let Ok(dir) = dir.to_str() else {
        return false;
    };

    // which suffix a platform uses is `DLSUFFIX`, which is not in the bindings; both are
    // cheaper to test than to thread through
    ["so", "dylib"]
        .iter()
        .any(|suffix| Path::new(dir).join(format!("{name}.{suffix}")).exists())
}

/// Loads a library, reporting whether it loaded rather than raising. Nothing unloads a
/// provider, so a second call for one already loaded is a no-op inside Postgres.
fn load(library: &str) -> bool {
    PgTryBuilder::new(|| {
        unsafe { load_file(library.as_pg_cstr(), false) };
        true
    })
    .catch_others(|_| false)
    .execute()
}

/// Loads the provider for a format if this backend has not already.
///
/// A provider registers from its `_PG_init`, so loading the library is registration. The
/// guessed library name is a convenience, not a contract: if it is wrong, nothing registers,
/// and the caller reports the format as unknown — which is the same message as for a genuine
/// typo, and the right one, because from the user's side those are the same mistake.
fn try_load(format: &str) -> Option<Arc<dyn FormatProvider>> {
    if format.contains('/') || format.contains('$') {
        // a format name is an identifier, not a path; refusing one that looks like a path
        // keeps `WITH (format '../../evil')` from naming a library
        return None;
    }

    let library = library_for(format);

    if !library_file_exists(&library) {
        return None;
    }

    // `load_file` ereports if the library is there but will not load, and that is not an error
    // yet — the caller still has an unknown-format message to produce, with the full list of
    // what *is* registered, which is far more useful than "could not access file"
    let loaded = load(&library);

    if !loaded {
        return None;
    }

    registry::lookup(format)
}

/// Whether every installed provider has already been given its chance to register in this
/// backend, so that the discovery below is a once-per-backend cost rather than a per-statement
/// one.
static DISCOVERED: AtomicBool = AtomicBool::new(false);

/// What a provider's control file names in its `requires` to say it is one.
const BASE_EXTENSION: &str = "pg_arrow_base";

/// Where the control file of every installed extension is.
fn extension_dir() -> Option<PathBuf> {
    let mut buffer = [0 as std::ffi::c_char; MAXPGPATH as usize];

    // both are flexible arrays in the bindings, so they are read and written through raw
    // pointers rather than borrowed
    unsafe {
        pgrx::pg_sys::get_share_path(
            (&raw const my_exec_path) as *const std::ffi::c_char,
            buffer.as_mut_ptr(),
        );

        Some(
            Path::new(CStr::from_ptr(buffer.as_ptr()).to_str().ok()?)
                .join("extension")
                .to_path_buf(),
        )
    }
}

/// The library of every installed extension that declares itself built on this one, read off
/// the control files rather than guessed from a name.
fn installed_provider_libraries() -> Vec<String> {
    match extension_dir() {
        Some(dir) => provider_libraries_in(&dir),
        None => vec![],
    }
}

/// The parsing half, given the directory to read, so that it can be tested on control files
/// that are not the ones this cluster happens to have installed.
fn provider_libraries_in(dir: &Path) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return vec![];
    };

    let mut libraries = vec![];

    for entry in entries.flatten() {
        let path = entry.path();

        if path.extension().is_none_or(|e| e != "control") {
            continue;
        }

        let Ok(control) = std::fs::read_to_string(&path) else {
            continue;
        };

        // `requires` is what a provider uses to say it is one; the base itself and anything
        // unrelated is left alone, so discovery never loads somebody else's library
        if !control
            .lines()
            .filter(|line| line.trim_start().starts_with("requires"))
            .any(|line| line.contains(BASE_EXTENSION))
        {
            continue;
        }

        // the control file names the library outright, which is the point of reading it: the
        // name is not derivable from the extensions a provider claims
        let library = control
            .lines()
            .find_map(|line| {
                let (key, value) = line.split_once('=')?;

                (key.trim() == "module_pathname").then(|| value.trim().trim_matches('\'').trim())
            })
            .map(|pathname| pathname.to_string())
            .or_else(|| Some(format!("$libdir/{}", path.file_stem()?.to_str()?)));

        libraries.extend(library);
    }

    libraries
}

/// Loads every installed provider, so that a format can be found by something other than the
/// convention that its library is named after it.
///
/// That convention holds for the format a provider is named for and not for the others it
/// claims: `.arrows`, `.ipc` and `.feather` are all pg_arrow's, and there is no `pg_arrows`
/// library to guess. Declining them is how a `COPY t TO '/tmp/t.arrows'` reached core `COPY`,
/// which wrote a *text* file under that name. Runs at most once per backend.
fn discover_providers() {
    if DISCOVERED.swap(true, Ordering::Relaxed) {
        return;
    }

    for library in installed_provider_libraries() {
        // the same tolerance as `try_load`: a control file whose library is missing or will not
        // load leaves the caller with its unknown-format message, which is the more useful one
        if library_file_exists(&library) {
            load(&library);
        }
    }
}

/// The provider a statement means: the format it named, else the one the session's
/// `arrow.default_format` names, else the one the uri's extension implies.
///
/// An explicit format always wins over an inferred one, and an inferred format is never
/// guessed from more than the extension — a uri is not fetched to find out what it holds,
/// because that would make a syntax error cost a network round trip.
pub(crate) fn resolve(format: Option<&str>, uri: &str) -> Arc<dyn FormatProvider> {
    if let Some(format) = format {
        let format = format.to_ascii_lowercase();

        return registry::lookup(&format)
            .or_else(|| try_load(&format))
            .or_else(|| load_installed(&format))
            .unwrap_or_else(|| unknown_format(&format));
    }

    if let Some(default) = crate::default_format() {
        return registry::lookup(&default)
            .or_else(|| try_load(&default))
            .or_else(|| load_installed(&default))
            .unwrap_or_else(|| unknown_format(&default));
    }

    registry::lookup_by_path(uri)
        .or_else(|| load_by_path(uri))
        .unwrap_or_else(|| {
            ereport!(
                PgLogLevel::ERROR,
                PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
                format!("could not tell what format {uri} is in"),
                format!(
                    "Name it with the format option, or set arrow.default_format. {}",
                    registered_formats_hint()
                )
            );

            unreachable!("ereport at ERROR does not return");
        })
}

/// Loads the provider a uri's extension implies, for a session that has not loaded one.
///
/// Inference asks the registry which format claims an extension, so in a backend where no
/// provider library has been loaded there is nothing to ask — which is every first statement
/// in a cluster that does not preload them. The library is guessed from the extension by the
/// same convention as from a format name; when it is wrong, every installed provider is asked
/// instead.
pub(crate) fn load_by_path(uri: &str) -> Option<Arc<dyn FormatProvider>> {
    let extension = registry::inferable_extension(uri)?;

    try_load(&extension);

    if let Some(provider) = registry::lookup_by_path(uri) {
        return Some(provider);
    }

    discover_providers();

    registry::lookup_by_path(uri)
}

/// Gives every installed provider the chance to register, for a caller that reports what
/// formats there are rather than uses one: before discovery, a session that happened not to
/// have read a parquet file yet was told there was no such format.
pub(crate) fn load_installed_providers() {
    discover_providers();
}

/// The provider for a format, after giving every installed provider the chance to register.
fn load_installed(format: &str) -> Option<Arc<dyn FormatProvider>> {
    discover_providers();

    registry::lookup(format)
}

fn unknown_format(format: &str) -> ! {
    ereport!(
        PgLogLevel::ERROR,
        PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
        format!("there is no provider for format \"{format}\""),
        format!(
            "A format comes from its own extension -- parquet from pg_parquet, arrow from \
             pg_arrow. {}",
            registered_formats_hint()
        )
    );

    unreachable!("ereport at ERROR does not return");
}

/// Phrased for an `errhint`, and careful to say *nothing is registered* rather than listing an
/// empty set, because an empty registry means no provider extension is installed at all and
/// that is the thing worth saying.
fn registered_formats_hint() -> String {
    let mut names = registry::names();

    // an empty registry here is as likely to mean nothing has been loaded yet as that nothing
    // is installed, and this is an error path already, so it is worth finding out which
    if names.is_empty() {
        discover_providers();

        names = registry::names();
    }

    if names.is_empty() {
        return "No format providers are registered in this session; arrow.formats() lists \
                them."
            .to_string();
    }

    format!("Registered formats: {}.", names.join(", "))
}

// no backend needed: discovery is a directory read and some control file parsing
#[cfg(test)]
mod tests {
    use super::provider_libraries_in;

    #[test]
    fn test_a_provider_is_discovered_by_what_its_control_file_requires() {
        let dir = std::path::Path::new("/tmp/pg_arrow_base_control_files");

        std::fs::remove_dir_all(dir).ok();
        std::fs::create_dir_all(dir).expect("failed to make the directory");

        let write = |name: &str, body: &str| {
            std::fs::write(dir.join(name), body).expect("failed to write the control file")
        };

        // a provider: discovered, and by the library it names rather than by its own name,
        // which is the whole reason the file is read
        write(
            "pg_arrow.control",
            "comment = 'arrow'\nmodule_pathname = '$libdir/pg_arrow'\nrequires = 'pg_arrow_base'\n",
        );

        // a provider that names no library falls back to being named after itself
        write("pg_nameless.control", "requires = 'pg_arrow_base'\n");

        // not a provider, and so not something to dlopen on anybody's behalf
        write(
            "pg_stranger.control",
            "module_pathname = '$libdir/pg_stranger'\nrequires = 'postgis'\n",
        );
        write("pg_alone.control", "comment = 'no requires at all'\n");

        // the base itself requires nothing of itself, and the sql files beside it are not
        // control files
        write("pg_arrow_base--0.1.0.sql", "requires pg_arrow_base\n");

        let mut libraries = provider_libraries_in(dir);
        libraries.sort();

        assert_eq!(libraries, ["$libdir/pg_arrow", "$libdir/pg_nameless"]);

        std::fs::remove_dir_all(dir).ok();
    }
}
