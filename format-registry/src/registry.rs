//! Which formats this backend knows about.
//!
//! Registration happens from a provider's `_PG_init`, which means it happens once per backend
//! per loaded provider and never concurrently — Postgres loads a library under the
//! single-threaded backend. The lock is still a real lock rather than a `thread_local!`,
//! because a provider registered in a leader has to be visible to code running on a tokio
//! worker thread owned by tier 1.

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock, RwLock};

use crate::provider::FormatProvider;
use crate::Result;

type Providers = RwLock<BTreeMap<String, Arc<dyn FormatProvider>>>;

fn providers() -> &'static Providers {
    static PROVIDERS: OnceLock<Providers> = OnceLock::new();

    PROVIDERS.get_or_init(|| RwLock::new(BTreeMap::new()))
}

/// Adds a provider. Fails if the name is taken, rather than replacing: two providers claiming
/// `parquet` in one cluster is a misconfiguration, and silently picking one would make which
/// implementation ran depend on library load order.
pub fn register(provider: Arc<dyn FormatProvider>) -> Result<()> {
    let name = provider.name().to_ascii_lowercase();

    if name.is_empty() {
        return Err("a format provider must have a name".to_string());
    }

    let mut providers = providers()
        .write()
        .map_err(|_| "the format registry is poisoned".to_string())?;

    if providers.contains_key(&name) {
        return Err(format!(
            "a provider for format \"{name}\" is already registered"
        ));
    }

    providers.insert(name, provider);

    Ok(())
}

/// Removes a provider. There is no unload path in Postgres, so this exists for tests and for a
/// provider whose `_PG_init` fails after registering.
pub fn unregister(name: &str) -> bool {
    let Ok(mut providers) = providers().write() else {
        return false;
    };

    providers.remove(&name.to_ascii_lowercase()).is_some()
}

pub fn lookup(name: &str) -> Option<Arc<dyn FormatProvider>> {
    providers()
        .read()
        .ok()?
        .get(&name.to_ascii_lowercase())
        .cloned()
}

/// The provider whose extensions include the one on `path`, if exactly one does.
///
/// Extensions are only an inference, so an ambiguity is not an error here: the caller asks for
/// a format explicitly and gets a clear message, rather than this returning a guess.
/// The extension a format would be inferred from, lower case and without the dot.
///
/// Public because the base loads a provider on demand from it: nothing can be inferred in a
/// session where no provider library has been loaded yet, and the convention is that the
/// library for an extension is named after it.
pub fn inferable_extension(path: &str) -> Option<String> {
    let extension = path.rsplit_once('.')?.1.to_ascii_lowercase();

    // a compressed name like `data.parquet.gz` should still infer parquet
    match extension.as_str() {
        "gz" | "zst" | "zstd" | "lz4" | "snappy" | "bz2" => {
            let stem = path.strip_suffix(&format!(".{extension}"))?;

            Some(stem.rsplit_once('.')?.1.to_ascii_lowercase())
        }
        _ => Some(extension),
    }
}

pub fn lookup_by_path(path: &str) -> Option<Arc<dyn FormatProvider>> {
    let extension = inferable_extension(path)?;

    let providers = providers().read().ok()?;

    let mut matches = providers
        .values()
        .filter(|provider| {
            provider
                .file_extensions()
                .iter()
                .any(|candidate| candidate.eq_ignore_ascii_case(&extension))
        })
        .cloned();

    let first = matches.next()?;

    match matches.next() {
        Some(_) => None,
        None => Some(first),
    }
}

/// Every registered format, by name, with the extensions each claims and the two capabilities
/// that decide which path a statement takes: whether a read can fetch parts of a file and
/// whether a write can go straight into an upload.
pub fn registered() -> Vec<(String, Vec<String>, bool, bool)> {
    let Ok(providers) = providers().read() else {
        return Vec::new();
    };

    providers
        .iter()
        .map(|(name, provider)| {
            (
                name.clone(),
                provider.file_extensions(),
                provider.can_read_ranges(),
                provider.can_stream_write(),
            )
        })
        .collect()
}

pub fn names() -> Vec<String> {
    let Ok(providers) = providers().read() else {
        return Vec::new();
    };

    providers.keys().cloned().collect()
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::SchemaRef;
    use arrow::record_batch::RecordBatchReader;
    use bytes::Bytes;

    use super::*;
    use crate::options::FormatOptions;
    use crate::provider::{FileInfo, RecordBatchSink};

    /// A provider that can do nothing but be registered, which is all these tests ask of one.
    struct Stub {
        name: &'static str,
        extensions: &'static [&'static str],
    }

    impl FormatProvider for Stub {
        fn name(&self) -> String {
            self.name.to_string()
        }

        fn file_extensions(&self) -> Vec<String> {
            self.extensions.iter().map(|e| e.to_string()).collect()
        }

        fn validate_option(&self, _name: &str, _value: &str) -> Result<()> {
            Ok(())
        }

        fn schema(&self, _data: &[u8]) -> Result<SchemaRef> {
            Err("stub".to_string())
        }

        fn kv_metadata(&self, _data: &[u8]) -> Result<Vec<(String, Option<String>)>> {
            Err("stub".to_string())
        }

        fn file_info(&self, _data: &[u8]) -> Result<FileInfo> {
            Err("stub".to_string())
        }

        fn open_reader(
            &self,
            _data: Bytes,
            _options: &FormatOptions,
        ) -> Result<Box<dyn RecordBatchReader + Send>> {
            Err("stub".to_string())
        }

        fn open_writer(
            &self,
            _schema: SchemaRef,
            _options: &FormatOptions,
        ) -> Result<Box<dyn RecordBatchSink>> {
            Err("stub".to_string())
        }
    }

    fn stub(name: &'static str, extensions: &'static [&'static str]) -> Arc<dyn FormatProvider> {
        Arc::new(Stub { name, extensions })
    }

    /// The registry is process-wide, so each test registers under its own names and cleans up
    /// after itself rather than assuming it starts empty.
    struct Registered(&'static [&'static str]);

    impl Drop for Registered {
        fn drop(&mut self) {
            for name in self.0 {
                unregister(name);
            }
        }
    }

    #[test]
    fn a_registered_provider_is_found_by_name_case_insensitively() {
        let _cleanup = Registered(&["t_parquet"]);

        register(stub("T_Parquet", &["tpq"])).expect("registration succeeds");

        assert!(lookup("t_parquet").is_some());
        assert!(lookup("T_PARQUET").is_some());
        assert!(lookup("t_parque").is_none());
    }

    #[test]
    fn a_second_provider_for_a_format_is_refused() {
        let _cleanup = Registered(&["t_dup"]);

        register(stub("t_dup", &["dup"])).expect("the first registration succeeds");

        let error = register(stub("t_dup", &["dup2"])).expect_err("the second is refused");

        // silently replacing would make which implementation ran depend on load order
        assert!(error.contains("already registered"), "{error}");
    }

    #[test]
    fn a_format_is_inferred_from_an_extension_including_through_a_compression_suffix() {
        let _cleanup = Registered(&["t_infer"]);

        register(stub("t_infer", &["tinf"])).expect("registration succeeds");

        for path in [
            "/tmp/data.tinf",
            "s3://bucket/dir/data.TINF",
            "/tmp/data.tinf.gz",
            "/tmp/data.tinf.zst",
        ] {
            assert!(
                lookup_by_path(path).is_some(),
                "{path} should infer t_infer"
            );
        }

        for path in ["/tmp/data.other", "/tmp/data", "/tmp/tinf"] {
            assert!(
                lookup_by_path(path).is_none(),
                "{path} should infer nothing"
            );
        }
    }

    #[test]
    fn an_extension_two_providers_claim_infers_neither() {
        let _cleanup = Registered(&["t_first", "t_second"]);

        register(stub("t_first", &["tshared"])).expect("registration succeeds");
        register(stub("t_second", &["tshared"])).expect("registration succeeds");

        // an ambiguity is not an error here: the caller names the format explicitly and gets a
        // clear message, rather than this returning a guess
        assert!(lookup_by_path("/tmp/data.tshared").is_none());
    }
}
