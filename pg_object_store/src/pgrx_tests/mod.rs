//! The extension's tests, grouped by what they are about.
//!
//! None of them reach a cloud provider. What a provider's own client does with a credential is
//! that client's contract and is covered by `object_store`'s tests; what this extension owns is
//! which store a uri resolves to, who may reach it, and how long a resolved store is kept —
//! and all three can be proven against a local path, an unreachable host, or the cache itself.

mod cache;
mod common;
mod multipart;
mod privileges;
mod udfs;
mod vtable;
