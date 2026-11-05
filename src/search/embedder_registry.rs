//! Explicit provider selection over the unchanged local model registry.
//!
//! The static registry describes only installed local models. The external
//! HTTP embedding provider was removed by the local-only conversion: `external`
//! is no longer a selectable provider name, legacy `CASS_EXTERNAL_*`
//! configuration is ignored, and stored external vector spaces are diagnosed
//! as [`crate::search::semantic_manifest::LEGACY_EXTERNAL_EMBEDDING_SPACE`]
//! without being loaded.

use std::path::Path;
use std::sync::Arc;

// Preserve the local registry and all its regression tests byte-for-byte in a
// child module. These aliases retain its existing sibling imports.
use super::{embedder, fastembed_embedder, hash_embedder};
use embedder::{Embedder, EmbedderInfo, EmbedderResult};

#[path = "embedder_registry/local.rs"]
mod local;

pub use local::{
    BAKEOFF_ELIGIBILITY_CUTOFF, DEFAULT_EMBEDDER, EMBEDDERS, EmbedderRegistry, HASH_EMBEDDER,
    REQUIRED_NATIVE_MODEL_FILES, RegisteredEmbedder,
};

/// Validate a named selection without constructing a provider. Legacy names
/// such as `external` are simply unknown embedders: the removed HTTP provider
/// is never selected, and its configuration is never consulted.
pub fn validate_selection(data_dir: &Path, name: &str) -> EmbedderResult<()> {
    EmbedderRegistry::new(data_dir).validate(name).map(|_| ())
}

/// Load the explicitly selected provider. Missing local assets are errors,
/// never reasons to switch to another vector space.
pub fn get_embedder(data_dir: &Path, name: Option<&str>) -> EmbedderResult<Arc<dyn Embedder>> {
    local::get_embedder(data_dir, name)
}

/// Metadata inspection never constructs a provider.
pub fn get_embedder_info(data_dir: &Path, name: Option<&str>) -> Option<EmbedderInfo> {
    local::get_embedder_info(data_dir, name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn removed_external_name_is_just_an_unknown_local_embedder() {
        let tmp = tempfile::tempdir().unwrap();
        // `external` resolves through the ordinary registry: nothing selects an
        // HTTP provider, and the ordinary unknown-embedder error lists only
        // the local choices.
        let error = validate_selection(tmp.path(), "external").unwrap_err();
        let text = error.to_string();
        assert!(text.contains("unknown embedder"), "{text}");
        assert!(text.contains("minilm"), "{text}");
        assert!(text.contains("hash"), "{text}");
        assert!(!text.contains("external-"), "{text}");
        // Identity-shaped strings were never consent either; now they are just
        // unrecognized names.
        let identity = format!("external-v1-384-{}", "a".repeat(64));
        assert!(validate_selection(tmp.path(), &identity).is_err());
    }

    #[test]
    fn local_factory_serves_only_registered_local_providers() {
        let tmp = tempfile::tempdir().unwrap();
        let hash = get_embedder(tmp.path(), Some("hash")).unwrap();
        assert_eq!(hash.id(), "fnv1a-384");
        assert_eq!(hash.embed_sync("private session text").unwrap().len(), 384);
        for name in [None, Some("minilm"), Some("external"), Some("unknown")] {
            assert!(
                get_embedder(tmp.path(), name).is_err(),
                "{name:?} unexpectedly selected a provider"
            );
        }
    }
}
