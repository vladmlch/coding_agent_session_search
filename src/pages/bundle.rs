//! Bundle builder for pages export.
//!
//! Creates the local static site bundle (site/) and private offline artifacts (private/)
//! from an export. Output is a self-contained archive opened locally or via
//! `cass pages --preview` (loopback only).

mod assets;
mod builder;
mod integrity;
mod private_artifacts;
mod publish;
mod publish_journal;

#[cfg(test)]
mod tests;

pub use builder::{
    BundleBuilder, BundleConfig, BundleResult, SiteMetadata, UnencryptedConfig, UnencryptedPayload,
};
pub use integrity::{IntegrityEntry, IntegrityManifest};

pub(crate) use assets::validate_pinned_vendor_assets;
#[cfg(test)]
pub(crate) use assets::{PAGES_VENDOR_ASSETS, PinnedVendorAsset, write_pinned_vendor_assets};
#[cfg(test)]
pub(crate) use integrity::{compute_fingerprint, generate_integrity_manifest};
