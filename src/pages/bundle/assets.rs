use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::Path;

use super::integrity::build_integrity_entry;

/// Files embedded from pages_assets at compile time
pub(super) const PAGES_ASSETS: &[(&str, &[u8])] = &[
    (
        "index.html",
        include_bytes!("../../pages_assets/index.html"),
    ),
    (
        "styles.css",
        include_bytes!("../../pages_assets/styles.css"),
    ),
    ("auth.js", include_bytes!("../../pages_assets/auth.js")),
    ("viewer.js", include_bytes!("../../pages_assets/viewer.js")),
    ("router.js", include_bytes!("../../pages_assets/router.js")),
    ("share.js", include_bytes!("../../pages_assets/share.js")),
    ("stats.js", include_bytes!("../../pages_assets/stats.js")),
    (
        "storage.js",
        include_bytes!("../../pages_assets/storage.js"),
    ),
    ("search.js", include_bytes!("../../pages_assets/search.js")),
    (
        "conversation.js",
        include_bytes!("../../pages_assets/conversation.js"),
    ),
    (
        "database.js",
        include_bytes!("../../pages_assets/database.js"),
    ),
    (
        "session.js",
        include_bytes!("../../pages_assets/session.js"),
    ),
    ("sw.js", include_bytes!("../../pages_assets/sw.js")),
    (
        "sw-register.js",
        include_bytes!("../../pages_assets/sw-register.js"),
    ),
    (
        "virtual-list.js",
        include_bytes!("../../pages_assets/virtual-list.js"),
    ),
    (
        "coi-detector.js",
        include_bytes!("../../pages_assets/coi-detector.js"),
    ),
    (
        "attachments.js",
        include_bytes!("../../pages_assets/attachments.js"),
    ),
    (
        "settings.js",
        include_bytes!("../../pages_assets/settings.js"),
    ),
];

/// Immutable third-party runtime files embedded in every Pages bundle.
///
/// These are exact bytes from the upstream releases recorded in
/// `pages_assets/vendor/manifest.json`. Keep the byte include, size, SHA-256,
/// manifest record, and license in lockstep when updating a dependency.
#[derive(Debug, Clone, Copy)]
pub struct PinnedVendorAsset {
    pub path: &'static str,
    pub bytes: &'static [u8],
    pub size: u64,
    pub sha256: &'static str,
}

macro_rules! pinned_vendor_asset {
    ($path:literal, $size:literal, $sha256:literal) => {
        PinnedVendorAsset {
            path: concat!("vendor/", $path),
            bytes: include_bytes!(concat!("../../pages_assets/vendor/", $path)),
            size: $size,
            sha256: $sha256,
        }
    };
}

pub const PAGES_VENDOR_ASSETS: &[PinnedVendorAsset] = &[
    pinned_vendor_asset!(
        "sqlite3.mjs",
        578_559,
        "f80870f0fa03a39a3338d17ed3fbea04808d344c88e724d90d5f37b9b7b83154"
    ),
    pinned_vendor_asset!(
        "sqlite3.wasm",
        864_752,
        "02d7e48164395fa68f81c6ec33e9da5461be397dc57602ac0cd89b4bbba1d312"
    ),
    pinned_vendor_asset!(
        "sqlite3-opfs-async-proxy.js",
        32_289,
        "502762db7bd24130f345df6a42883166ec0a91815207940328c9739bb0f5ec2f"
    ),
    pinned_vendor_asset!(
        "LICENSE-sqlite-wasm.txt",
        11_358,
        "cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30"
    ),
    pinned_vendor_asset!(
        "manifest.json",
        1_776,
        "09f68c8e09427ecb21b23032aaa744aca0286aed67d2981b45a66aee8e49561a"
    ),
];

pub(super) fn validate_embedded_vendor_asset(asset: &PinnedVendorAsset) -> Result<()> {
    let actual_size = u64::try_from(asset.bytes.len()).context("Vendor asset size overflow")?;
    if actual_size != asset.size {
        bail!(
            "Embedded Pages vendor asset {} has size {}, expected {}",
            asset.path,
            actual_size,
            asset.size
        );
    }

    let actual_sha256 = hex::encode(Sha256::digest(asset.bytes));
    if actual_sha256 != asset.sha256 {
        bail!(
            "Embedded Pages vendor asset {} has SHA-256 {}, expected {}",
            asset.path,
            actual_sha256,
            asset.sha256
        );
    }

    Ok(())
}

/// Materialize the exact self-hosted runtime dependency set into `site/vendor`.
///
/// The embedded bytes are checked against independent hard-coded pins before
/// any file is written. This turns accidental vendor replacement into a build
/// failure instead of silently publishing unreviewed executable code.
pub fn write_pinned_vendor_assets(site_dir: &Path) -> Result<()> {
    for asset in PAGES_VENDOR_ASSETS {
        validate_embedded_vendor_asset(asset)?;
    }

    fs::create_dir_all(site_dir.join("vendor"))
        .context("Failed to create site/vendor directory")?;
    for asset in PAGES_VENDOR_ASSETS {
        fs::write(site_dir.join(asset.path), asset.bytes)
            .with_context(|| format!("Failed to write Pages vendor asset {}", asset.path))?;
    }

    Ok(())
}

/// Verify that a materialized bundle contains every pinned vendor file as an
/// exact regular-file byte match.
pub fn validate_pinned_vendor_assets(site_dir: &Path) -> Result<()> {
    for asset in PAGES_VENDOR_ASSETS {
        let path = site_dir.join(asset.path);
        let metadata = fs::symlink_metadata(&path)
            .with_context(|| format!("Missing Pages vendor asset {}", asset.path))?;
        if !metadata.file_type().is_file() {
            bail!("Pages vendor asset {} is not a regular file", asset.path);
        }
        if metadata.len() != asset.size {
            bail!(
                "Pages vendor asset {} has size {}, expected {}",
                asset.path,
                metadata.len(),
                asset.size
            );
        }

        let entry = build_integrity_entry(&path)
            .with_context(|| format!("Failed to hash Pages vendor asset {}", asset.path))?;
        if entry.sha256 != asset.sha256 {
            bail!(
                "Pages vendor asset {} has SHA-256 {}, expected {}",
                asset.path,
                entry.sha256,
                asset.sha256
            );
        }
    }

    Ok(())
}
