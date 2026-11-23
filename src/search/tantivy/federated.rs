use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use anyhow::{Context, Result};
use frankensearch::lexical_tantivy::Index;
use frankensearch::quill::cass::CASS_SCHEMA_HASH;
use serde::{Deserialize, Serialize};

use crate::evidence_bundle::{
    EVIDENCE_BUNDLE_MANIFEST_FILE, EvidenceBundleChunk, EvidenceBundleChunkRole,
    EvidenceBundleKind, EvidenceBundleManifest,
};

use super::legacy_merge::ensure_empty_merge_output_directory;
use super::{Fields, TantivyIndex, cass_field_handles, ensure_tokenizer};

pub(super) const FEDERATED_SEARCH_MANIFEST_FILE: &str = "federated-search-manifest.json";
pub(super) const FEDERATED_SEARCH_MANIFEST_VERSION: u32 = 1;
pub(super) const FEDERATED_SEARCH_MANIFEST_KIND: &str = "cass-federated-lexical-index";
pub(super) const EVIDENCE_BUNDLE_MANIFEST_TEMP_FILE: &str = "evidence-bundle-manifest.json.tmp";

/// Cass-owned sidecar metadata files that live alongside the Tantivy
/// artifacts in the published lexical index directory (path helpers:
/// `lexical_rebuild_state_path` / `lexical_refresh_ledger_path` /
/// `lexical_rebuild_equivalence_evidence_path` in `crate::indexer` and
/// `LEXICAL_GENERATION_MANIFEST_FILE` in `crate::indexer::lexical_generation`).
///
/// `materialize_federated_search_bundle_for_write` must carry these over when
/// it flattens a federated shard bundle back into a single mutable index:
/// the materialized directory atomically replaces the live one, and dropping
/// the just-persisted `.lexical-rebuild-state.json` checkpoint sent every
/// subsequent search into the "lexical rebuild checkpoint missing" WARN +
/// repeated-authoritative-rebuild loop (#289). The federated manifest itself
/// is intentionally absent here: after materialization the bundle is no
/// longer federated.
pub(super) const LEXICAL_SIDECAR_FILES_CARRIED_ACROSS_MATERIALIZATION: &[&str] = &[
    ".lexical-rebuild-state.json",
    ".lexical-refresh-ledger.json",
    "lexical-generation-manifest.json",
    ".lexical-rebuild-equivalence.json",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchableIndexSummary {
    pub docs: usize,
    pub segments: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct FederatedSearchManifest {
    pub(super) version: u32,
    pub(super) kind: String,
    pub(super) schema_hash: String,
    pub(super) shards: Vec<FederatedSearchShardManifest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct FederatedSearchShardManifest {
    pub(super) relative_path: String,
    pub(super) docs: usize,
    pub(super) segments: usize,
    pub(super) meta_fingerprint: String,
}

pub fn federated_search_manifest_path(index_path: &Path) -> PathBuf {
    index_path.join(FEDERATED_SEARCH_MANIFEST_FILE)
}

/// Write the schema-generation sentinel.
///
/// Deliberately a plain `fs::write` rather than a temp-file-plus-rename. A crash
/// mid-write leaves a truncated file, which `current_schema_hash_file_matches`
/// fails to parse and `wipe_index_dir_on_schema_hash_mismatch` therefore treats
/// as a mismatch — the index is discarded and rebuilt. That is the FAIL-SAFE
/// direction: the cost of a torn sentinel is a rebuild, never a stale-schema
/// index read as current. Making it atomic would only save an unnecessary
/// rebuild in a rare crash window, and the atomic helpers in this tree are
/// private to other modules.
pub(super) fn write_root_schema_hash_file(index_path: &Path) -> Result<()> {
    fs::write(
        index_path.join("schema_hash.json"),
        format!("{{\"schema_hash\":\"{CASS_SCHEMA_HASH}\"}}"),
    )
    .with_context(|| {
        format!(
            "writing cass schema hash metadata for searchable index {}",
            index_path.display()
        )
    })?;
    Ok(())
}

fn manifest_relative_shard_path(shard_idx: usize) -> String {
    format!("shards/shard-{shard_idx:05}")
}

pub(super) fn meta_fingerprint_for_existing_index_dir(index_path: &Path) -> Result<String> {
    let quill_manifest = index_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER);
    match fs::read(&quill_manifest) {
        Ok(bytes) => return Ok(blake3::hash(&bytes).to_hex().to_string()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            return Err(err).with_context(|| {
                format!("reading Quill index manifest {}", quill_manifest.display())
            });
        }
    }
    let meta_path = index_path.join("meta.json");
    let bytes = fs::read(&meta_path)
        .with_context(|| format!("reading Tantivy meta file {}", meta_path.display()))?;
    Ok(blake3::hash(&bytes).to_hex().to_string())
}

fn validate_federated_shard_relative_path(relative_path: &str) -> Result<()> {
    if relative_path.trim().is_empty() {
        return Err(anyhow::anyhow!(
            "federated lexical shard path must not be empty"
        ));
    }

    let path = Path::new(relative_path);
    let mut components = path.components();
    match components.next() {
        Some(std::path::Component::Normal(component))
            if component == std::ffi::OsStr::new("shards") => {}
        _ => {
            return Err(anyhow::anyhow!(
                "federated lexical shard path must stay under shards/: {}",
                relative_path
            ));
        }
    }

    let mut has_child = false;
    for component in components {
        match component {
            std::path::Component::Normal(_) => has_child = true,
            _ => {
                return Err(anyhow::anyhow!(
                    "federated lexical shard path must be a clean relative path: {}",
                    relative_path
                ));
            }
        }
    }

    if !has_child {
        return Err(anyhow::anyhow!(
            "federated lexical shard path must name a shard directory under shards/: {}",
            relative_path
        ));
    }

    Ok(())
}

fn validate_federated_shard_meta_fingerprint(fingerprint: &str) -> Result<()> {
    if fingerprint.len() != 64 || !fingerprint.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow::anyhow!(
            "federated lexical shard meta fingerprint must be a 64-character hex BLAKE3 digest"
        ));
    }
    Ok(())
}

fn federated_search_manifest_summary(
    index_path: &Path,
    manifest: &FederatedSearchManifest,
) -> Result<SearchableIndexSummary> {
    let mut docs = 0usize;
    let mut segments = 0usize;
    for shard in &manifest.shards {
        docs = docs.checked_add(shard.docs).with_context(|| {
            format!(
                "federated search manifest doc count overflows platform usize: {}",
                index_path.display()
            )
        })?;
        segments = segments.checked_add(shard.segments).with_context(|| {
            format!(
                "federated search manifest segment count overflows platform usize: {}",
                index_path.display()
            )
        })?;
    }
    Ok(SearchableIndexSummary { docs, segments })
}

fn validate_federated_search_manifest(
    index_path: &Path,
    manifest: &FederatedSearchManifest,
    verify_shard_fingerprints: bool,
) -> Result<()> {
    if manifest.version != FEDERATED_SEARCH_MANIFEST_VERSION {
        return Err(anyhow::anyhow!(
            "unsupported federated search manifest version: expected {}, got {}",
            FEDERATED_SEARCH_MANIFEST_VERSION,
            manifest.version
        ));
    }
    if manifest.kind != FEDERATED_SEARCH_MANIFEST_KIND {
        return Err(anyhow::anyhow!(
            "unexpected federated search manifest kind: expected {}, got {}",
            FEDERATED_SEARCH_MANIFEST_KIND,
            manifest.kind
        ));
    }
    if manifest.schema_hash != CASS_SCHEMA_HASH {
        return Err(anyhow::anyhow!(
            "federated search manifest schema mismatch: expected {}, got {}",
            CASS_SCHEMA_HASH,
            manifest.schema_hash
        ));
    }
    if manifest.shards.is_empty() {
        return Err(anyhow::anyhow!(
            "federated search manifest must contain at least one shard"
        ));
    }

    let mut seen_relative_paths = BTreeSet::new();
    for shard in &manifest.shards {
        validate_federated_shard_relative_path(&shard.relative_path)?;
        validate_federated_shard_meta_fingerprint(&shard.meta_fingerprint)?;
        if !seen_relative_paths.insert(shard.relative_path.clone()) {
            return Err(anyhow::anyhow!(
                "federated search manifest contains duplicate shard path: {}",
                shard.relative_path
            ));
        }

        if verify_shard_fingerprints {
            let shard_path = index_path.join(&shard.relative_path);
            let actual = meta_fingerprint_for_existing_index_dir(&shard_path)?;
            if actual != shard.meta_fingerprint {
                return Err(anyhow::anyhow!(
                    "federated lexical shard fingerprint mismatch for {}: expected {}, got {}",
                    shard_path.display(),
                    shard.meta_fingerprint,
                    actual
                ));
            }
        }
    }

    federated_search_manifest_summary(index_path, manifest)?;
    Ok(())
}

fn federated_evidence_chunk_role(relative_path: &str) -> EvidenceBundleChunkRole {
    if relative_path == FEDERATED_SEARCH_MANIFEST_FILE {
        EvidenceBundleChunkRole::Manifest
    } else if relative_path == "schema_hash.json"
        || relative_path == "meta.json"
        || relative_path.ends_with("/meta.json")
    {
        EvidenceBundleChunkRole::Metadata
    } else if relative_path.starts_with("shards/") {
        EvidenceBundleChunkRole::LexicalShard
    } else {
        EvidenceBundleChunkRole::Other
    }
}

fn standard_lexical_evidence_chunk_role(relative_path: &str) -> EvidenceBundleChunkRole {
    if relative_path == "schema_hash.json" || relative_path == "meta.json" {
        EvidenceBundleChunkRole::Metadata
    } else {
        EvidenceBundleChunkRole::LexicalShard
    }
}

fn current_schema_hash_file_matches(index_path: &Path) -> Result<()> {
    let schema_hash_path = index_path.join("schema_hash.json");
    let content = fs::read_to_string(&schema_hash_path).with_context(|| {
        format!(
            "reading cass schema hash metadata for lexical artifact {}",
            schema_hash_path.display()
        )
    })?;
    let value: serde_json::Value = serde_json::from_str(&content).with_context(|| {
        format!(
            "parsing cass schema hash metadata for lexical artifact {}",
            schema_hash_path.display()
        )
    })?;
    let stored_hash = value
        .get("schema_hash")
        .and_then(|value| value.as_str())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "lexical artifact schema hash metadata is missing schema_hash: {}",
                schema_hash_path.display()
            )
        })?;
    if stored_hash != CASS_SCHEMA_HASH {
        return Err(anyhow::anyhow!(
            "lexical artifact schema mismatch: expected {}, got {}",
            CASS_SCHEMA_HASH,
            stored_hash
        ));
    }
    Ok(())
}

fn relative_artifact_path_string(relative_path: &Path) -> Result<String> {
    let mut parts = Vec::new();
    for component in relative_path.components() {
        match component {
            std::path::Component::Normal(part) => {
                let part = part.to_str().ok_or_else(|| {
                    anyhow::anyhow!(
                        "lexical artifact path is not UTF-8: {}",
                        relative_path.display()
                    )
                })?;
                parts.push(part);
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "lexical artifact path contains an unsafe component: {}",
                    relative_path.display()
                ));
            }
        }
    }
    if parts.is_empty() {
        return Err(anyhow::anyhow!("lexical artifact path must not be empty"));
    }
    Ok(parts.join("/"))
}

fn is_evidence_bundle_writer_file(relative_path: &str) -> bool {
    relative_path == EVIDENCE_BUNDLE_MANIFEST_FILE
        || relative_path == EVIDENCE_BUNDLE_MANIFEST_TEMP_FILE
}

fn collect_federated_evidence_artifact_paths(
    root: &Path,
    current: &Path,
    relative_paths: &mut Vec<String>,
) -> Result<()> {
    let entries = fs::read_dir(current)
        .with_context(|| format!("reading artifact dir {}", current.display()))?;
    for entry in entries {
        let entry =
            entry.with_context(|| format!("reading artifact entry in {}", current.display()))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .with_context(|| format!("reading artifact file type {}", path.display()))?;
        if file_type.is_dir() {
            collect_federated_evidence_artifact_paths(root, &path, relative_paths)?;
        } else if file_type.is_file() {
            let relative_path = path.strip_prefix(root).with_context(|| {
                format!(
                    "computing relative artifact path for {} under {}",
                    path.display(),
                    root.display()
                )
            })?;
            let relative_path = relative_artifact_path_string(relative_path)?;
            if !is_evidence_bundle_writer_file(&relative_path) {
                relative_paths.push(relative_path);
            }
        } else {
            return Err(anyhow::anyhow!(
                "lexical artifact contains unsupported non-file entry: {}",
                path.display()
            ));
        }
    }
    Ok(())
}

fn lexical_evidence_bundle_id(chunks: &[EvidenceBundleChunk]) -> Result<String> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"cass-lexical-evidence-v1\n");
    for chunk in chunks {
        let bytes = serde_json::to_vec(chunk).context("serializing evidence bundle chunk")?;
        hasher.update(&bytes);
        hasher.update(b"\n");
    }
    Ok(format!("cass-lexical-{}", hasher.finalize().to_hex()))
}

fn lexical_search_evidence_bundle_manifest_with_roles(
    index_path: &Path,
    role_for_path: fn(&str) -> EvidenceBundleChunkRole,
) -> Result<EvidenceBundleManifest> {
    let mut relative_paths = Vec::new();
    collect_federated_evidence_artifact_paths(index_path, index_path, &mut relative_paths)?;
    relative_paths.sort();

    let chunks = relative_paths
        .into_iter()
        .map(|relative_path| {
            let role = role_for_path(&relative_path);
            EvidenceBundleChunk::from_file(index_path, relative_path, role, true, None)
        })
        .collect::<Result<Vec<_>>>()?;
    let bundle_id = lexical_evidence_bundle_id(&chunks)?;
    let mut evidence =
        EvidenceBundleManifest::new(bundle_id, EvidenceBundleKind::LexicalGeneration, 0);
    evidence.chunks = chunks;
    Ok(evidence)
}

/// Build a deterministic evidence manifest for any cass lexical artifact.
///
/// Normal Tantivy directories and federated lexical bundles both use this proof
/// surface before remote exchange. Federated bundles get their contract and
/// shard fingerprints validated; standard indexes require the current
/// `schema_hash.json` and a readable searchable summary.
pub fn lexical_search_evidence_bundle_manifest(
    index_path: &Path,
) -> Result<EvidenceBundleManifest> {
    if let Some(manifest) = load_federated_search_manifest_internal(index_path)? {
        validate_federated_search_manifest(index_path, &manifest, true)?;
        return lexical_search_evidence_bundle_manifest_with_roles(
            index_path,
            federated_evidence_chunk_role,
        );
    }

    current_schema_hash_file_matches(index_path)?;
    searchable_index_summary(index_path)?.ok_or_else(|| {
        anyhow::anyhow!(
            "cannot build lexical evidence bundle because no searchable index exists in {}",
            index_path.display()
        )
    })?;
    lexical_search_evidence_bundle_manifest_with_roles(
        index_path,
        standard_lexical_evidence_chunk_role,
    )
}

/// Build a deterministic evidence manifest for a federated lexical bundle.
///
/// This is the admission proof remote artifact exchange can compare before
/// accepting a copied bundle: the federated manifest is contract-validated,
/// every shard `meta.json` fingerprint is checked, then every regular file in
/// the bundle is recorded with a BLAKE3 digest. The evidence manifest itself is
/// excluded so repeated verification does not self-mutate the proof.
pub fn federated_search_evidence_bundle_manifest(
    index_path: &Path,
) -> Result<EvidenceBundleManifest> {
    let Some(manifest) = load_federated_search_manifest_internal(index_path)? else {
        return Err(anyhow::anyhow!(
            "cannot build federated lexical evidence bundle without {} in {}",
            FEDERATED_SEARCH_MANIFEST_FILE,
            index_path.display()
        ));
    };
    validate_federated_search_manifest(index_path, &manifest, true)?;
    lexical_search_evidence_bundle_manifest_with_roles(index_path, federated_evidence_chunk_role)
}

pub fn write_federated_search_evidence_bundle_manifest(index_path: &Path) -> Result<PathBuf> {
    let manifest = federated_search_evidence_bundle_manifest(index_path)?;
    manifest.save(index_path)
}

pub fn write_lexical_search_evidence_bundle_manifest(index_path: &Path) -> Result<PathBuf> {
    let manifest = lexical_search_evidence_bundle_manifest(index_path)?;
    manifest.save(index_path)
}

pub(super) fn load_federated_search_manifest_internal(
    index_path: &Path,
) -> Result<Option<FederatedSearchManifest>> {
    let manifest_path = federated_search_manifest_path(index_path);
    match fs::read(&manifest_path) {
        Ok(bytes) => {
            let manifest =
                serde_json::from_slice::<FederatedSearchManifest>(&bytes).with_context(|| {
                    format!(
                        "parsing federated search manifest {}",
                        manifest_path.display()
                    )
                })?;
            validate_federated_search_manifest(index_path, &manifest, false).with_context(
                || {
                    format!(
                        "validating federated search manifest {}",
                        manifest_path.display()
                    )
                },
            )?;
            Ok(Some(manifest))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err).with_context(|| {
            format!(
                "reading federated search manifest {}",
                manifest_path.display()
            )
        }),
    }
}

pub fn searchable_index_exists(index_path: &Path) -> bool {
    // A Quill index announces itself with its MANIFEST; `meta.json` is
    // the Tantivy-era marker and is still accepted so a not-yet-migrated
    // directory is recognized as an index (and therefore rebuilt) rather than
    // being mistaken for an empty one.
    index_path
        .join(crate::search::quill_bridge::QUILL_INDEX_MARKER)
        .exists()
        || index_path.join("meta.json").exists()
        || federated_search_manifest_path(index_path).exists()
}

/// Readers and their source path travel together from admission into search.
/// Retaining these owners avoids reopening (and revalidating) every segment
/// after the strict read-only contract check has already succeeded.
pub struct OpenedLexicalIndex {
    pub path: PathBuf,
    pub reader: Option<(frankensearch::quill::QuillSearchIndex, Fields)>,
    pub federated_readers: Option<Vec<(frankensearch::quill::QuillSearchIndex, Fields)>>,
}

impl std::fmt::Debug for OpenedLexicalIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenedLexicalIndex")
            .field("path", &self.path)
            .field("standard_reader", &self.reader.is_some())
            .field(
                "federated_readers",
                &self.federated_readers.as_ref().map(Vec::len),
            )
            .finish()
    }
}

pub fn validate_searchable_index_contract(index_path: &Path) -> Result<()> {
    open_validated_lexical_index(index_path).map(|_| ())
}

/// [`validate_searchable_index_contract`] plus the [`SearchableIndexSummary`]
/// of the same open. Asking for the summary first and validating afterwards
/// opened the index twice, and every Quill open hashes every live segment.
pub fn validated_searchable_index_summary(index_path: &Path) -> Result<SearchableIndexSummary> {
    let opened = open_validated_lexical_index(index_path)?;
    if let Some((reader, _)) = opened.reader.as_ref() {
        return Ok(SearchableIndexSummary {
            docs: usize::try_from(reader.doc_count()?).unwrap_or(usize::MAX),
            segments: reader.segment_count()?,
        });
    }
    // A federated bundle validated each shard above; its summary comes from
    // the bundle manifest without another open.
    searchable_index_summary(index_path)?.ok_or_else(|| {
        anyhow::anyhow!(
            "validated lexical index has no summary: {}",
            index_path.display()
        )
    })
}

pub fn open_validated_lexical_index(index_path: &Path) -> Result<OpenedLexicalIndex> {
    if let Some(readers) = open_federated_search_readers(index_path)? {
        return Ok(OpenedLexicalIndex {
            path: index_path.to_path_buf(),
            reader: None,
            federated_readers: Some(readers),
        });
    }

    // No `meta.json` fallback here, unlike `searchable_index_exists` — the
    // asymmetry is deliberate. `exists` answers "is there an index generation
    // here?", so a pre-migration Tantivy directory counts (it is an index, and
    // recognizing it is what makes the flip REBUILD rather than mistake the
    // directory for empty). This function answers "is the index usable by the
    // current engine?", and a Tantivy `meta.json` is not: Quill cannot read
    // those segments. Accepting it here would report a contract-valid index
    // that no reader can open.
    if !index_path
        .join(crate::search::quill_bridge::QUILL_INDEX_MARKER)
        .exists()
    {
        return Err(anyhow::anyhow!(
            "standard lexical index metadata is missing in {}",
            index_path.display()
        ));
    }
    current_schema_hash_file_matches(index_path)?;
    let reader = crate::search::quill_bridge::open_cass_reader(index_path).with_context(|| {
        format!(
            "opening standard lexical index reader {}",
            index_path.display()
        )
    })?;
    Ok(OpenedLexicalIndex {
        path: index_path.to_path_buf(),
        reader: Some((reader, cass_field_handles())),
        federated_readers: None,
    })
}

pub fn searchable_index_modified_time(index_path: &Path) -> Option<SystemTime> {
    // Prefer the active engine's publication authority. The remaining paths
    // support legacy Tantivy generations and federated lexical bundles.
    let quill_manifest = index_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER);
    if quill_manifest.exists() {
        return fs::metadata(quill_manifest).and_then(|m| m.modified()).ok();
    }

    let meta_path = index_path.join("meta.json");
    if meta_path.exists() {
        return fs::metadata(meta_path).and_then(|m| m.modified()).ok();
    }
    fs::metadata(federated_search_manifest_path(index_path))
        .and_then(|m| m.modified())
        .ok()
}

pub fn searchable_index_fingerprint(index_path: &Path) -> Result<Option<String>> {
    // A Quill index publishes its state as MANIFEST; hashing it gives the same
    // "changes exactly when a new generation is published" property the
    // Tantivy-era `meta.json` hash had. Checked FIRST because it is the current
    // format — `meta.json` below is only reachable for a not-yet-migrated
    // directory, and returning None for a live Quill index silently broke the
    // daemon's served-generation tracking and the rebuild checkpoint's meta
    // fingerprint (both treat None as "no published index").
    let quill_manifest = index_path.join(crate::search::quill_bridge::QUILL_INDEX_MARKER);
    match fs::read(&quill_manifest) {
        Ok(bytes) => return Ok(Some(blake3::hash(&bytes).to_hex().to_string())),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            return Err(err).with_context(|| {
                format!("reading Quill index manifest {}", quill_manifest.display())
            });
        }
    }

    let meta_path = index_path.join("meta.json");
    match fs::read(&meta_path) {
        Ok(bytes) => Ok(Some(blake3::hash(&bytes).to_hex().to_string())),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            let manifest_path = federated_search_manifest_path(index_path);
            match fs::read(&manifest_path) {
                Ok(bytes) => Ok(Some(blake3::hash(&bytes).to_hex().to_string())),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(err) => Err(err).with_context(|| {
                    format!(
                        "reading federated search manifest {}",
                        manifest_path.display()
                    )
                }),
            }
        }
        Err(err) => {
            Err(err).with_context(|| format!("reading Tantivy meta file {}", meta_path.display()))
        }
    }
}

pub fn searchable_index_summary(index_path: &Path) -> Result<Option<SearchableIndexSummary>> {
    if let Some(manifest) = load_federated_search_manifest_internal(index_path)? {
        return federated_search_manifest_summary(index_path, &manifest).map(Some);
    }

    // A Quill index reports its own doc and segment counts; the Tantivy-era
    // `meta.json` read below remains for a not-yet-migrated directory so a
    // pre-flip index still summarizes (and therefore still rebuilds) instead of
    // reading as "no index".
    if index_path
        .join(crate::search::quill_bridge::QUILL_INDEX_MARKER)
        .exists()
    {
        let reader = crate::search::quill_bridge::open_cass_reader(index_path)?;
        return Ok(Some(SearchableIndexSummary {
            docs: usize::try_from(reader.doc_count()?).unwrap_or(usize::MAX),
            segments: reader.segment_count()?,
        }));
    }

    let meta_path = index_path.join("meta.json");
    if !meta_path.exists() {
        return Ok(None);
    }

    if let Some(summary) = searchable_index_summary_from_tantivy_meta(index_path)? {
        return Ok(Some(summary));
    }

    let mut index = Index::open_in_dir(index_path).with_context(|| {
        format!(
            "opening searchable Tantivy index directory for summary: {}",
            index_path.display()
        )
    })?;
    ensure_tokenizer(&mut index);
    let segment_metas = index
        .searchable_segment_metas()
        .context("reading searchable segment metadata for Tantivy summary")?;
    Ok(Some(SearchableIndexSummary {
        docs: segment_metas
            .iter()
            .map(|segment| segment.num_docs() as usize)
            .sum(),
        segments: segment_metas.len(),
    }))
}

/// Live-document count of a searchable lexical generation from manifest
/// metadata alone (GH #457): the Quill MANIFEST of a single generation, or
/// the sum over every shard of a federated bundle. Never opens a segment.
///
/// `None` for a Tantivy-era directory (no live count without an engine open)
/// and when no manifest decodes.
#[must_use]
pub fn searchable_index_live_doc_count(index_path: &Path) -> Option<u64> {
    searchable_index_live_docs(index_path).map(|live| live.live_docs)
}

/// What a searchable lexical generation serves, from manifest metadata alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchableLiveDocs {
    pub live_docs: u64,
    /// The worst MANIFEST slot any shard was read from (mknmt): a federated
    /// bundle serves an older generation as soon as one shard does.
    pub manifest_source: crate::search::quill_bridge::QuillManifestSource,
}

/// [`searchable_index_live_doc_count`] together with the MANIFEST slot it was
/// read from. Same `None` cases.
#[must_use]
pub fn searchable_index_live_docs(index_path: &Path) -> Option<SearchableLiveDocs> {
    if let Ok(Some(manifest)) = load_federated_search_manifest_internal(index_path) {
        let mut total = SearchableLiveDocs {
            live_docs: 0,
            manifest_source: crate::search::quill_bridge::QuillManifestSource::Current,
        };
        for shard in &manifest.shards {
            let shard_live = crate::search::quill_bridge::manifest_live_doc_count(
                &index_path.join(&shard.relative_path),
            )?;
            total.live_docs = total.live_docs.saturating_add(shard_live.live_docs);
            total.manifest_source = total.manifest_source.max(shard_live.source);
        }
        return Some(total);
    }
    crate::search::quill_bridge::manifest_live_doc_count(index_path).map(|live| {
        SearchableLiveDocs {
            live_docs: live.live_docs,
            manifest_source: live.source,
        }
    })
}

pub(super) fn searchable_index_summary_from_tantivy_meta(
    index_path: &Path,
) -> Result<Option<SearchableIndexSummary>> {
    let meta_path = index_path.join("meta.json");
    let bytes = fs::read(&meta_path)
        .with_context(|| format!("reading Tantivy meta file {}", meta_path.display()))?;
    let meta: serde_json::Value = serde_json::from_slice(&bytes)
        .with_context(|| format!("parsing Tantivy meta file {}", meta_path.display()))?;
    let Some(segments) = meta.get("segments").and_then(|value| value.as_array()) else {
        return Ok(None);
    };

    let mut docs = 0usize;
    for segment in segments {
        if segment
            .get("deletes")
            .is_some_and(|deletes| !deletes.is_null())
        {
            return Ok(None);
        }
        let Some(max_doc) = segment.get("max_doc").and_then(|value| value.as_u64()) else {
            return Ok(None);
        };
        let max_doc = usize::try_from(max_doc).with_context(|| {
            format!(
                "Tantivy segment max_doc exceeds platform usize in {}",
                meta_path.display()
            )
        })?;
        docs = docs.checked_add(max_doc).with_context(|| {
            format!(
                "Tantivy segment doc count overflows platform usize in {}",
                meta_path.display()
            )
        })?;
    }

    Ok(Some(SearchableIndexSummary {
        docs,
        segments: segments.len(),
    }))
}

/// Open one reader per federated shard.
///
/// `reload_policy` is retained for caller shape but has no Quill equivalent: a
/// Quill reader is bound to the snapshot it opened, so freshness comes from
/// reopening rather than from a watch policy.
pub fn open_federated_search_readers(
    index_path: &Path,
) -> Result<Option<Vec<(frankensearch::quill::QuillSearchIndex, Fields)>>> {
    let Some(manifest) = load_federated_search_manifest_internal(index_path)? else {
        return Ok(None);
    };
    validate_federated_search_manifest(index_path, &manifest, true)?;

    let readers = manifest
        .shards
        .into_iter()
        .map(|shard| {
            let shard_path = index_path.join(&shard.relative_path);
            crate::search::quill_bridge::open_cass_reader(&shard_path)
                .map(|reader| (reader, cass_field_handles()))
                .with_context(|| {
                    format!(
                        "opening federated lexical shard reader {}",
                        shard_path.display()
                    )
                })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Some(readers))
}

pub(super) fn materialize_federated_search_bundle_for_write(index_path: &Path) -> Result<()> {
    let Some(manifest) = load_federated_search_manifest_internal(index_path)? else {
        return Ok(());
    };
    validate_federated_search_manifest(index_path, &manifest, true)?;

    let stage_parent = index_path.parent().unwrap_or(index_path);
    let materialize_root = tempfile::Builder::new()
        .prefix("cass-federated-materialize-")
        .tempdir_in(stage_parent)
        .with_context(|| {
            format!(
                "creating staging directory to materialize federated lexical bundle {}",
                index_path.display()
            )
        })?;
    let materialized_index_path = materialize_root.path().join("index");
    let shard_paths = manifest
        .shards
        .iter()
        .map(|shard| index_path.join(&shard.relative_path))
        .collect::<Vec<_>>();

    TantivyIndex::assemble_compatible_index_directory_files(&materialized_index_path, &shard_paths)
        .with_context(|| {
            format!(
                "materializing federated lexical bundle into mutable Tantivy index {}",
                index_path.display()
            )
        })?;

    // #289: the atomic swap below replaces the entire live directory, so any
    // cass sidecar metadata (rebuild checkpoint, refresh ledger, generation
    // manifest, equivalence evidence) that is not carried into the
    // materialized directory is silently destroyed. Losing the completed
    // rebuild checkpoint in particular makes every later search WARN about a
    // missing checkpoint and re-arms heavyweight authoritative rebuilds.
    for sidecar in LEXICAL_SIDECAR_FILES_CARRIED_ACROSS_MATERIALIZATION {
        let source = index_path.join(sidecar);
        match fs::symlink_metadata(&source) {
            Ok(metadata) if metadata.file_type().is_file() => {
                fs::copy(&source, materialized_index_path.join(sidecar)).with_context(|| {
                    format!(
                        "carrying lexical sidecar {} across federated materialization of {}",
                        sidecar,
                        index_path.display()
                    )
                })?;
            }
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(err).with_context(|| {
                    format!(
                        "checking lexical sidecar {} before federated materialization of {}",
                        sidecar,
                        index_path.display()
                    )
                });
            }
        }
    }

    if ensure_replaceable_federated_materialization_root(index_path)? {
        fs::remove_dir_all(index_path).with_context(|| {
            format!(
                "removing federated lexical bundle before mutable materialization {}",
                index_path.display()
            )
        })?;
    }
    fs::rename(&materialized_index_path, index_path).with_context(|| {
        format!(
            "publishing materialized mutable Tantivy index {} -> {}",
            materialized_index_path.display(),
            index_path.display()
        )
    })?;
    materialize_root
        .close()
        .context("closing federated lexical materialization staging directory")?;
    Ok(())
}

pub(super) fn ensure_replaceable_federated_materialization_root(index_path: &Path) -> Result<bool> {
    match fs::symlink_metadata(index_path) {
        Ok(metadata) => {
            let file_type = metadata.file_type();
            if file_type.is_symlink() {
                return Err(anyhow::anyhow!(
                    "refusing to materialize federated lexical bundle through symlink: {}",
                    index_path.display()
                ));
            }
            if !file_type.is_dir() {
                return Err(anyhow::anyhow!(
                    "refusing to materialize federated lexical bundle because root is not a directory: {}",
                    index_path.display()
                ));
            }
            Ok(true)
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err).with_context(|| {
            format!(
                "checking federated lexical bundle root before materialization: {}",
                index_path.display()
            )
        }),
    }
}

pub fn publish_federated_searchable_index_directories<P: AsRef<Path>>(
    output_path: &Path,
    input_paths: &[P],
) -> Result<SearchableIndexSummary> {
    if input_paths.is_empty() {
        return Err(anyhow::anyhow!(
            "cannot publish federated lexical bundle without at least one input shard"
        ));
    }
    let mut input_summaries = Vec::with_capacity(input_paths.len());
    for input_path in input_paths {
        let input_path = input_path.as_ref();
        let summary = searchable_index_summary(input_path)?.ok_or_else(|| {
            anyhow::anyhow!(
                "federated lexical publish input is not a searchable index: {}",
                input_path.display()
            )
        })?;
        input_summaries.push((input_path.to_path_buf(), summary));
    }
    publish_federated_searchable_index_directories_with_summaries(output_path, &input_summaries)
}

pub fn publish_federated_searchable_index_directories_with_summaries(
    output_path: &Path,
    input_shards: &[(PathBuf, SearchableIndexSummary)],
) -> Result<SearchableIndexSummary> {
    if input_shards.is_empty() {
        return Err(anyhow::anyhow!(
            "cannot publish federated lexical bundle without at least one input shard"
        ));
    }
    ensure_empty_merge_output_directory(output_path)?;

    let shard_root = output_path.join("shards");
    fs::create_dir_all(&shard_root).with_context(|| {
        format!(
            "creating federated lexical shard root {}",
            shard_root.display()
        )
    })?;

    let mut manifest = FederatedSearchManifest {
        version: FEDERATED_SEARCH_MANIFEST_VERSION,
        kind: FEDERATED_SEARCH_MANIFEST_KIND.to_string(),
        schema_hash: CASS_SCHEMA_HASH.to_string(),
        shards: Vec::with_capacity(input_shards.len()),
    };
    let mut total_docs = 0usize;
    let mut total_segments = 0usize;

    for (shard_idx, (input_path, summary)) in input_shards.iter().enumerate() {
        if !searchable_index_exists(input_path) {
            return Err(anyhow::anyhow!(
                "federated lexical publish input is not a searchable index: {}",
                input_path.display()
            ));
        }
        let meta_fingerprint = meta_fingerprint_for_existing_index_dir(input_path)?;
        let relative_path = manifest_relative_shard_path(shard_idx);
        let destination_path = output_path.join(&relative_path);
        if let Some(parent) = destination_path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "creating parent directory for federated lexical shard {}",
                    destination_path.display()
                )
            })?;
        }
        fs::rename(input_path, &destination_path).with_context(|| {
            format!(
                "moving staged lexical shard {} into federated publish bundle {}",
                input_path.display(),
                destination_path.display()
            )
        })?;

        total_docs = total_docs.checked_add(summary.docs).with_context(|| {
            format!(
                "federated lexical publish doc count overflows platform usize for {}",
                output_path.display()
            )
        })?;
        total_segments = total_segments
            .checked_add(summary.segments)
            .with_context(|| {
                format!(
                    "federated lexical publish segment count overflows platform usize for {}",
                    output_path.display()
                )
            })?;
        manifest.shards.push(FederatedSearchShardManifest {
            relative_path,
            docs: summary.docs,
            segments: summary.segments,
            meta_fingerprint,
        });
    }

    let manifest_bytes =
        serde_json::to_vec_pretty(&manifest).context("serializing federated search manifest")?;
    fs::write(federated_search_manifest_path(output_path), &manifest_bytes).with_context(|| {
        format!(
            "writing federated search manifest {}",
            federated_search_manifest_path(output_path).display()
        )
    })?;
    write_root_schema_hash_file(output_path)?;

    Ok(SearchableIndexSummary {
        docs: total_docs,
        segments: total_segments,
    })
}

/// Discard an index whose recorded schema hash is not the current one.
///
/// This is the mismatch-triggered rebuild. The Tantivy backend performed it
/// inside its own `open_or_create`; with Quill owning the index, cass has to do
/// it, and losing it would be silent corruption rather than a loud failure —
/// the engine would happily open a directory written under a different field
/// layout and answer queries from it.
///
/// A missing hash file is NOT a mismatch: a directory with no recorded hash is
/// either brand new or pre-dates the sentinel, and both cases are handled by
/// writing the current hash after creation.
///
/// # This deletes the sidecars too, deliberately
///
/// The generation directory holds more than segments: `.lexical-rebuild-state.json`,
/// `.lexical-rebuild-equivalence.json`, `.lexical-refresh-ledger.json`,
/// `.lexical-refresh-evidence.json`, and `lexical-generation-manifest.json` all
/// live here. Every one of them describes THIS generation's index, so once the
/// index is discarded for a schema mismatch they describe something that no
/// longer exists — a checkpoint whose committed offsets index documents in a
/// different field layout is worse than no checkpoint, because resuming from it
/// would append current-schema documents onto a stale-schema index.
///
/// `load_lexical_rebuild_state` maps a missing state file to `Ok(None)`, which
/// the caller reads as restart-from-zero. That is exactly right here: a schema
/// mismatch means the whole generation is rebuilt regardless.
///
/// This matches the Tantivy backend's behavior, which called
/// `remove_dir_all(path)` on the same condition (`cass_compat.rs`), so the flip
/// preserves it rather than introducing it. Anything that must OUTLIVE a
/// generation belongs in the index ROOT (e.g. `.lexical-publish-backups/`),
/// not inside the generation directory.
pub(super) fn wipe_index_dir_on_schema_hash_mismatch(index_path: &Path) -> Result<()> {
    let schema_hash_path = index_path.join("schema_hash.json");
    if !schema_hash_path.exists() {
        return Ok(());
    }
    if current_schema_hash_file_matches(index_path).is_ok() {
        return Ok(());
    }
    for entry in fs::read_dir(index_path).with_context(|| {
        format!(
            "reading lexical index directory for schema-mismatch rebuild {}",
            index_path.display()
        )
    })? {
        let entry = entry.with_context(|| {
            format!(
                "reading a lexical index entry for schema-mismatch rebuild {}",
                index_path.display()
            )
        })?;
        let entry_path = entry.path();
        let removal = if entry.file_type().is_ok_and(|kind| kind.is_dir()) {
            fs::remove_dir_all(&entry_path)
        } else {
            fs::remove_file(&entry_path)
        };
        removal.with_context(|| {
            format!(
                "removing stale lexical artifact during schema-mismatch rebuild {}",
                entry_path.display()
            )
        })?;
    }
    Ok(())
}
