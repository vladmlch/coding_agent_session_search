use super::federated::{
    EVIDENCE_BUNDLE_MANIFEST_TEMP_FILE, FEDERATED_SEARCH_MANIFEST_FILE,
    FEDERATED_SEARCH_MANIFEST_KIND, FEDERATED_SEARCH_MANIFEST_VERSION, FederatedSearchManifest,
    FederatedSearchShardManifest, LEXICAL_SIDECAR_FILES_CARRIED_ACROSS_MATERIALIZATION,
    ensure_replaceable_federated_materialization_root, load_federated_search_manifest_internal,
    meta_fingerprint_for_existing_index_dir, write_root_schema_hash_file,
};
use super::*;
use crate::connectors::{NormalizedConversation, NormalizedMessage};
use crate::evidence_bundle::{EvidenceBundleChunkRole, EvidenceBundleManifest};
use frankensearch::quill::cass::CASS_SCHEMA_HASH;
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

#[test]
// Exercises the Tantivy-only shard-merge / federated-bundle machinery.
// Quill has no N-directory merge, and the staged-shard rebuild strategy
// that needed one is disabled for the Quill backend (see the comment at
// the `staged_shard_plan` binding in indexer/mod.rs). Ignored rather than
// deleted or weakened: if an N-directory merge is ever built these are the
// tests that must pass, and a weakened assertion would hide that they no
// longer run.
#[ignore = "Tantivy-only shard merge; unreachable on the Quill backend"]
fn merge_compatible_index_directories_roundtrips_docs_into_single_segment() {
    let root = TempDir::new().expect("temp dir");
    let shard_a = root.path().join("shard-a");
    let shard_b = root.path().join("shard-b");
    let merged = root.path().join("merged");

    let mut shard_a_index = TantivyIndex::open_or_create(&shard_a).expect("create shard a");
    let mut shard_b_index = TantivyIndex::open_or_create(&shard_b).expect("create shard b");

    let make_conv = |external_id: &str, title: &str, content: &str| NormalizedConversation {
        agent_slug: "codex".to_string(),
        external_id: Some(external_id.to_string()),
        title: Some(title.to_string()),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        source_path: PathBuf::from(format!("/tmp/{external_id}.jsonl")),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_100),
        metadata: Value::Null,
        messages: vec![
            NormalizedMessage {
                idx: 0,
                role: "user".to_string(),
                author: None,
                created_at: Some(1_700_000_000_010),
                content: format!("{content}-a"),
                extra: Value::Null,
                snippets: Vec::new(),
                invocations: Vec::new(),
            },
            NormalizedMessage {
                idx: 1,
                role: "assistant".to_string(),
                author: None,
                created_at: Some(1_700_000_000_020),
                content: format!("{content}-b"),
                extra: Value::Null,
                snippets: Vec::new(),
                invocations: Vec::new(),
            },
        ],
    };

    let conv_a = make_conv("merge-a", "Merge A", "alpha");
    let conv_b = make_conv("merge-b", "Merge B", "beta");
    shard_a_index
        .add_conversation_with_id(&conv_a, Some(10))
        .expect("index shard a");
    shard_b_index
        .add_conversation_with_id(&conv_b, Some(20))
        .expect("index shard b");
    shard_a_index.commit().expect("commit shard a");
    shard_b_index.commit().expect("commit shard b");
    drop(shard_a_index);
    drop(shard_b_index);

    let merged_index =
        TantivyIndex::merge_compatible_index_directories(&merged, &[&shard_a, &shard_b])
            .expect("merge shard indices");
    assert_eq!(
        merged_index.segment_count(),
        1,
        "merged shard indices should collapse into a single searchable segment"
    );
    let reader = merged_index.reader().expect("reader");
    crate::search::quill_bridge::refresh_reader(&reader).expect("reload");
    assert_eq!(reader.doc_count().expect("doc count"), 4);
}

#[test]
// Exercises the Tantivy-only shard-merge / federated-bundle machinery.
// Quill has no N-directory merge, and the staged-shard rebuild strategy
// that needed one is disabled for the Quill backend (see the comment at
// the `staged_shard_plan` binding in indexer/mod.rs). Ignored rather than
// deleted or weakened: if an N-directory merge is ever built these are the
// tests that must pass, and a weakened assertion would hide that they no
// longer run.
#[ignore = "Tantivy-only shard merge; unreachable on the Quill backend"]
fn merge_compatible_index_directories_rejects_non_empty_output_directory() {
    let root = TempDir::new().expect("temp dir");
    let shard = root.path().join("shard");
    let merged = root.path().join("merged");
    fs::create_dir_all(&merged).expect("create merged dir");
    fs::write(merged.join("sentinel.txt"), "occupied").expect("write sentinel");

    let mut shard_index = TantivyIndex::open_or_create(&shard).expect("create shard");
    let conv = NormalizedConversation {
        agent_slug: "codex".to_string(),
        external_id: Some("merge-occupied".to_string()),
        title: Some("Occupied".to_string()),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        source_path: PathBuf::from("/tmp/merge-occupied.jsonl"),
        started_at: Some(1_700_000_000_000),
        ended_at: Some(1_700_000_000_100),
        metadata: Value::Null,
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "assistant".to_string(),
            author: None,
            created_at: Some(1_700_000_000_010),
            content: "occupied".to_string(),
            extra: Value::Null,
            snippets: Vec::new(),
            invocations: Vec::new(),
        }],
    };
    shard_index
        .add_conversation_with_id(&conv, Some(1))
        .expect("index shard");
    shard_index.commit().expect("commit shard");
    drop(shard_index);

    let result = TantivyIndex::merge_compatible_index_directories(&merged, &[&shard]);
    assert!(
        result.is_err(),
        "non-empty merge output dir should be rejected"
    );
    let error = result.err().expect("merge result should contain an error");
    assert!(
        format!("{error:#}").contains("must be empty"),
        "unexpected error: {error:#}"
    );
}

#[test]
// Exercises the Tantivy-only shard-merge / federated-bundle machinery.
// Quill has no N-directory merge, and the staged-shard rebuild strategy
// that needed one is disabled for the Quill backend (see the comment at
// the `staged_shard_plan` binding in indexer/mod.rs). Ignored rather than
// deleted or weakened: if an N-directory merge is ever built these are the
// tests that must pass, and a weakened assertion would hide that they no
// longer run.
#[ignore = "Tantivy-only shard merge; unreachable on the Quill backend"]
fn assemble_compatible_index_directories_roundtrips_docs_into_multi_segment_generation() {
    let root = TempDir::new().expect("temp dir");
    let shard_a = root.path().join("shard-a");
    let shard_b = root.path().join("shard-b");
    let assembled = root.path().join("assembled");

    let mut shard_a_index = TantivyIndex::open_or_create(&shard_a).expect("create shard a");
    let mut shard_b_index = TantivyIndex::open_or_create(&shard_b).expect("create shard b");

    let make_conv = |external_id: &str, title: &str, content: &str| NormalizedConversation {
        agent_slug: "codex".to_string(),
        external_id: Some(external_id.to_string()),
        title: Some(title.to_string()),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        source_path: PathBuf::from(format!("/tmp/{external_id}.jsonl")),
        started_at: Some(1_700_000_001_000),
        ended_at: Some(1_700_000_001_100),
        metadata: Value::Null,
        messages: vec![
            NormalizedMessage {
                idx: 0,
                role: "user".to_string(),
                author: None,
                created_at: Some(1_700_000_001_010),
                content: format!("{content}-a"),
                extra: Value::Null,
                snippets: Vec::new(),
                invocations: Vec::new(),
            },
            NormalizedMessage {
                idx: 1,
                role: "assistant".to_string(),
                author: None,
                created_at: Some(1_700_000_001_020),
                content: format!("{content}-b"),
                extra: Value::Null,
                snippets: Vec::new(),
                invocations: Vec::new(),
            },
        ],
    };

    let conv_a = make_conv("assemble-a", "Assemble A", "alpha");
    let conv_b = make_conv("assemble-b", "Assemble B", "beta");
    shard_a_index
        .add_conversation_with_id(&conv_a, Some(10))
        .expect("index shard a");
    shard_b_index
        .add_conversation_with_id(&conv_b, Some(20))
        .expect("index shard b");
    shard_a_index.commit().expect("commit shard a");
    shard_b_index.commit().expect("commit shard b");
    drop(shard_a_index);
    drop(shard_b_index);

    let assembled_index =
        TantivyIndex::assemble_compatible_index_directories(&assembled, &[&shard_a, &shard_b])
            .expect("assemble shard indices");
    let reader = assembled_index.reader().expect("reader");
    crate::search::quill_bridge::refresh_reader(&reader).expect("reload");
    assert_eq!(reader.doc_count().expect("doc count"), 4);
    assert_eq!(
        assembled_index.segment_count(),
        2,
        "assembled shard indices should preserve one searchable segment per input artifact"
    );
    assert!(
        assembled.join(".managed.json").exists(),
        "assembled index generation should persist a Tantivy managed-file manifest"
    );
}

#[test]
// Exercises the Tantivy-only shard-merge / federated-bundle machinery.
// Quill has no N-directory merge, and the staged-shard rebuild strategy
// that needed one is disabled for the Quill backend (see the comment at
// the `staged_shard_plan` binding in indexer/mod.rs). Ignored rather than
// deleted or weakened: if an N-directory merge is ever built these are the
// tests that must pass, and a weakened assertion would hide that they no
// longer run.
#[ignore = "Tantivy-only shard merge; unreachable on the Quill backend"]
fn publish_federated_searchable_index_directories_writes_manifest_without_root_meta() {
    let root = TempDir::new().expect("temp dir");
    let shard_a = root.path().join("shard-a");
    let shard_b = root.path().join("shard-b");
    let published = root.path().join("published");

    let mut shard_a_index = TantivyIndex::open_or_create(&shard_a).expect("create shard a");
    let mut shard_b_index = TantivyIndex::open_or_create(&shard_b).expect("create shard b");

    let make_conv = |external_id: &str, title: &str, content: &str| NormalizedConversation {
        agent_slug: "codex".to_string(),
        external_id: Some(external_id.to_string()),
        title: Some(title.to_string()),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        source_path: PathBuf::from(format!("/tmp/{external_id}.jsonl")),
        started_at: Some(1_700_000_002_000),
        ended_at: Some(1_700_000_002_100),
        metadata: Value::Null,
        messages: vec![
            NormalizedMessage {
                idx: 0,
                role: "user".to_string(),
                author: None,
                created_at: Some(1_700_000_002_010),
                content: format!("{content}-a"),
                extra: Value::Null,
                snippets: Vec::new(),
                invocations: Vec::new(),
            },
            NormalizedMessage {
                idx: 1,
                role: "assistant".to_string(),
                author: None,
                created_at: Some(1_700_000_002_020),
                content: format!("{content}-b"),
                extra: Value::Null,
                snippets: Vec::new(),
                invocations: Vec::new(),
            },
        ],
    };

    shard_a_index
        .add_conversation_with_id(&make_conv("fed-a", "Fed A", "alpha"), Some(10))
        .expect("index shard a");
    shard_b_index
        .add_conversation_with_id(&make_conv("fed-b", "Fed B", "beta"), Some(20))
        .expect("index shard b");
    shard_a_index.commit().expect("commit shard a");
    shard_b_index.commit().expect("commit shard b");
    drop(shard_a_index);
    drop(shard_b_index);

    let summary = publish_federated_searchable_index_directories(&published, &[&shard_a, &shard_b])
        .expect("publish federated bundle");
    assert_eq!(summary.docs, 4);
    assert_eq!(summary.segments, 2);
    assert!(
        !published.join("meta.json").exists(),
        "federated publish root should not force a standard single-index meta.json"
    );
    assert!(
        published.join(FEDERATED_SEARCH_MANIFEST_FILE).exists(),
        "federated publish root should persist its manifest"
    );
    let manifest = load_federated_search_manifest_internal(&published)
        .expect("load manifest")
        .expect("manifest present");
    assert_eq!(manifest.shards.len(), 2);
    assert_eq!(
        searchable_index_summary(&published)
            .expect("summary")
            .expect("searchable summary")
            .docs,
        4
    );
}

fn write_federated_manifest_for_test(index_path: &Path, manifest: &FederatedSearchManifest) {
    fs::write(
        federated_search_manifest_path(index_path),
        serde_json::to_vec_pretty(manifest).expect("serialize manifest"),
    )
    .expect("write manifest");
}

fn publish_test_federated_bundle(root: &Path) -> PathBuf {
    let shard_a = root.join("shard-a");
    let shard_b = root.join("shard-b");
    let published = root.join("published");

    let mut shard_a_index = TantivyIndex::open_or_create(&shard_a).expect("create shard a");
    let mut shard_b_index = TantivyIndex::open_or_create(&shard_b).expect("create shard b");

    let make_conv = |external_id: &str, content: &str| NormalizedConversation {
        agent_slug: "codex".to_string(),
        external_id: Some(external_id.to_string()),
        title: Some(format!("Bundle {external_id}")),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        source_path: PathBuf::from(format!("/tmp/{external_id}.jsonl")),
        started_at: Some(1_700_000_002_000),
        ended_at: Some(1_700_000_002_100),
        metadata: Value::Null,
        messages: vec![NormalizedMessage {
            idx: 0,
            role: "assistant".to_string(),
            author: None,
            created_at: Some(1_700_000_002_010),
            content: content.to_string(),
            extra: Value::Null,
            snippets: Vec::new(),
            invocations: Vec::new(),
        }],
    };

    shard_a_index
        .add_conversation_with_id(&make_conv("bundle-a", "alpha"), Some(10))
        .expect("index shard a");
    shard_b_index
        .add_conversation_with_id(&make_conv("bundle-b", "beta"), Some(20))
        .expect("index shard b");
    shard_a_index.commit().expect("commit shard a");
    shard_b_index.commit().expect("commit shard b");
    drop(shard_a_index);
    drop(shard_b_index);

    publish_federated_searchable_index_directories(&published, &[&shard_a, &shard_b])
        .expect("publish federated bundle");
    published
}

#[test]
fn federated_manifest_validation_rejects_unsupported_remote_contracts() {
    let root = TempDir::new().expect("temp dir");
    let published = root.path().join("published");
    fs::create_dir_all(&published).expect("create bundle root");
    let base_manifest = FederatedSearchManifest {
        version: FEDERATED_SEARCH_MANIFEST_VERSION,
        kind: FEDERATED_SEARCH_MANIFEST_KIND.to_string(),
        schema_hash: CASS_SCHEMA_HASH.to_string(),
        shards: vec![FederatedSearchShardManifest {
            relative_path: "shards/shard-00000".to_string(),
            docs: 1,
            segments: 1,
            meta_fingerprint: "a".repeat(64),
        }],
    };

    let mut manifest = base_manifest.clone();
    manifest.version = FEDERATED_SEARCH_MANIFEST_VERSION + 1;
    write_federated_manifest_for_test(&published, &manifest);
    let error = load_federated_search_manifest_internal(&published).unwrap_err();
    assert!(
        format!("{error:#}").contains("unsupported federated search manifest version"),
        "unexpected version error: {error:#}"
    );

    let mut manifest = base_manifest.clone();
    manifest.kind = "cass-unknown-artifact".to_string();
    write_federated_manifest_for_test(&published, &manifest);
    let error = load_federated_search_manifest_internal(&published).unwrap_err();
    assert!(
        format!("{error:#}").contains("unexpected federated search manifest kind"),
        "unexpected kind error: {error:#}"
    );

    let mut manifest = base_manifest;
    manifest
        .shards
        .first_mut()
        .expect("test manifest should contain a shard")
        .relative_path = "../escape".to_string();
    write_federated_manifest_for_test(&published, &manifest);
    let error = load_federated_search_manifest_internal(&published).unwrap_err();
    assert!(
        format!("{error:#}").contains("must stay under shards/"),
        "unexpected path error: {error:#}"
    );
}

#[test]
// Exercises the Tantivy-only shard-merge / federated-bundle machinery.
// Quill has no N-directory merge, and the staged-shard rebuild strategy
// that needed one is disabled for the Quill backend (see the comment at
// the `staged_shard_plan` binding in indexer/mod.rs). Ignored rather than
// deleted or weakened: if an N-directory merge is ever built these are the
// tests that must pass, and a weakened assertion would hide that they no
// longer run.
#[ignore = "Tantivy-only shard merge; unreachable on the Quill backend"]
fn open_federated_search_readers_rejects_corrupt_shard_fingerprint() {
    let root = TempDir::new().expect("temp dir");
    let published = publish_test_federated_bundle(root.path());
    let mut manifest = load_federated_search_manifest_internal(&published)
        .expect("load manifest")
        .expect("manifest present");
    manifest
        .shards
        .first_mut()
        .expect("test manifest should contain a shard")
        .meta_fingerprint = "0".repeat(64);
    write_federated_manifest_for_test(&published, &manifest);

    let result = open_federated_search_readers(&published);
    assert!(
        result.is_err(),
        "corrupt federated shard fingerprint should be rejected"
    );
    let error = result.err().expect("open result should contain an error");
    assert!(
        format!("{error:#}").contains("federated lexical shard fingerprint mismatch"),
        "unexpected fingerprint error: {error:#}"
    );
}

fn write_minimal_federated_artifact(root: &Path, segment_bytes: &[u8]) {
    let shard = root.join("shards/shard-00000");
    fs::create_dir_all(&shard).expect("create shard");
    fs::write(shard.join("meta.json"), br#"{"segments":[]}"#).expect("write shard meta");
    fs::write(shard.join("segment.bin"), segment_bytes).expect("write shard segment");
    write_root_schema_hash_file(root).expect("write schema hash");

    let manifest = FederatedSearchManifest {
        version: FEDERATED_SEARCH_MANIFEST_VERSION,
        kind: FEDERATED_SEARCH_MANIFEST_KIND.to_string(),
        schema_hash: CASS_SCHEMA_HASH.to_string(),
        shards: vec![FederatedSearchShardManifest {
            relative_path: "shards/shard-00000".to_string(),
            docs: 0,
            segments: 0,
            meta_fingerprint: meta_fingerprint_for_existing_index_dir(&shard)
                .expect("meta fingerprint"),
        }],
    };
    write_federated_manifest_for_test(root, &manifest);
}

fn write_minimal_standard_lexical_artifact(root: &Path, segment_bytes: &[u8]) {
    fs::create_dir_all(root).expect("create standard lexical root");
    fs::write(root.join("meta.json"), br#"{"segments":[]}"#).expect("write root meta");
    fs::write(root.join("segment.bin"), segment_bytes).expect("write segment");
    write_root_schema_hash_file(root).expect("write schema hash");
}

#[test]
fn lexical_evidence_manifest_supports_standard_searchable_index() {
    let root = TempDir::new().expect("temp dir");
    write_minimal_standard_lexical_artifact(root.path(), b"standard segment bytes");

    let manifest = lexical_search_evidence_bundle_manifest(root.path()).expect("standard manifest");
    assert!(manifest.verify(root.path()).is_complete());
    assert!(manifest.chunks.iter().any(|chunk| {
        chunk.path == "meta.json" && chunk.role == EvidenceBundleChunkRole::Metadata
    }));
    assert!(manifest.chunks.iter().any(|chunk| {
        chunk.path == "segment.bin" && chunk.role == EvidenceBundleChunkRole::LexicalShard
    }));
}

#[test]
fn lexical_evidence_manifest_excludes_writer_temp_file_before_save() {
    let root = TempDir::new().expect("temp dir");
    write_minimal_standard_lexical_artifact(root.path(), b"standard segment bytes");
    fs::write(
        root.path().join(EVIDENCE_BUNDLE_MANIFEST_TEMP_FILE),
        b"leftover temp manifest bytes",
    )
    .expect("write stale evidence manifest temp file");

    let manifest = lexical_search_evidence_bundle_manifest(root.path()).expect("standard manifest");
    assert!(
        manifest
            .chunks
            .iter()
            .all(|chunk| chunk.path != EVIDENCE_BUNDLE_MANIFEST_TEMP_FILE),
        "writer temp file must not become part of the saved proof: {manifest:?}"
    );

    manifest.save(root.path()).expect("save evidence manifest");
    let report = crate::evidence_bundle::verify_evidence_bundle_manifest_file(
        root.path(),
        &crate::evidence_bundle::EvidenceBundleManifest::path(root.path()),
    );
    assert!(report.is_complete(), "{report:?}");
}

#[test]
fn lexical_evidence_manifest_rejects_standard_schema_mismatch() {
    let root = TempDir::new().expect("temp dir");
    write_minimal_standard_lexical_artifact(root.path(), b"standard segment bytes");
    fs::write(
        root.path().join("schema_hash.json"),
        r#"{"schema_hash":"stale"}"#,
    )
    .expect("write stale schema hash");

    let error = lexical_search_evidence_bundle_manifest(root.path()).unwrap_err();
    assert!(
        format!("{error:#}").contains("lexical artifact schema mismatch"),
        "unexpected schema error: {error:#}"
    );
}

#[test]
fn federated_evidence_manifest_is_deterministic_and_detects_mutation() {
    let left = TempDir::new().expect("left temp dir");
    let right = TempDir::new().expect("right temp dir");
    write_minimal_federated_artifact(left.path(), b"same segment bytes");
    write_minimal_federated_artifact(right.path(), b"same segment bytes");

    let left_manifest =
        federated_search_evidence_bundle_manifest(left.path()).expect("left manifest");
    let right_manifest =
        federated_search_evidence_bundle_manifest(right.path()).expect("right manifest");
    assert_eq!(
        serde_json::to_value(&left_manifest).expect("left json"),
        serde_json::to_value(&right_manifest).expect("right json"),
        "byte-identical federated artifacts should produce byte-identical evidence manifests"
    );
    assert!(left_manifest.verify(left.path()).is_complete());

    fs::write(
        left.path().join("shards/shard-00000/segment.bin"),
        b"SAME segment bytes",
    )
    .expect("mutate shard segment");
    let report = left_manifest.verify(left.path());
    assert!(report.is_unsafe(), "{report:?}");
    assert!(
        report
            .issues
            .iter()
            .any(|issue| issue.kind
                == crate::evidence_bundle::EvidenceBundleIssueKind::DigestMismatch),
        "expected digest mismatch after segment mutation: {report:?}"
    );
}

#[cfg(unix)]
#[test]
fn federated_evidence_manifest_rejects_symlink_artifacts() {
    use std::os::unix::fs::symlink;

    let root = TempDir::new().expect("temp dir");
    write_minimal_federated_artifact(root.path(), b"segment bytes");
    symlink(
        "/tmp/not-a-bundle-file",
        root.path().join("shards/shard-00000/link"),
    )
    .expect("create artifact symlink");

    let error = federated_search_evidence_bundle_manifest(root.path()).unwrap_err();
    assert!(
        format!("{error:#}").contains("unsupported non-file entry"),
        "unexpected symlink error: {error:#}"
    );
}

#[test]
// Exercises the Tantivy-only shard-merge / federated-bundle machinery.
// Quill has no N-directory merge, and the staged-shard rebuild strategy
// that needed one is disabled for the Quill backend (see the comment at
// the `staged_shard_plan` binding in indexer/mod.rs). Ignored rather than
// deleted or weakened: if an N-directory merge is ever built these are the
// tests that must pass, and a weakened assertion would hide that they no
// longer run.
#[ignore = "Tantivy-only shard merge; unreachable on the Quill backend"]
fn open_or_create_materializes_federated_bundle_back_into_mutable_index() {
    let root = TempDir::new().expect("temp dir");
    let shard_a = root.path().join("shard-a");
    let shard_b = root.path().join("shard-b");
    let published = root.path().join("published");

    let mut shard_a_index = TantivyIndex::open_or_create(&shard_a).expect("create shard a");
    let mut shard_b_index = TantivyIndex::open_or_create(&shard_b).expect("create shard b");

    let make_conv = |external_id: &str, title: &str, content: &str| NormalizedConversation {
        agent_slug: "codex".to_string(),
        external_id: Some(external_id.to_string()),
        title: Some(title.to_string()),
        workspace: Some(PathBuf::from("/tmp/workspace")),
        source_path: PathBuf::from(format!("/tmp/{external_id}.jsonl")),
        started_at: Some(1_700_000_003_000),
        ended_at: Some(1_700_000_003_100),
        metadata: Value::Null,
        messages: vec![
            NormalizedMessage {
                idx: 0,
                role: "user".to_string(),
                author: None,
                created_at: Some(1_700_000_003_010),
                content: format!("{content}-a"),
                extra: Value::Null,
                snippets: Vec::new(),
                invocations: Vec::new(),
            },
            NormalizedMessage {
                idx: 1,
                role: "assistant".to_string(),
                author: None,
                created_at: Some(1_700_000_003_020),
                content: format!("{content}-b"),
                extra: Value::Null,
                snippets: Vec::new(),
                invocations: Vec::new(),
            },
        ],
    };

    shard_a_index
        .add_conversation_with_id(&make_conv("mat-a", "Mat A", "alpha"), Some(10))
        .expect("index shard a");
    shard_b_index
        .add_conversation_with_id(&make_conv("mat-b", "Mat B", "beta"), Some(20))
        .expect("index shard b");
    shard_a_index.commit().expect("commit shard a");
    shard_b_index.commit().expect("commit shard b");
    drop(shard_a_index);
    drop(shard_b_index);

    publish_federated_searchable_index_directories(&published, &[&shard_a, &shard_b])
        .expect("publish federated bundle");
    assert!(
        published.join(FEDERATED_SEARCH_MANIFEST_FILE).exists(),
        "test fixture should start in federated bundle form"
    );

    let mutable_index =
        TantivyIndex::open_or_create(&published).expect("materialize mutable index");
    let reader = mutable_index.reader().expect("reader");
    crate::search::quill_bridge::refresh_reader(&reader).expect("reload");
    assert_eq!(reader.doc_count().expect("doc count"), 4);
    assert!(
        published.join("meta.json").exists(),
        "writer open should materialize a standard writable Tantivy index"
    );
    assert!(
        !published.join(FEDERATED_SEARCH_MANIFEST_FILE).exists(),
        "materialization should replace the federated bundle manifest"
    );
}

/// Materialization-direction counterpart of the publish-direction
/// contract test
/// `publish_staged_lexical_index_moves_generation_audit_files_with_the_staged_directory`
/// (src/indexer/mod.rs): flattening a federated bundle back into a
/// mutable index must carry the cass sidecar metadata — most importantly
/// the just-persisted completed rebuild checkpoint — into the
/// materialized directory instead of wiping it with the federated root
/// (#289).
#[test]
// Exercises the Tantivy-only shard-merge / federated-bundle machinery.
// Quill has no N-directory merge, and the staged-shard rebuild strategy
// that needed one is disabled for the Quill backend (see the comment at
// the `staged_shard_plan` binding in indexer/mod.rs). Ignored rather than
// deleted or weakened: if an N-directory merge is ever built these are the
// tests that must pass, and a weakened assertion would hide that they no
// longer run.
#[ignore = "Tantivy-only shard merge; unreachable on the Quill backend"]
fn open_or_create_materialization_carries_lexical_sidecar_files() {
    let root = TempDir::new().expect("temp dir");
    let shard_a = root.path().join("shard-a");
    let shard_b = root.path().join("shard-b");
    let published = root.path().join("published");

    for (shard_path, external_id, conversation_id) in
        [(&shard_a, "side-a", 10), (&shard_b, "side-b", 20)]
    {
        let mut shard_index =
            TantivyIndex::open_or_create(shard_path).expect("create sidecar shard");
        let conv = NormalizedConversation {
            agent_slug: "codex".to_string(),
            external_id: Some(external_id.to_string()),
            title: Some(external_id.to_string()),
            workspace: Some(PathBuf::from("/tmp/workspace")),
            source_path: PathBuf::from(format!("/tmp/{external_id}.jsonl")),
            started_at: Some(1_700_000_003_000),
            ended_at: Some(1_700_000_003_100),
            metadata: Value::Null,
            messages: vec![NormalizedMessage {
                idx: 0,
                role: "user".to_string(),
                author: None,
                created_at: Some(1_700_000_003_010),
                content: format!("{external_id} content"),
                extra: Value::Null,
                snippets: Vec::new(),
                invocations: Vec::new(),
            }],
        };
        shard_index
            .add_conversation_with_id(&conv, Some(conversation_id))
            .expect("index sidecar shard");
        shard_index.commit().expect("commit sidecar shard");
    }

    publish_federated_searchable_index_directories(&published, &[&shard_a, &shard_b])
        .expect("publish federated bundle");
    assert!(
        published.join(FEDERATED_SEARCH_MANIFEST_FILE).exists(),
        "test fixture should start in federated bundle form"
    );

    // Simulate the post-publish state from #289: the staged rebuild has
    // just persisted its completed checkpoint + audit sidecars into the
    // freshly published (federated) live directory.
    let sidecar_payloads = LEXICAL_SIDECAR_FILES_CARRIED_ACROSS_MATERIALIZATION
        .iter()
        .map(|sidecar| {
            let payload = format!("{{\"sidecar\":\"{sidecar}\",\"completed\":true}}");
            fs::write(published.join(sidecar), &payload).expect("write sidecar fixture");
            (*sidecar, payload)
        })
        .collect::<Vec<_>>();

    let mutable_index =
        TantivyIndex::open_or_create(&published).expect("materialize mutable index");
    let reader = mutable_index.reader().expect("reader");
    crate::search::quill_bridge::refresh_reader(&reader).expect("reload");
    assert_eq!(reader.doc_count().expect("doc count"), 2);

    for (sidecar, payload) in &sidecar_payloads {
        assert_eq!(
            fs::read_to_string(published.join(sidecar)).unwrap_or_else(|err| panic!(
                "lexical sidecar {sidecar} must survive federated materialization: {err}"
            )),
            *payload,
            "lexical sidecar {sidecar} must be carried byte-for-byte"
        );
    }
    assert!(
        !published.join(FEDERATED_SEARCH_MANIFEST_FILE).exists(),
        "the federated manifest must not be carried into the materialized mutable index"
    );
}

#[test]
fn federated_materialization_target_rejects_file_root() {
    let root = TempDir::new().expect("temp dir");
    let published = root.path().join("published");
    fs::write(&published, "not a directory").expect("write file root");

    let err = ensure_replaceable_federated_materialization_root(&published)
        .expect_err("file roots must not be materialized over");

    assert!(
        err.to_string().contains("not a directory"),
        "unexpected error: {err:#}"
    );
    assert_eq!(
        fs::read_to_string(&published).expect("file root should remain"),
        "not a directory"
    );
}

#[test]
#[cfg(unix)]
fn federated_materialization_target_rejects_dangling_symlink_root() {
    use std::os::unix::fs::symlink;

    let root = TempDir::new().expect("temp dir");
    let published = root.path().join("published");
    let missing_target = root.path().join("missing-published");
    symlink(&missing_target, &published).expect("create dangling symlink");

    let err = ensure_replaceable_federated_materialization_root(&published)
        .expect_err("symlink roots must not be materialized over");

    assert!(
        err.to_string().contains("through symlink"),
        "unexpected error: {err:#}"
    );
    assert!(
        fs::symlink_metadata(&published)
            .expect("symlink root should remain")
            .file_type()
            .is_symlink()
    );
    assert!(!missing_target.exists());
}
