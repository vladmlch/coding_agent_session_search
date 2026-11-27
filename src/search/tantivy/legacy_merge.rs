use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use frankensearch::lexical_tantivy::{Index, tantivy_crate};
use frankensearch::quill::cass::CASS_SCHEMA_HASH;

use super::{TantivyIndex, ensure_tokenizer};

impl TantivyIndex {
    pub fn merge_compatible_index_directories<P: AsRef<Path>>(
        output_path: &Path,
        input_paths: &[P],
    ) -> Result<Self> {
        if input_paths.is_empty() {
            return Err(anyhow::anyhow!(
                "cannot merge Tantivy index directories without at least one input"
            ));
        }
        ensure_empty_merge_output_directory(output_path)?;

        let indices = input_paths
            .iter()
            .map(|input_path| {
                let input_path = input_path.as_ref();
                let mut index = Index::open_in_dir(input_path).with_context(|| {
                    format!(
                        "opening compatible Tantivy index directory for merge: {}",
                        input_path.display()
                    )
                })?;
                ensure_tokenizer(&mut index);
                Ok(index)
            })
            .collect::<Result<Vec<_>>>()?;
        let output_directory = tantivy_crate::directory::MmapDirectory::open(output_path)
            .with_context(|| {
                format!(
                    "opening Tantivy output directory for merged index: {}",
                    output_path.display()
                )
            })?;
        let mut merged = tantivy_crate::indexer::merge_indices(&indices, output_directory)
            .with_context(|| {
                format!(
                    "merging {} compatible Tantivy index directories into {}",
                    indices.len(),
                    output_path.display()
                )
            })?;
        ensure_tokenizer(&mut merged);
        fs::write(
            output_path.join("schema_hash.json"),
            format!("{{\"schema_hash\":\"{CASS_SCHEMA_HASH}\"}}"),
        )
        .with_context(|| {
            format!(
                "writing cass schema hash metadata for merged Tantivy index {}",
                output_path.display()
            )
        })?;
        drop(merged);
        Self::open_or_create(output_path)
    }

    pub fn assemble_compatible_index_directories<P: AsRef<Path>>(
        output_path: &Path,
        input_paths: &[P],
    ) -> Result<Self> {
        Self::assemble_compatible_index_directory_files(output_path, input_paths)?;
        Self::open_or_create(output_path)
    }

    pub(super) fn assemble_compatible_index_directory_files<P: AsRef<Path>>(
        output_path: &Path,
        input_paths: &[P],
    ) -> Result<()> {
        if input_paths.is_empty() {
            return Err(anyhow::anyhow!(
                "cannot assemble Tantivy index directories without at least one input"
            ));
        }
        ensure_empty_merge_output_directory(output_path)?;

        let mut combined_index_meta: Option<tantivy_crate::IndexMeta> = None;
        let mut combined_segments = Vec::new();
        let mut max_opstamp = 0u64;
        let mut managed_paths = BTreeSet::new();

        for input_path in input_paths {
            let input_path = input_path.as_ref();
            let mut index = Index::open_in_dir(input_path).with_context(|| {
                format!(
                    "opening compatible Tantivy index directory for assembly: {}",
                    input_path.display()
                )
            })?;
            ensure_tokenizer(&mut index);
            let metas = index.load_metas().with_context(|| {
                format!(
                    "loading Tantivy metadata for assembled index input {}",
                    input_path.display()
                )
            })?;

            match &mut combined_index_meta {
                Some(combined_meta) => {
                    if metas.schema != combined_meta.schema {
                        return Err(anyhow::anyhow!(
                            "attempted to assemble Tantivy index directories with different schemas"
                        ));
                    }
                    if metas.index_settings != combined_meta.index_settings {
                        return Err(anyhow::anyhow!(
                            "attempted to assemble Tantivy index directories with different index settings"
                        ));
                    }
                    // tantivy 0.26.1 (the registry pin frankensearch 0.4.2
                    // re-exports) has no persisted custom plugin extensions on
                    // `IndexMeta`; the 0.27-only extension-mismatch guard
                    // returns with the 0.27 upgrade.
                }
                None => {
                    combined_index_meta = Some(tantivy_crate::IndexMeta {
                        index_settings: metas.index_settings.clone(),
                        segments: Vec::new(),
                        schema: metas.schema.clone(),
                        opstamp: 0,
                        payload: None,
                    });
                }
            }

            max_opstamp = max_opstamp.max(metas.opstamp);
            for segment in metas.segments {
                // tantivy 0.27 removed `SegmentMeta::list_files()` (per-segment
                // file sets now depend on the persisted plugin extensions).
                // Every file belonging to a segment is named `<uuid>.<ext>`,
                // so copy the input directory entries whose file name starts
                // with this segment's uuid — the same "copy what exists" set
                // the old enumeration produced.
                let segment_prefix = segment.id().uuid_string();
                for entry in std::fs::read_dir(input_path).with_context(|| {
                    format!(
                        "listing Tantivy index directory for assembly: {}",
                        input_path.display()
                    )
                })? {
                    let entry = entry.with_context(|| {
                        format!(
                            "reading Tantivy index directory entry for assembly: {}",
                            input_path.display()
                        )
                    })?;
                    let file_name = entry.file_name();
                    let Some(name) = file_name.to_str() else {
                        continue;
                    };
                    if !name.starts_with(segment_prefix.as_str()) {
                        continue;
                    }
                    let relative_path = std::path::PathBuf::from(name);
                    // ubs:ignore[rust.security.path-traversal] — single filename component from DirEntry::file_name
                    let source_path = input_path.join(&relative_path);
                    if !source_path.is_file() {
                        continue;
                    }
                    link_or_copy_searchable_index_file(&source_path, output_path, &relative_path)?;
                    if !managed_paths.insert(relative_path.clone()) {
                        return Err(anyhow::anyhow!(
                            "assembled Tantivy index would contain duplicate segment file path {}",
                            relative_path.display()
                        ));
                    }
                }
                combined_segments.push(segment);
            }
        }

        let mut combined_index_meta = combined_index_meta.ok_or_else(|| {
            anyhow::anyhow!("cannot assemble Tantivy index directories without index metadata")
        })?;
        combined_index_meta.segments = combined_segments;
        combined_index_meta.opstamp = max_opstamp;
        combined_index_meta.payload = Some(format!(
            "Cass assembled {} compatible Tantivy segments from {} input directories",
            combined_index_meta.segments.len(),
            input_paths.len()
        ));

        write_searchable_generation_metadata(
            output_path,
            &combined_index_meta,
            &mut managed_paths,
        )?;
        Ok(())
    }
}

pub(super) fn ensure_empty_merge_output_directory(output_path: &Path) -> Result<()> {
    match fs::metadata(output_path) {
        Ok(metadata) => {
            if !metadata.is_dir() {
                return Err(anyhow::anyhow!(
                    "merged Tantivy output path is not a directory: {}",
                    output_path.display()
                ));
            }
            let mut entries = fs::read_dir(output_path).with_context(|| {
                format!(
                    "reading merged Tantivy output directory before merge: {}",
                    output_path.display()
                )
            })?;
            if entries.next().transpose()?.is_some() {
                return Err(anyhow::anyhow!(
                    "merged Tantivy output directory must be empty before merge: {}",
                    output_path.display()
                ));
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir_all(output_path).with_context(|| {
                format!(
                    "creating merged Tantivy output directory before merge: {}",
                    output_path.display()
                )
            })?;
        }
        Err(err) => {
            return Err(err).with_context(|| {
                format!(
                    "checking merged Tantivy output directory before merge: {}",
                    output_path.display()
                )
            });
        }
    }
    Ok(())
}

fn link_or_copy_searchable_index_file(
    source_path: &Path,
    output_path: &Path,
    relative_path: &Path,
) -> Result<()> {
    let mut components = relative_path.components();
    if !matches!(
        (components.next(), components.next()),
        (Some(std::path::Component::Normal(_)), None)
    ) {
        return Err(anyhow::anyhow!(
            "assembled Tantivy relative path must be a single normal filename: {}",
            relative_path.display()
        ));
    }
    // ubs:ignore[rust.security.path-traversal] — single normal component validated above
    let destination_path = output_path.join(relative_path);
    if destination_path.exists() {
        return Err(anyhow::anyhow!(
            "assembled Tantivy output path already exists: {}",
            destination_path.display()
        ));
    }

    match fs::hard_link(source_path, &destination_path) {
        Ok(()) => Ok(()),
        Err(err)
            if matches!(
                err.kind(),
                std::io::ErrorKind::PermissionDenied
                    | std::io::ErrorKind::CrossesDevices
                    | std::io::ErrorKind::Unsupported
            ) =>
        {
            fs::copy(source_path, &destination_path).with_context(|| {
                format!(
                    "copying Tantivy segment file into assembled generation {} -> {}",
                    source_path.display(),
                    destination_path.display()
                )
            })?;
            Ok(())
        }
        Err(err) => Err(err).with_context(|| {
            format!(
                "hard-linking Tantivy segment file into assembled generation {} -> {}",
                source_path.display(),
                destination_path.display()
            )
        }),
    }
}

fn write_searchable_generation_metadata(
    output_path: &Path,
    index_meta: &tantivy_crate::IndexMeta,
    managed_paths: &mut BTreeSet<std::path::PathBuf>,
) -> Result<()> {
    let meta_path = output_path.join("meta.json");
    fs::write(
        &meta_path,
        serde_json::to_vec_pretty(index_meta).context("serializing assembled Tantivy meta.json")?,
    )
    .with_context(|| {
        format!(
            "writing assembled Tantivy meta.json for {}",
            output_path.display()
        )
    })?;
    managed_paths.insert(std::path::PathBuf::from("meta.json"));
    fs::write(
        output_path.join(".managed.json"),
        serde_json::to_vec(managed_paths).context("serializing assembled Tantivy managed paths")?,
    )
    .with_context(|| {
        format!(
            "writing assembled Tantivy managed file manifest for {}",
            output_path.display()
        )
    })?;
    fs::write(
        output_path.join("schema_hash.json"),
        format!("{{\"schema_hash\":\"{CASS_SCHEMA_HASH}\"}}"),
    )
    .with_context(|| {
        format!(
            "writing cass schema hash metadata for assembled Tantivy index {}",
            output_path.display()
        )
    })?;
    Ok(())
}
