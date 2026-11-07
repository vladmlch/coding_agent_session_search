use anyhow::Result;
use std::path::Path;

/// `sources.toml` source name under which `cass import chatgpt` registers its
/// output directory so the indexer scans it without relying on connector
/// auto-detection (GH #378).
pub(crate) const CHATGPT_IMPORT_SOURCE_NAME: &str = "chatgpt-import";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ImportScanRootRegistration {
    /// The source did not exist and was created with this path.
    Created,
    /// The source existed; this path was appended to it.
    PathAdded,
    /// The source already listed this path; nothing written.
    AlreadyRegistered,
}

impl ImportScanRootRegistration {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::PathAdded => "path_added",
            Self::AlreadyRegistered => "already_registered",
        }
    }
}

/// Register `path` as a local explicit scan root under the named source in
/// `sources.toml` (GH #378). Loads the current config, mutates only the named
/// source, and saves atomically through `SourcesConfig::save`.
pub(crate) fn register_import_scan_root(
    source_name: &str,
    path: &Path,
) -> Result<ImportScanRootRegistration, crate::sources::config::ConfigError> {
    let mut config = crate::sources::config::SourcesConfig::load()?;
    let state = register_import_scan_root_in(&mut config, source_name, path)?;
    if state != ImportScanRootRegistration::AlreadyRegistered {
        config.save()?;
    }
    Ok(state)
}

/// Pure half of [`register_import_scan_root`]: mutates `config` in memory and
/// reports what changed, so callers decide whether a save is needed.
pub(crate) fn register_import_scan_root_in(
    config: &mut crate::sources::config::SourcesConfig,
    source_name: &str,
    path: &Path,
) -> Result<ImportScanRootRegistration, crate::sources::config::ConfigError> {
    use crate::sources::config::SourceDefinition;

    let path_str = path.to_string_lossy().into_owned();
    if let Some(source) = config.find_source_mut(source_name) {
        if source.paths.iter().any(|existing| existing == &path_str) {
            return Ok(ImportScanRootRegistration::AlreadyRegistered);
        }
        source.paths.push(path_str);
        source.validate()?;
        Ok(ImportScanRootRegistration::PathAdded)
    } else {
        let mut source = SourceDefinition::local(source_name);
        source.paths.push(path_str);
        config.add_source(source)?;
        Ok(ImportScanRootRegistration::Created)
    }
}
