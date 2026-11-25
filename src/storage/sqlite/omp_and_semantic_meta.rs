#[cfg(test)]
use crate::franken_sync::Connection as FrankenConnection;
use crate::franken_sync::{Row as FrankenRow, compat::RowExt as FrankenRowExt};
use anyhow::{Result, bail};
use std::collections::HashMap;

/// Public schema version constant for external checks.
pub const CURRENT_SCHEMA_VERSION: i64 = 23;
pub(crate) const MIN_IN_PLACE_MIGRATION_SCHEMA_VERSION: i64 = 13;
pub(super) const LEGACY_OMP_RECLASSIFICATION_META_KEY: &str = "legacy_omp_reclassification_v2";
pub(super) const PREVIOUS_LEGACY_OMP_RECLASSIFICATION_META_KEY: &str =
    "legacy_omp_reclassification_v1";
pub(super) const LEGACY_OMP_LEXICAL_PUBLISHED_META_KEY: &str =
    "legacy_omp_reclassification_lexical_published_v1";
pub(super) const LEGACY_OMP_ANALYTICS_REBUILT_META_KEY: &str =
    "legacy_omp_reclassification_analytics_rebuilt_v1";
pub(super) const LEGACY_OMP_ANALYTICS_CURSOR_CONTEXT_META_KEY: &str =
    "legacy_omp_reclassification_analytics_cursor_context_v1";
pub(super) const LEGACY_OMP_ANALYTICS_CURSOR_LAST_ID_META_KEY: &str =
    "legacy_omp_reclassification_analytics_cursor_last_id_v1";
pub(super) const LEGACY_OMP_ANALYTICS_CURSOR_PROCESSED_META_KEY: &str =
    "legacy_omp_reclassification_analytics_cursor_processed_v1";
pub(super) const SEMANTIC_FAST_IDENTITY_REBUILD_META_KEY: &str =
    "semantic_fast_identity_rebuild_v1";
pub(super) const SEMANTIC_QUALITY_IDENTITY_REBUILD_META_KEY: &str =
    "semantic_quality_identity_rebuild_v1";

/// Read the durable analytics phase authority without opening a writer or
/// changing migration state. Row parity cannot certify an identity rewrite.
#[cfg(test)]
pub(crate) fn legacy_omp_analytics_pending(conn: &FrankenConnection) -> Result<bool> {
    legacy_omp_analytics_pending_with_query(|sql| conn.query(sql))
}

pub(crate) fn legacy_omp_analytics_pending_with_query(
    query: impl Fn(&str) -> std::result::Result<Vec<FrankenRow>, crate::franken_sync::FrankenError>,
) -> Result<bool> {
    if query("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'meta' LIMIT 1")?
        .is_empty()
    {
        return Ok(false);
    }
    let rows = query(
        "SELECT key, value FROM meta WHERE key IN ('legacy_omp_reclassification_v2', 'legacy_omp_reclassification_v1', 'legacy_omp_reclassification_analytics_rebuilt_v1')",
    )?;
    let values = rows
        .iter()
        .map(|row| Ok((row.get_typed::<String>(0)?, row.get_typed::<String>(1)?)))
        .collect::<Result<HashMap<_, _>>>()?;
    let state = values
        .get(LEGACY_OMP_RECLASSIFICATION_META_KEY)
        .or_else(|| values.get(PREVIOUS_LEGACY_OMP_RECLASSIFICATION_META_KEY));
    let Some(state) = state else { return Ok(false) };
    if state.starts_with("complete:") || state == "complete" {
        return Ok(false);
    }
    if state == "analytics_pending" {
        return Ok(true);
    }
    let Some(context) = state
        .strip_prefix("analytics_pending:")
        .filter(|s| !s.is_empty())
    else {
        bail!("unexpected legacy OMP migration authority {state:?}");
    };
    Ok(values
        .get(LEGACY_OMP_ANALYTICS_REBUILT_META_KEY)
        .map(String::as_str)
        != Some(context))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SemanticIdentityTier {
    Fast,
    Quality,
}

impl SemanticIdentityTier {
    pub(super) const fn meta_key(self) -> &'static str {
        match self {
            Self::Fast => SEMANTIC_FAST_IDENTITY_REBUILD_META_KEY,
            Self::Quality => SEMANTIC_QUALITY_IDENTITY_REBUILD_META_KEY,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LegacyOmpReclassificationResult {
    pub conversations_reclassified: usize,
    pub lexical_rebuild_required: bool,
    pub analytics_rebuild_required: bool,
}
