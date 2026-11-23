mod engine_and_filters;
mod publish_and_sidecars;

use super::publish_guard::{
    ExportPublishGuard, inspect_export_regular_file, write_export_publish_journal,
};
use anyhow::Result;
use std::path::Path;

pub(super) fn write_test_publish_journal(
    final_path: &Path,
    backup_path: &Path,
    candidate_path: &Path,
    publish_guard: &ExportPublishGuard,
) -> Result<()> {
    let prior = inspect_export_regular_file(backup_path, "test prior generation")?;
    let candidate = inspect_export_regular_file(candidate_path, "test candidate generation")?;
    write_export_publish_journal(final_path, backup_path, &prior, &candidate, publish_guard)?;
    Ok(())
}
