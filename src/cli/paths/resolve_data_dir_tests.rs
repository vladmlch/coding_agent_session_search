use super::{default_data_dir, resolve_data_dir};
use std::path::PathBuf;

#[test]
pub(crate) fn no_overrides_yields_default() {
    assert_eq!(resolve_data_dir(&None, None), default_data_dir());
}

#[test]
pub(crate) fn explicit_data_dir_override_always_wins() {
    let dir = PathBuf::from("/explicit/data-dir");
    let db = PathBuf::from("/scratch/probe.db");
    assert_eq!(resolve_data_dir(&Some(dir.clone()), Some(&db)), dir);
}

#[test]
pub(crate) fn db_inside_default_data_dir_keeps_default() {
    let db = default_data_dir().join("agent_search.db");
    assert_eq!(resolve_data_dir(&None, Some(&db)), default_data_dir());
}

#[test]
pub(crate) fn scratch_db_isolates_data_dir_to_its_parent() {
    // GH #403: `--db /scratch/probe.db` must not leave derived assets
    // (lexical index, raw-mirror, locks) pointing at the live data dir.
    let tmp = tempfile::tempdir().expect("tempdir");
    let db = tmp.path().join("probe.db");
    assert_eq!(resolve_data_dir(&None, Some(&db)), tmp.path());
}
