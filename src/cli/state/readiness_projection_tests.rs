use super::semantic_readiness_from_state::semantic_readiness_from_state;
use crate::search::readiness::SemanticReadinessState;
use serde_json::json;

#[test]
pub(crate) fn semantic_readiness_requires_current_db_match_for_fast_tier() {
    let state = json!({
        "semantic": {
            "availability": "index_building",
            "can_search": false,
            "fast_tier": {
                "ready": true,
                "current_db_matches": false
            },
            "quality_tier": {
                "ready": false,
                "current_db_matches": null
            },
            "checkpoint": {
                "active": true
            },
            "backlog": {
                "pending_work": true
            }
        }
    });

    assert_eq!(
        semantic_readiness_from_state(&state),
        SemanticReadinessState::Backfilling
    );
}

#[test]
pub(crate) fn semantic_readiness_accepts_current_fast_tier() {
    let state = json!({
        "semantic": {
            "availability": "index_building",
            "can_search": true,
            "fast_tier": {
                "ready": true,
                "current_db_matches": true
            },
            "quality_tier": {
                "ready": false,
                "current_db_matches": null
            },
            "checkpoint": {
                "active": false
            },
            "backlog": {
                "pending_work": false
            }
        }
    });

    assert_eq!(
        semantic_readiness_from_state(&state),
        SemanticReadinessState::FastTierReady
    );
}

/// GH #368 defect 2: a non-quarantine asset-inspection error (the
/// lexical-fingerprint open failing on a corrupt FTS5 structure) must NOT
/// be classified searchable via the `fresh=false` fall-through.
#[test]
pub(crate) fn errored_lexical_index_is_corrupt_not_searchable() {
    use crate::cli::state::state_meta_json::lexical_readiness_from_state;
    use crate::search::readiness::LexicalReadinessState;
    let state = json!({
        "index": {
            "exists": true,
            "status": "error",
            "fresh": false,
            "reason": "asset inspection failed: computing lexical storage fingerprint for /x: fts5: corrupt %_data record: structure segment count exceeds FTS5 maximum"
        },
        "rebuild": { "active": false }
    });
    let lexical = lexical_readiness_from_state(&state, false);
    assert_eq!(lexical, LexicalReadinessState::CorruptQuarantined);
    assert!(
        !lexical.is_searchable(),
        "an errored (corrupt-FTS) index must not report search usable (GH #368 defect 2)"
    );
}

/// A genuine `quarantin`ed error keeps mapping to CorruptQuarantined
/// (unchanged by the defect-2 fix).
#[test]
pub(crate) fn quarantined_lexical_error_stays_corrupt_quarantined() {
    use crate::cli::state::state_meta_json::lexical_readiness_from_state;
    use crate::search::readiness::LexicalReadinessState;
    let state = json!({
        "index": { "exists": true, "status": "error", "fresh": false,
                   "reason": "lexical index quarantined: schema_drift" },
        "rebuild": { "active": false }
    });
    assert_eq!(
        lexical_readiness_from_state(&state, false),
        LexicalReadinessState::CorruptQuarantined
    );
}

/// GH #369 non-regression: a merely-stale index (e.g. an *absent* FTS5
/// shadow where the fingerprint open still succeeds and Tantivy serves
/// search) has status != "error", so it stays StaleButSearchable — the
/// defect-2 fix must not downgrade legitimately-searchable stale indexes.
#[test]
pub(crate) fn stale_healthy_index_stays_searchable() {
    use crate::cli::state::state_meta_json::lexical_readiness_from_state;
    use crate::search::readiness::LexicalReadinessState;
    let state = json!({
        "index": { "exists": true, "status": "stale", "fresh": false },
        "rebuild": { "active": false }
    });
    let lexical = lexical_readiness_from_state(&state, false);
    assert_eq!(lexical, LexicalReadinessState::StaleButSearchable);
    assert!(
        lexical.is_searchable(),
        "a stale-but-healthy index must remain searchable (GH #369 absent-shadow case)"
    );

    let ready = json!({
        "index": { "exists": true, "status": "ready", "fresh": true },
        "rebuild": { "active": false }
    });
    assert_eq!(
        lexical_readiness_from_state(&ready, false),
        LexicalReadinessState::Ready
    );
}
