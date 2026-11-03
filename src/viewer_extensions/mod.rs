//! CASS-owned Viewer extension profile.
//!
//! The `viewer_*` tables live inside the SAME canonical `agent_search.db`
//! as the upstream schema-22 tables (spec decision 1: one physical SQLite,
//! no `viewer_read_model.db`/`cass_facts.db`/`user_meta.db` sidecar). They
//! hold user annotations, tags, narrowly derived session facts and logical
//! revisions — never copies of canonical `messages`/`conversations` content
//! or of already-measured canonical columns.
//!
//! [`schema::ensure_schema`] is the only write entry point this module
//! provides; it is additive, idempotent, and must run under an authorized
//! CASS writer before the strict read-only archive opens.
//!
//! [`facts`] is the ingest-pass counterpart (spec decision 5): provider-aware
//! session fact extraction (`extract_session_facts`) and the idempotent
//! `viewer_session_facts` upsert (`publish_session_facts`) the indexer calls
//! inside its persist pass.
//!
//! [`changes`] is the bounded replay journal (spec decision 7, plan Task 11):
//! `viewer_change_log` reads (`changes_since`), the post-commit coalesce/
//! `index_ready`/retention pass (`notify_committed_change`) and the explicit
//! append lane (`append_change_event`). The journal rows themselves are
//! written inside the writers' commits by the observer triggers.
//! [`projection`] is the Task-8 structural projection (spec decisions 4/5):
//! stable steps/turns/tools anchored to canonical messages —
//! `project_session_structure` computes `StepLink`/`TurnLink`/`ToolCallLink`
//! rows and `publish_session_structure` persists them idempotently on
//! `(projection_version, source_revision)`. No `SessionTrajectory` blob or
//! message-body copies are stored; every row is a structural pointer.
//!
//! Facts and structure are ONE extension generation of a canonical source
//! revision, published by `projection::publish_ingest_generation`:
//! computed from a single `ConversationPacket::from_canonical_replay` and
//! committed — with a durable `viewer_meta` ingest receipt — inside one
//! transaction. The receipt (`pending|complete|failed` plus the exact
//! reason) is the publication's durable state: `complete` is only honest
//! when both lane markers are durably visible at the bound
//! `source_revision`; an interrupted or failed generation is reconciled
//! by the explicit resumable backfill, never at request/startup time.
//! Canonical removal drops both the cascade-owned extension rows and the
//! receipt itself (schema-v3 `viewer_ingest_receipt_cleanup` trigger).
//!
//! [`backfill`] is the Task-14 repair lane: facts/projection are ingest-pass
//! publications, so archives indexed BEFORE the profile was installed keep
//! empty extension tables. `backfill_viewer_extensions` hydrates committed
//! canonical rows (messages + snippets) and republishes through the same
//! `ConversationPacket::from_canonical_replay` path the ingest hooks use —
//! bounded per-batch transactions, idempotent on the publishers' revision
//! keys, and a single end-of-run journal notify owned by the caller.
pub mod backfill;
pub mod changes;
pub mod facts;
pub mod projection;
mod providers;
pub mod schema;
