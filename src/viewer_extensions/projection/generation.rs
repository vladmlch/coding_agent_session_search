use anyhow::{Context, Result};

use crate::franken_sync::compat::{ParamValue, RowExt};
use crate::franken_sync::{Connection, TransactionBehavior};
use crate::model::conversation_packet::ConversationPacket;
use crate::model::types::Conversation;
use crate::viewer_extensions::facts::{FactPublication, FactsDb, extract_session_facts};

use super::parts::project_session_structure;
use super::publish::{now_ms, provenance_of, publish_session_structure};
use super::{PROJECTION_VERSION, ProjectionPublishOutcome, StructuralPublication};

// ---------------------------------------------------------------------------
// Ingest-lane coherent generation publication (completion task)
// ---------------------------------------------------------------------------
//
// Facts and the structural projection are ONE extension generation of a
// canonical source revision: they must be computed from the same replayed
// bytes and committed — with their durable receipt — inside a single
// transaction. A crash before that commit leaves the pre-attempt state
// (the prior receipt — possibly a `complete` at an older revision, honest
// for that revision, since its rows still exist — or no receipt at all);
// a failed generation leaves the exact reason; only rows+receipt
// committed together may ever read `complete`.
//
// Recovery is the explicit `--backfill-viewer-extensions` lane: it scans
// canonical conversations and republishes through THIS same code, so
// `failed`/missing receipts — and stale `complete` receipts at older
// revisions — converge without a startup or request-time reconstruction
// path.

/// Facts + structure computed from ONE canonical replay — the generation
/// unit the ingest lane publishes. `project_session_structure` copies
/// `facts.source_revision`, so the two sides are revision-coherent by
/// construction; the durable [`crate::viewer_extensions::facts::IngestReceipt`] makes that coherence
/// auditable (the receipt's `source_revision` is the generation's own
/// revision, certified only when the rows commit beside it).
pub struct IngestGeneration {
    pub facts: FactPublication,
    pub structure: StructuralPublication,
    /// The shared replay both lanes derived from — the search-region
    /// publication reuses it so every lane binds the identical committed
    /// bytes and `source_revision`.
    pub packet: crate::model::conversation_packet::ConversationPacket,
}

/// Outcome of one coherent generation publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GenerationPublishOutcome {
    /// Facts, structure, parent fixup and the `complete` receipt committed
    /// in one transaction.
    Published,
    /// A `complete` receipt already binds this exact
    /// `(source_revision, extractor_version, projection_version)` — the
    /// durable idempotence key. Nothing was rewritten, so unchanged
    /// reingest moves no row and no revision counter.
    SkippedUnchanged,
    /// Crash-probe short-circuit (test-only): the generation transaction
    /// never committed — the pre-attempt durable state (the prior receipt
    /// or none) is exactly what a pre-commit kill leaves.
    Interrupted,
}

/// The durable-visibility half of `complete`: a receipt only certifies a
/// generation when BOTH lane markers are committed at the bound
/// `source_revision`. A receipt claiming `complete` over absent or stale
/// rows is divergence, never coverage — callers republish rather than
/// trusting it, so manual/accidental extension-row loss self-heals on the
/// next ingest pass or backfill instead of masquerading as complete.
pub fn generation_markers_visible(
    db: &impl FactsDb,
    conversation_id: i64,
    source_revision: i64,
) -> Result<bool> {
    let facts_present = !db
        .facts_query_map(
            "SELECT 1 FROM viewer_session_facts \
             WHERE conversation_id = ?1 AND source_revision = ?2 LIMIT 1",
            &[
                ParamValue::from(conversation_id),
                ParamValue::from(source_revision),
            ],
            |_| Ok(()),
        )?
        .is_empty();
    if !facts_present {
        return Ok(false);
    }
    let structure_present = !db
        .facts_query_map(
            "SELECT 1 FROM viewer_session_projection \
             WHERE conversation_id = ?1 AND source_revision = ?2 LIMIT 1",
            &[
                ParamValue::from(conversation_id),
                ParamValue::from(source_revision),
            ],
            |_| Ok(()),
        )?
        .is_empty();
    if !structure_present {
        return Ok(false);
    }
    // The Task-5 region lane is the third marker `complete` must bind: a
    // receipt over a missing/stale region inventory is divergence the
    // republish below repairs in the same commit. The marks table only
    // exists at schema v7+; profiles at v6 return false here too (the
    // `sqlite_master` probe keeps the check engine-safe), so an upgraded
    // binary republishes every session once and converges.
    let marks_table = !db
        .facts_query_map(
            "SELECT 1 FROM sqlite_master WHERE type='table' \
             AND name='viewer_search_region_marks' LIMIT 1",
            &[],
            |_| Ok(()),
        )?
        .is_empty();
    if !marks_table {
        return Ok(false);
    }
    Ok(!db
        .facts_query_map(
            "SELECT 1 FROM viewer_search_region_marks \
             WHERE conversation_id = ?1 AND source_revision = ?2 \
             AND region_version = ?3 LIMIT 1",
            &[
                ParamValue::from(conversation_id),
                ParamValue::from(source_revision),
                ParamValue::from(crate::viewer_extensions::search_regions::REGION_VERSION),
            ],
            |_| Ok(()),
        )?
        .is_empty())
}

/// Deterministic crash injection for the receipt protocol's restart path:
/// `CASS_TEST_VIEWER_GENERATION_CRASH` returns early BEFORE the generation
/// transaction writes anything — the durable state any pre-commit kill
/// leaves now that the receipt rides the transaction (no receipt for the
/// attempted revision, no generation rows; the prior receipt, if any,
/// stays honest for its own revision).
fn generation_crash_probe() -> bool {
    #[cfg(test)]
    if test_generation_crash_probe_override() {
        return true;
    }
    dotenvy::var("CASS_TEST_VIEWER_GENERATION_CRASH")
        .map(|value| !value.is_empty() && value != "0")
        .unwrap_or(false)
}

/// Bounded re-resolve attempts for the evidence-guarded tie write. A
/// canonical identity change between the resolve probes and the
/// in-transaction guarded write costs one attempt: three races in a row
/// means something is actively mutating the row mid-index — the durable
/// `failed` receipt then records the race and the backfill lane retries,
/// instead of spinning unbounded inside one index run.
const TIE_EVIDENCE_RACE_ATTEMPTS: usize = 3;

/// Deterministic failure injection inside the generation transaction:
/// `CASS_TEST_VIEWER_GENERATION_FAIL` aborts AFTER both lanes published
/// but BEFORE commit — the whole generation (rows AND the in-tx `complete`
/// receipt) rolls back together, so the archive can never hold a facts row
/// without its structure.
fn generation_fail_probe() -> bool {
    #[cfg(test)]
    if test_generation_fail_probe_override() {
        return true;
    }
    dotenvy::var("CASS_TEST_VIEWER_GENERATION_FAIL")
        .map(|value| !value.is_empty() && value != "0")
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// Test-only instrumentation for the publication lane's durable-commit budget.
//
// `TEST_PUBLICATION_COMMIT_CALLS` counts the durable-commit calls
// `publish_ingest_generation` issues on this thread: the generation
// transaction commit, and — on the error path only — the standalone
// `failed` receipt write (an autocommit commit). The publication lane is
// synchronous on the caller's thread, so a thread-local counter keeps
// parallel lib tests from counting each other's commits. Lib tests use it
// to pin the per-conversation commit count without reaching into the
// engine; it compiles away entirely in production builds. The probe
// overrides below are thread-local for the same reason.
// ---------------------------------------------------------------------------

#[cfg(test)]
thread_local! {
    static TEST_PUBLICATION_COMMIT_CALLS: std::cell::Cell<u64> =
        const { std::cell::Cell::new(0) };
}

/// Record one durable-commit call in the publication lane (test builds only).
#[cfg(test)]
fn note_publication_commit_call() {
    TEST_PUBLICATION_COMMIT_CALLS.with(|calls| calls.set(calls.get() + 1));
}

/// Durable-commit calls issued by the publication lane on this thread so far
/// (test builds only).
#[cfg(test)]
pub fn test_publication_commit_calls() -> u64 {
    TEST_PUBLICATION_COMMIT_CALLS.with(std::cell::Cell::get)
}

/// Reset the publication-lane commit-call counter (test builds only).
#[cfg(test)]
pub fn test_reset_publication_commit_calls() {
    TEST_PUBLICATION_COMMIT_CALLS.with(|calls| calls.set(0));
}

#[cfg(test)]
thread_local! {
    static TEST_GENERATION_CRASH_PROBE: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
    static TEST_GENERATION_FAIL_PROBE: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

#[cfg(test)]
fn test_generation_crash_probe_override() -> bool {
    TEST_GENERATION_CRASH_PROBE.with(std::cell::Cell::get)
}

#[cfg(test)]
pub fn test_set_generation_crash_probe(enabled: bool) {
    TEST_GENERATION_CRASH_PROBE.with(|probe| probe.set(enabled));
}

#[cfg(test)]
fn test_generation_fail_probe_override() -> bool {
    TEST_GENERATION_FAIL_PROBE.with(std::cell::Cell::get)
}

#[cfg(test)]
pub fn test_set_generation_fail_probe(enabled: bool) {
    TEST_GENERATION_FAIL_PROBE.with(|probe| probe.set(enabled));
}

/// Deterministic stale-tie-race injection (test-only):
/// `CASS_TEST_VIEWER_TIE_RACE_SQL` holds SQL executed on this connection
/// ONCE per publication attempt-1 — after the tie evidence/resolve probes
/// and before the generation transaction opens. That is exactly the
/// interleaving of a cross-connection canonical identity write racing the
/// publication: the conversations trigger NULLs `tie_key`, and the
/// evidence-guarded write must refuse the stale key rather than persist it.
fn tie_race_probe() -> Option<String> {
    dotenvy::var("CASS_TEST_VIEWER_TIE_RACE_SQL")
        .ok()
        .filter(|sql| !sql.is_empty())
}

/// Compute the whole extension generation for one committed canonical
/// `Conversation` with a single `ConversationPacket::from_canonical_replay`
/// — the same replay `facts_for_canonical` and `structure_for_canonical`
/// each run on their own today, shared here so both sides bind the
/// identical committed bytes (and therefore the identical
/// `source_revision`). Canonical token/cost totals fill `facts.health`
/// through the same `fill_canonical_health` read
/// `publish_facts_for_canonical` performs.
pub fn generation_for_canonical(
    db: &impl FactsDb,
    conv: &Conversation,
    conversation_id: i64,
) -> Result<IngestGeneration> {
    let packet = ConversationPacket::from_canonical_replay(conv, provenance_of(conv));
    let mut facts = extract_session_facts(&packet, &conv.agent_slug);
    crate::viewer_extensions::facts::fill_canonical_health(db, conversation_id, &mut facts)?;
    let structure = project_session_structure(&packet, &facts);
    Ok(IngestGeneration {
        facts,
        structure,
        packet,
    })
}

/// The generation transaction body — callable on any `FactsDb` (an open
/// transaction, or a savepoint body inside the backfill batch). Publishes
/// facts, then structure, then the targeted child-before-parent fixup,
/// then the `complete` receipt. `receipt` must carry
/// `state == RECEIPT_COMPLETE` bound to `generation`'s source revision.
/// Returns the per-lane publication outcomes so callers (the backfill
/// lane's honest stats) can distinguish writes from idempotence skips.
///
/// Nothing here commits: commit/rollback stays with the caller's
/// transaction boundary, so every piece of the generation — rows AND
/// receipt — lands or vanishes together.
pub fn publish_ingest_generation_in_tx(
    db: &impl FactsDb,
    conv: &Conversation,
    conversation_id: i64,
    generation: &IngestGeneration,
    receipt: &crate::viewer_extensions::facts::IngestReceipt,
) -> Result<(
    crate::viewer_extensions::facts::FactPublishOutcome,
    ProjectionPublishOutcome,
)> {
    let facts_outcome = match crate::viewer_extensions::facts::publish_session_facts(
        db,
        conversation_id,
        &generation.facts,
    )? {
        crate::viewer_extensions::facts::FactPublishOutcome::ExtensionNotInstalled => {
            anyhow::bail!("viewer_session_facts missing inside an ensured extension profile");
        }
        outcome => outcome,
    };
    let structure_outcome =
        match publish_session_structure(db, conversation_id, &generation.structure)? {
            ProjectionPublishOutcome::ExtensionNotInstalled => {
                anyhow::bail!(
                    "viewer_session_projection missing inside an ensured extension profile"
                );
            }
            outcome => outcome,
        };
    // Child-before-parent: this generation may BE the parent evidence a
    // previously-ingested child is still waiting on. The fixup is scoped
    // to rows that durably recorded this exact `external_id` — no catalog
    // rescan, no child republish.
    crate::viewer_extensions::facts::fixup_pending_parent_links(
        db,
        conversation_id,
        &generation.facts.provider,
        &conv.source_id,
        conv.external_id.as_deref().unwrap_or_default(),
    )?;
    // Search regions publish AFTER the parent fixup: a child-before-parent
    // landing here may have just linked prior children to this session,
    // and its `subagents` meta regions must see those links in the same
    // generation commit. When this session itself resolved to a parent,
    // the parent's `subagents` regions gain a new descriptor — targeted
    // meta replacement on that parent's rows inside the same transaction
    // (its message-bound region ordinals never renumber).
    crate::viewer_extensions::search_regions::publish_search_regions(
        db,
        conversation_id,
        &generation.packet,
        &generation.structure,
    )?;
    let resolved_parent: Option<i64> = db
        .facts_query_map(
            "SELECT parent_conversation_id FROM viewer_session_facts \
             WHERE conversation_id = ?1",
            &[crate::franken_sync::compat::ParamValue::from(
                conversation_id,
            )],
            |row| row.get_typed::<Option<i64>>(0),
        )?
        .into_iter()
        .next()
        .flatten();
    if let Some(parent_id) = resolved_parent
        && parent_id != conversation_id
    {
        crate::viewer_extensions::search_regions::replace_meta_regions(db, parent_id)?;
    }
    crate::viewer_extensions::facts::write_ingest_receipt(db, receipt)?;
    Ok((facts_outcome, structure_outcome))
}

/// Publish ONE coherent extension generation for a just-committed
/// canonical `Conversation` on a bare writer connection:
///
/// 1. `ensure_profile_for_ingest` — the additive profile exists before
///    any extraction/publication touches it.
/// 2. Compute the generation once (shared replay → shared
///    `source_revision`).
/// 3. Skip when a `complete` receipt already binds this exact generation.
/// 4. Commit the whole generation in ONE durable commit: facts + structure +
///    parent fixup + search/meta regions + sort tie key + the `complete`
///    receipt all ride one transaction. The receipt only becomes durable
///    with the rows it certifies — one fewer durable commit per published
///    conversation than the old standalone `pending` mark.
/// 5. On error, commit a `failed` receipt carrying the exact reason, and
///    return the error (canonical ingest is unaffected — the caller
///    already committed).
///
/// A crash before step 4's commit leaves the pre-attempt state: no receipt
/// for the attempted revision, or the prior receipt — possibly a `complete`
/// at an older revision, honest for that revision since its rows still
/// exist. Both the next ingest run's skip check (revision-keyed: a
/// mismatch never skips) and the backfill lane's full revision-keyed
/// conversation scan republish it. No state here can ever masquerade as
/// `complete` for the current revision.
pub fn publish_ingest_generation(
    connection: &Connection,
    conv: &Conversation,
    conversation_id: i64,
) -> Result<GenerationPublishOutcome> {
    crate::viewer_extensions::schema::ensure_profile_for_ingest(connection)
        .context("ensuring viewer extension profile for ingest publication")?;

    let generation = generation_for_canonical(connection, conv, conversation_id)
        .context("building extension generation from committed canonical bytes")?;
    let source_revision = generation.facts.source_revision;

    let previous =
        crate::viewer_extensions::facts::ingest_receipt_for(connection, conversation_id)?;
    if let Some(receipt) = &previous
        && receipt.state == crate::viewer_extensions::facts::RECEIPT_COMPLETE
        && receipt.source_revision == source_revision
        && receipt.extractor_version == crate::viewer_extensions::facts::FACTS_EXTRACTOR_VERSION
        && receipt.projection_version == PROJECTION_VERSION
        // `complete` is only honest while both lane markers are durably
        // visible at the bound revision — the task's definition of a
        // completed generation. A receipt over missing rows is divergence:
        // republish and let the same commit rewrite the receipt truthfully.
        && generation_markers_visible(connection, conversation_id, source_revision)?
    {
        return Ok(GenerationPublishOutcome::SkippedUnchanged);
    }

    let mut receipt = crate::viewer_extensions::facts::IngestReceipt {
        conversation_id,
        state: crate::viewer_extensions::facts::RECEIPT_PENDING.to_string(),
        source_revision,
        extractor_version: crate::viewer_extensions::facts::FACTS_EXTRACTOR_VERSION,
        projection_version: PROJECTION_VERSION,
        reason: None,
        attempts: previous.map(|r| r.attempts + 1).unwrap_or(1),
        updated_at_ms: now_ms(),
    };
    // The receipt rides the generation transaction below — it only becomes
    // durable when the rows it certifies do, so a published conversation
    // costs ONE durable commit instead of two. A crash before that commit
    // leaves the pre-attempt state (the prior receipt — a `complete` at an
    // older revision, honest for that revision — or none at all); the
    // revision-keyed skip check and the backfill scan both repair it.
    // Never a false `complete`, never a silently missing row-set.
    if generation_crash_probe() {
        return Ok(GenerationPublishOutcome::Interrupted);
    }

    receipt.state = crate::viewer_extensions::facts::RECEIPT_COMPLETE.to_string();
    let work = (|| -> Result<()> {
        // Recompute the browse tie key (verified public identity) on the
        // just-committed canonical row so its persisted value lands inside
        // the same generation transaction as the facts/structure rows it
        // orders browse alongside. A resolver refusal is durable sort state
        // — `tie_error` — never a reason to abort the publication; engine
        // failures DO abort it so the `failed` receipt stays honest.
        //
        // The resolve runs outside the generation transaction; a
        // cross-connection canonical identity change can land between it
        // and the in-transaction write (the conversations trigger NULLs
        // `tie_key`, then a blind overwrite would persist a stale verified
        // key nothing re-invalidates). The evidence-guarded write re-reads
        // the identity columns inside the transaction and refuses on
        // mismatch — the loop rolls back and re-resolves, so the committed
        // key always verifies against the canonical row it commits beside.
        for attempt in 1..=TIE_EVIDENCE_RACE_ATTEMPTS {
            let (sort_tie, evidence) =
                crate::viewer_extensions::sort::resolve_published_tie_with_evidence(
                    connection,
                    conversation_id,
                )
                .context("resolving browse sort tie key")?;
            if attempt == 1
                && let Some(race_sql) = tie_race_probe()
            {
                connection
                    .execute_batch(&race_sql)
                    .context("injecting tie-race canonical mutation")?;
            }
            let mut tx = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .context("opening ingest generation transaction")?;
            publish_ingest_generation_in_tx(&tx, conv, conversation_id, &generation, &receipt)?;
            if crate::viewer_extensions::sort::publish_sort_tie_guarded(
                &tx,
                conversation_id,
                &sort_tie,
                evidence.as_ref(),
            )
            .context("persisting browse sort tie key")?
            {
                if generation_fail_probe() {
                    anyhow::bail!("CASS_TEST_VIEWER_GENERATION_FAIL injected publication fault");
                }
                tx.commit()
                    .context("committing ingest generation transaction")?;
                #[cfg(test)]
                note_publication_commit_call();
                return Ok(());
            }
            // The guard refused: canonical identity moved between the
            // resolve probes and the in-transaction write. Roll the whole
            // generation attempt back (dropping discharges the mandatory
            // rollback before the next statement) and re-resolve against
            // the moved row — never persist the stale key.
            drop(tx);
        }
        anyhow::bail!(
            "canonical identity changed during ingest publication \
             {TIE_EVIDENCE_RACE_ATTEMPTS} times; the durable failed receipt \
             records the race and the backfill lane retries"
        )
    })();

    match work {
        Ok(()) => Ok(GenerationPublishOutcome::Published),
        Err(error) => {
            // The failed mark is a separate durable commit: the generation
            // rolled back, but the archive must remember that THIS source
            // revision's publication was attempted and why it did not land.
            receipt.state = crate::viewer_extensions::facts::RECEIPT_FAILED.to_string();
            receipt.reason = Some(format!("{error:#}"));
            receipt.updated_at_ms = now_ms();
            if let Err(mark_error) =
                crate::viewer_extensions::facts::write_ingest_receipt(connection, &receipt)
            {
                tracing::warn!(
                    conversation_id,
                    error = %mark_error,
                    "failed to record the ingest publication failure durably"
                );
            } else {
                #[cfg(test)]
                note_publication_commit_call();
            }
            Err(error)
        }
    }
}

/// Best-effort single-conversation hook: publish the coherent generation,
/// logging any failure without aborting the (already committed) canonical
/// ingest. The durable receipt records the exact reason either way.
pub fn publish_ingest_generation_best_effort(
    connection: &Connection,
    conv: &Conversation,
    conversation_id: i64,
) {
    if let Err(error) = publish_ingest_generation(connection, conv, conversation_id) {
        tracing::warn!(
            conversation_id,
            error = %error,
            "viewer extension generation publication failed; \
             the durable ingest receipt records the reason and the \
             --backfill-viewer-extensions lane reconciles it"
        );
    }
}

/// Best-effort batch hook: the same coherent-generation protocol per
/// conversation, so a single bad row leaves an honest `failed` receipt
/// instead of aborting the batch's other work.
pub fn publish_ingest_generations_batch_best_effort<'a>(
    connection: &Connection,
    pairs: impl IntoIterator<Item = (&'a Conversation, i64)>,
) {
    for (conv, conversation_id) in pairs {
        if let Err(error) = publish_ingest_generation(connection, conv, conversation_id) {
            tracing::warn!(
                conversation_id,
                error = %error,
                "viewer extension generation publication failed; \
                 the durable ingest receipt records the reason and the \
                 --backfill-viewer-extensions lane reconciles it"
            );
        }
    }
}
