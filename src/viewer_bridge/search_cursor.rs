//! Worker-local leases pin the whole reader set until complete/cancel/reload.
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::time::{Duration, Instant};

use anyhow::Result;
use frankensearch::quill::query::CassQueryFilters;
use serde_json::{Value, json};

use super::protocol::{
    BridgeFilters, MAX_CURSORS, PAGE_LIMIT, ScanBudget, TIMEOUT_MS, encode_line, page_limit, refuse,
};
use super::reader::{ArchiveStamp, StrictArchive};
use super::search::{
    RawRankHit, ReaderSet, decode_hit, engine_filters, parse_lexical_query, raw_page,
};

const IDLE_TTL: Duration = Duration::from_secs(60);

struct RankCursor {
    readers: Rc<ReaderSet>,
    query: String,
    filters: CassQueryFilters,
    query_digest: String,
    issues: Vec<String>,
    totals: Vec<usize>,
    total: usize,
    reader_index: usize,
    offset: usize,
    scanned: usize,
    seen_docs: HashSet<(usize, u32)>,
    archive_stamp: ArchiveStamp,
    catalog_revision: String,
    deadline: Instant,
    last_used: Instant,
}

impl RankCursor {
    fn check(&self, archive: &mut StrictArchive) -> Result<()> {
        if Instant::now() >= self.deadline {
            return Err(refuse(
                "ranking_budget_exceeded",
                "Raw enumeration exceeded its scan deadline",
            ));
        }
        if self.last_used.elapsed() >= IDLE_TTL
            || archive.stamp != self.archive_stamp
            || archive.observe()? != self.catalog_revision
        {
            return Err(refuse(
                "stale_snapshot",
                "Raw enumeration lease or canonical snapshot changed",
            ));
        }
        Ok(())
    }

    fn page(&mut self, archive: &mut StrictArchive, max_hits: usize) -> Result<Vec<RawRankHit>> {
        self.check(archive)?;
        let mut hits = Vec::new();
        while hits.len() < max_hits && self.reader_index < self.totals.len() {
            self.check(archive)?;
            let expected = self.totals[self.reader_index];
            if self.offset == expected {
                self.reader_index += 1;
                self.offset = 0;
                continue;
            }
            let reader = &self.readers.readers[self.reader_index];
            let count = (max_hits - hits.len()).min(expected - self.offset);
            let page = raw_page(reader, &self.query, &self.filters, self.offset, count)?;
            if page.total_count != Some(expected) || page.hits.len() != count {
                return Err(refuse(
                    "stale_snapshot",
                    "Pinned raw total or page cardinality changed",
                ));
            }
            for hit in page.hits {
                if !self.seen_docs.insert((self.reader_index, hit.global_docid)) {
                    return Err(refuse(
                        "stale_snapshot",
                        "Raw pagination repeated an indexed document",
                    ));
                }
                hits.push(decode_hit(
                    reader,
                    &hit,
                    &self.readers.keys[self.reader_index],
                )?);
            }
            self.offset += count;
            self.scanned += count;
        }
        self.check(archive)?;
        self.last_used = Instant::now();
        Ok(hits)
    }

    fn result(&self, hits: Vec<RawRankHit>, token: &str) -> Result<Value> {
        let complete = self.scanned == self.total;
        let value = json!({
            "hits":hits,"next_cursor":if complete { None } else { Some(token) },
            "enumeration_complete":complete,"exact_raw_total":self.total,
            "index_epoch":self.readers.index_epoch,"archive_stamp":self.archive_stamp,
            "catalog_revision":self.catalog_revision,"issues":self.issues,"query_digest":self.query_digest,
            "raw_hits_scanned":self.scanned,"reader_keys":self.readers.keys,
        });
        // Reserve room for the envelope before advancing an externally visible cursor.
        if encode_line(&json!({"schema_version":1,"id":u64::MAX,"ok":true,"result":&value}))
            .is_err()
        {
            return Err(refuse(
                "ranking_budget_exceeded",
                "Raw hit page exceeds the wire byte budget",
            ));
        }
        Ok(value)
    }
}

#[derive(Default)]
pub struct CursorStore {
    active: HashMap<String, RankCursor>,
}

impl CursorStore {
    pub fn clear(&mut self) {
        self.active.clear();
    }

    pub fn cancel(&mut self, token: &str) -> bool {
        self.active.remove(token).is_some()
    }

    pub fn remaining_timeout_ms(&self, token: &str) -> u64 {
        self.active.get(token).map_or(TIMEOUT_MS, |cursor| {
            (cursor
                .deadline
                .saturating_duration_since(Instant::now())
                .as_millis()
                .min(u128::from(TIMEOUT_MS)) as u64)
                .max(1_000)
        })
    }

    pub fn begin(
        &mut self,
        readers: Rc<ReaderSet>,
        archive: &mut StrictArchive,
        query: String,
        filters: BridgeFilters,
        budget: ScanBudget,
    ) -> Result<Value> {
        filters.validate()?;
        budget.validate()?;
        if query.trim().is_empty() || query.len() > 4096 {
            return Err(refuse(
                "invalid_request",
                "Lexical query must contain 1..4096 UTF-8 bytes",
            ));
        }
        let compiled_filters = engine_filters(&filters);
        let (_, issues) = parse_lexical_query(&query, &compiled_filters)?;
        self.active.retain(|_, cursor| {
            cursor.last_used.elapsed() < IDLE_TTL && Instant::now() < cursor.deadline
        });
        if self.active.len() >= MAX_CURSORS {
            return Err(refuse(
                "ranking_budget_exceeded",
                "Eight raw enumeration leases are already active",
            ));
        }
        let deadline = Instant::now() + Duration::from_millis(budget.deadline_ms);
        let revision = archive.observe()?;
        let (mut totals, mut total) = (Vec::new(), 0_usize);
        for reader in &readers.readers {
            if Instant::now() >= deadline {
                return Err(refuse(
                    "ranking_budget_exceeded",
                    "Raw count deadline exceeded",
                ));
            }
            let page = raw_page(reader, &query, &compiled_filters, 0, 1)?;
            let count = page.total_count.ok_or_else(|| {
                refuse(
                    "ranking_budget_exceeded",
                    "Engine supplied no exact raw count",
                )
            })?;
            if page.hits.len() != count.min(1) || count > page.doc_count {
                return Err(refuse(
                    "stale_snapshot",
                    "Raw exact count contradicts the first indexed page",
                ));
            }
            total = total
                .checked_add(count)
                .ok_or_else(|| refuse("ranking_budget_exceeded", "Raw count overflow"))?;
            if total > budget.max_raw_hits {
                return Err(refuse(
                    "ranking_budget_exceeded",
                    "Exact raw match count exceeds the supplied scan cap",
                ));
            }
            totals.push(count);
        }
        let query_digest = blake3::hash(&serde_json::to_vec(&(&query, &filters))?)
            .to_hex()
            .to_string();
        let token = format!("{:032x}", rand::random::<u128>());
        let mut cursor = RankCursor {
            readers,
            query,
            filters: compiled_filters,
            query_digest,
            issues,
            totals,
            total,
            reader_index: 0,
            offset: 0,
            scanned: 0,
            seen_docs: HashSet::new(),
            archive_stamp: archive.stamp.clone(),
            catalog_revision: revision,
            deadline,
            last_used: Instant::now(),
        };
        let hits = cursor.page(archive, PAGE_LIMIT)?;
        let result = cursor.result(hits, &token)?;
        if cursor.scanned < cursor.total {
            self.active.insert(token, cursor);
        }
        Ok(result)
    }

    pub fn next(
        &mut self,
        archive: &mut StrictArchive,
        token: &str,
        max_hits: usize,
    ) -> Result<Value> {
        page_limit(max_hits)?;
        if token.len() != 32 {
            return Err(refuse("stale_snapshot", "Invalid raw cursor"));
        }
        let mut cursor = self.active.remove(token).ok_or_else(|| {
            refuse(
                "stale_snapshot",
                "Raw cursor expired, was cancelled, or belongs to another worker",
            )
        })?;
        let hits = cursor.page(archive, max_hits)?;
        let result = cursor.result(hits, token)?;
        if cursor.scanned < cursor.total {
            self.active.insert(token.to_owned(), cursor);
        }
        Ok(result)
    }
}
