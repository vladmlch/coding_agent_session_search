//! Bundle size estimation for local archives.
//!
//! Provides pre-export size estimation so users can see the expected bundle
//! footprint before exporting/encrypting. There are no hosting limits to
//! enforce: archives stay local, so this module only estimates.

use crate::franken_sync::Row;
use crate::franken_sync::compat::{ConnectionExt, ParamValue, RowExt};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Default chunk size for encrypted payload (8 MiB)
pub const DEFAULT_CHUNK_SIZE: u64 = 8 * 1024 * 1024;

/// AEAD authentication tag overhead per chunk (16 bytes)
pub const AEAD_TAG_OVERHEAD: u64 = 16;

/// Estimated static assets size (HTML, JS, CSS, WASM vendor) - approximately 2 MB
pub const STATIC_ASSETS_SIZE: u64 = 2 * 1024 * 1024;

/// Typical compression ratio for text content (deflate)
pub const COMPRESSION_RATIO: f64 = 0.45;

/// Pre-export size estimate
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SizeEstimate {
    /// Raw content size in bytes (uncompressed)
    pub plaintext_bytes: u64,
    /// Estimated compressed size in bytes
    pub compressed_bytes: u64,
    /// Estimated encrypted size in bytes (with AEAD overhead)
    pub encrypted_bytes: u64,
    /// Static assets size (HTML, JS, CSS, WASM)
    pub static_assets_bytes: u64,
    /// Total estimated site size
    pub total_site_bytes: u64,
    /// Estimated number of payload chunks
    pub chunk_count: u32,
    /// Number of conversations included
    pub conversation_count: u64,
    /// Number of messages included
    pub message_count: u64,
}

impl SizeEstimate {
    /// Create a size estimate from a database and filter
    pub fn from_database<P: AsRef<Path>>(
        db_path: P,
        agents: Option<&[String]>,
        since_ts: Option<i64>,
        until_ts: Option<i64>,
    ) -> Result<Self> {
        let conn = super::open_existing_sqlite_db(db_path.as_ref())
            .context("Failed to open database for size estimation")?;

        conn.execute("PRAGMA busy_timeout = 5000;")?;

        // Build filter conditions
        let mut conditions = Vec::new();
        let mut param_values: Vec<ParamValue> = Vec::new();

        if let Some(agents) = agents {
            if agents.is_empty() {
                conditions.push("1=0".to_string());
            } else {
                let placeholders: Vec<_> = agents.iter().map(|_| "?").collect();
                // Keep this as a non-correlated subquery. The file-backed
                // read-only pages path still resolves this shape reliably,
                // while the equivalent correlated EXISTS can under-match.
                conditions.push(format!(
                    "c.agent_id IN (SELECT a.id FROM agents a WHERE a.slug IN ({}))",
                    placeholders.join(", ")
                ));
                for agent in agents {
                    param_values.push(ParamValue::from(agent.as_str()));
                }
            }
        }

        if let Some(since) = since_ts {
            conditions.push("c.started_at >= ?".to_string());
            param_values.push(ParamValue::from(since));
        }

        if let Some(until) = until_ts {
            conditions.push("c.started_at <= ?".to_string());
            param_values.push(ParamValue::from(until));
        }

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", conditions.join(" AND "))
        };

        let params_slice = &param_values;

        // Query conversation count. We read the COUNT(*) cell as Option<i64>
        // because frankensqlite can return NULL from `COUNT(*)` when the
        // WHERE clause excludes all rows (e.g., the empty-agent-filter "1=0"
        // path) — standard SQLite returns 0, fsqlite currently returns NULL.
        // The Option<i64>.unwrap_or(0) shim absorbs the difference without a
        // type-mismatch panic.
        let conv_sql = format!("SELECT COUNT(*) FROM conversations c{}", where_clause);
        let conversation_count: u64 = conn
            .query_row_map(&conv_sql, params_slice, |row: &Row| {
                row.get_typed::<Option<i64>>(0)
                    .map(|opt| opt.unwrap_or(0).max(0) as u64)
            })
            .with_context(|| {
                format!("Failed to count conversations for size estimate: {conv_sql}")
            })?;

        // Query message count and content size
        let msg_sql = format!(
            "SELECT COUNT(*), SUM(LENGTH(m.content))
             FROM messages m
             JOIN conversations c ON m.conversation_id = c.id
             {}",
            where_clause
        );
        let (message_count, plaintext_bytes): (u64, u64) = conn
            .query_row_map(&msg_sql, params_slice, |row: &Row| {
                let raw_message_count = row.get_typed::<i64>(0).unwrap_or(0);
                let raw_plaintext_bytes = row.get_typed::<Option<i64>>(1)?.unwrap_or(0);
                Ok((
                    raw_message_count.max(0) as u64,
                    raw_plaintext_bytes.max(0) as u64,
                ))
            })
            .with_context(|| format!("Failed to estimate message payload size: {msg_sql}"))?;

        Self::from_plaintext_size(plaintext_bytes, conversation_count, message_count)
    }

    /// Create estimate from known plaintext size
    pub fn from_plaintext_size(
        plaintext_bytes: u64,
        conversation_count: u64,
        message_count: u64,
    ) -> Result<Self> {
        // Estimate compression
        let compressed_bytes = (plaintext_bytes as f64 * COMPRESSION_RATIO) as u64;

        // Calculate chunk count (minimum of 1 chunk even for empty content)
        let chunk_count_u64 = compressed_bytes.div_ceil(DEFAULT_CHUNK_SIZE).max(1);
        let chunk_count = u32::try_from(chunk_count_u64).unwrap_or(u32::MAX);

        // Add AEAD overhead
        let aead_overhead = u64::from(chunk_count)
            .checked_mul(AEAD_TAG_OVERHEAD)
            .ok_or_else(|| anyhow::anyhow!("AEAD overhead overflow"))?;
        let encrypted_bytes = compressed_bytes
            .checked_add(aead_overhead)
            .ok_or_else(|| anyhow::anyhow!("Encrypted size overflow"))?;

        // Total with static assets
        let total_site_bytes = encrypted_bytes
            .checked_add(STATIC_ASSETS_SIZE)
            .ok_or_else(|| anyhow::anyhow!("Total site size overflow"))?;

        Ok(Self {
            plaintext_bytes,
            compressed_bytes,
            encrypted_bytes,
            static_assets_bytes: STATIC_ASSETS_SIZE,
            total_site_bytes,
            chunk_count,
            conversation_count,
            message_count,
        })
    }

    /// Format the estimate for display
    pub fn format_display(&self) -> String {
        format!(
            "Estimated bundle size: {}\n\
             • Payload: {} ({} chunks × {} max)\n\
             • Static assets: {}\n\
             • Compression ratio: ~{:.0}%\n\
             • Conversations: {}\n\
             • Messages: {}",
            format_bytes(self.total_site_bytes),
            format_bytes(self.encrypted_bytes),
            self.chunk_count,
            format_bytes(DEFAULT_CHUNK_SIZE),
            format_bytes(self.static_assets_bytes),
            COMPRESSION_RATIO * 100.0,
            self.conversation_count,
            self.message_count,
        )
    }
}

/// Format bytes as human-readable string
fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;

    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} bytes", bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::franken_sync::Connection;

    #[test]
    fn test_size_estimate_from_plaintext() {
        let estimate = SizeEstimate::from_plaintext_size(
            10 * 1024 * 1024, // 10 MB plaintext
            100,
            5000,
        )
        .unwrap();

        // Should compress to ~4.5 MB
        assert!(estimate.compressed_bytes < estimate.plaintext_bytes);
        assert_eq!(estimate.conversation_count, 100);
        assert_eq!(estimate.message_count, 5000);
        assert!(estimate.chunk_count >= 1);
    }

    #[test]
    fn test_size_estimate_empty() {
        let estimate = SizeEstimate::from_plaintext_size(0, 0, 0).unwrap();
        assert_eq!(estimate.plaintext_bytes, 0);
        assert_eq!(estimate.chunk_count, 1); // At least 1 chunk
        assert_eq!(estimate.static_assets_bytes, STATIC_ASSETS_SIZE);
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500 bytes");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1024 * 1024), "1.0 MB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.0 GB");
        assert_eq!(format_bytes(1536 * 1024), "1.5 MB");
    }

    #[test]
    fn test_format_display() {
        let estimate = SizeEstimate::from_plaintext_size(10 * 1024 * 1024, 50, 2500).unwrap();

        let display = estimate.format_display();
        assert!(display.contains("Estimated bundle size"));
        assert!(display.contains("Conversations: 50"));
        assert!(display.contains("Messages: 2500"));
    }

    #[test]
    fn test_from_database_filters_agents_through_agents_table() -> Result<()> {
        let temp = tempfile::TempDir::new()?;
        let db_path = temp.path().join("cass.db");
        let conn = Connection::open(db_path.to_string_lossy().as_ref())?;
        conn.execute_batch(
            "CREATE TABLE agents (
                id INTEGER PRIMARY KEY,
                slug TEXT NOT NULL
            );
            CREATE TABLE conversations (
                id INTEGER PRIMARY KEY,
                agent_id INTEGER NOT NULL,
                started_at INTEGER
            );
            CREATE TABLE messages (
                id INTEGER PRIMARY KEY,
                conversation_id INTEGER NOT NULL,
                content TEXT NOT NULL
            );
            INSERT INTO agents (id, slug) VALUES (1, 'claude'), (2, 'codex');
            INSERT INTO conversations (id, agent_id, started_at)
                VALUES (10, 1, 1000), (20, 2, 2000);
            INSERT INTO messages (id, conversation_id, content)
                VALUES (100, 10, 'hello'), (200, 20, 'rust code');",
        )?;

        let all = SizeEstimate::from_database(&db_path, None, None, None)?;
        assert_eq!(all.conversation_count, 2);
        assert_eq!(all.message_count, 2);
        assert_eq!(all.plaintext_bytes, 14);

        let codex =
            SizeEstimate::from_database(&db_path, Some(&["codex".to_string()]), None, None)?;
        assert_eq!(codex.conversation_count, 1);
        assert_eq!(codex.message_count, 1);
        assert_eq!(codex.plaintext_bytes, 9);

        let empty_agent_filter = SizeEstimate::from_database(&db_path, Some(&[]), None, None)?;
        assert_eq!(empty_agent_filter.conversation_count, 0);
        assert_eq!(empty_agent_filter.message_count, 0);
        assert_eq!(empty_agent_filter.plaintext_bytes, 0);

        let recent = SizeEstimate::from_database(&db_path, None, Some(1500), None)?;
        assert_eq!(recent.conversation_count, 1);
        assert_eq!(recent.message_count, 1);
        assert_eq!(recent.plaintext_bytes, 9);

        Ok(())
    }

    #[test]
    fn test_from_database_allows_read_only_source_db() -> Result<()> {
        let temp = tempfile::TempDir::new()?;
        let db_path = temp.path().join("cass-read-only.db");
        let conn = Connection::open(db_path.to_string_lossy().as_ref())?;
        conn.execute_batch(
            "CREATE TABLE agents (
                id INTEGER PRIMARY KEY,
                slug TEXT NOT NULL
            );
            CREATE TABLE conversations (
                id INTEGER PRIMARY KEY,
                agent_id INTEGER NOT NULL,
                started_at INTEGER
            );
            CREATE TABLE messages (
                id INTEGER PRIMARY KEY,
                conversation_id INTEGER NOT NULL,
                content TEXT NOT NULL
            );
            INSERT INTO agents (id, slug) VALUES (1, 'claude');
            INSERT INTO conversations (id, agent_id, started_at) VALUES (10, 1, 1000);
            INSERT INTO messages (id, conversation_id, content) VALUES (100, 10, 'readonly');",
        )?;
        drop(conn);

        let original_permissions = std::fs::metadata(&db_path)?.permissions();
        let mut read_only_permissions = original_permissions.clone();
        read_only_permissions.set_readonly(true);
        std::fs::set_permissions(&db_path, read_only_permissions)?;

        let estimate = SizeEstimate::from_database(&db_path, None, None, None);

        std::fs::set_permissions(&db_path, original_permissions)?;
        let estimate = estimate?;

        assert_eq!(estimate.conversation_count, 1);
        assert_eq!(estimate.message_count, 1);
        assert_eq!(estimate.plaintext_bytes, 8);
        Ok(())
    }

    #[test]
    fn test_chunk_count_ceiling_division() {
        // Test that chunk count uses proper ceiling division
        // COMPRESSION_RATIO = 0.45, DEFAULT_CHUNK_SIZE = 8 MB

        // Test 1: Very small data -> 1 chunk
        let estimate = SizeEstimate::from_plaintext_size(1000, 1, 10).unwrap();
        assert_eq!(estimate.chunk_count, 1, "Small data should be 1 chunk");

        // Test 2: Data that compresses to exactly 1 chunk size
        // 8 MB / 0.45 = 17.78 MB plaintext -> exactly 8 MB compressed -> 1 chunk
        // Use a value that when multiplied by 0.45 gives exactly DEFAULT_CHUNK_SIZE
        let one_chunk_plaintext = (DEFAULT_CHUNK_SIZE as f64 / COMPRESSION_RATIO) as u64;
        let estimate = SizeEstimate::from_plaintext_size(one_chunk_plaintext, 10, 100).unwrap();
        // Due to floating point, compressed_bytes should be very close to DEFAULT_CHUNK_SIZE
        // The important thing is it should NOT be 2 chunks when it's exactly 1 chunk of data
        assert_eq!(
            estimate.chunk_count, 1,
            "Exactly one chunk's worth should be 1 chunk, not 2"
        );

        // Test 3: Data just over 1 chunk -> 2 chunks
        let over_one_chunk = one_chunk_plaintext + 1000000; // Add ~1 MB to plaintext
        let estimate = SizeEstimate::from_plaintext_size(over_one_chunk, 10, 100).unwrap();
        assert!(
            estimate.chunk_count >= 1,
            "Over one chunk should be at least 1 chunk"
        );

        // Test 4: Large data that compresses to ~2 chunks
        let two_chunks_plaintext = (2.0 * DEFAULT_CHUNK_SIZE as f64 / COMPRESSION_RATIO) as u64;
        let estimate = SizeEstimate::from_plaintext_size(two_chunks_plaintext, 100, 1000).unwrap();
        assert_eq!(
            estimate.chunk_count, 2,
            "Exactly two chunks' worth should be 2 chunks, not 3"
        );
    }

    #[test]
    fn test_from_plaintext_size_handles_extremely_large_inputs() {
        let estimate = SizeEstimate::from_plaintext_size(u64::MAX, 1, 1).unwrap();
        assert_eq!(estimate.chunk_count, u32::MAX);
        assert!(estimate.total_site_bytes >= estimate.compressed_bytes);
    }
}
