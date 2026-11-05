# Version History

This document tracks schema versions and breaking changes for cass archives and databases.

## Schema Versions

### Database Schema

The internal database uses a versioned schema stored in the `meta` table. The schema version is checked on open and automatically migrated when possible.

| Version | Date | Changes |
|---------|------|---------|
| 8 | Current | Added source provenance tracking, multi-source support |
| 7 | 2024-Q4 | Added snippet support, code FTS indexing |
| 6 | 2024-Q4 | Added workspace display names |
| 5 | 2024-Q3 | Initial stable schema |

#### Migration Support

- **Forward compatible**: Databases from schema v5+ can be opened by current code
- **Automatic migration**: v5-v7 databases are automatically upgraded
- **Rebuild required**: v0-v4 databases require full reindex

### Encryption Config Schema

Encrypted archives use a versioned config.json format.

| Version | Date | Changes |
|---------|------|---------|
| 2 | Current | Added recovery key slots, HKDF support |
| 1 | 2024-Q3 | Initial format with Argon2id + AES-256-GCM |

## Version Compatibility Matrix

| cass Version | DB Schema | Encryption Format | Notes |
|--------------|-----------|-------------------|-------|
| 0.11.x | 8 | 1-2 | Local-only; robot contract v2 |
| 0.1.x | 5-8 | 1-2 | Prior stable |
| 0.0.x | 1-4 | - | Pre-encryption, requires rebuild |

## Breaking Changes

### 0.11.0 - Local-only conversion; robot contract v2

cass is fully local-only. Every removed invocation fails fast with a
structured `removed-command` error that names the local equivalent.

**Removed commands:** `cass swarm` (dashboard, macros, resource-plan,
privacy-preview, repro-capsule, evidence, status), `cass lessons`,
`cass fleet`, `cass release-verify`, `cass upgrade`,
`cass sources sync|setup|discover|reingest|artifact-manifest|schedule`
(and the `sources add user@host` remote form), `cass models check-update`.
The TUI "Swarm" tab and the update banner went with them.

**Removed flags/values:** `cass guide --allow-rch`; `cass models install`
without `--from-file` plus `--mirror` and `--yes`; `cass models verify
--repair`; `--embedder external` under every command; `cass pages --target
github|cloudflare` (`local` remains the only value) and the hosted deploy
flags `--project`, `--repo`, `--branch`, `--account-id`, `--api-token`;
`cass export-html --no-cdns` (exports are always self-contained).

**Deprecated-ignored environment variables** (diagnosed, never honored):
`CASS_SSH_CONFIG`, `CASS_SEMANTIC_DOWNLOAD_POLICY`,
`CASS_EXTERNAL_EMBEDDING_URL|MODEL|DIMENSION|REVISION|TIMEOUT_MS|BATCH_SIZE|
MAX_REQUEST_BYTES|API_KEY|MAX_RETRIES` (and `CASS_EXTERNAL_EMBEDDINGS`),
`CASS_SWARM_AGENT_MAIL_URL`, `CASS_SWARM_AGENT_MAIL_TOKEN`,
`CASS_FLEET_BUDGET_MS`, `CASS_FLEET_PER_HOST_BUDGET_MS`,
`CODING_AGENT_SEARCH_NO_UPDATE_PROMPT`, `CASS_SKIP_UPDATE`,
`CASS_UPDATE_API_BASE_URL`.

**Removed machinery:** SSH/SFTP sync (`ssh2`), the HTTP model downloader and
external HTTP embedder (`reqwest`/`rustls`), vendored `openssl`, direct
`url`, network installers (`install.sh`/`install.ps1`), the Homebrew
formula, release publishing, and update checks/prompts; `semver` moved to
dev-dependencies; `asupersync` `tls-native-roots` feature dropped.

**Migration paths:**
- Install/upgrade: `cargo install --path . --locked --bin cass` from a
  checkout. There is no update checker; `cass upgrade` says so.
- Models: `cass models install --from-file <dir>` installs the pinned
  MiniLM manifest with SHA256 verification. Model files are fetched
  outside cass by the operator.
- Remote archives: historical `type = "ssh"` sources.toml entries remain
  readable as inert archive records; existing
  `<data-dir>/remotes/<name>/mirror/` directories keep indexing read-only
  via `cass index` (`--full` or `--watch-once`).
- Pages: `cass pages --export-only <dir>` writes the local bundle and
  `cass pages --preview` serves it on `127.0.0.1`; there is no hosted
  target to deploy to.
- Archive retention policy: missing former remote sources never prune
  indexed rows; use `cass mirror prune` for explicit raw-mirror retention.

**Contract:** `contract_version` bumped `"1" → "2"`: robot `--json`/`--robot`
envelopes and `cass-trace` headers now report `"2"`. Automation that pins
contract v1 must update.

### 0.1.50 - Source Provenance
- Added `sources` table for tracking import sources
- Migration: Automatic, adds table with default local source

### 0.1.40 - Encryption
- Introduced encrypted export format
- Breaking: Exported archives now require password

### 0.1.30 - Workspace Support
- Added `workspaces` table
- Migration: Automatic, extracts workspaces from conversation paths

## Upgrading

### Database Upgrades

Database upgrades happen automatically when opening with `SqliteStorage::open_with_migration()`:

```rust
use coding_agent_search::storage::sqlite::SqliteStorage;

let storage = SqliteStorage::open_with_migration(&db_path)?;
// Database is now at current schema version
```

### Archive Decryption

Old archives (v1) are compatible with current code:

```rust
use coding_agent_search::pages::encrypt::decrypt_archive;

// Works with both v1 and v2 archives
let data = decrypt_archive(&archive_path, password)?;
```

## Data Preservation

When migration fails:
1. A backup is created at `{db_path}.backup.{timestamp}`
2. Original file is preserved
3. Full reindex is required

## Testing Version Compatibility

```bash
# Run upgrade tests (local Cargo; there is no remote build offload)
cargo test --test upgrade

# Test specific version migration
cargo test test_schema_version_5_to_current
```

## Future Versions

Planned changes for future versions:

### v9 (Planned)
- Semantic search embeddings table
- Vector index metadata

### Encryption v3 (Planned)
- Additional KDF algorithms
- Key rotation support

## FAQ

### Can I open old archives with new versions?
Yes, all encryption v1+ archives are forward compatible.

### What if migration fails?
The original database is backed up. You can restore it or reindex from source files.

### How do I check my database version?

```bash
cass health  # Shows schema version
# or
sqlite3 ~/.local/share/cass/cass.db "SELECT * FROM meta WHERE key='schema_version'"
```

### Can I downgrade?
Not recommended. Newer features may store data incompatible with old versions. Always backup before upgrading.
