//! The README and AGENTS dependency tables must name the exact pins that
//! Cargo.toml actually uses (coding_agent_session_search-2l1b0.69).
//!
//! Observed drift: on 2026-09-26 the manifest moved franken-agent-detection
//! to `=0.3.2` while both tables still said `=0.3.1`, and the 2026-09-23 audit
//! found the tables two releases stale.
//!
//! Since 2026-11-07 the fsqlite family resolves from the owner's
//! frankensqlite fork through the committed `[patch.crates-io]` table: the
//! `[dependencies]` entries keep registry-shaped `=0.4.12` requirements, so
//! the fork's git revision is adopted from the patch entry here — otherwise
//! a row documenting the fork source would be unchecked (and the family row
//! would silently drop out of the `checked >= 6` floor).

use std::collections::BTreeMap;
use std::path::Path;

/// An exactly pinned `[dependencies]` entry: its version, and the git
/// revision when it resolves from git instead of the registry — either its
/// own `git`/`rev` fields or the committed `[patch.crates-io]` redirect.
struct Pin {
    version: String,
    rev: Option<String>,
}

/// `[dependencies]` entries pinned to an exact version: key -> pin.
fn exact_pins(manifest: &str) -> BTreeMap<String, Pin> {
    let manifest: toml::Table = manifest.parse().expect("Cargo.toml parses");
    let patches = manifest
        .get("patch")
        .and_then(|patch| patch.get("crates-io"))
        .and_then(toml::Value::as_table);
    // A patch entry contributes a rev only when it is a git redirect.
    let patch_rev = |package: &str| {
        let entry = patches?.get(package)?.as_table()?;
        entry.get("git").and_then(toml::Value::as_str)?;
        entry
            .get("rev")
            .and_then(toml::Value::as_str)
            .map(str::to_owned)
    };
    let deps = manifest["dependencies"]
        .as_table()
        .expect("[dependencies] table");
    deps.iter()
        .filter_map(|(key, spec)| {
            let (version, rev) = match spec {
                toml::Value::String(version) => (version.as_str(), None),
                toml::Value::Table(table) => (
                    table.get("version")?.as_str()?,
                    table.get("rev").and_then(toml::Value::as_str),
                ),
                _ => return None,
            };
            let exact = version.strip_prefix('=')?;
            let package = match spec {
                toml::Value::Table(table) => table.get("package").and_then(toml::Value::as_str),
                _ => None,
            };
            let rev = match rev {
                Some(rev) => Some(rev.to_owned()),
                None => package
                    .map(str::to_owned)
                    .or_else(|| Some(key.clone()))
                    .and_then(|package| patch_rev(&package)),
            };
            Some((
                key.clone(),
                Pin {
                    version: exact.trim().to_owned(),
                    rev,
                },
            ))
        })
        .collect()
}

/// Every mismatch between a doc's pin rows and the manifest, plus how many
/// rows were checked. A registry row is `| names | crates.io `=X` ... |`; a
/// row for a git-pinned dependency names git in its source cell and must
/// carry the exact revision and the version. A row's pin is matched against
/// the first backticked name that is an exactly pinned dependency
/// (`frankentui` rows are keyed by `ftui`).
fn pin_mismatches(doc: &str, pins: &BTreeMap<String, Pin>) -> (Vec<String>, usize) {
    let mut mismatches = Vec::new();
    let mut checked = 0;
    for line in doc.lines() {
        let Some(rest) = line.strip_prefix('|') else {
            continue;
        };
        let mut cells = rest.split('|');
        let (Some(names), Some(source)) = (cells.next(), cells.next()) else {
            continue;
        };
        let source = source.trim();
        let registry = source
            .strip_prefix("crates.io `=")
            .and_then(|pin| pin.split('`').next());
        let dependency = names
            .split('`')
            .skip(1)
            .step_by(2)
            .find(|name| pins.contains_key(*name));
        let Some(dependency) = dependency else {
            if registry.is_some() {
                mismatches.push(format!(
                    "row names no exactly pinned dependency: {}",
                    names.trim()
                ));
            }
            continue;
        };
        let pin = &pins[dependency];
        match (&pin.rev, registry) {
            (Some(rev), _) if registry.is_some() || source.contains("git") => {
                checked += 1;
                if !source.contains(rev.as_str()) || !source.contains(pin.version.as_str()) {
                    mismatches.push(format!(
                        "{dependency}: row does not name git rev `{rev}` and version {}",
                        pin.version
                    ));
                }
            }
            (None, Some(documented)) => {
                checked += 1;
                if pin.version != documented {
                    mismatches.push(format!(
                        "{dependency}: documented `={documented}`, Cargo.toml pins `={}`",
                        pin.version
                    ));
                }
            }
            _ => {}
        }
    }
    (mismatches, checked)
}

fn read(name: &str) -> String {
    std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(name))
        .unwrap_or_else(|error| panic!("read {name}: {error}"))
}

#[test]
fn readme_and_agents_dependency_pins_match_cargo_toml() {
    let pins = exact_pins(&read("Cargo.toml"));
    for doc in ["README.md", "AGENTS.md"] {
        let (mismatches, checked) = pin_mismatches(&read(doc), &pins);
        assert!(
            mismatches.is_empty(),
            "{doc} dependency table disagrees with Cargo.toml:\n{}",
            mismatches.join("\n")
        );
        // frankensqlite, franken-agent-detection, asupersync, frankensearch,
        // frankentui and toon each have a row today.
        assert!(checked >= 6, "{doc}: only {checked} pin rows found");
    }
}

#[test]
fn a_stale_registry_pin_row_is_reported() {
    let pins = exact_pins(&read("Cargo.toml"));
    let current = &pins["frankensearch"].version;
    let stale = read("README.md").replace(
        &format!("| `frankensearch` | crates.io `={current}`"),
        "| `frankensearch` | crates.io `=0.0.1`",
    );
    let (mismatches, _) = pin_mismatches(&stale, &pins);
    assert_eq!(
        mismatches,
        vec![format!(
            "frankensearch: documented `=0.0.1`, Cargo.toml pins `={current}`"
        )]
    );
}

#[test]
fn a_stale_git_pin_row_is_reported() {
    let pins = exact_pins(&read("Cargo.toml"));
    let pin = &pins["franken-agent-detection"];
    let rev = pin
        .rev
        .as_deref()
        .expect("franken-agent-detection resolves from git");
    let stale = read("README.md").replace(rev, "0000000000000000000000000000000000000000");
    let (mismatches, _) = pin_mismatches(&stale, &pins);
    assert_eq!(
        mismatches,
        vec![format!(
            "franken-agent-detection: row does not name git rev `{rev}` and version {}",
            pin.version
        )]
    );
}
