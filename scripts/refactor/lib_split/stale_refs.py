#!/usr/bin/env python3
"""Find stale text references to src/lib.rs.

As items move out of src/lib.rs, textual references in tests, docs and
include_str!/include! directives go stale. This script reports every
`src/lib.rs` path reference and every include_str!()/include!() directive
that resolves inside the crate, so each stage can retarget them and the
reviewer can see nothing was missed.

Usage:
  stale_refs.py [--repo DIR] [--allow FILE]...

Without --allow every hit is reported. --allow files contain one
`path:line-glob` per line that is tolerated (e.g. AGENTS.md plan text,
this script's own docs). Exit code 1 when non-allowed hits exist, so the
gate can run it as a check.
"""

from __future__ import annotations

import fnmatch
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]

# Two classes of hit:
#   consumer — code that actually reads src/lib.rs as text
#              (include_str!/include!/read_to_string/read_text paths or a
#              SOURCES-style table entry). These must be listed in
#              scanner_lists.toml or they are stale.
#   mention  — any other textual occurrence (docs, panic messages,
#              fixture payloads, comments). Informational only.
CONSUMER_RX = re.compile(
    r'include_str!\s*\(\s*"(?:\.\./)*(?:src/)?lib\.rs"'
    r'|include!\s*\(\s*"[^"]*lib\.rs"'
    r'|read_(?:to_string|text)\([^)]*src/lib\.rs'
    r'|"src/lib\.rs",\s*include_str!'
    r'|\.join\(\s*"src/lib\.rs"'
    r'|format!\s*\([^)]*src/lib\.rs'
    r'|join\([^)]*"src/lib\.rs"'
)
MENTION_RX = re.compile(r"src/lib\.rs|lib\.rs\"\)")

# Statement-level patterns for scan_source: every literal form a
# consumer can use to address src/lib.rs (not just direct reads).
_STATEMENT_PATTERNS = [
    (re.compile(r'include_str!\s*\(\s*"(?P<arg>(?:\.\./)*[^"]*lib\.rs)"'),
     "include_str!"),
    (re.compile(r'include!\s*\(\s*"(?P<arg>[^"]*lib\.rs)"'), "include!"),
    (re.compile(r'\.join\(\s*"(?P<arg>src/lib\.rs)"'), "join"),
    (re.compile(r'"(?P<arg>src/lib\.rs)"\s*,\s*include_str!'), "scan-table"),
    (re.compile(r'format!\s*\([^)]*"(?P<arg>[^"]*src/lib\.rs)[^"]*"'), "format!"),
    (re.compile(r'read_(?:to_string|text)\s*\([^)]*"(?P<arg>[^"]*src/lib\.rs)[^"]*"'),
     "read-call"),
]


def scan_source(text: str) -> list:
    """Statement-level hits: every expression addressing src/lib.rs.

    Returns [(line_no, kind, arg)] for each consumer statement —
    include_str!/include!, join("src/lib.rs"), format! producing the
    path, and read_* calls on a literal path.
    """
    hits = []
    for m in re.finditer(r"[^\n;]*;", text, re.S):
        stmt = m.group(0)
        line = text.count("\n", 0, m.start()) + 1
        # fs::write(...join("src/lib.rs")...) builds a temp fixture —
        # a writer, not a reader. Writers are reported separately and
        # never require retargets (evidence: e2e-run-bundle/proof_artifact
        # write fake lib.rs bytes under a TempDir).
        is_write = re.search(
            r"\b(?:std::)?fs::write\s*\(|\.write_all\s*\(", stmt) is not None
        for rx, kind in _STATEMENT_PATTERNS:
            sm = rx.search(stmt)
            if sm:
                arg = sm.group("arg")
                if arg == "lib.rs" or arg.endswith("src/lib.rs") or \
                        arg.endswith("/lib.rs"):
                    hits.append((line, "write" if is_write else kind, arg))
                break
    return hits

SKIP_DIRS = {".git", "target", ".scratch", "node_modules", ".venv"}

# Scanner tests listed in scanner_lists.toml legitimately read src/lib.rs;
# the splitter's own docs/scripts may name it too.
# src/indexer/mod.rs:41927 assigns `file_path: Some(dir.path().join(
# "src/lib.rs"))` — snippet metadata for a TempDir fixture (evidence:
# `dir` is a tempfile::TempDir; the test maps snippets to fixture paths).
# Data, not a source read — allowed at that line only.
BUILTIN_ALLOW = [
    "scripts/refactor/lib_split/*",
    "src/indexer/mod.rs:4192*",
]


def iter_files(repo: Path):
    for p in sorted(repo.rglob("*")):
        if not p.is_file():
            continue
        if any(part in SKIP_DIRS for part in p.relative_to(repo).parts):
            continue
        try:
            p.read_text()
        except (UnicodeDecodeError, PermissionError):
            continue
        yield p


def scan(repo: Path = REPO):
    consumers = []
    mentions = []
    for p in iter_files(repo):
        rel = str(p.relative_to(repo))
        text = p.read_text()
        hit_lines = set()
        for line, kind, arg in scan_source(text):
            consumers.append((rel, line, f"{kind}({arg})"))
            hit_lines.add(line)
        for i, line in enumerate(text.splitlines(), 1):
            if i in hit_lines:
                continue
            if CONSUMER_RX.search(line):
                if re.search(r"\b(?:std::)?fs::write\s*\(|\.write_all\s*\(",
                             line):
                    consumers.append(
                        (rel, i, f"write({line.strip()[:140]})"))
                else:
                    consumers.append((rel, i, line.strip()[:160]))
            elif MENTION_RX.search(line):
                mentions.append((rel, i, line.strip()[:160]))
    return consumers, mentions


def allowed(rel: str, line: int, allow_globs: list[str]) -> bool:
    """`path` globs allow a whole file; `path:line-glob` entries allow
    one line region only (fixture-data exemptions)."""
    for g in allow_globs:
        if ":" in g:
            p, lg = g.rsplit(":", 1)
            if fnmatch.fnmatch(rel, p) and fnmatch.fnmatch(str(line), lg):
                return True
        elif fnmatch.fnmatch(rel, g):
            return True
    return False


def main(argv):
    repo = REPO
    allow = list(BUILTIN_ALLOW)
    i = 1
    while i < len(argv):
        if argv[i] == "--repo":
            repo = Path(argv[i + 1])
            i += 2
        elif argv[i] == "--allow":
            allow += [
                l.strip().split("#")[0].strip()
                for l in Path(argv[i + 1]).read_text().splitlines()
                if l.strip() and not l.strip().startswith("#")
            ]
            i += 2
        elif argv[i] == "--allow-glob":
            allow.append(argv[i + 1])
            i += 2
        else:
            i += 1
    consumers, mentions = scan(repo)
    # consumers outside scanner test files are stale refs to fix
    tomllib = __import__("tomllib")
    listed = {e["test"] for e in
              tomllib.loads(
                  (HERE / "scanner_lists.toml").read_text()
              )["scanner"].values()}
    # [[retarget]] entries already sanction in-lib.rs include sites:
    # a hit inside src/lib.rs whose (kind, arg) matches a declared
    # retarget find is scheduled for rewrite at its stage, not stale.
    man = tomllib.loads((HERE / "manifest.toml").read_text())
    rt_finds = set()
    for rt in man.get("retarget", []):
        m = re.match(
            r'\s*(include_str!|include!)\s*\(\s*"([^"]+)"\s*\)',
            rt.get("find", ""))
        if m:
            rt_finds.add((m.group(1), m.group(2)))

    def retargeted(rel: str, desc: str) -> bool:
        if rel != "src/lib.rs" or "(" not in desc:
            return False
        kind, _, rest = desc.partition("(")
        arg = rest.rstrip(")")
        return (kind, arg) in rt_finds

    writers = [h for h in consumers if h[2].startswith("write(")]
    bad = [
        h for h in consumers
        if not h[2].startswith("write(")
        and not retargeted(h[0], h[2])
        and h[0] not in listed and not allowed(h[0], h[1], allow)
    ]
    for rel, ln, text in bad:
        print(f"STALE-CONSUMER {rel}:{ln}: {text}")
    print(
        f"stale_refs: {len(consumers)} consumers ({len(bad)} unlisted, "
        f"{len(writers)} fixture writers), {len(mentions)} text mentions"
    )
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
