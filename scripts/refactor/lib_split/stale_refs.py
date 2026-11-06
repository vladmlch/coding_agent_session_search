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
    r'include_str!\s*\(\s*"(?:\.\./)*src/lib\.rs"'
    r'|include!\s*\(\s*"[^"]*lib\.rs"'
    r'|read_(?:to_string|text)\([^)]*src/lib\.rs'
    r'|"src/lib\.rs",\s*include_str!'
)
MENTION_RX = re.compile(r"src/lib\.rs|lib\.rs\"\)")

SKIP_DIRS = {".git", "target", ".scratch", "node_modules", ".venv"}

# Scanner tests listed in scanner_lists.toml legitimately read src/lib.rs;
# the splitter's own docs/scripts may name it too.
BUILTIN_ALLOW = [
    "scripts/refactor/lib_split/*",
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
        for i, line in enumerate(p.read_text().splitlines(), 1):
            if CONSUMER_RX.search(line):
                consumers.append((rel, i, line.strip()[:160]))
            elif MENTION_RX.search(line):
                mentions.append((rel, i, line.strip()[:160]))
    return consumers, mentions


def allowed(rel: str, allow_globs: list[str]) -> bool:
    return any(fnmatch.fnmatch(rel, g) for g in allow_globs)


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
    listed = {e["test"] for e in
              __import__("tomllib").loads(
                  (HERE / "scanner_lists.toml").read_text()
              )["scanner"].values()}
    bad = [
        h for h in consumers
        if h[0] not in listed and not allowed(h[0], allow)
    ]
    for rel, ln, text in bad:
        print(f"STALE-CONSUMER {rel}:{ln}: {text}")
    print(
        f"stale_refs: {len(consumers)} consumers ({len(bad)} unlisted), "
        f"{len(mentions)} text mentions"
    )
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
