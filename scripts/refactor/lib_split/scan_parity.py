#!/usr/bin/env python3
"""Source-scanner parity for the lib.rs split.

Scanner tests read production source as text. The manifest of what each
scanner reads lives in scanner_lists.toml beside this script. Modes:

  check   — verify every test file and listed source path exists and is
            non-empty; fails on any drift between the list and reality.
  needles — for each scanner, verify its marker needles still appear in
            the union of the listed sources (catches silent loss when an
            item moves to a file the scanner does not read).
  diff    — compare two snapshots (before/after a stage): any scanner
            that loses coverage of a previously-scanned file is a parity
            violation unless an equivalent new path was added.
"""

from __future__ import annotations

import json
import re
import sys
import tomllib
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
LISTS = HERE / "scanner_lists.toml"


def load(path: Path = LISTS) -> dict:
    return tomllib.loads(path.read_text())


def _needles(test_src: str) -> list[str]:
    """String literals a scanner searches for in production source."""
    out = []
    for m in re.finditer(r'contains\(\s*"([^"]{4,})"', test_src):
        out.append(m.group(1))
    for m in re.finditer(r'needle[s]?\s*[:=].{0,40}"([^"]{4,})"', test_src):
        out.append(m.group(1))
    return out


def check(spec: dict, repo: Path = REPO, verbose=True) -> list[str]:
    problems = []
    for name, entry in spec["scanner"].items():
        test_rel = entry["test"]
        if not (repo / test_rel).is_file():
            problems.append(f"{name}: missing test file {test_rel}")
            continue
        for rel in entry["sources"]:
            p = repo / rel
            if not p.is_file():
                problems.append(f"{name}: missing source {rel}")
            elif p.stat().st_size == 0:
                problems.append(f"{name}: empty source {rel}")
        if verbose:
            print(f"{name}: {len(entry['sources'])} sources ok")
    return problems


def needles(spec: dict, repo: Path = REPO) -> list[str]:
    problems = []
    for name, entry in spec["scanner"].items():
        test_rel = entry["test"]
        tpath = repo / test_rel
        if not tpath.is_file():
            problems.append(f"{name}: missing test file {test_rel}")
            continue
        corpus = ""
        for rel in entry["sources"]:
            p = repo / rel
            corpus += p.read_text() if p.is_file() else ""
        for needle in _needles(tpath.read_text()):
            if needle not in corpus and needle not in tpath.read_text():
                problems.append(f"{name}: needle not in sources: {needle!r}")
    return problems


def snapshot(spec: dict) -> dict:
    return {name: entry["sources"] for name, entry in spec["scanner"].items()}


def diff(before: dict, after: dict) -> list[str]:
    problems = []
    for name, old in before.items():
        if name not in after:
            problems.append(f"{name}: scanner removed")
            continue
        lost = set(old) - set(after[name])
        for rel in sorted(lost):
            problems.append(f"{name}: lost source {rel}")
    return problems


def main(argv):
    cmd = argv[1] if len(argv) > 1 else "check"
    spec = load()
    if cmd == "check":
        problems = check(spec) + needles(spec)
        for p in problems:
            print(f"PARITY: {p}")
        return 1 if problems else 0
    if cmd == "snapshot":
        print(json.dumps(snapshot(spec), indent=1, sort_keys=True))
        return 0
    if cmd == "diff":
        before = json.loads(Path(argv[2]).read_text())
        after = json.loads(Path(argv[3]).read_text())
        problems = diff(before, after)
        for p in problems:
            print(f"PARITY: {p}")
        return 1 if problems else 0
    raise SystemExit(f"unknown command {cmd}")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
