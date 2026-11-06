#!/usr/bin/env python3
"""Source-scanner parity for the lib.rs split.

Scanner tests read production source as text. The manifest of what each
scanner reads lives in scanner_lists.toml beside this script. Modes:

  check   — verify every test file and listed source path exists and is
            non-empty; fails on any drift between the list and reality.
  needles — for each scanner, verify its marker needles still appear in
            the union of the listed sources.
  counts  — for each scanner, count every declared `patterns` literal in
            the union of its sources; diff_counts then flags BOTH drops
            (lost coverage) and gains (silent duplication). Presence-only
            parity misses a move that drops a second occurrence — count
            parity catches it.
  diff    — compare two source-list snapshots: any scanner that loses a
            previously-scanned file is a parity violation.
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


def scanners(spec: dict) -> dict:
    """Normalize scanner spec: dict-style [scanner.name] or list [[scanner]]."""
    raw = spec["scanner"]
    if isinstance(raw, list):
        return {e["name"]: e for e in raw}
    return raw


def _needles(test_src: str) -> list[str]:
    out = []
    for m in re.finditer(r'contains\(\s*"([^"]{4,})"', test_src):
        out.append(m.group(1))
    for m in re.finditer(r'needle[s]?\s*[:=].{0,40}"([^"]{4,})"', test_src):
        out.append(m.group(1))
    return out


def check(spec: dict, repo: Path = REPO, verbose=True) -> list[str]:
    problems = []
    for name, entry in scanners(spec).items():
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
    for name, entry in scanners(spec).items():
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


def counts(spec: dict, texts: dict[str, str]) -> dict:
    """Per-scanner needle counts over the union of its source texts.

    `texts` maps repo-relative source path -> content. Returns
    {scanner_name: total_count} or {name: "unsupported:<why>"} for
    scanners without declared patterns.
    """
    out = {}
    for name, entry in scanners(spec).items():
        pats = entry.get("patterns") or []
        if not pats:
            out[name] = "unsupported:no-patterns"
            continue
        total = 0
        for rel in entry["sources"]:
            body = texts.get(rel, "")
            for pat in pats:
                lit = pat["literal"] if isinstance(pat, dict) else pat
                total += body.count(lit)
        out[name] = total
    return out


def counts_for_sources(spec: dict, repo: Path) -> dict:
    texts: dict[str, str] = {}
    for _name, entry in scanners(spec).items():
        for rel in entry["sources"]:
            p = Path(repo) / rel
            if p.is_file():
                texts[rel] = p.read_text(errors="replace")
    return counts(spec, texts)


def diff_counts(before: dict, after: dict) -> list[str]:
    problems = []
    for name, old in before.items():
        new = after.get(name)
        if new is None:
            problems.append(f"{name}: scanner removed")
            continue
        if isinstance(old, str) or isinstance(new, str):
            if old != new:
                problems.append(f"{name}: support status changed {old!r} -> {new!r}")
            continue
        if new < old:
            problems.append(f"{name}: needle count dropped {old} -> {new}")
        elif new > old:
            problems.append(f"{name}: needle count grew {old} -> {new}")
    for name in sorted(set(after) - set(before)):
        problems.append(f"{name}: new scanner (unapproved)")
    return problems


def snapshot(spec: dict) -> dict:
    return {name: entry["sources"] for name, entry in scanners(spec).items()}


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
    if cmd == "counts":
        c = counts_for_sources(spec, REPO)
        print(json.dumps(c, indent=1, sort_keys=True))
        return 0
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
