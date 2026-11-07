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
            previously-scanned file is a parity violation. Input must be
            a {scanner: [source paths]} map — counts snapshots are
            rejected.
  diff-counts — compare two `counts` snapshots: per-pattern drops and
            gains are both flagged. Input must be a
            {scanner: {pattern: int}} (or "unsupported:<why>" status)
            map — source-list snapshots are rejected.
            (Added after a wrong-mode `diff` invocation on counts JSON
            compared scanner names only and returned vacuous success
            on a real coverage loss.)
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


# A "read var" must be initialized from a file read whose statement
# names a Rust source path — reads of temp fixtures, DB files and
# /proc do not create source-scan variables.
_READ_VAR = re.compile(
    r"\blet\s+(\w+)\s*=[^;]*?"
    r"(?:read_to_string|include_str!|fs::read|fs::read_to_string)"
    r"[^;]*?\.rs")
# `for v in [a, b]` — loop var over source vars (sqlite's dual-source
# absence scan).
_FOR_READ_VAR = re.compile(
    r"\bfor\s+(\w+)\s+in\s+\[([^\]]+)\]")
# `X.contains("lit")` / `X.matches("lit")` — X may carry field/slice
# suffixes (`repair_err.message.`, `src[ls..le].`); a bare call receiver
# like `logs().contains(` does not match, which is correct.
_CONTAINS = re.compile(
    r'(!)?\s*(\w+)(?:\.\w+|\[[^\]]*\])*\s*\.\s*'
    r'(?:contains|matches)\s*\(\s*"([^"]{4,})"')
_DECL_NEEDLE = re.compile(r'needle[s]?\s*[:=][^"\n]{0,40}"([^"]{4,})"')


def _source_vars(test_src: str) -> set:
    """Variables initialized (possibly transitively) from a file read —
    `lib_source = include_str!`, `src = read_to_string`, `view = &src[..]`.
    A `contains` on anything else asserts over runtime data, not source.
    """
    direct = set(_READ_VAR.findall(test_src))
    out = set(direct)
    # Only shallow content views propagate: `&v[..]`, `v.trim()`,
    # `v.clone()`, `&v`, `v[..]` — expressions where v2 still IS the
    # file text. Arbitrary `f(v, …)` results are not source views.
    props = re.findall(
        r"\blet\s+(\w+)\s*=\s*&?\s*(\w+)\s*"
        r"(?:\[[^\]]*\]|\.(?:trim|clone|to_string|to_owned|as_str)\(\)\s*)*;",
        test_src)
    fors = _FOR_READ_VAR.findall(test_src)
    for _ in range(4):
        grew = False
        for name, base in props:
            if name not in out and base in out:
                out.add(name)
                grew = True
        for name, elems in fors:
            if name not in out and any(
                    re.search(rf"\b{re.escape(v)}\b", elems) for v in out):
                out.add(name)
                grew = True
        if not grew:
            break
    return out


def _needles(test_src: str) -> tuple[list[str], list[str]]:
    """(positive, absent) needle literals against the scanned sources.

    Positive: `srcvar.contains("…")` / `srcvar.matches("…")` where srcvar
    holds file contents — the literal must exist in the sources.
    Absent: `!srcvar.contains("…")` — the literal must NOT exist (the
    `fn run_doctor(` ban in src/doctor.rs's dispatch test).
    """
    srcs = _source_vars(test_src)
    pos, neg = [], []
    for m in _CONTAINS.finditer(test_src):
        bang, recv, lit = m.group(1), m.group(2), m.group(3)
        if recv not in srcs:
            continue  # runtime-data assert, not a source scan
        (neg if bang else pos).append(lit)
    for m in _DECL_NEEDLE.finditer(test_src):
        pos.append(m.group(1))
    return pos, neg


def _corpus(entry: dict, repo: Path, exclude: str | None = None) -> str:
    """Union of a scanner's file sources plus every .rs under its
    directory scans (`dirs`). `exclude` drops one source — the needle
    check must not let the scanning test file itself prove (or break)
    coverage: a needle living only in the test is a dead scan, and a
    `!contains` ban would self-match its own literal."""
    out = []
    for rel in entry["sources"]:
        if rel == exclude:
            continue
        p = repo / rel
        if p.is_file():
            out.append(p.read_text(errors="replace"))
    for rel in entry.get("dirs", []):
        d = repo / rel
        if d.is_dir():
            for p in sorted(d.rglob("*.rs")):
                if p.is_file():
                    rel_p = str(p.relative_to(repo)).replace("\\", "/")
                    if rel_p == exclude:
                        continue
                    out.append(p.read_text(errors="replace"))
    return "".join(out)


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
        for rel in entry.get("dirs", []):
            d = repo / rel
            if not d.is_dir():
                problems.append(f"{name}: missing dir-scan root {rel}")
            elif not any(d.rglob("*.rs")):
                problems.append(f"{name}: dir-scan root {rel} has no .rs")
        if verbose:
            print(f"{name}: {len(entry['sources'])} sources, "
                  f"{len(entry.get('dirs', []))} dirs ok")
    return problems


def needles(spec: dict, repo: Path = REPO) -> list[str]:
    problems = []
    for name, entry in scanners(spec).items():
        test_rel = entry["test"]
        tpath = repo / test_rel
        if not tpath.is_file():
            problems.append(f"{name}: missing test file {test_rel}")
            continue
        corpus = _corpus(entry, repo, exclude=test_rel)
        pos, neg = _needles(tpath.read_text())
        for needle in pos:
            # A needle must live in the scanned sources themselves —
            # surviving only inside the test file is a dead scan.
            if needle not in corpus:
                problems.append(f"{name}: needle not in sources: {needle!r}")
        for needle in neg:
            # `!srcvar.contains(…)` is an absence scan — the needle
            # must stay out of the scanned sources.
            if needle in corpus:
                problems.append(
                    f"{name}: banned needle present in sources: {needle!r}")
    return problems


def _pattern_spec(pat) -> tuple[str, object, str | None]:
    """Normalize one `patterns` entry to (key, matcher, suffix).

    Accepted shapes:
      "literal"                       — count `body.count(literal)`
      {literal = "…"}                 — same, table form
      {name = "…", regex = "…", suffix = ".rs"}
                                      — count regex matches PER FILE
                                        (never across joined sources),
                                        restricted to sources whose
                                        path ends in `suffix` when a
                                        suffix is declared.
    `name`/`suffix` are regex-only options: a literal counts over the
    joined union, so a literal table carrying either is rejected —
    silently dropping a declared scope would undercount.
    Anything else is malformed: raise rather than silently count zero.
    """
    if isinstance(pat, str):
        return pat, pat, None
    if not isinstance(pat, dict):
        raise ValueError(f"pattern must be a string or table, got {pat!r}")
    if "literal" in pat and "regex" in pat:
        raise ValueError(f"ambiguous pattern declares both literal and regex: {pat!r}")
    unknown = set(pat) - {"literal", "regex", "name", "suffix"}
    if unknown:
        raise ValueError(f"pattern has unknown keys {sorted(unknown)}: {pat!r}")
    suffix = pat.get("suffix")
    if suffix is not None and not isinstance(suffix, str):
        raise ValueError(f"pattern suffix must be a string: {pat!r}")
    if "literal" in pat:
        lit = pat["literal"]
        if not isinstance(lit, str) or not lit:
            raise ValueError(f"literal pattern must be a nonempty string: {pat!r}")
        scoped = sorted(set(pat) - {"literal"})
        if scoped:
            raise ValueError(
                f"literal pattern ignores options {scoped} "
                f"(name/suffix are regex-only): {pat!r}")
        return lit, lit, None
    if "regex" in pat:
        rx = pat["regex"]
        if not isinstance(rx, str) or not rx:
            raise ValueError(f"regex pattern must be a nonempty string: {pat!r}")
        try:
            compiled = re.compile(rx, re.MULTILINE)
        except re.error as e:
            raise ValueError(f"regex pattern does not compile: {pat!r}: {e}")
        key = pat.get("name")
        if key is not None and not isinstance(key, str):
            raise ValueError(f"pattern name must be a string: {pat!r}")
        return key or rx, compiled, suffix
    raise ValueError(f"pattern needs a literal or regex key: {pat!r}")


def counts(spec: dict, texts: dict[str, str],
             repo: Path | None = None) -> dict:
    """Per-scanner PER-PATTERN needle counts over the union of its
    source texts — a lost match for one pattern cannot be hidden by a
    gained match for another.

    `texts` maps repo-relative source path -> content. Returns
    {scanner_name: {pattern: count}} or {name: "unsupported:<why>"} for
    scanners without declared patterns. When `repo` is given, `dirs`
    directory scans are expanded and read from disk.

    Literal patterns count over the joined source union (historical
    behavior). Regex patterns count PER FILE and may declare a `suffix`
    (e.g. ".rs") so documentation sources are excluded — matching the
    Rust source-reader extractors, which scan each file separately and
    can never match a pair spanning a file boundary.
    """
    out = {}
    for name, entry in scanners(spec).items():
        pats = entry.get("patterns") or []
        if not pats:
            out[name] = "unsupported:no-patterns"
            continue
        per = {}
        files: list[tuple[str, str]] = []
        for rel in entry["sources"]:
            files.append((rel, texts.get(rel, "")))
        if repo is not None and entry.get("dirs"):
            for rel in entry["dirs"]:
                d = Path(repo) / rel
                if d.is_dir():
                    for p in sorted(d.rglob("*.rs")):
                        if p.is_file():
                            rp = str(p.relative_to(repo)).replace("\\", "/")
                            files.append((rp, p.read_text(errors="replace")))
        body = "".join(t for _, t in files)
        for pat in pats:
            key, matcher, suffix = _pattern_spec(pat)
            if isinstance(matcher, str):
                per[key] = body.count(matcher)
            else:
                per[key] = sum(
                    len(matcher.findall(t))
                    for rp, t in files
                    if suffix is None or rp.endswith(suffix))
        out[name] = per
    return out


def counts_for_sources(spec: dict, repo: Path) -> dict:
    texts: dict[str, str] = {}
    for _name, entry in scanners(spec).items():
        for rel in entry["sources"]:
            p = Path(repo) / rel
            if p.is_file():
                texts[rel] = p.read_text(errors="replace")
    return counts(spec, texts, repo=repo)


def _validate_source_snapshot(obj, label: str) -> None:
    """`diff` input must map scanner -> list of source paths. Feeding a
    counts snapshot here used to compare scanner names only and return
    vacuous success on a real coverage loss — fail closed instead."""
    if not isinstance(obj, dict):
        raise ValueError(
            f"{label}: expected a map of scanner -> source list, "
            f"got {type(obj).__name__}")
    for name, val in obj.items():
        if not isinstance(val, list) or not all(
                isinstance(s, str) for s in val):
            raise ValueError(
                f"{label}: {name}: expected a list of source paths, "
                f"got {type(val).__name__} — a counts snapshot belongs "
                f"to `diff-counts`")


def _validate_counts_snapshot(obj, label: str) -> None:
    """`diff-counts` input must map scanner -> {pattern: int} or a
    "unsupported:<why>" status string (counts output shape)."""
    if not isinstance(obj, dict):
        raise ValueError(
            f"{label}: expected a map of scanner -> counts, "
            f"got {type(obj).__name__}")
    for name, val in obj.items():
        if isinstance(val, str):
            continue
        if not isinstance(val, dict):
            raise ValueError(
                f"{label}: {name}: expected a per-pattern count map or "
                f"status string, got {type(val).__name__} — a source-list "
                f"snapshot belongs to `diff`")
        for pat, c in val.items():
            if not isinstance(c, int):
                raise ValueError(
                    f"{label}: {name}.{pat}: count must be an int, "
                    f"got {type(c).__name__}")


def diff_counts(before: dict, after: dict) -> list[str]:
    _validate_counts_snapshot(before, "before")
    _validate_counts_snapshot(after, "after")
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
        # per-pattern comparison — drops and gains flagged independently
        for pat, oc in old.items():
            nc = new.get(pat)
            if nc is None:
                problems.append(f"{name}: pattern removed {pat!r}")
            elif nc < oc:
                problems.append(
                    f"{name}: needle count dropped for {pat!r} {oc} -> {nc}")
            elif nc > oc:
                problems.append(
                    f"{name}: needle count grew for {pat!r} {oc} -> {nc}")
        for pat in sorted(set(new) - set(old)):
            problems.append(f"{name}: new pattern {pat!r} (unapproved)")
    for name in sorted(set(after) - set(before)):
        problems.append(f"{name}: new scanner (unapproved)")
    return problems


def snapshot(spec: dict, repo: Path | None = None) -> dict:
    """Sources each scanner reads; `dirs` expand to their .rs file set
    when `repo` is provided (so stage moves surface as path renames)."""
    out = {}
    for name, entry in scanners(spec).items():
        srcs = list(entry["sources"])
        if repo is not None:
            for rel in entry.get("dirs", []):
                d = Path(repo) / rel
                if d.is_dir():
                    srcs += sorted(
                        str(p.relative_to(repo)).replace("\\", "/")
                        for p in d.rglob("*.rs"))
        out[name] = srcs
    return out


def diff(before: dict, after: dict) -> list[str]:
    _validate_source_snapshot(before, "before")
    _validate_source_snapshot(after, "after")
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
        print(json.dumps(snapshot(spec, repo=REPO), indent=1, sort_keys=True))
        return 0
    if cmd == "diff":
        before = json.loads(Path(argv[2]).read_text())
        after = json.loads(Path(argv[3]).read_text())
        try:
            problems = diff(before, after)
        except ValueError as e:
            print(f"PARITY: {e}", file=sys.stderr)
            return 2
        for p in problems:
            print(f"PARITY: {p}")
        return 1 if problems else 0
    if cmd == "diff-counts":
        before = json.loads(Path(argv[2]).read_text())
        after = json.loads(Path(argv[3]).read_text())
        try:
            problems = diff_counts(before, after)
        except ValueError as e:
            print(f"PARITY: {e}", file=sys.stderr)
            return 2
        for p in problems:
            print(f"PARITY: {p}")
        return 1 if problems else 0
    raise SystemExit(f"unknown command {cmd}")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
