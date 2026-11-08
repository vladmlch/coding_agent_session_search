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
from collections import Counter, deque
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
sys.path.insert(0, str(HERE))
import rust_items as ri  # noqa: E402

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


def _scan_source_with_offsets(text: str) -> list:
    """(byte_off, line, kind, arg) per consumer statement.

    `byte_off` is the utf-8 byte offset of the matched expression inside
    `text` — the exact site a named-item span binds to. Line numbers and
    (kind, arg) match scan_source hit-for-hit, same order.
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
                    off = len(
                        text[:m.start() + sm.start()].encode("utf-8"))
                    hits.append(
                        (off, line, "write" if is_write else kind, arg))
                break
    return hits


def scan_source(text: str) -> list:
    """Statement-level hits: every expression addressing src/lib.rs.

    Returns [(line_no, kind, arg)] for each consumer statement —
    include_str!/include!, join("src/lib.rs"), format! producing the
    path, and read_* calls on a literal path.
    """
    return [(line, kind, arg)
            for _off, line, kind, arg in _scan_source_with_offsets(text)]

SKIP_DIRS = {".git", "target", ".scratch", "node_modules", ".venv"}

# Scanner tests listed in scanner_lists.toml legitimately read src/lib.rs;
# the splitter's own docs/scripts may name it too.
# src/indexer/mod.rs:42569 assigns `file_path: Some(dir.path().join(
# "src/lib.rs"))` — snippet metadata for a TempDir fixture (evidence:
# `dir` is a tempfile::TempDir; the test maps snippets to fixture paths).
# Data, not a source read — allowed at that exact line only.
BUILTIN_ALLOW = [
    "scripts/refactor/lib_split/*",
    "src/indexer/mod.rs:42569",
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


_WITH_INCLUDE_RX = re.compile(
    r'(include_str!|include!)\s*\(\s*"([^"]+)"\s*\)')


def fn_item_spans(text: str) -> list[tuple[str, int, int]]:
    """(name, start_byte, end_byte) of every fn item, recursing into
    inline mods and brace macros.

    Item spans are exact rust_items byte offsets into `text.encode()`
    (attr/doc trivia included) — code on the same physical line but
    outside the item stays outside. A file the lexer rejects yields no
    spans at all — callers treat that as "nothing can be bound", which
    fails closed.
    """
    try:
        items, _cls, _gaps = ri.parse_items(text)
    except ri.LexError:
        return []
    out = []

    def walk(its):
        for it in its:
            if it.kind == "fn":
                out.append((it.name, it.start, it.end))
            walk(it.inner)

    walk(items)
    return out


def retarget_with_allowances(man: dict) -> dict:
    """Emitted-output include allowances: test_file -> test fn ->
    Counter[(kind, arg)].

    A [[retarget]] `with` is the sanctioned reader expression the
    splitter emitted inside its named test: the include multiset it
    declares is exactly what that fn may contain. Staged rewrites of
    one site (lib.rs -> ../lib.rs -> run.rs) share the (file, fn) key;
    the allowance per arg is the most any single `with` declares —
    a sum across stages would admit a state no stage emits. Retargets
    without a named test or test_file bind nothing and admit nothing.
    """
    out = {}
    for rt in man.get("retarget", []):
        test, tfile = rt.get("test"), rt.get("test_file")
        if not test or not tfile:
            continue
        declared = Counter(
            (m.group(1), m.group(2))
            for m in _WITH_INCLUDE_RX.finditer(rt.get("with", "")))
        if not declared:
            continue
        allow = out.setdefault(tfile, {}).setdefault(test, Counter())
        for key, n in declared.items():
            allow[key] = max(allow[key], n)
    return out


def emitted_exemptions(text: str, allowances: dict) -> tuple:
    """(admitted byte offsets, offset hits) for one file's text.

    `allowances` maps test fn name -> Counter[(kind, arg)]. A hit whose
    include byte offset sits inside the uniquely-named test fn's item
    byte span consumes one declared allowance of its kind — code on the
    same physical line but outside that item span, a missing or
    ambiguous fn name, and occurrences past the declared multiplicity
    admit nothing. The returned `hits` are the file's
    (byte_off, line, kind, arg) consumer statements in source order,
    so callers can map each reported hit back to its exact site.
    """
    spans = fn_item_spans(text)
    hits = _scan_source_with_offsets(text)
    adm = set()
    for test, declared in allowances.items():
        named = [s for s in spans if s[0] == test]
        if len(named) != 1:
            continue
        _name, lo, hi = named[0]
        remaining = Counter(declared)
        for off, _line, kind, arg in hits:
            if lo <= off < hi and remaining[(kind, arg)] > 0:
                remaining[(kind, arg)] -= 1
                adm.add(off)
    return adm, hits


def _desc_parts(desc: str):
    """Split a consumer descriptor `kind(arg)` back into (kind, arg)."""
    if "(" not in desc:
        return None
    kind, _, rest = desc.partition("(")
    return kind, rest.rstrip(")")


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

    # Emitted retarget output: the includes a `with` declares are
    # sanctioned only inside that retarget's own named test fn, up to
    # the declared multiplicity. Binding to (test_file, kind, arg)
    # alone was the 9171 over-admission: a matching reader anywhere in
    # the file — an unrelated fn or a duplicate site — was exempt. The
    # binding is byte-exact: a site sharing the named fn's line but
    # sitting outside its item span is not admitted.
    allowances = retarget_with_allowances(man)
    hits_by_file: dict[str, list] = {}
    for rel, line, desc in consumers:
        parts = _desc_parts(desc)
        if parts is not None:
            hits_by_file.setdefault(rel, []).append((line, *parts))
    # rel -> (admitted byte offsets, (line, kind, arg) -> deque[off]).
    # Per-key queues map each reported hit to its exact include site in
    # scan order — same-line same-arg occurrences stay distinct.
    exempt: dict[str, tuple] = {}
    for rel, file_allows in allowances.items():
        if rel not in hits_by_file:
            continue
        try:
            text = (repo / rel).read_text()
        except OSError:
            continue
        adm, off_hits = emitted_exemptions(text, file_allows)
        queues: dict[tuple, deque] = {}
        for off, line, kind, arg in off_hits:
            queues.setdefault((line, kind, arg), deque()).append(off)
        exempt[rel] = (adm, queues)

    def retargeted(rel: str, line: int, desc: str) -> bool:
        parts = _desc_parts(desc)
        if parts is None:
            return False
        kind, arg = parts
        if rel == "src/lib.rs" and (kind, arg) in rt_finds:
            return True
        entry = exempt.get(rel)
        if not entry:
            return False
        adm, queues = entry
        site_q = queues.get((line, kind, arg))
        return bool(site_q) and site_q.popleft() in adm

    writers = [h for h in consumers if h[2].startswith("write(")]
    bad = [
        h for h in consumers
        if not h[2].startswith("write(")
        and not retargeted(h[0], h[1], h[2])
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
