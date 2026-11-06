#!/usr/bin/env python3
"""Test inventory for the lib.rs split.

Walks the rust_items item tree and records every #[test]/proptest test
with its module path, cfg stack and a sha256 body hash. Identities are
stable across module moves via an explicit `origins` map (new module
path -> original path prefix). Body normalization only excludes the two
sanctioned transforms the splitter may apply to test items:

- `pub(crate) ` inserted at the head (position-scoped, not blanket)
- `super::` -> `crate::` inside dissolved mods

Include-path changes are NOT normalized away — a retargeted include_str!
is a body change and requires a declared per-stage Exception.

`dump` writes a JSON snapshot; `diff` fails on missing, added, duplicate,
cfg-drifted or body-changed tests unless a declared exception covers the
change.
"""

from __future__ import annotations

import hashlib
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import rust_items as ri

_TEST_ATTR = re.compile(r"(^|\W)(test|tokio::test|test_case|proptest)(\W|$)")
_SUPER_RX = re.compile(rb"\bsuper::")
_HEAD_PROMO = b"pub(crate) "


@dataclass
class Exception:
    """A declared per-stage body change for one test."""
    stage: str
    find: bytes
    with_: bytes


@dataclass
class Diff:
    missing: list = field(default_factory=list)
    added: list = field(default_factory=list)
    cfg_drift: list = field(default_factory=list)
    changed: list = field(default_factory=list)
    duplicate_origins: list = field(default_factory=list)
    unblessed: list = field(default_factory=list)

    def problems(self) -> list:
        out = []
        out += [f"missing test: {k}" for k in self.missing]
        out += [f"added test: {k}" for k in self.added]
        out += [f"cfg drift: {k}" for k in self.cfg_drift]
        out += [f"body changed: {k}" for k in self.changed]
        out += [f"duplicate origin: {k}" for k in self.duplicate_origins]
        return out


def normalize_body(body: bytes) -> bytes:
    """Exclude only the splitter's sanctioned in-test transforms."""
    body = _SUPER_RX.sub(b"crate::", body)
    # position-scoped head visibility: `#[test] pub(crate) fn` vs `#[test] fn`
    body = re.sub(rb"(?m)^(\s*)pub\(crate\) ", rb"\1", body)
    return body


def _is_test_fn(item) -> bool:
    if item.kind != "fn":
        return False
    return any(_TEST_ATTR.search(a.text) for a in item.attrs)


def _cfg_of(item, parent_cfgs):
    cfgs = list(parent_cfgs)
    for a in item.attrs:
        inner = a.text.strip().lstrip("#!").strip().lstrip("[").rstrip("]").strip()
        if inner.startswith("cfg"):
            cfgs.append(" ".join(inner.split()))
    return cfgs


def collect(src: str, module_prefix: str = "", file_cfgs: list | None = None):
    """Inventory tests; `module_prefix` prepends the file's module path.

    `file_cfgs` are cfg expressions inherited from the module DECLARATION
    that gates this file (e.g. a dissolved `#[cfg(test)] mod x;`) — the
    emitted file's items see it as their outermost context, matching the
    gate they had inside the original inline mod.
    """
    items, _cls, _gaps = ri.parse_items(src)
    raw = src.encode("utf-8")
    out = {}
    ordinals: dict = {}
    prefix = [p for p in module_prefix.split("::") if p]
    inherited = [" ".join(c.replace("#[", "").rstrip("]").split())
                 for c in (file_cfgs or []) if "cfg" in c]

    def walk(item, path, cfgs):
        key_prefix = "::".join(path + [item.name])
        idx = ordinals.get(key_prefix, 0)
        ordinals[key_prefix] = idx + 1
        key = key_prefix if idx == 0 else f"{key_prefix}#{idx}"
        my_cfgs = _cfg_of(item, cfgs)
        if _is_test_fn(item):
            body = normalize_body(raw[item.start:item.end])
            out[key] = {
                "path": "::".join(path + [item.name]),
                "cfg": my_cfgs,
                "sha256": hashlib.sha256(body).hexdigest()[:32],
                "lines": body.count(b"\n") + 1,
                "_body": body,  # internal: needed for exception checks
            }
        for sub in item.inner:
            walk(sub, path + [item.name], my_cfgs)

    for it in items:
        walk(it, prefix, inherited)
    return out


def _origin_key(key: str, origins: dict) -> str:
    """Map a post-move inventory key back to its original path."""
    if not origins:
        return key
    base, _, ord_s = key.partition("#")
    parts = base.split("::")
    # find the longest module prefix that has an origin mapping
    for n in range(len(parts) - 1, 0, -1):
        mod = "::".join(parts[:n])
        if mod in origins:
            orig = origins[mod]
            leaf = "::".join(parts[n:])
            mapped = f"{orig}::{leaf}" if orig else leaf
            return mapped + (f"#{ord_s}" if ord_s else "")
    return key


def diff(before: dict, after: dict, exceptions: dict | None = None,
         origins: dict | None = None) -> Diff:
    d = Diff()
    exceptions = exceptions or {}
    # translate `after` keys to origin space
    mapped: dict = {}
    for k, v in after.items():
        ok = _origin_key(k, origins or {})
        if ok in mapped:
            d.duplicate_origins.append(ok)
        mapped[ok] = v
    d.missing = sorted(set(before) - set(mapped))
    d.added = sorted(set(mapped) - set(before))
    for k in sorted(set(before) & set(mapped)):
        if before[k]["cfg"] != mapped[k]["cfg"]:
            d.cfg_drift.append(k)
            continue
        if before[k]["sha256"] == mapped[k]["sha256"]:
            continue
        # declared exception? expected = transform(before_body, exceptions)
        excs = (exceptions.get(k) or exceptions.get(before[k]["path"])
                or exceptions.get(k.rsplit("::", 1)[-1]) or [])
        expected = before[k].get("_body", b"")
        for ex in excs:
            if ex.find in expected:
                expected = expected.replace(ex.find, ex.with_, 1)
        if excs and hashlib.sha256(expected).hexdigest()[:32] == mapped[k]["sha256"]:
            continue
        d.changed.append(k)
    return d


def main(argv):
    cmd = argv[1] if len(argv) > 1 else "dump"
    if cmd == "dump":
        src = Path(argv[2]).read_text()
        inv = collect(src)
        out = Path(argv[3]) if len(argv) > 3 else None
        pub = {k: {kk: vv for kk, vv in v.items() if not kk.startswith("_")}
               for k, v in inv.items()}
        text = json.dumps(pub, indent=1, sort_keys=True)
        if out:
            out.write_text(text + "\n")
        print(f"{len(inv)} tests inventoried")
    elif cmd == "diff":
        before = json.loads(Path(argv[2]).read_text())
        after = json.loads(Path(argv[3]).read_text())
        d = diff(before, after)
        report = {
            "missing": d.missing, "added": d.added,
            "cfg_drift": d.cfg_drift, "body_changed": d.changed,
            "duplicate_origins": d.duplicate_origins,
        }
        print(json.dumps(report, indent=1))
        if d.problems():
            return 1
    else:
        raise SystemExit(f"unknown command {cmd}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
