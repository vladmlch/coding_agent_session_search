#!/usr/bin/env python3
"""Test inventory for the lib.rs split.

Walks the rust_items item tree and records every #[test]/proptest test
with its module path, cfg stack and a sha256 body hash. `dump` writes a
JSON snapshot; `diff` compares two snapshots and reports tests that are
missing, added, renamed, cfg-drifted or body-changed — the preservation
gate the plan requires at every stage.
"""

from __future__ import annotations

import hashlib
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import rust_items as ri

_TEST_ATTR = re.compile(r"(^|\W)(test|tokio::test|test_case|proptest)(\W|$)")

# Transformations the splitter legitimately applies; hashing normalizes
# them so a test moved + promoted + include-retargeted keeps its key.
_PROMO_RX = re.compile(rb"(?m)^(\s*)pub\(crate\) ")
_INCLUDE_RX = re.compile(rb'(include(?:_str)?!\s*\(\s*")(?:\.\./)+')


def normalize_body(body: bytes) -> bytes:
    body = _PROMO_RX.sub(rb"\1", body)
    body = _INCLUDE_RX.sub(rb"\1", body)
    return body


def _is_test_fn(item) -> bool:
    if item.kind != "fn":
        return False
    return any(_TEST_ATTR.search(a.text) for a in item.attrs)


def _cfg_of(item, parent_cfgs):
    cfgs = list(parent_cfgs)
    for a in item.attrs:
        # a.text retains the "#[..]" wrapper
        inner = a.text.strip().lstrip("#!").strip().lstrip("[").rstrip("]").strip()
        if inner.startswith("cfg"):
            cfgs.append(" ".join(inner.split()))
    return cfgs


def collect(src: str):
    items, _cls, _gaps = ri.parse_items(src)
    raw = src.encode("utf-8")
    out = {}

    def walk(item, path, cfgs, ordinals):
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
            }
        for sub in item.inner:
            walk(sub, path + [item.name], my_cfgs, ordinals)

    for it in items:
        walk(it, [], [], {})
    return out


def diff(before: dict, after: dict):
    """Return (missing, added, cfg_drift, body_changed)."""
    missing = sorted(set(before) - set(after))
    added = sorted(set(after) - set(before))
    cfg_drift = sorted(
        k for k in set(before) & set(after) if before[k]["cfg"] != after[k]["cfg"]
    )
    body_changed = sorted(
        k
        for k in set(before) & set(after)
        if before[k]["sha256"] != after[k]["sha256"]
    )
    return missing, added, cfg_drift, body_changed


def main(argv):
    cmd = argv[1] if len(argv) > 1 else "dump"
    if cmd == "dump":
        src = Path(argv[2]).read_text()
        inv = collect(src)
        out = Path(argv[3]) if len(argv) > 3 else None
        text = json.dumps(inv, indent=1, sort_keys=True)
        if out:
            out.write_text(text + "\n")
        print(f"{len(inv)} tests inventoried")
    elif cmd == "diff":
        before = json.loads(Path(argv[2]).read_text())
        after = json.loads(Path(argv[3]).read_text())
        missing, added, cfg_drift, body_changed = diff(before, after)
        report = {
            "missing": missing,
            "added": added,
            "cfg_drift": cfg_drift,
            "body_changed": body_changed,
        }
        print(json.dumps(report, indent=1))
        if missing or cfg_drift or body_changed:
            return 1
    else:
        raise SystemExit(f"unknown command {cmd}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
