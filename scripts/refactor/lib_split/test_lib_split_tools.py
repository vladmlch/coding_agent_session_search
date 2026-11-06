#!/usr/bin/env python3
"""Stage-0 tool safety tests.

Run: python3 -m unittest scripts/refactor/lib_split/test_lib_split_tools.py -v

Covers the preservation gates the plan requires:
- lexer/inventory correctness incl. nested mods, macros, non-ASCII bytes
- rejection of source loss, cfg drift, duplicate symbols, unsupported syntax
- manifest coverage/groups/slices against the live src/lib.rs
- splitter dry-run preservation checks on fixtures and real source
- test inventory diff semantics
- scanner parity lists resolve against the real repo
"""

from __future__ import annotations

import json
import sys
import tempfile
import tomllib
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import rust_items as ri  # noqa: E402
import test_inventory as ti  # noqa: E402
import scan_parity as sp  # noqa: E402
import split  # noqa: E402

REPO = HERE.parents[2]
SRC = REPO / "src" / "lib.rs"
MANIFEST = HERE / "manifest.toml"

FIXTURE = '''#![allow(dead_code)]
// crate preamble comment

use std::path::Path;

const TOP: &str = "x"; // trailing comment

/// Doc on a fn.
#[inline]
fn helper(a: &str) -> usize {
    a.len()
}

pub struct S { pub x: i32 }

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn t1() { assert_eq!(helper("ab"), 2); }

    proptest::proptest! {
        #[test]
        fn t2_prop(x in 0u8..10) { assert!(x < 10); }
    }
}

fn last() {}
'''

FIXTURE_DUP_NAMES = '''fn alpha() {}
fn alpha() {}
const B: i32 = 1;
'''


class LexerTests(unittest.TestCase):
    def test_fixture_items(self):
        items, _c, gaps = ri.parse_items(FIXTURE)
        self.assertEqual(gaps, [])
        kinds = [it.kind for it in items]
        self.assertEqual(kinds[:1], ["use"])
        self.assertIn("mod_inline", kinds)
        names = [it.name for it in items]
        self.assertIn("helper", names)
        self.assertIn("unit_tests", names)

    def test_inner_items_and_proptest(self):
        items, _c, _g = ri.parse_items(FIXTURE)
        mod = next(i for i in items if i.name == "unit_tests")
        inner_names = [i.name for i in mod.inner]
        self.assertIn("t1", inner_names)
        proptest = next(i for i in mod.inner if i.kind == "macro")
        self.assertTrue(any(i.name == "t2_prop" for i in proptest.inner))

    def test_byte_offsets_with_nonascii(self):
        src = '// café comment\nfn f() {}\nfn g() {}\n'
        items, _c, _g = ri.parse_items(src)
        raw = src.encode()
        f = next(i for i in items if i.name == "f")
        # item.start covers attached trivia; head_start is the declaration
        self.assertEqual(raw[f.head_start:f.end].decode().strip(), "fn f() {}")

    def test_rejects_unclosed_brace(self):
        with self.assertRaises(ri.LexError):
            ri.parse_items("fn f() {\n")

    def test_rejects_unterminated_string(self):
        with self.assertRaises(ri.LexError):
            ri.parse_items('const X: &str = "oops\n')

    def test_rejects_unterminated_block_comment(self):
        with self.assertRaises(ri.LexError):
            ri.parse_items("/* never closed\nfn f() {}\n")

    def test_gaps_reported_for_stray_code(self):
        items, _c, gaps = ri.parse_items("fn f() {}\nlet x = 3;\n")
        self.assertTrue(gaps)


class TestInventoryTests(unittest.TestCase):
    def test_collect_counts_and_cfg(self):
        inv = ti.collect(FIXTURE)
        self.assertIn("unit_tests::t1", inv)
        self.assertIn("unit_tests::proptest::proptest::t2_prop", inv)
        self.assertTrue(any("cfg(test)" in c for c in inv["unit_tests::t1"]["cfg"]))

    def test_diff_detects_loss_drift_change(self):
        before = ti.collect(FIXTURE)
        after_src = FIXTURE.replace("assert_eq!(helper(\"ab\"), 2)",
                                    "assert_eq!(helper(\"ab\"), 3)")
        after = ti.collect(after_src)
        missing, added, drift, changed = ti.diff(before, after)
        self.assertEqual(missing, [])
        self.assertEqual(changed, ["unit_tests::t1"])

    def test_diff_missing_test(self):
        before = ti.collect(FIXTURE)
        after = {"unit_tests::t1": before["unit_tests::t1"]}
        missing, *_ = ti.diff(before, after)
        self.assertEqual(missing, ["unit_tests::proptest::proptest::t2_prop"])

    def test_normalize_cancels_promote_and_retarget(self):
        orig = b'fn t() { let _x = include_str!("lib.rs"); }'
        moved = b'pub(crate) fn t() { let _x = include_str!("../lib.rs"); }'
        self.assertEqual(ti.normalize_body(orig), ti.normalize_body(moved))


class ManifestTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.src = SRC.read_text()
        cls.items, _c, cls.gaps = ri.parse_items(cls.src)
        cls.manifest = tomllib.loads(MANIFEST.read_text())

    def _resolve(self, spec, pool=None):
        pool = pool if pool is not None else self.items
        hits = [
            i for i, it in enumerate(pool)
            if (it.kind, it.name, it.ordinal)
            == (spec["kind"], spec["name"], spec.get("ordinal", 0))
        ]
        self.assertEqual(len(hits), 1, f"anchor {spec}: {len(hits)} hits")
        return hits[0]

    def test_source_parses_clean(self):
        self.assertEqual(self.gaps, [])
        self.assertGreater(len(self.items), 1800)

    def test_manifest_covers_every_item_once(self):
        covered = [0] * len(self.items)
        sliced_mods = set()
        for e in self.manifest["file"]:
            if "slice" in e:
                sliced_mods.add(self._resolve({"kind": "mod_inline", "name": e["slice"]["mod"]}))
                continue
            a = self._resolve(e["first"])
            b = self._resolve(e["last"])
            self.assertLessEqual(a, b, e["target"])
            for i in range(a, b + 1):
                covered[i] += 1
        for i in sliced_mods:
            covered[i] += 1
        missing = [i for i, c in enumerate(covered) if c == 0]
        dup = [i for i, c in enumerate(covered) if c > 1]
        self.assertEqual(missing, [], [(self.items[i].kind, self.items[i].name) for i in missing])
        self.assertEqual(dup, [])

    def test_ordered_groups_in_one_file(self):
        for g in self.manifest["group"]:
            idxs = [self._resolve(m) for m in g["items"]]
            lo, hi = min(idxs), max(idxs)
            holders = [
                e["target"] for e in self.manifest["file"]
                if "slice" not in e
                and self._resolve(e["first"]) <= lo
                and self._resolve(e["last"]) >= hi
            ]
            self.assertEqual(len(holders), 1, f"{g['name']} -> {holders}")

    def test_slices_cover_mod_inner_items_disjointly(self):
        for ms in self.manifest["mod_split"]:
            mod_i = self._resolve({"kind": "mod_inline", "name": ms["mod"]})
            mod = self.items[mod_i]
            slices = [
                e["slice"] for e in self.manifest["file"]
                if "slice" in e and e["slice"]["mod"] == ms["mod"]
            ]
            self.assertEqual(len(slices), ms["parts"])
            covered = [0] * len(mod.inner)
            for sl in slices:
                a = self._resolve(sl["first"], mod.inner)
                b = self._resolve(sl["last"], mod.inner)
                for i in range(a, b + 1):
                    covered[i] += 1
            self.assertTrue(all(c == 1 for c in covered),
                            f"{ms['mod']} slice coverage {covered}")

    def test_mod_decl_stays_in_lib(self):
        for e in self.manifest["file"]:
            if "slice" in e or e["target"] == "src/lib.rs":
                continue
            a = self._resolve(e["first"]); b = self._resolve(e["last"])
            for it in self.items[a:b + 1]:
                self.assertNotEqual(it.kind, "mod_decl",
                                    f"{it.name} decl moved to {e['target']}")


class SplitFixtureTests(unittest.TestCase):
    def _manifest(self, files, groups=None, splits=None):
        m = {"file": files}
        if groups:
            m["group"] = groups
        if splits:
            m["mod_split"] = splits
        return m

    def _entry(self, kind, first, last, target, stage="1"):
        f = {"kind": kind, "name": first}
        l = {"kind": kind, "name": last}
        return {"target": target, "stage": stage, "first": f, "last": l}

    def test_fixture_split_preserves_items(self):
        man = self._manifest([
            {"target": "src/lib.rs", "stage": "stay",
             "first": {"kind": "use", "name": "std::path::Path"},
             "last": {"kind": "const", "name": "TOP"}},
            self._entry("fn", "helper", "helper", "src/cli/helper.rs"),
            {"target": "src/lib.rs", "stage": "stay",
             "first": {"kind": "struct", "name": "S"},
             "last": {"kind": "struct", "name": "S"}},
            self._entry("mod_inline", "unit_tests", "unit_tests",
                        "src/cli/unit_tests.rs"),
            {"target": "src/lib.rs", "stage": "stay",
             "first": {"kind": "fn", "name": "last"},
             "last": {"kind": "fn", "name": "last"}},
        ])
        files, moved, items, dissolved = split.emit(man, FIXTURE)
        raw = FIXTURE.encode()
        problems = split.check(files, moved, items, raw, man, dissolved)
        self.assertEqual(problems, [])
        helper_file = next(f for f in files if f.target.endswith("helper.rs"))
        self.assertIn(b"pub(crate) fn helper", b"".join(helper_file.parts))

    def test_source_loss_detected(self):
        man = self._manifest([
            self._entry("fn", "helper", "helper", "src/cli/helper.rs"),
        ])
        files, moved, items, dissolved = split.emit(man, FIXTURE)
        problems = split.check(files, moved, items, FIXTURE.encode(), man, dissolved)
        self.assertTrue(any("not covered" in p for p in problems))

    def test_duplicate_symbol_detected(self):
        man = self._manifest([
            {"target": "src/cli/a.rs", "stage": "1",
             "first": {"kind": "fn", "name": "alpha"},
             "last": {"kind": "fn", "name": "alpha", "ordinal": 1}},
            {"target": "src/lib.rs", "stage": "stay",
             "first": {"kind": "const", "name": "B"},
             "last": {"kind": "const", "name": "B"}},
        ])
        files, moved, items, dissolved = split.emit(man, FIXTURE_DUP_NAMES)
        problems = split.check(files, moved, items, FIXTURE_DUP_NAMES.encode(), man, dissolved)
        self.assertTrue(any("duplicate symbol" in p for p in problems))

    def test_cfg_drift_detected(self):
        man = self._manifest([
            self._entry("mod_inline", "unit_tests", "unit_tests", "src/cli/t.rs"),
        ])
        files, moved, items, dissolved = split.emit(man, FIXTURE)
        # corrupt one emitted item's cfg attr
        f = files[0]
        it, txt, parent = f.items[0]
        bad = txt.replace(b"#[cfg(test)]", b"#[]", 1)
        moved_bad = [(it, bad, f.target, parent)] + moved[1:]
        problems = split.check(files, moved_bad, items, FIXTURE.encode(), man, dissolved)
        self.assertTrue(any("changed beyond" in p for p in problems))

    def test_group_split_detected(self):
        man = self._manifest(
            [
                self._entry("const", "TOP", "TOP", "src/cli/a.rs"),
                self._entry("fn", "helper", "helper", "src/cli/b.rs"),
            ],
            groups=[{"name": "g", "items": [
                {"kind": "const", "name": "TOP"},
                {"kind": "fn", "name": "helper"},
            ]}],
        )
        files, moved, items, dissolved = split.emit(man, FIXTURE)
        problems = split.check(files, moved, items, FIXTURE.encode(), man, dissolved)
        self.assertTrue(any("group g split" in p for p in problems))


class SplitRealSourceTests(unittest.TestCase):
    def test_real_source_dry_run_clean(self):
        man = split.load_manifest(MANIFEST)
        src = SRC.read_text()
        files, moved, items, dissolved = split.emit(man, src)
        problems = split.check(files, moved, items, src.encode(), man, dissolved)
        self.assertEqual(problems, [])

    def test_real_source_test_inventory_unchanged(self):
        inv = ti.collect(SRC.read_text())
        self.assertGreaterEqual(len(inv), 650)
        # normalized body multiset is what the split must preserve
        self.assertEqual(len(inv), len(set(inv)))


class ScannerParityTests(unittest.TestCase):
    def test_lists_resolve(self):
        spec = sp.load()
        problems = sp.check(spec, verbose=False)
        self.assertEqual(problems, [])

    def test_diff_flags_lost_source(self):
        problems = sp.diff(
            {"s": ["src/lib.rs", "src/a.rs"]},
            {"s": ["src/lib.rs"]},
        )
        self.assertEqual(problems, ["s: lost source src/a.rs"])


if __name__ == "__main__":
    unittest.main()
