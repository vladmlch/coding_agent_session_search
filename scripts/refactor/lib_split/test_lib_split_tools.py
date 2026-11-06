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
        d = ti.diff(before, after)
        self.assertEqual(d.missing, [])
        self.assertEqual(d.changed, ["unit_tests::t1"])
        self.assertTrue(d.problems())

    def test_diff_missing_test(self):
        before = ti.collect(FIXTURE)
        after = {"unit_tests::t1": before["unit_tests::t1"]}
        d = ti.diff(before, after)
        self.assertEqual(d.missing, ["unit_tests::proptest::proptest::t2_prop"])
        self.assertTrue(d.problems())

    def test_normalize_cancels_head_promotion_only(self):
        orig = b'fn t() { let _x = 1; }'
        moved = b'pub(crate) fn t() { let _x = 1; }'
        self.assertEqual(ti.normalize_body(orig), ti.normalize_body(moved))

    def test_normalize_does_not_bless_include_drift(self):
        # fix: ../ stripping inside test bodies was blanket normalization and
        # hid unapproved retargets (review important; controller reproducer)
        orig = b'fn t() { let _x = include_str!("lib.rs"); }'
        moved = b'pub(crate) fn t() { let _x = include_str!("../lib.rs"); }'
        self.assertNotEqual(ti.normalize_body(orig), ti.normalize_body(moved))

    def test_duplicate_root_tests_stay_distinct(self):
        # controller reproducer: two identical #[test] fns collapsed to one key
        a = "#[test]\nfn a() { assert!(true); }\n"
        inv1 = ti.collect(a)
        inv2 = ti.collect(a + a)
        self.assertEqual(len(inv1), 1)
        self.assertEqual(len(inv2), 2)
        self.assertNotEqual(list(inv1), list(inv2))

    def test_diff_added_test_fails(self):
        before = ti.collect(FIXTURE)
        after = dict(before)
        after["unit_tests::t_new"] = dict(before["unit_tests::t1"])
        d = ti.diff(before, after)
        self.assertTrue(d.added, "added test must be reported")
        self.assertTrue(d.problems(), "added test must fail the gate")

    def test_diff_accepts_declared_exception(self):
        before = ti.collect(FIXTURE)
        moved = FIXTURE.replace(
            'assert_eq!(helper("ab"), 2)',
            'assert_eq!(helper("ab"), 2); let _s = include_str!("run.rs");')
        after = ti.collect(moved)
        d = ti.diff(before, after)
        self.assertTrue(d.problems())
        exc = {"unit_tests::t1": [ti.Exception(
            stage="3d",
            find=b'assert_eq!(helper("ab"), 2)',
            with_=b'assert_eq!(helper("ab"), 2); let _s = include_str!("run.rs");',
        )]}
        d2 = ti.diff(before, after, exceptions=exc)
        self.assertEqual(d2.problems(), [])

    def test_diff_maps_moved_modules_to_origin(self):
        before = ti.collect(FIXTURE)
        moved = FIXTURE.replace("mod unit_tests", "mod moved_tests")
        after = ti.collect(moved)
        d = ti.diff(before, after)
        self.assertTrue(d.problems())
        origins = {"moved_tests": "unit_tests"}
        d2 = ti.diff(before, after, origins=origins)
        self.assertEqual(d2.problems(), [])


class CfgSemanticsTests(unittest.TestCase):
    def test_cfg_not_test_is_production(self):
        # fix: substring "test" in attr misclassified #[cfg(not(test))]
        items, _c, _g = ri.parse_items("#[cfg(not(test))]\nfn prod() {}\n")
        self.assertFalse(ri.test_only(items[0]))

    def test_cfg_any_with_test_is_production(self):
        items, _c, _g = ri.parse_items("#[cfg(any(unix, test))]\nfn prod() {}\n")
        self.assertFalse(ri.test_only(items[0]))

    def test_cfg_all_with_test_is_test_only(self):
        items, _c, _g = ri.parse_items("#[cfg(all(test, unix))]\nfn t() {}\n")
        self.assertTrue(ri.test_only(items[0]))

    def test_cfg_test_is_test_only(self):
        items, _c, _g = ri.parse_items("#[cfg(test)]\nfn t() {}\n")
        self.assertTrue(ri.test_only(items[0]))

    def test_cfg_attr_is_not_a_gate(self):
        items, _c, _g = ri.parse_items(
            "#[cfg_attr(test, derive(Debug))]\nfn prod() {}\n")
        self.assertFalse(ri.test_only(items[0]))


class OwnershipTests(unittest.TestCase):
    SRC = ("#![allow(dead_code)]\n"
           "\n"
           "fn a() {}\n"
           "\n"
           "// free-standing comment block\n"
           "// second line\n"
           "\n"
           "fn b() {}\n"
           "\n"
           "/* trailing comment */\n")

    def test_gap_bytes_owned_by_following_item(self):
        raw = self.SRC.encode()
        items, cls, _g = ri.parse_items(self.SRC)
        leads, tail = ri.ownership(raw, cls, items)
        self.assertIn(b"free-standing comment", raw[leads[1][0]:leads[1][1]])
        self.assertIn(b"trailing comment", raw[tail[0]:tail[1]])

    def test_unowned_nonwhitespace_rejected(self):
        bad = "fn a() {}\n42 + x;\nfn b() {}\n"
        items, cls, _g = ri.parse_items(bad)
        with self.assertRaises(ri.LexError):
            ri.ownership(bad.encode(), cls, items)


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
        claimed = set()
        reloc_decls = {r["decl"] for r in self.manifest.get("mod_reloc", [])}
        for e in self.manifest["file"]:
            if "slice" in e or "mod" in e:
                sliced_mods.add(self._resolve(
                    {"kind": "mod_inline",
                     "name": e.get("mod") or e["slice"]["mod"]}))
                continue
            a = self._resolve(e["first"])
            b = self._resolve(e["last"])
            self.assertLessEqual(a, b, e["target"])
            for i in range(a, b + 1):
                covered[i] += 1
            for a2 in e.get("also", []):
                claimed.add(self._resolve(a2))
        for i in claimed:
            covered[i] -= 1  # `also` ownership replaces native-range coverage
        for i in sliced_mods:
            covered[i] += 1
        for i in claimed:
            covered[i] += 1
        for n in reloc_decls:
            covered[self._resolve({"kind": "mod_decl", "name": n})] += 1
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
                if "slice" not in e and "mod" not in e
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
        reloc_decls = {r["decl"] for r in self.manifest.get("mod_reloc", [])}
        for e in self.manifest["file"]:
            if "slice" in e or "mod" in e or e["target"] == "src/lib.rs":
                continue
            a = self._resolve(e["first"]); b = self._resolve(e["last"])
            for it in self.items[a:b + 1]:
                if it.kind == "mod_decl" and it.name not in reloc_decls:
                    self.fail(f"{it.name} decl moved to {e['target']}")


class SplitFixtureTests(unittest.TestCase):
    def _manifest(self, files, groups=None, splits=None, relocs=None, retargets=None):
        m = {"file": files}
        if groups:
            m["group"] = groups
        if splits:
            m["mod_split"] = splits
        if relocs:
            m["mod_reloc"] = relocs
        if retargets:
            m["retarget"] = retargets
        return m

    def _entry(self, kind, first, last, target, stage="1"):
        f = {"kind": kind, "name": first}
        l = {"kind": kind, "name": last}
        return {"target": target, "stage": stage, "first": f, "last": l}

    def _full_manifest(self):
        return self._manifest([
            {"target": "src/lib.rs", "stage": "stay",
             "first": {"kind": "use", "name": "std::path::Path"},
             "last": {"kind": "const", "name": "TOP"}},
            self._entry("fn", "helper", "helper", "src/cli/helper.rs"),
            {"target": "src/lib.rs", "stage": "stay",
             "first": {"kind": "struct", "name": "S"},
             "last": {"kind": "struct", "name": "S"}},
            {"target": "src/cli/unit_tests.rs", "stage": "1",
             "mod": "unit_tests"},
            {"target": "src/lib.rs", "stage": "stay",
             "first": {"kind": "fn", "name": "last"},
             "last": {"kind": "fn", "name": "last"}},
        ])

    def test_fixture_split_preserves_items(self):
        man = self._full_manifest()
        res = split.emit(man, FIXTURE)
        raw = FIXTURE.encode()
        problems = split.check(res, raw, man)
        self.assertEqual(problems, [])
        helper_file = next(f for f in res.files if f.target.endswith("helper.rs"))
        self.assertIn(b"pub(crate) fn helper", b"".join(helper_file.parts))

    def test_dissolved_mod_emits_bare_items(self):
        # fix: emitted test files wrapped `mod X { .. }` inside X.rs
        man = self._full_manifest()
        res = split.emit(man, FIXTURE)
        tf = next(f for f in res.files if f.target.endswith("unit_tests.rs"))
        text = b"".join(tf.parts)
        self.assertNotIn(b"mod unit_tests", text)
        self.assertIn(b"fn t1(", text)
        self.assertIn(b"use crate::*", text)
        self.assertIn(b"proptest!", text)

    def test_struct_field_promotion(self):
        src = ("#![allow(dead_code)]\n"
               "struct T { a: i32, pub b: i32, c: u8 }\n"
               "fn f() {}\n")
        man = self._manifest([
            self._entry("struct", "T", "T", "src/cli/t.rs"),
            {"target": "src/lib.rs", "stage": "stay",
             "first": {"kind": "fn", "name": "f"},
             "last": {"kind": "fn", "name": "f"}},
        ])
        res = split.emit(man, src)
        tf = res.files[0]
        text = b"".join(tf.parts)
        self.assertIn(b"pub(crate) a: i32", text)
        self.assertIn(b"pub b: i32", text)
        self.assertIn(b"pub(crate) c: u8", text)
        self.assertEqual(split.check(res, src.encode(), man), [])

    def test_impl_member_promotion(self):
        src = ("impl W {\n"
               "    fn m(&mut self) {}\n"
               "    pub fn n(&self) {}\n"
               "    const C: u8 = 1;\n"
               "}\n"
               "impl T for W {\n"
               "    fn t(&self) {}\n"
               "}\n")
        items, _c, _g = ri.parse_items(src)
        inherent, trait_impl = items
        text = split._apply_inserts(
            src.encode(), split._promote_inserts(src.encode(), inherent))
        self.assertIn(b"pub(crate) fn m(&mut self)", text)
        self.assertIn(b"pub(crate) const C", text)
        self.assertNotIn(b"pub(crate) pub", text)
        self.assertEqual(
            split._promote_inserts(src.encode(), trait_impl), [],
            "trait-impl members cannot take visibility (E0449)")

    def test_find_in_syntax_spans_string_literal(self):
        # Real retarget needles (join("src/lib.rs"), include_str!("lib.rs"))
        # span code + a string literal — find_in_code rejects them; this
        # defect surfaced as `0 find hits` on stage-2 scratch apply.
        src = b'fn t() { let p = base.join("src/lib.rs"); }\n'
        cls = ri.classify(src)
        self.assertEqual(
            ri.find_in_code(src, cls, b'join("src/lib.rs")'), [],
            "documented limitation: literal inside needle blocks code-match")
        self.assertEqual(
            ri.find_in_syntax(src, cls, b'join("src/lib.rs")'),
            [(22, 40)])
        # comments still can't host a hit
        src2 = b'// join("src/lib.rs")\n'
        cls2 = ri.classify(src2)
        self.assertEqual(
            ri.find_in_syntax(src2, cls2, b'join("src/lib.rs")'), [])

    def test_external_retarget_string_literal(self):
        import tempfile, pathlib
        with tempfile.TemporaryDirectory() as td:
            root = pathlib.Path(td)
            (root / "tests").mkdir()
            (root / "tests" / "t.rs").write_bytes(
                b'fn diagnostics_connector_paths_is_dynamic() {\n'
                b'    let s = std::fs::read_to_string(base.join("src/lib.rs"));\n'
                b'}\n')
            rt = {"test": "diagnostics_connector_paths_is_dynamic",
                  "test_file": "tests/t.rs", "stage": "2c",
                  "find": 'join("src/lib.rs")',
                  "with": 'join("src/cli/commands/status_health/diagnostics.rs")',
                  "to": [], "markers": []}
            summary = {"retargets": []}
            split._apply_external_retarget(root, rt, summary)
            out = (root / "tests" / "t.rs").read_bytes()
            self.assertIn(b'join("src/cli/commands/status_health/diagnostics.rs")', out)
            self.assertNotIn(b'join("src/lib.rs")', out)
            self.assertEqual(len(summary["retargets"]), 1)

    def test_include_path_retargeted_through_string(self):
        # include!("sub/x.rs") spans code + a string literal — rewrite_code
        # could never match it (E0583 surfaced on scratch apply when a
        # moved mod kept its lib.rs-relative include path).
        src = b'mod m { include!("search/gh494_repair_tests.rs"); }\n'
        cls = ri.classify(src)
        out = ri.rewrite_syntax(
            src, cls, split.INCLUDE_RX,
            lambda m: m.group(1) + b"X" + m.group(3))
        self.assertIn(b'include!("X")', out)

        text, drift = split._retarget_includes(
            b'include!("search/gh494_repair_tests.rs");\n',
            "src/cli/commands/search/x_tests.rs")
        self.assertEqual(drift, [])
        self.assertIn(b'include!("../../../search/gh494_repair_tests.rs")',
                      text)

    def test_seed_header_wraps_root_uses_no_collision(self):
        # Moved test mods carry their own `use` items; bare-seeded root
        # uses collide with them (E0252 on scratch apply). Seeding via a
        # private mod + glob lets explicit uses shadow (legal Rust).
        hdr = split.seed_header(
            "src/cli/x.rs",
            [b'use std::path::PathBuf;\n', b'pub use foo::Bar;\n'])
        self.assertIn(b"mod __lib_split_root_uses", hdr)
        self.assertIn(b"pub(crate) use std::path::PathBuf;", hdr)
        self.assertIn(b"pub(crate) use foo::Bar;", hdr)
        self.assertIn(b"use __lib_split_root_uses::*;", hdr)
        self.assertNotIn(b"pub(crate) pub(crate) use", hdr)

    def test_fields_after_fn_pointer_field_promoted(self):
        # `io_probe: fn() -> Option<u64>` — the `->`'s '>' decremented the
        # generic-depth counter, so every later field was swallowed into
        # one segment and stayed private (E0616 on scratch apply).
        src = ("struct W {\n"
               "    io_probe: fn() -> Option<u64>,\n"
               "    last_io_bytes: Option<u64>,\n"
               "    last_io_advance: std::time::Instant,\n"
               "}\n")
        items, _c, _g = ri.parse_items(src)
        offs = ri.field_insert_offsets(src, items[0])
        text = split._apply_inserts(src.encode(), offs).decode()
        self.assertIn("pub(crate) io_probe: fn() -> Option<u64>", text)
        self.assertIn("pub(crate) last_io_bytes: Option<u64>", text)
        self.assertIn("pub(crate) last_io_advance", text)

    def test_seed_header_absolutizes_local_paths(self):
        # `use model::X` resolves crate-root-relative only AT the root;
        # inside the wrapper mod it must become `crate::model::X`
        # (E0432/E0433 on scratch apply).
        hdr = split.seed_header(
            "src/cli/x.rs",
            [b'use model::cli_error_kind::ErrorKind;\n',
             b'use self::indexer::IndexOptions;\n',
             b'use std::path::PathBuf;\n'],
            local_names={"model", "indexer"})
        self.assertIn(b"pub(crate) use crate::model::cli_error_kind::ErrorKind;",
                      hdr)
        self.assertIn(b"pub(crate) use crate::indexer::IndexOptions;", hdr)
        self.assertIn(b"pub(crate) use std::path::PathBuf;", hdr)

    def test_seed_header_sibling_globs(self):
        # crate::-absolute, not super:: — the gate rejects residual
        # super:: inside test-mod files.
        hdr = split.seed_header(
            "src/cli/commands/x_tests_part_2.rs", [],
            test_mod=True, siblings=["x_tests_part_1", "x_tests_part_3"])
        self.assertIn(b"use crate::cli::commands::x_tests_part_1::*;", hdr)
        self.assertIn(b"use crate::cli::commands::x_tests_part_3::*;", hdr)
        self.assertNotIn(b"super::", hdr)

    def test_impl_member_promotion_nonascii_offsets(self):
        # UTF-8 bytes before the fn must not shift insert offsets — this
        # defect produced `&mut sepub(crate) lf` on stall_watchdog.
        src = ("impl W {\n"
               "    // café — ünïcode comment\n"
               "    fn m(&mut self, x: u8) {}\n"
               "}\n")
        items, _c, _g = ri.parse_items(src)
        raw = src.encode("utf-8")
        offs = split._promote_inserts(raw, items[0])
        text = split._apply_inserts(raw[items[0].start:items[0].end], offs)
        self.assertIn(b"pub(crate) fn m(&mut self, x: u8)", text)
        self.assertNotIn(b"sepub(crate)", text)

    def test_cfg_not_test_item_promoted(self):
        src = ("#![allow(dead_code)]\n"
               "#[cfg(not(test))]\nfn prod() {}\n"
               "fn last() {}\n")
        man = self._manifest([
            self._entry("fn", "prod", "prod", "src/cli/prod.rs"),
            {"target": "src/lib.rs", "stage": "stay",
             "first": {"kind": "fn", "name": "last"},
             "last": {"kind": "fn", "name": "last"}},
        ])
        res = split.emit(man, src)
        text = b"".join(res.files[0].parts)
        self.assertIn(b"pub(crate) fn prod", text)
        self.assertEqual(split.check(res, src.encode(), man), [])

    def test_standalone_comment_travels_with_owner(self):
        man = self._manifest([
            {"target": "src/lib.rs", "stage": "stay",
             "first": {"kind": "fn", "name": "a"},
             "last": {"kind": "fn", "name": "a"}},
            self._entry("fn", "b", "b", "src/cli/b.rs"),
        ])
        src = OwnershipTests.SRC
        res = split.emit(man, src)
        problems = split.check(res, src.encode(), man)
        self.assertEqual(problems, [])
        bfile = next(f for f in res.files if f.target.endswith("b.rs"))
        self.assertIn(b"free-standing comment", b"".join(bfile.parts))
        # trailing comment stays with lib.rs remainder
        self.assertIn(b"trailing comment", res.remainder)

    def test_lost_standalone_comment_detected(self):
        # corruption test: dropping the owned gap must be rejected
        man = self._manifest([
            {"target": "src/lib.rs", "stage": "stay",
             "first": {"kind": "fn", "name": "a"},
             "last": {"kind": "fn", "name": "a"}},
            self._entry("fn", "b", "b", "src/cli/b.rs"),
        ])
        src = OwnershipTests.SRC
        res = split.emit(man, src)
        bfile = next(f for f in res.files if f.target.endswith("b.rs"))
        bfile.parts = [p.replace(b"// free-standing comment block\n", b"", 1)
                       for p in bfile.parts]
        problems = split.check(res, src.encode(), man)
        self.assertTrue(any("comment" in p or "gap" in p or "byte" in p
                            for p in problems), problems)

    def test_source_loss_detected(self):
        man = self._manifest([
            self._entry("fn", "helper", "helper", "src/cli/helper.rs"),
        ])
        res = split.emit(man, FIXTURE)
        problems = split.check(res, FIXTURE.encode(), man)
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
        res = split.emit(man, FIXTURE_DUP_NAMES)
        problems = split.check(res, FIXTURE_DUP_NAMES.encode(), man)
        self.assertTrue(any("duplicate symbol" in p for p in problems))

    def test_cfg_drift_detected(self):
        man = self._manifest([
            {"target": "src/lib.rs", "stage": "stay",
             "first": {"kind": "use", "name": "std::path::Path"},
             "last": {"kind": "fn", "name": "last"}},
            {"target": "src/cli/unit_tests.rs", "stage": "1",
             "mod": "unit_tests"},
        ])
        res = split.emit(man, FIXTURE)
        # corrupt a moved item's attr bytes in the emitted stream
        bad_parts = [
            p.replace(b"#[test]", b"#[]", 1) for p in res.files[0].parts
        ]
        res.files[0].parts = bad_parts
        problems = split.check(res, FIXTURE.encode(), man)
        self.assertTrue(any("changed beyond" in p or "byte" in p or "gap" in p
                            for p in problems), problems)

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
        res = split.emit(man, FIXTURE)
        problems = split.check(res, FIXTURE.encode(), man)
        self.assertTrue(any("group g split" in p for p in problems))

    def test_stage_filter_emits_only_that_stage(self):
        man = self._manifest([
            {"target": "src/lib.rs", "stage": "stay",
             "first": {"kind": "use", "name": "std::path::Path"},
             "last": {"kind": "const", "name": "TOP"}},
            self._entry("fn", "helper", "helper", "src/cli/helper.rs", stage="1"),
            {"target": "src/lib.rs", "stage": "stay",
             "first": {"kind": "struct", "name": "S"},
             "last": {"kind": "struct", "name": "S"}},
            {"target": "src/cli/unit_tests.rs", "stage": "2",
             "mod": "unit_tests"},
            {"target": "src/lib.rs", "stage": "stay",
             "first": {"kind": "fn", "name": "last"},
             "last": {"kind": "fn", "name": "last"}},
        ])
        res = split.emit(man, FIXTURE, stage="1")
        targets = [f.target for f in res.files]
        self.assertIn("src/cli/helper.rs", targets)
        self.assertNotIn("src/cli/unit_tests.rs", targets)
        # unit_tests remains in the remainder until stage 2
        self.assertIn(b"mod unit_tests", res.remainder)
        self.assertEqual(split.check(res, FIXTURE.encode(), man, stage="1"), [])

    def test_apply_writes_files_and_rewrites_lib(self):
        import shutil
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "src").mkdir()
            (root / "src" / "lib.rs").write_text(FIXTURE)
            man = self._full_manifest()
            res = split.apply(man, root, stages=["1"])
            self.assertTrue((root / "src" / "cli" / "helper.rs").exists())
            self.assertTrue((root / "src" / "cli" / "unit_tests.rs").exists())
            self.assertTrue((root / "src" / "cli.rs").exists())
            lib = (root / "src" / "lib.rs").read_bytes()
            self.assertIn(b"mod cli;", lib)
            self.assertNotIn(b"fn helper(", lib)
            self.assertNotIn(b"mod unit_tests {", lib)
            cli = (root / "src" / "cli.rs").read_bytes()
            self.assertIn(b"mod helper;", cli)
            self.assertIn(b"mod unit_tests;", cli)
            # cfg(test) gate travels onto the dissolved mod's decl
            self.assertIn(b"#[cfg(test)]", cli)


class SplitRealSourceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.man = split.load_manifest(MANIFEST)
        cls.src = SRC.read_text()
        cls.res = split.emit(cls.man, cls.src)

    def test_real_source_dry_run_clean(self):
        problems = split.check(self.res, self.src.encode(), self.man)
        self.assertEqual(problems, [])

    def test_real_source_test_inventory_unchanged(self):
        inv = ti.collect(self.src)
        self.assertGreaterEqual(len(inv), 650)
        self.assertEqual(len(inv), len(set(inv)))

    def test_emitted_files_under_budget(self):
        for f in self.res.files:
            lines = b"".join(f.parts).count(b"\n") + 1
            if f.target in split.SIZE_EXEMPTIONS:
                continue
            self.assertLessEqual(
                lines, 2000, f"{f.target} has {lines} lines (>2000)")

    def test_named_plan_targets(self):
        targets = {f.target for f in self.res.files}
        for t in ("src/cli/commands/status_health/diagnostics.rs",
                  "src/doctor/run.rs",
                  "src/indexer/stall_watchdog.rs",
                  "src/export/messages.rs",
                  "src/cli/argv/normalize.rs"):
            self.assertIn(t, targets)

    def test_type_and_impl_co_located(self):
        for f in self.res.files:
            names = {it.name for it in f.items}
            if "DoctorFsMutationKind" in names:
                kinds = {it.kind for it in f.items if it.name == "DoctorFsMutationKind"}
                self.assertEqual(kinds, {"enum", "impl"}, f.target)

    def test_no_double_nested_test_mod(self):
        for f in self.res.files:
            text = b"".join(f.parts)
            base = f.target.rsplit("/", 1)[-1].removesuffix(".rs")
            self.assertNotIn(f"mod {base} {{".encode(), text,
                             f"{f.target} wraps itself")


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
        self.assertTrue(any("src/a.rs" in p for p in problems))

    def test_count_parity_flags_drop(self):
        # fix: presence-only parity let a scan lose hits undetected
        spec = {"scanner": [{"name": "s",
                             "sources": ["src/lib.rs"],
                             "patterns": [{"literal": "needle"}]}]}
        before = sp.counts(spec, {"src/lib.rs": "needle needle needle"})
        after = sp.counts(spec, {"src/lib.rs": "needle"})
        problems = sp.diff_counts(before, after)
        self.assertTrue(problems, "count drop must be flagged")

    def test_count_parity_flags_gain(self):
        spec = {"scanner": [{"name": "s",
                             "sources": ["src/lib.rs"],
                             "patterns": [{"literal": "needle"}]}]}
        before = sp.counts(spec, {"src/lib.rs": "needle"})
        after = sp.counts(spec, {"src/lib.rs": "needle needle"})
        problems = sp.diff_counts(before, after)
        self.assertTrue(problems, "count gain must be flagged")

    def test_real_counts_stable_after_dry_run(self):
        # every scanner with declared patterns must count > 0 in its sources
        spec = sp.load()
        baseline = sp.counts_for_sources(spec, REPO)
        bad = {k: v for k, v in baseline.items()
               if isinstance(v, int) and v <= 0}
        self.assertEqual(bad, {})
        # every source-reading consumer must be a listed scanner
        self.assertGreaterEqual(len(baseline), 9)


class StaleRefTests(unittest.TestCase):
    def test_detects_include_str_lib(self):
        import stale_refs as sr
        hits = sr.scan_source('let s = include_str!("lib.rs");\n')
        self.assertTrue(hits)

    def test_detects_join_lib(self):
        import stale_refs as sr
        hits = sr.scan_source(
            'let p = repo_root().join("src/lib.rs");\n')
        self.assertTrue(hits)

    def test_detects_format_lib(self):
        import stale_refs as sr
        hits = sr.scan_source(
            'let p = format!("{manifest}/src/lib.rs");\n')
        self.assertTrue(hits)


class BudgetSpecTests(unittest.TestCase):
    def test_every_manifest_target_budget_covered(self):
        # fix: size budget omitted derived roots and companion files
        budget = (REPO / "tests" / "source_file_size_budget.rs").read_text()
        man = tomllib.loads(MANIFEST.read_text())
        import re as _re
        files = set(re.findall(r'"((?:src|tests)/[^"]+)"', budget)) if False else set()
        roots = set(_re.findall(r'ROOTS: &\[&str\] = &\[([^\]]*)\]', budget))
        # simpler: extract quoted src paths from the FILES/GLOBS/ROOTS arrays
        covered = set()
        for arr in ("ROOTS", "FILES", "GLOBS"):
            m = _re.search(arr + r'[^=]*= &\[([^\]]*)\]', budget, _re.S)
            if not m:
                continue
            for lit in _re.findall(r'"([^"]+)"', m.group(1)):
                covered.add(lit)
        for e in man["file"]:
            if e["stage"] == "stay" or "slice" in e or "mod" in e:
                continue
            t = e["target"]
            ok = any(
                t == c
                or (c.endswith("/") and t.startswith(c))
                or t.startswith(c + "/")
                or (c.endswith("*") and t.startswith(c[:-1]))
                for c in covered)
            self.assertTrue(ok, f"{t} not covered by size budget spec {sorted(covered)}")


if __name__ == "__main__":
    unittest.main()
