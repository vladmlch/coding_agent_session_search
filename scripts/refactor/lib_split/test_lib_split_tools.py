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

    def test_cfg_test_alone_is_not_a_test_fn(self):
        # round2 ruling: #[cfg(test)] gates compilation, it is not a test
        # attribute — a helper carrying only it must not be inventoried.
        src = "#[cfg(test)]\nfn helper() { assert!(true); }\n"
        self.assertEqual(ti.collect(src), {})
        # and the real attrs still classify
        for attr in ("#[test]", "#[tokio::test]",
                     '#[tokio::test(flavor = "multi_thread")]',
                     "#[test_case(1)]", "#[proptest]"):
            inv = ti.collect(f"{attr}\nfn t() {{}}\n")
            self.assertEqual(len(inv), 1, f"{attr} must inventory as a test")

    def test_should_panic_test_still_inventoried(self):
        src = "#[test]\n#[should_panic]\nfn t() {}\n"
        inv = ti.collect(src)
        self.assertEqual(len(inv), 1)

    def test_normalize_keeps_nested_field_visibility(self):
        # controller reproducer: blanket (?m)^ pub(crate) strip hid a
        # nested visibility change inside the body.
        orig = b'fn t() { struct S { x: u8, } let _ = S { x: 1 }; }'
        moved = b'fn t() { struct S { pub(crate) x: u8, } let _ = S { x: 1 }; }'
        self.assertNotEqual(ti.normalize_body(orig, 0),
                            ti.normalize_body(moved, 0))

    def test_normalize_keeps_head_promotion_position_scoped(self):
        # the sanctioned transform is head-only: pub(crate) at the item
        # head (byte offset head_off) cancels; mid-body does not.
        orig = b'fn t() { let _x = 1; }'
        moved = b'pub(crate) fn t() { let _x = 1; }'
        self.assertEqual(ti.normalize_body(orig, 0),
                         ti.normalize_body(moved, 0))
        # attrs before the head: promotion lands after them
        orig2 = b'#[test]\nfn t() { let _x = 1; }'
        moved2 = b'#[test]\npub(crate) fn t() { let _x = 1; }'
        head = moved2.index(b'pub(crate)')  # head offset inside body
        self.assertEqual(ti.normalize_body(orig2, 0),
                         ti.normalize_body(moved2, head))

    def test_normalize_keeps_string_literal_super(self):
        # controller reproducer: _SUPER_RX ran over raw bytes including
        # string literals, hiding a `"super::"` -> `"crate::"` data change.
        orig = b'fn t() { let s = "super::x"; let _ = s; }'
        moved = b'fn t() { let s = "crate::x"; let _ = s; }'
        self.assertNotEqual(ti.normalize_body(orig, 0),
                            ti.normalize_body(moved, 0))

    def test_normalize_allows_code_position_super_rewrite(self):
        # the sanctioned dissolved-mod transform is code-position
        # super:: -> crate:: — that must still cancel.
        orig = b'fn t() { super::h(); }'
        moved = b'fn t() { crate::h(); }'
        self.assertEqual(ti.normalize_body(orig, 0),
                         ti.normalize_body(moved, 0))


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

    def test_seed_header_drops_unreferenced_root_uses(self):
        # round-2: a generated import the body never references is a
        # provable unused import — eliminate it before -D warnings.
        hdr = split.seed_header(
            "src/cli/x.rs",
            [b'use model::Session;\n',
             b'use indexer::IndexOptions;\n'],
            local_names={"model", "indexer"},
            body=b'let s: Session = f();\n',
            crate_exports={"Session", "IndexOptions"},
            super_exports=set())
        self.assertIn(b"pub(crate) use crate::model::Session;", hdr)
        self.assertNotIn(b"IndexOptions;", hdr,
                         "unreferenced root use must be dropped")

    def test_seed_header_drops_wrapper_when_no_use_needed(self):
        hdr = split.seed_header(
            "src/cli/x.rs",
            [b'use model::Session;\n'],
            body=b'fn f() -> u32 { 1 }\n',
            crate_exports=set(), super_exports=set())
        self.assertNotIn(b"__lib_split_root_uses", hdr)
        self.assertNotIn(b"use crate::*;", hdr,
                         "crate glob with no referenced root name must go")

    def test_seed_header_keeps_needed_globs(self):
        # bare `run_helper` is exported by a sibling file (re-exported at
        # crate root by the bridge) — the crate glob must stay.
        hdr = split.seed_header(
            "src/cli/x.rs", [],
            body=b'fn g() { run_helper(); }\n',
            crate_exports={"run_helper"},
            super_exports={"parent_helper"})
        self.assertIn(b"use crate::*;", hdr)
        self.assertNotIn(b"use super::*;", hdr,
                         "unreferenced super glob must go")

    def test_seed_header_drops_unreferenced_sibling_glob(self):
        hdr = split.seed_header(
            "src/cli/commands/x_tests_part_2.rs", [],
            test_mod=True,
            siblings=["x_tests_part_1", "x_tests_part_3"],
            body=b'#[test]\nfn t() { shared_setup(); }\n',
            sibling_exports={"x_tests_part_1": {"shared_setup"},
                             "x_tests_part_3": {"unused_helper"}})
        self.assertIn(b"use crate::cli::commands::x_tests_part_1::*;", hdr)
        self.assertNotIn(b"x_tests_part_3", hdr)

    def test_seed_header_comment_mention_does_not_keep_import(self):
        # a name inside a comment is not a use — strings/comments must
        # not keep a generated import alive.
        hdr = split.seed_header(
            "src/cli/x.rs",
            [b'use model::Session;\n'],
            body=b'// mentions Session here\nfn f() {}\n',
            crate_exports=set(), super_exports=set())
        self.assertNotIn(b"__lib_split_root_uses", hdr)

    def test_leaf_names_variants(self):
        self.assertEqual(
            split._leaf_names(b'use a::b::{C, D as E};\n'), {"C", "E"})
        self.assertEqual(split._leaf_names(b'use a::b::C;\n'), {"C"})
        self.assertEqual(split._leaf_names(b'use a::b as B;\n'), {"B"})
        self.assertEqual(split._leaf_names(b'use a::b::*;\n'), {"b"})

    def test_broadcast_use_filtered_per_part(self):
        # round-2: broadcast inner `use`s unused in a part produced
        # unused-import warnings on scratch --all-targets. Non-owner
        # parts drop provably-dead uses; trait-leaf/glob uses stay.
        src = ("mod m {\n"
               "    use foo::Alpha;\n"
               "    use bar::Context;\n"
               "    use baz::*;\n"
               "    fn setup() { let _ = Alpha; }\n"
               "    #[test]\n    fn t1() { setup(); }\n"
               "    #[test]\n    fn t2() {}\n"
               "}\n")
        man = self._manifest([
            {"target": "src/x/m_p1.rs", "stage": "1", "slice": {
                "mod": "m", "first": {"kind": "fn", "name": "setup"},
                "last": {"kind": "fn", "name": "t1"},
                "part": 1, "parts": 2}},
            {"target": "src/x/m_p2.rs", "stage": "1", "slice": {
                "mod": "m", "first": {"kind": "fn", "name": "t2"},
                "last": {"kind": "fn", "name": "t2"},
                "part": 2, "parts": 2}},
        ])
        res = split.emit(man, src)
        p1 = next(f for f in res.files if f.target.endswith("m_p1.rs"))
        p2 = next(f for f in res.files if f.target.endswith("m_p2.rs"))
        t1, t2 = b"".join(p1.parts), b"".join(p2.parts)
        self.assertIn(b"use foo::Alpha;", t1)   # owner preserves all
        self.assertIn(b"use bar::Context;", t1)
        self.assertNotIn(b"use foo::Alpha;", t2)  # unreferenced → gone
        self.assertIn(b"use bar::Context;", t2)   # trait leaf → kept
        self.assertIn(b"use baz::*;", t2)         # glob → kept
        self.assertIn(b"#[allow(unused_imports)]", t2)

    def test_use_needed_trait_and_glob_guards(self):
        code = b"fn t() {}\n"
        self.assertTrue(split._use_needed(b"use a::RowExt;\n", code))
        self.assertTrue(split._use_needed(b"use a::Context;\n", code))
        self.assertTrue(split._use_needed(b"use a::*;\n", code))
        self.assertFalse(split._use_needed(b"use a::TempDir;\n", code))
        self.assertTrue(split._use_needed(
            b"use a::TempDir;\n", b"let t = TempDir::new();\n"))

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

    def test_per_pattern_parity_flags_swap(self):
        # round2: summed parity let losing pattern A while gaining
        # pattern B cancel to a false pass.
        spec = {"scanner": [{"name": "s",
                             "sources": ["src/lib.rs"],
                             "patterns": ["alpha", "beta"]}]}
        before = sp.counts(spec, {"src/lib.rs": "alpha alpha beta"})
        after = sp.counts(spec, {"src/lib.rs": "alpha beta beta"})
        problems = sp.diff_counts(before, after)
        self.assertTrue(problems,
                        "alpha lost + beta gained must not cancel")

    def test_needle_only_in_test_file_fails(self):
        # round2: the `or needle in test_file` fallback excused a needle
        # that exists only in the scanner's own test source.
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            troot = Path(td)
            (troot / "tests").mkdir()
            (troot / "src").mkdir()
            (troot / "tests" / "t.rs").write_text(
                '#[test] fn t() {\n'
                '    let s = std::fs::read_to_string("src/lib.rs").unwrap();\n'
                '    assert!(s.contains("ghost_needle"));\n'
                '}')
            (troot / "src" / "lib.rs").write_text("fn real() {}\n")
            spec = {"scanner": [{"name": "s",
                                 "test": "tests/t.rs",
                                 "sources": ["src/lib.rs"]}]}
            problems = sp.needles(spec, repo=troot)
            self.assertTrue(any("ghost_needle" in p for p in problems),
                            "needle surviving only in the test file must fail")

    def test_real_counts_stable_after_dry_run(self):
        # every scanner with declared patterns must count > 0 per pattern
        # in its sources (absence-scanners excepted)
        spec = sp.load()
        baseline = sp.counts_for_sources(spec, REPO)
        bad = {}
        for name, v in baseline.items():
            if isinstance(v, dict):
                entry = sp.scanners(spec)[name]
                if entry.get("absence"):
                    if any(c != 0 for c in v.values()):
                        bad[name] = v
                elif any(c <= 0 for c in v.values()):
                    bad[name] = v
        self.assertEqual(bad, {})
        # every source-reading consumer must be a listed scanner
        self.assertGreaterEqual(len(baseline), 9)

    def test_needles_only_source_var_contains(self):
        # round-2: `x.contains("…")` is a source needle only when x
        # holds file contents — runtime/log/fixture asserts must not
        # be extracted (they produced false parity failures).
        import scan_parity as sp
        src = (
            'let lib_source = include_str!("lib.rs");\n'
            'assert!(lib_source.contains("real_marker"));\n'
            'assert!(repair_err.message.contains("cannot be empty"));\n'
            'assert!(logs().contains("watch mode"));\n'
            'assert!(!lib_source.contains("banned_marker"));\n')
        pos, neg = sp._needles(src)
        self.assertEqual(pos, ["real_marker"])
        self.assertEqual(neg, ["banned_marker"])

    def test_needles_source_var_only_rs_reads(self):
        # reads of non-.rs files (DB fixtures, /proc) are not source
        # scans even when named like one.
        import scan_parity as sp
        src = (
            'let bytes = std::fs::read(&db_path).unwrap();\n'
            'let status = std::fs::read_to_string("/proc/self/status")?;\n'
            'let lib = std::fs::read_to_string("src/lib.rs").unwrap();\n'
            'assert!(bytes.contains(b"marker_x".as_slice()));\n'
            'assert!(lib.contains("lib_marker"));\n')
        pos, _neg = sp._needles(src)
        self.assertEqual(pos, ["lib_marker"])

    def test_needles_decl_gap_does_not_swallow_code(self):
        # greedy gap in `needle = "…"` could skip a quote and capture a
        # multi-line code blob — must capture the nearest literal.
        import scan_parity as sp
        src = ('let needle = "Command::new";\n'
               'let mut off = 0;\n')
        pos, _ = sp._needles(src)
        self.assertEqual(pos, ["Command::new"])

    def test_needles_shallow_propagation_only(self):
        # `let v2 = &src[..]` keeps the source; `let x = f(src)` is a
        # derived value, not a source view.
        import scan_parity as sp
        src = (
            'let src = std::fs::read_to_string("src/lib.rs").unwrap();\n'
            'let view = &src[..];\n'
            'let n = count_items(&src);\n'
            'assert!(view.contains("view_marker"));\n')
        pos, _ = sp._needles(src)
        self.assertEqual(pos, ["view_marker"])

    def test_scanner_lists_cover_secondary_consumers(self):
        # round2 I7: the genuine src/storage/sqlite.rs reader and the
        # auditor companion files must be in the source unions.
        spec = sp.load()
        names = sp.scanners(spec)
        all_sources = set()
        for e in names.values():
            all_sources.update(e.get("sources", []))
        self.assertIn("src/storage/sqlite.rs", all_sources)
        aud = names["doctor_mutate_auditor"]["sources"]
        for f in ("src/doctor_chokepoint.rs", "src/doctor_runs.rs",
                  "src/doctor_undo.rs", "src/doctor_robot_docs.rs"):
            self.assertIn(f, aud, f"missing auditor source {f}")
        loa = names["local_only_audit"]
        self.assertIn("tests/cli_dispatch_coverage.rs", loa["sources"])
        self.assertIn("src", loa.get("dirs", []))


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

    def test_fixture_write_is_not_a_consumer(self):
        # round2: fs::write(...join("src/lib.rs")...) creates a temp
        # fixture — a writer, not a reader. Writers must not count as
        # consumers needing retargets.
        import stale_refs as sr
        hits = sr.scan_source(
            'fs::write(root.path().join("src/lib.rs"), b"fn x() {}")?;\n')
        self.assertTrue(hits, "writer hit must still be reported")
        self.assertEqual(hits[0][1], "write",
                         "fs::write context must classify as writer")
        consumers, _mentions = sr.scan(REPO)
        writer_files = {c[0] for c in consumers if c[2].startswith("write(")}
        self.assertIn("src/bin/e2e-run-bundle.rs", writer_files)
        self.assertIn("src/proof_artifact.rs", writer_files)

    def test_genuine_sqlite_consumer_listed(self):
        # src/storage/sqlite.rs include_str!s ../lib.rs — a real reader.
        import stale_refs as sr
        consumers, _m = sr.scan(REPO)
        sqlite = [c for c in consumers if c[0] == "src/storage/sqlite.rs"]
        self.assertTrue(sqlite, "sqlite.rs consumer must be detected")
        self.assertNotEqual(sqlite[0][1], "write")

    def test_indexer_fixture_allow_is_exact_line(self):
        # Controller ruling: the TempDir snippet-metadata fixture at
        # src/indexer/mod.rs:41996 is the only admitted reference; the
        # former 41920-41929 range and every adjacent line stay findings.
        import stale_refs as sr
        rel = "src/indexer/mod.rs"
        self.assertTrue(sr.allowed(rel, 41996, sr.BUILTIN_ALLOW))
        for line in (41927, 41995, 41997):
            self.assertFalse(sr.allowed(rel, line, sr.BUILTIN_ALLOW),
                             f"line {line} must not be exempt")


class SplitStageContractTests(unittest.TestCase):
    """Round-2 stage semantics: manual/unknown/empty/repeat stages."""

    def _fixture_manifest(self):
        return {"file": [
            {"target": "src/lib.rs", "stage": "stay",
             "first": {"kind": "use", "name": "std::path::Path"},
             "last": {"kind": "use", "name": "std::path::Path"}},
            {"target": "src/out.rs", "stage": "2a",
             "first": {"kind": "const", "name": "TOP"},
             "last": {"kind": "const", "name": "TOP"}},
            {"target": "src/lib.rs", "stage": "stay",
             "first": {"kind": "fn", "name": "helper"},
             "last": {"kind": "fn", "name": "last"}},
        ]}

    def test_manual_stage_is_nonzero_diagnostic(self):
        man = self._fixture_manifest()
        man["meta"] = {"manual_stages": ["5", "6"]}
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "src").mkdir()
            (root / "src" / "lib.rs").write_text(FIXTURE)
            with self.assertRaises(split.SplitError) as ctx:
                split.apply(man, root, ["5"])
            self.assertIn("manual", str(ctx.exception).lower())

    def test_unknown_stage_is_nonzero_diagnostic(self):
        man = self._fixture_manifest()
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "src").mkdir()
            (root / "src" / "lib.rs").write_text(FIXTURE)
            with self.assertRaises(split.SplitError) as ctx:
                split.apply(man, root, ["9x"])
            self.assertIn("unknown stage", str(ctx.exception).lower())

    def test_empty_move_stage_reports_truthfully(self):
        # a supported stage with no manifest entries in this fixture is
        # a deliberate no-op, reported — not a crash, not silent success.
        man = self._fixture_manifest()
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "src").mkdir()
            (root / "src" / "lib.rs").write_text(FIXTURE)
            summary = split.apply(man, root, ["2b"])
            self.assertEqual(summary["files"], [])
            self.assertTrue(any("no manifest entries" in n
                                for n in summary["notes"]),
                            summary["notes"])

    def test_repeated_stage_reports_already_applied(self):
        man = self._fixture_manifest()
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "src").mkdir()
            (root / "src" / "lib.rs").write_text(FIXTURE)
            first = split.apply(man, root, ["2a"])
            self.assertTrue(first["files"], "first apply must emit")
            second = split.apply(man, root, ["2a"])
            self.assertEqual(second["files"], [])
            self.assertTrue(any("already consumed" in n
                                for n in second["notes"]),
                            second["notes"])


class ManifestNamedPathTests(unittest.TestCase):
    """Plan §3 named-owner contract on the generated manifest."""

    def setUp(self):
        self.man = tomllib.loads(MANIFEST.read_text())
        self.files = self.man["file"]

    def _entry(self, target):
        return next((e for e in self.files if e["target"] == target), None)

    def test_cli_entry_rs_binds_entry_surface(self):
        e = self._entry("src/cli/entry.rs")
        self.assertIsNotNone(e, "src/cli/entry.rs missing")
        self.assertEqual(e["stage"], "4")
        self.assertEqual(e["first"], {"kind": "struct", "name": "ParsedCli"})
        self.assertEqual(e["last"], {"kind": "fn",
                                     "name": "try_run_with_parsed_fast"})

    def test_cli_dispatch_rs_binds_execute_cli(self):
        e = self._entry("src/cli/dispatch.rs")
        self.assertIsNotNone(e, "src/cli/dispatch.rs missing")
        self.assertEqual(e["stage"], "4")
        self.assertEqual(e["first"], {"kind": "fn", "name": "execute_cli"})
        self.assertEqual(e["last"], {"kind": "fn", "name": "execute_cli"})

    def test_stage2e_owns_capabilities_introspect_dir(self):
        e2 = [e for e in self.files if e["stage"] == "2e"]
        self.assertTrue(e2, "stage 2e must be populated")
        for e in e2:
            self.assertTrue(
                e["target"].startswith(
                    "src/cli/commands/capabilities_introspect/"),
                f"2e entry outside capabilities_introspect: {e['target']}")
        spans = [(e["first"]["name"], e["last"]["name"])
                 for e in e2 if "first" in e]
        self.assertIn(("CapabilitiesResponse", ), [s[0:1] for s in spans],
                      "2e must start at CapabilitiesResponse")
        self.assertIn("should_skip_arg", [s[1] for s in spans],
                      "2e must end at should_skip_arg")
        # nothing else may claim the capabilities/introspect dir
        for e in self.files:
            if e["target"].startswith(
                    "src/cli/commands/capabilities_introspect/"):
                self.assertEqual(e["stage"], "2e", e["target"])

    def test_stage1a_response_schema_tests_split_in_two(self):
        # plan §8 stage 1a: response_schema_tests divides into two
        # sibling part files under src/robot_schema/ — the manifest must
        # carry the partition as slices, not one dissolved file.
        parts = [e for e in self.files
                 if (e.get("slice") or {}).get("mod") == "response_schema_tests"]
        self.assertEqual(
            [e["target"] for e in parts],
            ["src/robot_schema/response_schema_tests_part_1.rs",
             "src/robot_schema/response_schema_tests_part_2.rs"])
        for e in parts:
            self.assertEqual(e["stage"], "1a", e["target"])
            self.assertEqual(e["slice"]["parts"], 2, e["target"])
        self.assertEqual([e["slice"]["part"] for e in parts], [1, 2])
        # contiguous inner coverage: part 1 owns the mod `use` head and
        # the fixture helper; part 2 resumes at the next item and owns
        # the readiness helper — no overlap, no skipped item.
        self.assertEqual(parts[0]["slice"]["first"],
                         {"kind": "use", "name": "super::*"})
        self.assertEqual(
            parts[0]["slice"]["last"]["name"],
            "doctor_storage_pressure_classifies_precious_and_reclaimable_bytes")
        self.assertEqual(
            parts[1]["slice"]["first"]["name"],
            "doctor_storage_pressure_accounts_for_symlinked_archive_root")
        self.assertEqual(
            parts[1]["slice"]["last"]["name"],
            "doctor_v2_schema_examples_are_redacted_contract_examples")
        # the whole-mod single file is gone — nothing else may claim it
        self.assertFalse(
            any(e.get("mod") == "response_schema_tests"
                for e in self.files))

    def test_no_run_rs_or_execute_cli_rs_targets(self):
        for e in self.files:
            self.assertNotIn(e["target"],
                             ("src/cli/run.rs", "src/cli/execute_cli.rs"),
                             f"mechanical path still present: {e['target']}")

    # --- round 3A: all §3 CLI named owners bind -------------------------

    def _span(self, e):
        return (e["first"]["name"], e["last"]["name"])

    def test_cli_args_sub_rs_binds_subcommand_enums(self):
        e = self._entry("src/cli/args_sub.rs")
        self.assertIsNotNone(e, "src/cli/args_sub.rs missing")
        self.assertEqual(e["stage"], "4")
        self.assertEqual(e["first"]["name"], "ScheduleCommand")
        self.assertEqual(e["last"]["name"], "Aggregations")

    def test_cli_args_rs_ends_at_commands(self):
        e = self._entry("src/cli/args.rs")
        self.assertIsNotNone(e, "src/cli/args.rs missing")
        self.assertEqual(e["first"]["name"], "Cli")
        self.assertEqual(e["last"]["name"], "Commands")

    def test_cli_error_rs_binds_error_surface(self):
        e = self._entry("src/cli/error.rs")
        self.assertIsNotNone(e, "src/cli/error.rs missing")
        self.assertEqual(e["stage"], "4")
        self.assertEqual(self._span(e), ("CliError", "WrapConfig"))

    def test_cli_paths_rs_binds_all_path_helpers(self):
        e = self._entry("src/cli/paths.rs")
        self.assertIsNotNone(e, "src/cli/paths.rs missing")
        # paths.rs owns the data-dir helpers up to the interrupting test
        # mod; the session-path tail lives under the same owner namespace.
        self.assertEqual(self._span(e),
                         ("default_db_path", "default_data_dir_with_source"))
        tail = self._entry("src/cli/paths/session_paths.rs")
        self.assertIsNotNone(tail, "paths/session_paths.rs missing")
        self.assertEqual(self._span(tail),
                         ("read_session_paths", "resolve_resume_target"))

    def test_quarantine_mirror_import_rs_binds_command_family(self):
        e = self._entry("src/cli/commands/quarantine_mirror_import.rs")
        self.assertIsNotNone(e, "quarantine_mirror_import.rs missing")
        self.assertEqual(e["stage"], "2b")
        self.assertEqual(self._span(e),
                         ("handle_import", "run_quarantine_clear"))

    def test_forget_rs_binds_forget_items(self):
        e = self._entry("src/cli/commands/forget.rs")
        self.assertIsNotNone(e, "forget.rs missing")
        self.assertEqual(e["stage"], "2b")
        # forget_selector_tests interrupts the span after forget_filter:
        # the owner keeps the named item, the rest are same-namespace
        # children (round-3B test-placement ruling).
        self.assertEqual(self._span(e), ("forget_filter", "forget_filter"))
        cmd = self._entry("src/cli/commands/forget/command.rs")
        self.assertIsNotNone(cmd, "forget/command.rs missing")
        self.assertEqual(self._span(cmd),
                         ("ForgetCommandOptions", "run_forget_exclusion_action"))

    def test_analytics_dir_covers_analytics_span(self):
        es = [e for e in self.files
              if e["target"].startswith("src/cli/commands/analytics/")]
        self.assertTrue(es, "no files under commands/analytics/")
        for e in es:
            self.assertEqual(e["stage"], "2b", e["target"])
        names = {n for e in es if "first" in e
                 for n in (e["first"]["name"], e["last"]["name"])}
        self.assertIn("run_analytics", names)
        self.assertIn("run_analytics_models", names)

    def test_stats_diag_storage_dir_covers_stats_and_diag(self):
        es = [e for e in self.files
              if e["target"].startswith("src/cli/commands/stats_diag_storage/")]
        self.assertTrue(es, "no files under commands/stats_diag_storage/")
        for e in es:
            self.assertEqual(e["stage"], "2b", e["target"])
        firsts = {e["first"]["name"] for e in es if "first" in e}
        self.assertEqual(firsts, {"run_stats", "run_diag"})

    def test_sessions_context_rs_binds_sessions_context(self):
        e = self._entry("src/cli/commands/sessions_context.rs")
        self.assertIsNotNone(e, "sessions_context.rs missing")
        self.assertEqual(e["stage"], "2d")
        self.assertEqual(e["first"]["name"], "run_sessions")

    def test_resume_rs_binds_run_resume(self):
        e = self._entry("src/cli/commands/resume.rs")
        self.assertIsNotNone(e, "resume.rs missing")
        self.assertEqual(e["stage"], "2f")
        self.assertEqual(e["first"]["name"], "run_resume")

    def test_export_html_rs_binds_run_export_html(self):
        e = self._entry("src/cli/commands/export_html.rs")
        self.assertIsNotNone(e, "export_html.rs missing")
        self.assertEqual(e["stage"], "2f")
        self.assertEqual(e["first"]["name"], "run_export_html")

    def test_export_dir_holds_run_export_and_formatters(self):
        es = [e for e in self.files
              if e["target"].startswith("src/cli/commands/export/")]
        self.assertTrue(es, "no files under commands/export/")
        spans = {(e.get("first", {}).get("name"), e.get("last", {}).get("name"))
                 for e in es}
        # run_export + clipboard/output helpers live inside this span
        # (run_export is between indexed_export_cli_error and
        # sync_export_parent_directory in source order).
        self.assertIn(("indexed_export_cli_error",
                       "sync_export_parent_directory"), spans)
        self.assertIn(("format_as_markdown", "html_escape"), spans)

    def test_expand_rs_and_timeline_rs_bind(self):
        for path, first in (("src/cli/commands/expand.rs", "run_expand"),
                            ("src/cli/commands/timeline.rs", "run_timeline")):
            e = self._entry(path)
            self.assertIsNotNone(e, f"{path} missing")
            self.assertEqual(e["first"]["name"], first, path)

    def test_mappings_rs_binds_mappings_commands(self):
        e = self._entry("src/cli/commands/mappings.rs")
        self.assertIsNotNone(e, "mappings.rs missing")
        self.assertEqual(e["stage"], "2g")
        self.assertEqual(self._span(e),
                         ("run_mappings_command", "run_mappings_test"))

    def test_daemon_rs_and_schedule_rs_bind(self):
        d = self._entry("src/cli/commands/daemon.rs")
        s = self._entry("src/cli/commands/schedule.rs")
        self.assertIsNotNone(d, "daemon.rs missing")
        self.assertIsNotNone(s, "schedule.rs missing")
        self.assertEqual(d["first"]["name"], "resolved_daemon_config")
        self.assertEqual(d["last"]["name"], "run_daemon")
        self.assertEqual(s["first"]["name"], "schedule_error_to_cli")
        self.assertEqual(s["last"]["name"], "run_schedule_command")
        for e in (d, s):
            self.assertEqual(e["stage"], "2g", e["target"])

    def test_pack_dir_covers_pack_span(self):
        es = [e for e in self.files
              if e["target"].startswith("src/cli/commands/pack/")]
        self.assertTrue(es, "no files under commands/pack/")
        for e in es:
            self.assertEqual(e["stage"], "2a", e["target"])
        names = {e["first"]["name"] for e in es if "first" in e}
        self.assertIn("pack_budget_ms", names)

    def test_named_owners_leave_no_mechanical_slugs(self):
        dead = {
            "src/cli/clierror.rs", "src/cli/sourcescommand.rs",
            "src/cli/default_db_path.rs", "src/cli/read_session_paths.rs",
            "src/cli/commands/handle_import.rs",
            "src/cli/commands/run_sessions.rs",
            "src/cli/commands/run_resume.rs",
            "src/cli/commands/run_export_html.rs",
            "src/cli/commands/format_as_markdown.rs",
            "src/cli/commands/resolved_daemon_config.rs",
            "src/cli/commands/run_stats.rs",
            "src/cli/commands/analyticsvalidationsummary.rs",
            "src/doctor/doctorassetclass.rs",
            "src/doctor/doctorrepairmode.rs",
            "src/doctor/doctorsourceinventorydbrow.rs",
            "src/doctor/doctor_source_authority_policy.rs",
            "src/doctor/doctor_now_ms.rs",
            "src/doctor/doctor_backup_kind_label.rs",
            "src/doctor/doctorsupportbundlecommandmode.rs",
            "src/doctor/doctorbaselinecommandmode.rs",
            "src/doctor/doctorboundedarchivedbprobe.rs",
            "src/doctor/collect_doctor_interrupted_operation_states.rs",
        }
        for e in self.files:
            self.assertNotIn(e["target"], dead,
                             f"mechanical slug still bound: {e['target']}")

    def test_interrupting_test_mods_emit_separate_adjacent_files(self):
        # Round-3B ruling: tests live in separate neighboring files under
        # the owner namespace, never embedded inside a production owner.
        wanted = {
            "forget_selector_tests":
                "src/cli/commands/forget/forget_selector_tests.rs",
            "resolve_data_dir_tests":
                "src/cli/paths/resolve_data_dir_tests.rs",
        }
        mods = {e["mod"]: e for e in self.files if "mod" in e}
        for name, target in wanted.items():
            self.assertIn(name, mods, f"{name} must be its own mod file")
            self.assertEqual(mods[name]["target"], target,
                             f"{name} must sit in the owner namespace")
        # no production range may span across the mod's source position
        # (the owner file must stop at the mod and resume in a sibling)
        for e in self.files:
            if "first" not in e:
                continue
            self.assertNotEqual(
                e["first"]["name"], "forget_selector_tests")
            self.assertNotEqual(
                e["first"]["name"], "resolve_data_dir_tests")

    # --- round 3B: all §3 doctor named owners bind ----------------------

    def test_doctor_policy_rs_binds_policy_surface(self):
        e = self._entry("src/doctor/policy.rs")
        self.assertIsNotNone(e, "src/doctor/policy.rs missing")
        self.assertEqual(e["stage"], "3a")
        self.assertEqual(e["first"]["name"], "DoctorRepairMode")
        # policy.rs holds the repair/outcome policy surface through the
        # vocabulary labels; asset policy rides policy/ children.
        self.assertEqual(e["last"]["name"], "doctor_data_loss_risk_label")
        children = [f["target"] for f in self.files
                    if f["target"].startswith("src/doctor/policy/")]
        self.assertTrue(children, "policy/ children must carry the "
                                  "asset-policy/vocabulary overflow")
        firsts = {f["first"]["name"] for f in self.files
                  if f["target"].startswith("src/doctor/policy/")
                  and "first" in f}
        self.assertIn("DOCTOR_ASSET_ALL_OPERATIONS", firsts)

    def test_doctor_report_dir_covers_report_spans(self):
        es = [e for e in self.files
              if e["target"].startswith("src/doctor/report/")]
        self.assertTrue(es, "no files under doctor/report/")
        firsts = {e["first"]["name"] for e in es if "first" in e}
        # report vocabulary types + the check/incident report builders
        self.assertIn("DoctorCheckReport", firsts)
        checks = self._entry("src/doctor/report/checks.rs")
        self.assertIsNotNone(checks, "report/checks.rs missing")
        self.assertEqual(
            self._span(checks),
            ("DoctorDatabaseIntegrityProbe",
             "doctor_top_level_operation_outcome"))

    def test_doctor_repair_state_dir_covers_state_spans(self):
        es = [e for e in self.files
              if e["target"].startswith("src/doctor/repair_state/")]
        self.assertTrue(es, "no files under doctor/repair_state/")
        names = {n for e in es if "first" in e
                 for n in (e["first"]["name"], e["last"]["name"])}
        self.assertIn("collect_doctor_interrupted_operation_states", names)

    def test_doctor_sources_rs_binds_source_inventory(self):
        e = self._entry("src/doctor/sources.rs")
        self.assertIsNotNone(e, "src/doctor/sources.rs missing")
        self.assertEqual(e["first"]["name"], "doctor_source_authority_policy")
        self.assertEqual(e["last"]["name"], "doctor_sources_config_check")

    def test_doctor_forensic_rs_binds_forensic_span(self):
        e = self._entry("src/doctor/forensic.rs")
        self.assertIsNotNone(e, "src/doctor/forensic.rs missing")
        self.assertEqual(e["first"]["name"], "doctor_now_ms")
        self.assertEqual(e["last"]["name"],
                         "capture_doctor_forensic_bundle")

    def test_doctor_support_bundle_rs_binds_bundle_span(self):
        e = self._entry("src/doctor/support_bundle.rs")
        self.assertIsNotNone(e, "src/doctor/support_bundle.rs missing")
        self.assertEqual(e["first"]["name"],
                         "DoctorSupportBundleCommandMode")
        # the redaction test mod interrupts mid-span; the capture/verify
        # tail resumes under the same owner namespace.
        self.assertEqual(e["last"]["name"],
                         "doctor_support_bundle_redact_local_value")
        tail = self._entry("src/doctor/support_bundle/capture.rs")
        self.assertIsNotNone(tail, "support_bundle/capture.rs missing")
        self.assertEqual(tail["last"]["name"],
                         "run_doctor_support_bundle_impl")

    def test_doctor_baseline_rs_binds_baseline_span(self):
        e = self._entry("src/doctor/baseline.rs")
        self.assertIsNotNone(e, "src/doctor/baseline.rs missing")
        self.assertEqual(e["first"]["name"], "DoctorBaselineCommandMode")
        self.assertEqual(e["last"]["name"],
                         "doctor_baseline_resolve_path")
        tail = self._entry("src/doctor/baseline/document.rs")
        self.assertIsNotNone(tail, "baseline/document.rs missing")
        self.assertEqual(tail["last"]["name"],
                         "doctor_baseline_perf_delta")

    def test_doctor_backup_restore_rs_binds_backup_span(self):
        e = self._entry("src/doctor/backup_restore.rs")
        self.assertIsNotNone(e, "src/doctor/backup_restore.rs missing")
        # the owner file holds the record/verify/restore implementation;
        # the kind-vocabulary consts sit in backup_restore/kinds.rs.
        self.assertEqual(e["first"]["name"], "DoctorBackupManifestRecord")
        self.assertEqual(e["last"]["name"],
                         "validate_doctor_diagnostic_read_only_controls")

    def test_doctor_probes_rs_binds_probe_span(self):
        e = self._entry("src/doctor/probes.rs")
        self.assertIsNotNone(e, "src/doctor/probes.rs missing")
        self.assertEqual(e["stage"], "3d")
        self.assertEqual(e["first"]["name"], "DoctorBoundedArchiveDbProbe")
        self.assertEqual(e["last"]["name"],
                         "doctor_archive_bundle_bytes")
        tail = self._entry("src/doctor/probes/integrity.rs")
        self.assertIsNotNone(tail, "probes/integrity.rs missing")
        self.assertEqual(tail["last"]["name"],
                         "DOCTOR_LEXICAL_COVERAGE_FLOOR_PERCENT")

    def test_doctor_owner_test_mods_live_under_owner_dirs(self):
        wanted = {
            "doctor_support_bundle_redaction_tests":
                "src/doctor/support_bundle/"
                "doctor_support_bundle_redaction_tests.rs",
            "doctor_baseline_path_tests":
                "src/doctor/baseline/doctor_baseline_path_tests.rs",
            "doctor_probe_timeout_tests":
                "src/doctor/probes/doctor_probe_timeout_tests.rs",
        }
        mods = {e["mod"]: e for e in self.files if "mod" in e}
        for name, target in wanted.items():
            self.assertIn(name, mods, f"{name} must be its own mod file")
            self.assertEqual(mods[name]["target"], target)

    def test_retarget_to_paths_exist_as_targets(self):
        targets = {e["target"] for e in self.files}
        for rt in self.man.get("retarget", []):
            for p in rt.get("to", []):
                self.assertTrue(
                    p == "src/lib.rs" or p in targets,
                    f"retarget {rt['test']}: `to` path {p} not a target")

    def test_retargets_follow_named_paths(self):
        for rt in self.man.get("retarget", []):
            for p in rt.get("to", []):
                self.assertNotIn(p, ("src/cli/run.rs",
                                     "src/cli/execute_cli.rs"),
                                 f"retarget to dead path {p}")

    def test_manual_stages_declared(self):
        manual = self.man["meta"].get("manual_stages", [])
        self.assertEqual(sorted(manual), ["5", "6"])
        for e in self.files:
            self.assertNotIn(e["stage"], ("5", "6"),
                             f"manual stage holds move entry {e['target']}")


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
