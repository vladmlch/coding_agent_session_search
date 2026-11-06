#!/usr/bin/env python3
"""Generate manifest.toml for the src/lib.rs modularization.

The plan's absolute line ranges are stale (written against a 118 996-line
file; current is 114 948). This builder anchors every region by item name
against the live inventory from rust_items.parse_items and emits one
[[file]] entry per target file covering every top-level item exactly once.

Rules encoded here (plan + controller rulings):
- `mod foo;` declarations for existing top-level files stay in lib.rs.
- The root `use` block stays in lib.rs (it is seed-copied into new files).
- Ordered text-marker groups must never be split across files.
- cfg test mods each become their own `<modname>.rs` sibling file.
- Large test mods listed in MOD_SPLITS are sliced by inner items.
- Non-test items pack greedily under BUDGET lines into files named after
  their first item; a single oversized item may exceed BUDGET alone.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import rust_items as ri

REPO = Path(__file__).resolve().parents[3]
SRC = REPO / "src" / "lib.rs"
OUT = Path(__file__).resolve().parent / "manifest.toml"

BUDGET = 1800

# (first anchor (kind, name[#ord]), stage, target dir, note)
REGIONS: list[tuple[tuple[str, str] | None, str, str, str]] = [
    (None, "stay", "src", "crate preamble: attrs, extern, mod decls, shutdown fn"),
    (("use", "*"), "stay", "src", "root use block (seed for new files)"),
    (("const", "CONTRACT_VERSION"), "4", "src/cli", "shared consts + watch-once + db helper"),
    (("struct", "Cli"), "4", "src/cli", "args: Cli, Commands, command enums"),
    (("struct", "CliError"), "4", "src/cli", "error: CliError, CliResult, WrapConfig"),
    (("fn", "root_structured_triage_rewrite"), "4", "src/cli/argv", "argv recovery/normalize/parse"),
    (("fn", "run"), "4", "src/cli", "entry + execute_cli dispatch"),
    (("fn", "handle_import"), "2b", "src/cli/commands", "import/quarantine/mirror/forget/analytics"),
    (("const", "STATE_DB_OPEN_TIMEOUT"), "4", "src/cli/state", "db-open helpers, index error mapping"),
    (("fn", "cli_error_json_payload"), "4", "src/cli/output", "cli error envelope output"),
    (("fn", "state_meta_json"), "4", "src/cli/state", "state_meta/readiness/state probes"),
    (("fn", "warn_tui_terminal_profile"), "4", "src/cli/output", "output/color/progress"),
    (("const", "DEFAULT_DEP_LOG_SUPPRESSION"), "4", "src/cli/output", "tracing + robot docs output"),
    (("struct", "TimeFilter"), "2a", "src/cli/commands/search", "search/pack command"),
    (("fn", "run_stats"), "2b", "src/cli/commands", "stats/diag/storage/dedup"),
    (("enum", "DoctorAssetClass"), "3a", "src/doctor", "doctor policy/report/taxonomy"),
    (("fn", "doctor_storage_probe_path"), "3a", "src/doctor", "storage pressure"),
    (("fn", "doctor_config_exclusion_targets"), "3a", "src/doctor", "config exclusion"),
    (("fn", "doctor_fallback_mode_from_state"), "3a", "src/doctor", "derived semantic + risk"),
    (("fn", "doctor_source_authority_policy"), "3c", "src/doctor", "source inventory"),
    (("fn", "doctor_bound_healthy_listing"), "3c", "src/doctor/raw_mirror", "raw mirror listing/manifest"),
    (("fn", "doctor_archive_coverage_snapshot"), "3c", "src/doctor/archive", "archive scan/normalize"),
    (("fn", "query_doctor_raw_mirror_backfill_candidates"), "3c", "src/doctor/raw_mirror", "raw mirror backfill"),
    (("fn", "build_doctor_sole_copy_warnings"), "3c", "src/doctor", "coverage/runtime/source-authority"),
    (("fn", "doctor_candidate_root"), "3d", "src/doctor/candidate", "candidate scan/reconstruct"),
    (("fn", "doctor_backup_kind_label"), "3c", "src/doctor", "backup/restore/baseline/dispatchers"),
    (("fn", "collect_doctor_interrupted_operation_states"), "3b", "src/doctor", "repair state/journal/probes"),
    (("fn", "apply_diag_quarantine_cleanup"), "3b", "src/doctor", "fs mutation + ordered groups + doctor test mods"),
    (("fn", "run_onboarding_interactive_wizard"), "2c", "src/cli/commands", "onboarding/guide"),
    (("fn", "diagnostics_connector_paths"), "2c", "src/cli/commands/status_health", "diagnostics/status/triage/health"),
    (("mod_inline", "cli_read_db_tests"), "2c", "src/cli/commands/status_health", "cli_read_db tests (split)"),
    (("struct", "DoctorBoundedArchiveDbProbe"), "3d", "src/doctor", "doctor probes"),
    (("fn", "run_doctor_impl"), "3d", "src/doctor", "doctor run"),
    (("mod_inline", "path_spelling_tests"), "2d", "src/cli/commands", "sessions/context/capabilities/introspect"),
    (("fn", "response_schema_index_state"), "1a", "src/robot_schema", "response schemas"),
    (("fn", "role_to_export_string"), "2d", "src/cli/commands/view", "view + followup identity"),
    (("struct", "IndexShutdownSignals"), "1b", "src/indexer", "indexer stall watchdog"),
    (("fn", "run_lexical_reconcile_cli"), "2f", "src/cli/commands/index", "index commands"),
    (("fn", "default_db_path"), "2f", "src/cli", "cli paths + session paths"),
    (("fn", "run_resume"), "2f", "src/cli/commands", "export/resume/expand/timeline"),
    (("enum", "AgentFormat"), "1c", "src/export", "export classification/grouping"),
    (("mod_inline", "export_timestamp_tests"), "1c", "src/export", "export test mods"),
    (("fn", "format_as_markdown"), "2f", "src/cli/commands", "export format + expand/timeline"),
    (("fn", "run_sources_command"), "2g", "src/cli/commands/sources", "sources"),
    (("fn", "run_models_command"), "2g", "src/cli/commands/models", "models/mappings/agents"),
    (("mod_inline", "timeline_datetime_parse_tests"), "2g", "src/cli/commands", "daemon/schedule tail"),
]

# Ordered text-marker groups: never split members across files.
GROUPS = [
    ("cleanup-order", [("fn", "apply_diag_quarantine_cleanup"), ("fn", "fill_cleanup_after_summary")]),
    ("fs-mutation-order", [("fn", "execute_doctor_fs_mutation"), ("fn", "cleanup_target_path_is_safe")]),
    ("watch-once-cfg-pair", [("fn", "read_watch_once_paths_env"), ("fn", "read_watch_once_paths_env#1")]),
]

# cfg test mods sliced across multiple files (by inner items).
MOD_SPLITS = {
    "doctor_asset_taxonomy_tests": 7,
    "cli_read_db_tests": 2,
    "indexed_conversation_fallback_tests": 2,
}


def slug(name: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9]+", "_", name).strip("_").lower()
    return (s or "item")[:52]


def resolve(items, kind, name):
    if "#" in name:
        name, ord_s = name.rsplit("#", 1)
        ordinal = int(ord_s)
    else:
        ordinal = 0
    hits = [
        i for i, it in enumerate(items)
        if it.kind == kind and (name == "*" or it.name == name) and it.ordinal == ordinal
    ]
    if name == "*":
        if not hits:
            raise SystemExit(f"anchor {kind} *: no match")
        return hits[0]
    if len(hits) != 1:
        raise SystemExit(f"anchor {kind} {name}#{ordinal}: {len(hits)} matches")
    return hits[0]


def is_cfg_test_mod(it) -> bool:
    return it.kind == "mod_inline" and any(
        "cfg" in a.text and "test" in a.text for a in it.attrs
    )


def ent(it) -> str:
    s = f'{{ kind = "{it.kind}", name = "{it.name}"'
    if it.ordinal:
        s += f", ordinal = {it.ordinal}"
    return s + " }"


def build() -> None:
    src = SRC.read_text()
    items, _cls, gaps = ri.parse_items(src)
    if gaps:
        raise SystemExit(f"unattributed code: {len(gaps)} gaps")
    raw = src.encode("utf-8")
    starts = ri.line_starts(raw)

    def item_lines(it):
        return ri.line_of(starts, it.end - 1) - ri.line_of(starts, it.start) + 1

    # Region boundaries (item indices).
    bounds = []
    for anchor, stage, dir_, note in REGIONS:
        pos = 0 if anchor is None else resolve(items, *anchor)
        bounds.append((pos, stage, dir_, note))
    for (a, *_), (b, *_) in zip(bounds, bounds[1:]):
        if b <= a:
            raise SystemExit("region anchors not strictly increasing")

    # Protected index ranges for ordered groups.
    protected = []  # list of [first_idx, last_idx]
    for _gname, members in GROUPS:
        idxs = [resolve(items, *m) for m in members]
        protected.append((min(idxs), max(idxs)))

    def inside_protected(i):
        return any(a <= i <= b for a, b in protected)

    entries = []  # dicts -> [[file]] rows
    for r_i, (pos, stage, dir_, _note) in enumerate(bounds):
        end = bounds[r_i + 1][0] if r_i + 1 < len(bounds) else len(items)
        i = pos
        run_first = None  # start index of current code run
        run_lines = 0
        prev = None

        def flush():
            nonlocal run_first, run_lines
            if run_first is None:
                return
            entries.append({
                "stage": stage, "target": None, "dir": dir_,
                "first": items[run_first], "last": items[prev],
            })
            run_first, run_lines = None, 0

        while i < end:
            it = items[i]
            if it.kind == "mod_decl":
                flush()
                entries.append({
                    "stage": "stay", "target": "src/lib.rs", "dir": "src",
                    "first": it, "last": it,
                })
                i += 1
                continue
            if stage == "stay":
                flush()
                j = i
                while j < end and items[j].kind != "mod_decl":
                    j += 1
                entries.append({
                    "stage": "stay", "target": "src/lib.rs", "dir": "src",
                    "first": items[i], "last": items[j - 1],
                })
                i = j
                continue
            if is_cfg_test_mod(it):
                flush()
                parts = MOD_SPLITS.get(it.name, 1)
                if parts == 1:
                    entries.append({
                        "stage": stage,
                        "target": f"{dir_}/{it.name}.rs",
                        "dir": dir_, "first": it, "last": it,
                    })
                else:
                    inner = it.inner or []
                    if not inner:
                        raise SystemExit(f"{it.name}: no inner items to split")
                    total = sum(item_lines(x) for x in inner)
                    per = (total + parts - 1) // parts
                    acc, first_inner, pno = 0, 0, 0
                    for k, x in enumerate(inner + [None]):
                        if x is None or (acc >= per and k > first_inner and pno < parts - 1):
                            entries.append({
                                "stage": stage,
                                "target": f"{dir_}/{it.name}" + ("" if pno == 0 else f"_part_{pno + 1}") + ".rs",
                                "dir": dir_, "first": it, "last": it,
                                "mod": it.name,
                                "inner_first": inner[first_inner], "inner_last": inner[k - 1],
                                "mod_part": pno + 1, "mod_parts": parts,
                            })
                            pno += 1
                            if x is None:
                                break
                            first_inner, acc = k, 0
                        acc += item_lines(x)
                i += 1
                prev = i - 1
                continue
            # regular item
            ln = item_lines(it)
            if run_first is None:
                run_first, run_lines = i, ln
            elif run_lines + ln > BUDGET and not inside_protected(i - 1) and not inside_protected(i):
                # split allowed only when neither side is mid-group
                flush()
                run_first, run_lines = i, ln
            else:
                run_lines += ln
            prev = i
            i += 1
        flush()

    # Assign file paths for code runs (slug of first item, dedup per dir).
    used = {}
    for e in entries:
        if e["target"] is not None:
            used.setdefault(e["target"], e)
            continue
        base = slug(e["first"].name)
        path = f"{e['dir']}/{base}.rs"
        n = 2
        while path in used:
            path = f"{e['dir']}/{base}_{n}.rs"
            n += 1
        e["target"] = path
        used[path] = e

    # Coverage check: every item exactly once; entries ordered.
    covered = [False] * len(items)
    prev_first_idx = -1
    for e in entries:
        a = items.index(e["first"]); b = items.index(e["last"])
        if a < prev_first_idx:
            raise SystemExit("entries out of order")
        prev_first_idx = a
        if e.get("mod"):
            covered[a] = True  # slice entries share the mod index
        else:
            for k in range(a, b + 1):
                if covered[k]:
                    raise SystemExit(f"double coverage at item {k}")
                covered[k] = True
    missing = [i for i, c in enumerate(covered) if not c]
    if missing:
        raise SystemExit(f"uncovered items: {[ (items[i].kind, items[i].name) for i in missing[:10] ]}")

    # Group containment check.
    for gname, members in GROUPS:
        idxs = [resolve(items, *m) for m in members]
        lo, hi = min(idxs), max(idxs)
        hits = [
            e["target"] for e in entries
            if not e.get("mod")
            and items.index(e["first"]) <= lo and items.index(e["last"]) >= hi
        ]
        if len(hits) != 1:
            raise SystemExit(f"group {gname} not contained in one file: {hits}")

    # Emit.
    lines = [
        "# Generated by manifest_build.py — do not edit by hand.",
        "# Regenerate: python3 scripts/refactor/lib_split/manifest_build.py",
        "",
        "[meta]",
        'source = "src/lib.rs"',
        f"source_lines = {len(starts)}",
        'generated_against = "ac28de473dc87ac60d4f5e6d7794a9c4504bfbe4"',
        "",
    ]
    for gname, members in GROUPS:
        lines += ["[[group]]", f'name = "{gname}"', "items = [ " +
                  ", ".join(f'{{ kind = "{k}", name = "{n.split("#")[0]}"' +
                            (f', ordinal = {n.split("#")[1]}' if "#" in n else "") + " }"
                            for k, n in members) + " ]", ""]
    for mod, parts in MOD_SPLITS.items():
        lines += ["[[mod_split]]", f'mod = "{mod}"', f"parts = {parts}", ""]
    for e in entries:
        lines.append("[[file]]")
        lines.append(f'target = "{e["target"]}"')
        lines.append(f'stage = "{e["stage"]}"')
        if e.get("mod"):
            lines.append(f'slice = {{ mod = "{e["mod"]}", '
                         f'first = {ent(e["inner_first"])}, '
                         f'last = {ent(e["inner_last"])}, '
                         f'part = {e["mod_part"]}, parts = {e["mod_parts"]} }}')
        else:
            lines.append(f"first = {ent(e['first'])}")
            lines.append(f"last = {ent(e['last'])}")
        lines.append("")
    OUT.write_text("\n".join(lines))
    files = [e for e in entries if e["stage"] != "stay"]
    print(f"wrote {OUT}: {len(entries)} file entries ({len(files)} moved)")
    print(f"coverage: {len(items)} items, 0 uncovered")


if __name__ == "__main__":
    build()
