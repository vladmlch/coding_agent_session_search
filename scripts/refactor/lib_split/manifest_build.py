#!/usr/bin/env python3
"""Generate manifest.toml for the src/lib.rs modularization.

The plan's absolute line ranges are stale (written against a 118 996-line
file; current is ~114 949). This builder anchors every region by item name
against the live inventory from rust_items.parse_items and emits one
[[file]] entry per target file covering every top-level item exactly once.

Rules encoded here (plan + controller rulings):
- `mod foo;` declarations for existing top-level files stay in lib.rs,
  except the two explicit [[mod_reloc]] relocations.
- The root `use` block stays in lib.rs (it is seed-copied into new files).
- Ordered text-marker groups must never be split across files; the packer
  flushes *before* a protected span when it cannot fit whole.
- cfg test mods each become their own `<modname>.rs` sibling file and are
  dissolved (the file IS the module).
- Large test mods listed in MOD_SPLITS are sliced by inner items.
- Non-test items pack greedily under BUDGET lines into files named after
  their first item (or a named plan target in NAMED_FILES); a single
  oversized item may exceed BUDGET alone.
- A type and its impl block are co-located: a far-away impl is emitted
  into the type's file via `also`.
- Every emitted file gets `also`/`slice` semantics; [[retarget]] entries
  enumerate the only sanctioned in-test source-path rewrites, per test
  and per stage, restricted to the resolved `to` files.
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
HARD_CAP = 2000
# Seed header + safety margin accounted against the working target so an
# emitted file cannot cross the 2000-line cap through generated glue.
# Measured header: 43 lines (GENERATED + allows + use super/crate + 17
# root uses incl. per-use #[allow]); 50 leaves slack for long `use` lines.
EMIT_OVERHEAD = 50

STAGE_ORDER = [
    "1a", "1b", "1c",
    "2a", "2b", "2c", "2d", "2e", "2f", "2g",
    "3a", "3b", "3c", "3d",
    "4", "5", "6",
]

# (first anchor (kind, name[#ord]), stage, target dir, note)
REGIONS: list[tuple[tuple[str, str] | None, str, str, str]] = [
    (None, "stay", "src", "crate preamble: attrs, extern, mod decls, shutdown fn"),
    (("use", "*"), "stay", "src", "root use block (seed for new files)"),
    (("const", "CONTRACT_VERSION"), "4", "src/cli", "shared consts + watch-once + db helper"),
    (("struct", "Cli"), "4", "src/cli", "args: Cli, Commands, command enums"),
    (("struct", "CliError"), "4", "src/cli", "error: CliError, CliResult, WrapConfig"),
    (("fn", "root_structured_triage_rewrite"), "4", "src/cli/argv", "argv recovery/normalize/parse"),
    (("struct", "ParsedCli"), "4", "src/cli", "entry.rs surface + dispatch"),
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
    (("enum", "DoctorFsMutationKind"), "3b", "src/doctor/fs_safety", "fs mutation kinds/types + ordered groups"),
    (("mod_inline", "doctor_asset_taxonomy_tests"), "3b", "src/doctor", "doctor test mods (split)"),
    (("fn", "run_onboarding_interactive_wizard"), "2c", "src/cli/commands", "onboarding/guide"),
    (("fn", "diagnostics_connector_paths"), "2c", "src/cli/commands/status_health", "diagnostics/status/triage/health"),
    (("mod_inline", "cli_read_db_tests"), "2c", "src/cli/commands/status_health", "cli_read_db tests (split)"),
    (("struct", "DoctorBoundedArchiveDbProbe"), "3d", "src/doctor", "doctor probes"),
    (("fn", "run_doctor_impl"), "3d", "src/doctor", "doctor run"),
    (("mod_inline", "path_spelling_tests"), "2d", "src/cli/commands", "sessions/context/ingest-reuse"),
    (("struct", "CapabilitiesResponse"), "2e", "src/cli/commands/capabilities_introspect", "capabilities/introspect/api_version + command-schema"),
    (("fn", "response_schema_index_state"), "1a", "src/robot_schema", "response schemas"),
    (("fn", "role_to_export_string"), "2d", "src/cli/commands/view", "view + followup identity"),
    (("struct", "IndexShutdownSignals"), "1b", "src/indexer", "indexer stall watchdog"),
    (("fn", "run_lexical_reconcile_cli"), "2f", "src/cli/commands/index", "index commands"),
    (("fn", "default_db_path"), "2f", "src/cli", "cli paths + session paths"),
    (("fn", "run_resume"), "2f", "src/cli/commands", "export/resume/expand/timeline"),
    (("enum", "AgentFormat"), "1c", "src/export", "export classification/grouping"),
    (("mod_inline", "export_timestamp_tests"), "2f", "src/cli/commands/export", "export cmd test mods"),
    (("fn", "format_as_markdown"), "2f", "src/cli/commands", "export format + expand/timeline"),
    (("fn", "run_sources_command"), "2g", "src/cli/commands/sources", "sources"),
    (("fn", "run_models_command"), "2g", "src/cli/commands/models", "models/mappings/agents"),
    (("mod_inline", "timeline_datetime_parse_tests"), "2g", "src/cli/commands", "daemon/schedule tail"),
]

# Named plan targets: file whose packed run starts at this item name.
# A NAMED_FILES member forces a packer flush *before* itself, so the
# named item always starts its own file (plan §3 owner paths bind
# exactly, not as best-effort prefixes).
NAMED_FILES = {
    "Cli": "src/cli/args.rs",
    "diagnostics_connector_paths": "src/cli/commands/status_health/diagnostics.rs",
    "normalize_args": "src/cli/argv/normalize.rs",
    "ParsedCli": "src/cli/entry.rs",
    "execute_cli": "src/cli/dispatch.rs",
    "run_doctor_impl": "src/doctor/run.rs",
    "IndexShutdownSignals": "src/indexer/stall_watchdog.rs",
    "AgentFormat": "src/export/messages.rs",
    "DoctorFsMutationKind": "src/doctor/fs_safety/mutation_kind.rs",
}

# Ordered text-marker groups: never split members across files.
GROUPS = [
    ("cleanup-order", [("fn", "apply_diag_quarantine_cleanup"), ("fn", "fill_cleanup_after_summary")]),
    ("fs-mutation-order", [("fn", "execute_doctor_fs_mutation"), ("fn", "cleanup_target_path_is_safe")]),
    ("watch-once-cfg-pair", [("fn", "read_watch_once_paths_env"), ("fn", "read_watch_once_paths_env#1")]),
]

# The file that ends up holding a group's full span gets this fixed target.
GROUP_FILE_NAMES = {
    "cleanup-order": "src/doctor/fs_safety/cleanup_apply.rs",
    "fs-mutation-order": "src/doctor/fs_safety/mutation_executor.rs",
}

# cfg test mods sliced across multiple files (by inner items).
MOD_SPLITS = {
    "doctor_asset_taxonomy_tests": 7,
    "cli_read_db_tests": 2,
    "indexed_conversation_fallback_tests": 2,
}

# cfg test mods whose (dir, stage) differ from their enclosing region.
MOD_TARGET_OVERRIDES = {
    "export_timestamp_tests": ("src/cli/commands/export", "2f"),
    "legacy_source_filter_tests": ("src/cli/commands/export", "2f"),
    "indexed_conversation_fallback_tests": ("src/cli/commands/export", "2f"),
    "robot_output_score_tests": ("src/cli/commands/export", "2f"),
    "message_grouping_tests": ("src/export", "1c"),
}

# `mod X;` decls that move to a new parent file at a stage; the file is
# git-mv'd and the decl rewritten (attrs preserved).
MOD_RELOCS = [
    {
        "decl": "doctor_backup_restore_tests",
        "stage": "3d",
        "new_parent": "src/doctor.rs",
        "new_name": "backup_restore_tests",
        "file_from": "src/doctor_backup_restore_tests.rs",
        "file_to": "src/doctor/backup_restore_tests.rs",
    },
    {
        "decl": "doctor_candidate_wal_tests",
        "stage": "3d",
        "new_parent": "src/doctor/candidate.rs",
        "new_name": "wal_tests",
        "file_from": "src/doctor_candidate_wal_tests.rs",
        "file_to": "src/doctor/candidate/wal_tests.rs",
    },
]

# §9 retargets — the ONLY sanctioned in-test source-path rewrites.
# `find` is the literal byte sequence replaced inside the named test's fn
# span (code positions only). `reads` are literal source paths;
# `reads_items` / `reads_markers` resolve to the emitted file containing
# that item/marker. `to` + `with` are materialized into the manifest.
RETARGETS = [
    {
        "test": "doctor_cleanup_apply_cannot_bypass_fs_mutation_executor_for_pruning",
        "test_file": "src/doctor/doctor_asset_taxonomy_tests_part_4.rs",
        "stage": "3b",
        "find": 'include_str!("lib.rs")',
        "reads_markers": ["fn apply_diag_quarantine_cleanup", "fn execute_doctor_fs_mutation"],
        "markers": ["fn apply_diag_quarantine_cleanup", "fn execute_doctor_fs_mutation"],
    },
    {
        "test": "run_doctor_stale_lock_removal_cannot_bypass_fs_mutation_executor",
        "test_file": "src/doctor/doctor_asset_taxonomy_tests_part_4.rs",
        "stage": "3b",
        "find": 'include_str!("lib.rs")',
        "reads": ["src/lib.rs"],
        "markers": ["fn run_doctor_impl"],
    },
    {
        "test": "run_doctor_stale_lock_removal_cannot_bypass_fs_mutation_executor",
        "test_file": "src/doctor/doctor_asset_taxonomy_tests_part_4.rs",
        "stage": "3d",
        "find": 'include_str!("../lib.rs")',
        "reads_items": [("fn", "run_doctor_impl")],
        "markers": ["fn run_doctor_impl"],
    },
    {
        "test": "legacy_cli_dispatch_routes_through_typed_doctor_module",
        "test_file": "src/doctor.rs",
        "stage": "3d",
        "find": 'include_str!("lib.rs")',
        "reads": ["src/lib.rs"],
        "reads_items": [("fn", "run_doctor_impl")],
        "markers": ["pub(crate) fn run_doctor_impl(", "doctor::DoctorCommandRequest::from_cli_flags"],
    },
    {
        "test": "legacy_cli_dispatch_routes_through_typed_doctor_module",
        "test_file": "src/doctor.rs",
        "stage": "4",
        "find": 'concat!(include_str!("lib.rs"), include_str!("doctor/run.rs"))',
        "reads_items": [("fn", "execute_cli"), ("fn", "run_doctor_impl")],
        "markers": ["pub(crate) fn run_doctor_impl(", "doctor::DoctorCommandRequest::from_cli_flags"],
    },
    {
        "test": "pass7_fixture_fm_cli_robot_schema_version_missing_doctor_root",
        "test_file": "src/doctor_chokepoint.rs",
        "stage": "3d",
        "find": 'format!("{manifest}/src/lib.rs")',
        "reads_items": [("fn", "run_doctor_impl")],
        "markers": ['"schema_version": 2,', '"doctor_contract_version": 1,'],
    },
    {
        "test": "diagnostics_connector_paths_is_dynamic",
        "test_file": "tests/agent_detection_completeness.rs",
        "stage": "2c",
        "find": 'join("src/lib.rs")',
        "reads_items": [("fn", "diagnostics_connector_paths")],
        "markers": ["fn diagnostics_connector_paths(", "detect_installed_agents"],
    },
    {
        "test": "alias_targets_from_source",
        "test_file": "tests/spec_aliases_resolve.rs",
        "stage": "4",
        "find": 'include_str!("../src/lib.rs")',
        "reads_markers": ["SUBCOMMAND_ALIASES"],
        "markers": ["SUBCOMMAND_ALIASES"],
    },
    {
        "test": "removed_command_matrix_is_fully_active",
        "test_file": "tests/local_only_audit.rs",
        "stage": "4",
        "find": 'read_text(&root.join("src/lib.rs"))',
        "reads_markers": ["const REMOVED_COMMANDS", "const REMOVED_FLAGS"],
        "markers": ["REMOVED_COMMANDS", "REMOVED_FLAGS", "was removed"],
    },
    {
        "test": "pass4_lib_dispatch_routes_through_doctor_chokepoint_apis",
        "test_file": "tests/doctor_mutate_auditor.rs",
        "stage": "3c",
        "find": 'fs::read_to_string(format!("{manifest}/src/lib.rs"))',
        "reads": ["src/lib.rs"],
        "reads_markers": ["fn run_doctor_ls"],
        "markers": [
            "crate::doctor_runs::list_runs", "crate::doctor_runs::find_latest_run",
            "crate::doctor_runs::RunId", "crate::doctor_undo::undo_run",
            "doctor_robot_docs_body", "RobotTopic::Doctor",
        ],
    },
    {
        "test": "pass4_lib_dispatch_routes_through_doctor_chokepoint_apis",
        "test_file": "tests/doctor_mutate_auditor.rs",
        "stage": "4",
        "find": 'format!("{manifest}/src/lib.rs")',
        # The 3c concat already reads the archive file; this stage-4
        # rewrite substitutes the surviving lib.rs read inside that
        # concat with the file that now holds print_robot_docs. `to`
        # lists everything the rewritten test reads (checked markers
        # span both files); `with` substitutes only the lib.rs part.
        "to": [
            "src/cli/output/extract_search_terms.rs",
            "src/doctor/doctor_archive_export_collect_assets.rs",
        ],
        "with": 'format!("{manifest}/src/cli/output/extract_search_terms.rs")',
        "reads_markers": ["doctor_robot_docs_body"],
        "markers": [
            "crate::doctor_runs::list_runs", "crate::doctor_runs::find_latest_run",
            "crate::doctor_runs::RunId", "crate::doctor_undo::undo_run",
            "doctor_robot_docs_body", "RobotTopic::Doctor",
        ],
    },
    {
        "test": "pass4_pass_3_subcommands_wired_in_clap",
        "test_file": "tests/doctor_mutate_auditor.rs",
        "stage": "4",
        "find": 'format!("{manifest}/src/lib.rs")',
        "reads_markers": ["robot_triage:"],
        "markers": ["robot_triage:", "diff: Option<String>", "gc_before: Option<String>"],
    },
    {
        "test": "pass8_explain_and_emit_capabilities_dispatch_present",
        "test_file": "tests/doctor_mutate_auditor.rs",
        "stage": "3c",
        "find": 'std::fs::read_to_string(format!("{manifest}/src/lib.rs"))',
        "reads_markers": ["fn run_doctor_explain",
                          "fn run_doctor_emit_capabilities"],
        "markers": ["fn run_doctor_explain", "fn run_doctor_emit_capabilities",
                    "doctor-capabilities", 'kind": "explain"'],
    },
    {
        "test": "pass6_watch_helper_routes_through_doctor_runs_apis",
        "test_file": "tests/doctor_mutate_auditor.rs",
        "stage": "3c",
        "find": 'format!("{manifest}/src/lib.rs")',
        "reads_markers": ["fn run_doctor_watch"],
        "markers": ["fn run_doctor_watch", "crate::doctor_runs::list_runs",
                    "crate::doctor_runs::find_in_flight_band", "watch-tick"],
    },
]


def slug(name: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9]+", "_", name).strip("_").lower()
    return (s or "item")[:52]


def stage_le(a: str, b: str) -> bool:
    if a == "stay":
        return True
    if b == "stay":
        return False
    return STAGE_ORDER.index(a) <= STAGE_ORDER.index(b)


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


def ent(it) -> str:
    s = f'{{ kind = "{it.kind}", name = "{it.name}"'
    if it.ordinal:
        s += f", ordinal = {it.ordinal}"
    return s + " }"


def impl_target_name(impl: "ri.Item") -> str | None:
    """Base type name this impl is for (`impl X` / `impl T for X`)."""
    header = impl.name
    m = re.search(r"\bfor\s+(.+)$", header)
    target = m.group(1) if m else header
    target = re.sub(r"<[^>]*>", "", target).strip()
    target = target.split("::")[-1].strip()
    return target or None


def path_of_module_file(target: str) -> str:
    """src/cli/commands/x.rs -> cli::commands::x (crate-relative path)."""
    p = target.removeprefix("src/").removesuffix(".rs")
    parts = p.split("/")
    if parts[-1] == "mod":
        parts = parts[:-1]
    return "::".join(parts)


def build() -> None:
    src = SRC.read_text()
    items, _cls, gaps = ri.parse_items(src)
    if gaps:
        raise SystemExit(f"unattributed code: {len(gaps)} gaps")
    raw = src.encode("utf-8")
    starts = ri.line_starts(raw)

    def item_lines(pool, i):
        """Emitted lines for pool[i] incl. its leading gap (standalone
        comments/whitespace travel with the item — see ri.ownership)."""
        it = pool[i]
        lead_s = pool[i - 1].end if i > 0 else it.start
        return ri.line_of(starts, it.end - 1) - ri.line_of(starts, lead_s) + 1

    reloc_names = {r["decl"] for r in MOD_RELOCS}

    # Region boundaries (item indices).
    bounds = []
    for anchor, stage, dir_, note in REGIONS:
        pos = 0 if anchor is None else resolve(items, *anchor)
        bounds.append((pos, stage, dir_, note))
    for (a, *_), (b, *_) in zip(bounds, bounds[1:]):
        if b <= a:
            raise SystemExit("region anchors not strictly increasing")

    # Protected index ranges: named groups plus every same-(kind,name)
    # cluster whose members differ only by cfg (cfg-variant pairs).
    protected = []  # list of (first_idx, last_idx)
    for _gname, members in GROUPS:
        idxs = [resolve(items, *m) for m in members]
        protected.append((min(idxs), max(idxs)))
    by_key: dict[tuple[str, str], list[int]] = {}
    for i, it in enumerate(items):
        if it.kind in ("fn", "const", "static", "type") and it.name:
            by_key.setdefault((it.kind, it.name), []).append(i)
    for (kind, name), idxs in by_key.items():
        if len(idxs) > 1:
            protected.append((min(idxs), max(idxs)))

    def span_start(i):
        for a, b in protected:
            if a == i:
                return b
        return None

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
                if it.name in reloc_names:
                    i += 1  # covered by its [[mod_reloc]] entry
                    continue
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
            if it.kind == "mod_inline" and not ri.mod_has_test_cfg(it):
                # A production inline mod cannot be packed like an item:
                # nesting it in a file changes `crate::X` to `crate::f::X`.
                raise SystemExit(
                    f"non-test inline mod {it.name} — needs an explicit owner")
            if it.kind == "mod_inline" and ri.mod_has_test_cfg(it):
                flush()
                tdir, tstage = MOD_TARGET_OVERRIDES.get(it.name, (dir_, stage))
                parts = MOD_SPLITS.get(it.name, 1)
                if parts == 1:
                    entries.append({
                        "stage": tstage,
                        "target": f"{tdir}/{it.name}.rs",
                        "dir": tdir, "first": it, "last": it,
                        "mod": it.name,
                    })
                else:
                    inner = it.inner or []
                    if not inner:
                        raise SystemExit(f"{it.name}: no inner items to split")
                    total = sum(item_lines(inner, k) for k in range(len(inner)))
                    per = (total + parts - 1) // parts
                    acc, first_inner, pno = 0, 0, 0
                    for k, x in enumerate(inner + [None]):
                        if x is None or (acc >= per and k > first_inner and pno < parts - 1):
                            entries.append({
                                "stage": tstage,
                                "target": f"{tdir}/{it.name}_part_{pno + 1}.rs",
                                "dir": tdir, "first": it, "last": it,
                                "mod": it.name,
                                "inner_first": inner[first_inner], "inner_last": inner[k - 1],
                                "mod_part": pno + 1, "mod_parts": parts,
                            })
                            pno += 1
                            if x is None:
                                break
                            first_inner, acc = k, 0
                        acc += item_lines(inner, k)
                i += 1
                prev = i - 1
                continue
            # regular item: protected spans enter the run atomically, and a
            # span that cannot fit whole forces a flush *before* it.
            ln = item_lines(items, i)
            first_i = i
            span_end = span_start(i)
            if span_end is not None:
                span_lines = sum(item_lines(items, k) for k in range(i, span_end + 1))
                if run_first is not None and run_lines + span_lines > BUDGET - EMIT_OVERHEAD:
                    flush()
                ln = span_lines
                i = span_end
            if run_first is None:
                run_first, run_lines = first_i, ln
            elif (run_lines + ln > BUDGET - EMIT_OVERHEAD
                  or (items[first_i].name in NAMED_FILES
                      and items[first_i].kind != "impl")) \
                    and not inside_protected(i - 1) and not inside_protected(i):
                # Budget overflow OR a plan-named owner arriving mid-run:
                # named targets always start their own file. (impls share
                # their type's name — excluded, they ride `also`.)
                flush()
                run_first, run_lines = first_i, ln
            else:
                run_lines += ln
            prev = i
            i += 1
        flush()

    # Assign file paths for code runs (named target, group target, or slug).
    used = {}
    for e in entries:
        if e["target"] is not None:
            used.setdefault(e["target"], e)
            continue
        a = items.index(e["first"]); b = items.index(e["last"])
        named = None
        for gname, members in GROUPS:
            idxs = [resolve(items, *m) for m in members]
            lo, hi = min(idxs), max(idxs)
            if a <= lo and b >= hi and gname in GROUP_FILE_NAMES:
                named = GROUP_FILE_NAMES[gname]
                break
        path = named or (
            NAMED_FILES.get(e["first"].name)
            if e["first"].kind != "impl" else None)
        if path is None:
            base = slug(e["first"].name)
            path = f"{e['dir']}/{base}.rs"
            n = 2
            while path in used:
                path = f"{e['dir']}/{base}_{n}.rs"
                n += 1
        if path in used:
            raise SystemExit(f"named target collision: {path}")
        e["target"] = path
        used[path] = e

    # Type+impl co-location: an impl whose type landed in another file is
    # claimed by the type's file via `also`.
    claimed: dict[int, dict] = {}  # item idx -> owner entry
    for i, it in enumerate(items):
        if it.kind != "impl":
            continue
        tname = impl_target_name(it)
        if not tname:
            continue
        type_hits = [
            j for j, t in enumerate(items)
            if t.kind in ("struct", "enum", "union") and t.name == tname
        ]
        if len(type_hits) != 1:
            continue
        tidx = type_hits[0]
        impl_file = next((e for e in entries if not e.get("mod")
                          and items.index(e["first"]) <= i <= items.index(e["last"])), None)
        type_file = next((e for e in entries if not e.get("mod")
                          and items.index(e["first"]) <= tidx <= items.index(e["last"])), None)
        if impl_file is type_file or impl_file is None or type_file is None:
            continue
        if type_file["stage"] == "stay":
            raise SystemExit(
                f"impl {it.name} moves but its type stays in src/lib.rs")
        type_file.setdefault("also", []).append(it)
        claimed[i] = type_file

    # Repair: `also`-claimed impls inflate the owner file — the packer
    # sized it without them. Any owner that would exceed the working
    # budget is split so each claimed type sits in its own entry with
    # its impls attached.
    def est(e) -> int:
        a = items.index(e["first"]); b = items.index(e["last"])
        return (sum(item_lines(items, k) for k in range(a, b + 1)
                    if k not in claimed)
                + sum(item_lines(items, items.index(x))
                      for x in e.get("also", [])))

    repaired = []
    for e in entries:
        if e.get("mod") or not e.get("also") or est(e) <= BUDGET - EMIT_OVERHEAD:
            repaired.append(e)
            continue
        a = items.index(e["first"]); b = items.index(e["last"])
        by_type: dict[str, list] = {}
        for x in e["also"]:
            by_type.setdefault(impl_target_name(x), []).append(x)
        tidx_of = {
            n: next((k for k in range(a, b + 1)
                     if items[k].kind in ("struct", "enum", "union")
                     and items[k].name == n), None)
            for n in by_type
        }
        cuts = sorted(i for i in tidx_of.values() if i is not None)
        if not cuts:
            raise SystemExit(
                f"also: claimed types {sorted(by_type)} absent from {e['target']}")
        sub = []
        prev = a
        for tidx in cuts:
            if prev < tidx:
                sub.append({**e, "first": items[prev],
                            "last": items[tidx - 1], "target": None,
                            "also": []})
            tn = items[tidx].name
            sub.append({**e, "first": items[tidx], "last": items[tidx],
                        "target": None, "also": by_type.get(tn, [])})
            prev = tidx + 1
        if prev <= b:
            sub.append({**e, "first": items[prev], "last": items[b],
                        "target": None, "also": []})
        stray = [x for n, xs in by_type.items()
                 if tidx_of.get(n) is None for x in xs]
        if stray:
            sub[-1].setdefault("also", []).extend(stray)
        bad = [s for s in sub if est(s) > BUDGET - EMIT_OVERHEAD]
        if bad:
            raise SystemExit(
                f"also: cannot fit {[(s['first'].name, est(s)) for s in bad]} "
                f"under {BUDGET - EMIT_OVERHEAD} in {e['target']}")
        used.pop(e["target"], None)
        repaired.extend(sub)

    entries = repaired
    # Re-name split-off entries (first sub-entry keeps the parent name
    # only when it covers the parent's first item — the naming pass below
    # assigns slugs to all target=None entries).
    for e in entries:
        if e.get("target") is None and not e.get("mod"):
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
        if e.get("mod") and e.get("inner_first"):
            covered[a] = True  # slice entries share the mod index
        elif e.get("mod"):
            covered[a] = True
        else:
            for k in range(a, b + 1):
                if covered[k]:
                    raise SystemExit(f"double coverage at item {k}")
                covered[k] = True
    for k in claimed:
        covered[k] = True
    reloc_decl_idx = {resolve(items, "mod_decl", n) for n in reloc_names}
    for k in reloc_decl_idx:
        covered[k] = True
    missing = [i for i, c in enumerate(covered) if not c]
    if missing:
        raise SystemExit(f"uncovered items: {[(items[i].kind, items[i].name) for i in missing[:10]]}")

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

    def file_containing(pred) -> set[str]:
        out = set()
        for e in entries:
            if e.get("mod") or e["stage"] == "stay":
                continue
            a = items.index(e["first"]); b = items.index(e["last"])
            rng = set(range(a, b + 1)) - set(claimed)
            if e.get("also"):
                rng |= {items.index(x) for x in e["also"]}
            for i in rng:
                if pred(items[i]):
                    out.add(e["target"])
                    break
        return out

    def marker_files(marker: str) -> set[str]:
        mb = marker.encode()
        return file_containing(lambda it: mb in raw[it.start:it.end])

    # Resolve retargets -> ordered `to` paths + mechanical `with` literal.
    import posixpath
    retarget_rows = []
    for rt in RETARGETS:
        to: list[str] = []
        for p in rt.get("reads", []):
            if p not in to:
                to.append(p)
        for kind, name in rt.get("reads_items", []):
            idx = resolve(items, kind, name)
            for p in file_containing(lambda it, i=idx: it is items[i]):
                if p not in to:
                    to.append(p)
        for marker in rt.get("reads_markers", []):
            for p in sorted(marker_files(marker)):
                if p not in to:
                    to.append(p)
        if rt.get("to"):
            to = rt["to"]
        if not to:
            raise SystemExit(f"retarget {rt['test']}: no `to` files resolved")
        if rt.get("with"):
            retarget_rows.append({**rt, "to": to, "with": rt["with"]})
            continue
        find = rt["find"]
        tdir = rt["test_file"].rsplit("/", 1)[0]
        if "include_str!" in find:
            parts = [f'include_str!("{posixpath.relpath(p, tdir)}")' for p in to]
            with_ = parts[0] if len(parts) == 1 else f"concat!({', '.join(parts)})"
        elif find.startswith("fs::read_to_string(") or find.startswith("std::fs::read_to_string("):
            if len(to) == 1:
                with_ = find.replace("src/lib.rs", to[0])
            else:
                exprs = " + &".join(
                    f'fs::read_to_string(format!("{{manifest}}/{p}")).unwrap()' for p in to)
                with_ = f"Ok::<_, std::io::Error>({exprs})"
        elif "read_text(" in find:
            if len(to) == 1:
                with_ = find.replace("src/lib.rs", to[0])
            else:
                with_ = " + &".join(f'read_text(&root.join("{p}"))' for p in to)
        else:
            if len(to) != 1:
                raise SystemExit(f"retarget {rt['test']}: multi-file `with` unsupported for {find}")
            with_ = find.replace("src/lib.rs", to[0])
        retarget_rows.append({**rt, "to": to, "with": with_})

    # Emit.
    lines = [
        "# Generated by manifest_build.py — do not edit by hand.",
        "# Regenerate: python3 scripts/refactor/lib_split/manifest_build.py",
        "",
        "[meta]",
        'source = "src/lib.rs"',
        f"source_lines = {len(starts)}",
        'generated_against = "ac28de473dc87ac60d4f5e6d7794a9c4504bfbe4"',
        "# Stages that exist in the plan but are manual structural work,",
        "# not move-only applies (plan §8): run_doctor_impl decomposition",
        "# (5) and final lib.rs shim/cleanup (6).",
        'manual_stages = [ "5", "6" ]',
        "",
    ]
    for gname, members in GROUPS:
        lines += ["[[group]]", f'name = "{gname}"', "items = [ " +
                  ", ".join(f'{{ kind = "{k}", name = "{n.split("#")[0]}"' +
                            (f', ordinal = {n.split("#")[1]}' if "#" in n else "") + " }"
                            for k, n in members) + " ]", ""]
    for mod, parts in MOD_SPLITS.items():
        lines += ["[[mod_split]]", f'mod = "{mod}"', f"parts = {parts}", ""]
    for r in MOD_RELOCS:
        lines += ["[[mod_reloc]]"]
        for k, v in r.items():
            lines.append(f'{k} = "{v}"')
        lines.append("")
    for rt in retarget_rows:
        lines += ["[[retarget]]"]
        for k in ("test", "test_file", "stage", "find", "with"):
            lines.append(f'{k} = "{rt[k].replace(chr(92), chr(92)*2).replace(chr(34), chr(92)+chr(34))}"')
        lines.append("to = [ " + ", ".join(f'"{p}"' for p in rt["to"]) + " ]")
        if rt.get("markers"):
            marks = ", ".join('"' + m.replace('"', '\\"') + '"' for m in rt["markers"])
            lines.append(f"markers = [ {marks} ]")
        lines.append("")
    for e in entries:
        lines.append("[[file]]")
        lines.append(f'target = "{e["target"]}"')
        lines.append(f'stage = "{e["stage"]}"')
        if e.get("inner_first"):
            lines.append(f'slice = {{ mod = "{e["mod"]}", '
                         f'first = {ent(e["inner_first"])}, '
                         f'last = {ent(e["inner_last"])}, '
                         f'part = {e["mod_part"]}, parts = {e["mod_parts"]} }}')
        elif e.get("mod"):
            lines.append(f'mod = "{e["mod"]}"')
        else:
            lines.append(f"first = {ent(e['first'])}")
            lines.append(f"last = {ent(e['last'])}")
            if e.get("also"):
                lines.append("also = [ " + ", ".join(ent(x) for x in e["also"]) + " ]")
        lines.append("")
    OUT.write_text("\n".join(lines))
    files = [e for e in entries if e["stage"] != "stay"]
    print(f"wrote {OUT}: {len(entries)} file entries ({len(files)} moved)")
    print(f"coverage: {len(items)} items, 0 uncovered")


if __name__ == "__main__":
    build()
