#!/usr/bin/env python3
"""Synthetic-archive A/B bench for cass indexing and strict viewer search.

Generates a deterministic, fully synthetic agent-session archive (Claude Code
+ Codex JSONL; no real user data) under a scratch ``HOME``, then measures two
PREBUILT cass builds A and B side by side in ONE invocation, interleaved
(A B A B ...), on:

  1. ``cass index --full`` wall time + peak RSS on a fresh data dir,
  2. incremental ``cass index`` after appending a small batch of new sessions,
  3. strict viewer search latency through ``cass-viewer-bridge
     search_sessions`` for a fixed query set (common term, rare term, phrase,
     NOT, tool: filter), reporting per-query median and p90 plus result
     counts. Counts must match between A and B or the report flags it.

Isolation: every child runs with a scratch ``HOME`` (the synthetic archive),
a scratch ``CASS_DATA_DIR``/``--data-dir``, scratch ``XDG_*`` and ``TMPDIR``,
and ``CASS_AUTO_REFRESH=0``. The child environment is constructed from
scratch, not inherited, so no real archive or store is ever read and no
stray ``CASS_*``/``CLAUDE_*``/``CODEX_*`` variables leak in.

The bench never builds anything: it takes the two builds' directories (each
containing ``cass`` and ``cass-viewer-bridge``) as arguments.

Python 3 stdlib only. See scripts/bench/README.md for usage.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import queue
import random
import re
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

GENERATOR_VERSION = 1
RUNNER_VERSION = 1
DEFAULT_MESSAGES_TARGET = 300_000
# Fixed past wall-clock mtime for the initial archive batch. File mtimes are
# what the incremental scan filters on, so the initial batch must predate the
# full-index run and the incremental batch must land after it; the initial
# batch is pinned to this fixed epoch and the incremental batch is stamped
# `now - 10 s` by the runner (comfortably past every full scan watermark and
# far from the 1 s file_modified_since grace boundary).
V1_MTIME_EPOCH = datetime(2026, 11, 10, 12, 0, 0, tzinfo=timezone.utc).timestamp()
# Session/message instants live in a fixed window so runs are reproducible.
V1_TIME_BASE = datetime(2026, 10, 1, 8, 0, 0, tzinfo=timezone.utc)

# ---------------------------------------------------------------------------
# Controlled vocabulary. Planted tokens are ONLY ever emitted by the planting
# rules below, never by the filler pool, so per-query session counts are
# provable from the generator's own bookkeeping.
# ---------------------------------------------------------------------------
COMMON_TERM = "quokka"  # planted in ~80% of sessions
RARE_TERM = "axolotl"  # planted in ~2% of sessions
PHRASE = "quantum walrus"  # planted verbatim (adjacent tokens) in ~5%
TOOL_TARGET = "Bash"  # planted as a Claude Code tool_use in ~70% of claude sessions

FILLER_WORDS = [
    "adapter", "allocator", "backoff", "batching", "benchmark", "binding",
    "boundary", "buffer", "builder", "cacheline", "calendar", "canonical",
    "cardinality", "checkpoint", "circuit", "collector", "compile", "cursor",
    "daemon", "deadline", "decoder", "detector", "dispatch", "downstream",
    "encoder", "endpoint", "envelope", "epoch", "estimator", "fallback",
    "fanout", "feature", "fingerprint", "fixture", "flatten", "gauge",
    "gradient", "granularity", "handler", "harness", "hashtable", "heuristic",
    "hologram", "horizon", "incremental", "ingest", "journal", "kernel",
    "lattice", "ledger", "lockstep", "manifold", "manifest", "matrix",
    "merkle", "middleware", "millisecond", "monolith", "mutex", "neuron",
    "nomad", "orphan", "paginated", "parallel", "partition", "patchwork",
    "pedigree", "pipeline", "pivot", "polyfill", "predicate", "prefetch",
    "primer", "prologue", "quantile", "radix", "ratchet", "readout",
    "reconcile", "reducer", "refinery", "relay", "replica", "reservoir",
    "retries", "rollout", "router", "runtime", "saga", "sampled", "scalar",
    "scatter", "schema", "shard", "sharding", "signal", "sketch", "snapshot",
    "socket", "spindle", "stack", "staging", "stepper", "stream", "subtree",
    "summary", "supervisor", "sweep", "tamper", "telemetry", "throughput",
    "timeout", "tolerance", "topology", "tracker", "trailing", "transient",
    "trigger", "tunnel", "upstream", "vector", "vertigo", "viewport",
    "walkway", "wavefront", "windowed", "wire", "workload", "wrapper",
    "zephyr", "zigzag",
]
for _banned in (COMMON_TERM, RARE_TERM, *PHRASE.split()):
    assert _banned not in FILLER_WORDS, f"filler pool collides with planted token {_banned!r}"

SENTENCE_TEMPLATES = [
    "the {a} {b} path needs a second look before the release",
    "we should refactor the {a} handler so the {b} stays deterministic",
    "the {a} test fails on the {b} boundary, probably a stale {c}",
    "run the {a} suite and paste the {b} output here",
    "this {a} change ripples into the {b} module, add a regression test",
    "the {a} ledger reports a {b} mismatch after the nightly {c}",
    "gate the {a} rollout on a green {b} and a clean {c} run",
    "the {a} worker stalls whenever the {b} queue drains below {c} depth",
    "keep the {a} encoding in sync with the {b} contract",
    "document why the {a} step runs before the {b} commit",
    "the {a} retry budget expires before the {b} handshake finishes",
    "split the {a} fixture so the {b} lane stops shadowing the {c} case",
]

TOOL_RESULT_LINES = [
    "ok: {a} applied in {b}",
    "checked {a} entries, {b} skipped",
    "warning: {a} drifted by {b} ms",
    "compiling {a} with {b} threads",
    "queued {a} behind the {b} gate",
    "snapshot {a} at revision {b}",
    "no {a} found in the {b} index",
]

CLAUDE_TOOLS = ["Read", "Edit", "Grep", "Bash"]
CODEX_TOOLS = ["shell", "apply_patch"]

BENCH_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "cass-ab-bench/v1")


# ---------------------------------------------------------------------------
# Small utilities
# ---------------------------------------------------------------------------

def log(message: str) -> None:
    print(message, flush=True)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def median(values):
    return statistics.median(values) if values else None


def percentile(values, fraction):
    """Nearest-rank percentile: the ceil(fraction*n)-th smallest value."""
    if not values:
        return None
    ordered = sorted(values)
    scaled = len(ordered) * fraction
    rank = int(scaled)
    if scaled > rank:
        rank += 1
    rank = min(max(rank, 1), len(ordered))
    return ordered[rank - 1]


def iso_ms(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


def compact_json(record) -> str:
    return json.dumps(record, ensure_ascii=False, separators=(",", ":"))


def iter_long_strings(node, minimum: int):
    """Yield string leaves of a parsed JSON value at least `minimum` long."""
    if isinstance(node, str):
        if len(node) >= minimum:
            yield node
    elif isinstance(node, dict):
        for value in node.values():
            yield from iter_long_strings(value, minimum)
    elif isinstance(node, list):
        for value in node:
            yield from iter_long_strings(value, minimum)


# ---------------------------------------------------------------------------
# Peak-RSS measurement via /usr/bin/time (macOS `-l`, GNU `-v`); None when the
# tool is missing (the report then says "n/a" instead of inventing a number).
# ---------------------------------------------------------------------------
TIME_BIN = "/usr/bin/time"


def time_wrapper_args():
    if Path(TIME_BIN).exists():
        if platform.system() == "Darwin":
            return [TIME_BIN, "-l"]
        return [TIME_BIN, "-v"]
    return None


def parse_peak_rss(stderr_text):
    """Return peak RSS in bytes, or None when /usr/bin/time did not report it."""
    if not stderr_text:
        return None
    match = re.search(r"(\d+)\s+maximum resident set size", stderr_text)
    if match:  # macOS reports bytes
        return int(match.group(1))
    match = re.search(r"Maximum resident set size \(kbytes\):\s*(\d+)", stderr_text)
    if match:  # GNU time reports KiB
        return int(match.group(1)) * 1024
    return None


# ---------------------------------------------------------------------------
# Synthetic archive generation
# ---------------------------------------------------------------------------

def fill_sentence(rng: random.Random) -> str:
    words = [rng.choice(FILLER_WORDS) for _ in range(3)]
    return rng.choice(SENTENCE_TEMPLATES).format(a=words[0], b=words[1], c=words[2])


def fill_paragraph(rng: random.Random, sentences: int) -> str:
    body = [fill_sentence(rng) for _ in range(sentences)]
    return ". ".join(body).capitalize() + "."


def file_path_token(rng: random.Random, project: str) -> str:
    word = rng.choice(FILLER_WORDS)
    return f"{project}/src/{word}_{rng.randint(1, 999)}.rs"


def big_text(rng: random.Random) -> str:
    """A >64K-char assistant text so region chunking matters."""
    target_bytes = rng.randint(65_000, 130_000)
    parts = []
    while sum(len(part) + 2 for part in parts) < target_bytes:
        parts.append(fill_paragraph(rng, rng.randint(3, 8)))
    return "\n\n".join(parts)


class SessionBookkeeping:
    def __init__(self):
        self.total_sessions = 0
        self.claude_sessions = 0
        self.codex_sessions = 0
        self.messages = 0
        self.common_sessions = 0
        self.rare_sessions = 0
        self.phrase_sessions = 0
        self.tool_target_sessions = 0
        self.big_message_sessions = 0

    def merged(self, other: "SessionBookkeeping") -> "SessionBookkeeping":
        out = SessionBookkeeping()
        for field in out.__dict__:
            setattr(out, field, getattr(self, field) + getattr(other, field))
        return out

    def as_dict(self) -> dict:
        return dict(self.__dict__)


def session_size_messages(rng: random.Random) -> int:
    roll = rng.random()
    if roll < 0.15:
        return rng.randint(6, 12)
    if roll < 0.60:
        return rng.randint(20, 38)
    if roll < 0.90:
        return rng.randint(55, 90)
    return rng.randint(120, 180)


def allocate_slots(rng: random.Random, slot_count: int, n_tools: int,
                   wants_big: bool, planted_tokens) -> dict:
    """Disjointly allocate plan slots to tool turns, the big text and planted
    tokens so a planted token can never be silently swallowed by a tool turn.

    Returns {"tools": {slot: tool}, "big": slot|None, "planted": {token: slot}}.
    """
    slots = list(range(slot_count))
    rng.shuffle(slots)
    allocation = {"tools": {}, "big": None, "planted": {}}
    for turn in range(min(n_tools, slot_count)):
        allocation["tools"][slots.pop()] = None  # tool name assigned by caller
    # Keep at least one free slot for ordinary text.
    if wants_big and slots:
        allocation["big"] = slots.pop(0)
    for token in planted_tokens:
        if slots:
            allocation["planted"][token] = slots.pop(0)
    return allocation


def session_spec(index: int, rng: random.Random, big_every: int) -> dict:
    is_claude = (index % 10) < 6  # 60% Claude Code, 40% Codex
    size = session_size_messages(rng)
    return {
        "format": "claude" if is_claude else "codex",
        "workspace": f"/synthws/proj-{index % 24:02d}",
        "start": V1_TIME_BASE + timedelta(minutes=13 * index),
        "size": size,
        "common": (index % 5) != 4,        # ~80% of sessions
        "rare": (index % 50) == 3,        # ~2%
        "phrase": (index % 20) == 5,       # ~5%
        "tool_target": is_claude and ((index // 10) % 10) < 7,  # ~70% of claude
        "big": bool(big_every) and index % big_every == 0 and size >= 8,
    }


def claude_workspace_slug(workspace: str) -> str:
    # Claude Code derives the projects/<slug> dir from the cwd path.
    return "-" + workspace.strip("/").replace("/", "-")


def generate_claude_session(path: Path, index: int, rng: random.Random,
                            book: SessionBookkeeping, spec: dict) -> None:
    sid = str(uuid.uuid5(BENCH_NAMESPACE, f"claude/{index}"))
    workspace = spec["workspace"]
    moment = spec["start"]
    parent = None

    def base_record(record_type: str) -> dict:
        nonlocal moment, parent
        moment = moment + timedelta(seconds=rng.randint(4, 75))
        record_uuid = str(uuid.uuid5(BENCH_NAMESPACE, f"claude/{index}/{book.messages}"))
        record = {
            "type": record_type,
            "uuid": record_uuid,
            "parentUuid": parent,
            "sessionId": sid,
            "timestamp": iso_ms(moment),
            "cwd": workspace,
            "gitBranch": "main",
        }
        parent = record_uuid
        return record

    def user_text(text: str) -> dict:
        record = base_record("user")
        record["message"] = {"role": "user", "content": text}
        return record

    def assistant_blocks(blocks) -> dict:
        record = base_record("assistant")
        record["message"] = {"role": "assistant", "model": "claude-opus-5-5", "content": blocks}
        return record

    def assistant_text(text: str) -> dict:
        return assistant_blocks([{"type": "text", "text": text}])

    plan_size = max(1, spec["size"] - 1)
    n_tools = min(max(1, plan_size // 3), max(1, plan_size // 2))
    planted = [token for token in ("common", "rare", "phrase") if spec[token]]
    allocation = allocate_slots(rng, plan_size, n_tools, spec["big"], planted)

    tool_pool = [name for name in CLAUDE_TOOLS if spec["tool_target"] or name != TOOL_TARGET]
    for slot in allocation["tools"]:
        allocation["tools"][slot] = rng.choice(tool_pool)
    if spec["tool_target"]:
        # Guarantee at least one Bash call in every tool-target session.
        first = sorted(allocation["tools"])[0]
        allocation["tools"][first] = TOOL_TARGET

    lines = []
    lines.append(compact_json({
        "type": "ai-title",
        "sessionId": sid,
        "timestamp": iso_ms(spec["start"]),
        "aiTitle": fill_sentence(rng).capitalize(),
    }))
    # First user prompt (projected task description).
    lines.append(compact_json(user_text(
        f"working in {workspace}: {fill_sentence(rng)} "
        f"focus on the {rng.choice(FILLER_WORDS)} story and keep the diff small."
    )))
    book.messages += 1

    planted_text = {
        "common": f" The {COMMON_TERM} invariant must hold here.",
        "rare": f" Trace the {RARE_TERM} branch as well.",
        "phrase": f" This is the {PHRASE} scenario we discussed.",
    }

    for slot in range(plan_size):
        if slot in allocation["tools"]:
            tool = allocation["tools"][slot]
            call_id = "toolu_" + uuid.uuid5(BENCH_NAMESPACE, f"claude-call/{index}/{slot}").hex[:20]
            if tool == "Bash":
                tool_input = {
                    "command": f"cargo test {rng.choice(FILLER_WORDS)} -- --nocapture",
                    "description": "run the targeted suite",
                }
            elif tool == "Read":
                tool_input = {"file_path": "/" + file_path_token(rng, workspace)}
            elif tool == "Edit":
                tool_input = {
                    "file_path": "/" + file_path_token(rng, workspace),
                    "old_string": fill_sentence(rng),
                    "new_string": fill_sentence(rng),
                }
            else:
                tool_input = {"pattern": rng.choice(FILLER_WORDS), "path": "/" + workspace}
            lines.append(compact_json(assistant_blocks([{
                "type": "tool_use", "id": call_id, "name": tool, "input": tool_input,
            }])))
            book.messages += 1
            is_error = rng.random() < 0.15
            if is_error:
                body = "error: " + fill_sentence(rng) + f" (exit code {rng.randint(1, 9)})"
            else:
                body = "\n".join(
                    rng.choice(TOOL_RESULT_LINES).format(
                        a=rng.choice(FILLER_WORDS), b=rng.randint(1, 9999)
                    )
                    for _ in range(rng.randint(3, 25))
                )
            result = base_record("user")
            result["message"] = {"role": "user", "content": [{
                "type": "tool_result", "tool_use_id": call_id,
                "content": body, "is_error": is_error,
            }]}
            lines.append(compact_json(result))
            book.messages += 1
            continue

        if slot == allocation["big"]:
            lines.append(compact_json(assistant_text(big_text(rng))))
            book.messages += 1
            book.big_message_sessions += 1
            continue

        text = fill_paragraph(rng, rng.randint(2, 9))
        for token, message_slot in allocation["planted"].items():
            if message_slot == slot:
                text += planted_text[token]
        if rng.random() < 0.45:
            lines.append(compact_json(user_text(text)))
        else:
            lines.append(compact_json(assistant_text(text)))
        book.messages += 1

    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as handle:
        handle.write("\n".join(lines) + "\n")
    book.claude_sessions += 1
    book.total_sessions += 1
    for token, field in (("common", "common_sessions"), ("rare", "rare_sessions"),
                         ("phrase", "phrase_sessions"),
                         ("tool_target", "tool_target_sessions")):
        if spec[token]:
            setattr(book, field, getattr(book, field) + 1)


def generate_codex_session(path: Path, index: int, rng: random.Random,
                           book: SessionBookkeeping, spec: dict) -> None:
    sid = str(uuid.uuid5(BENCH_NAMESPACE, f"codex/{index}"))
    workspace = spec["workspace"]
    moment = spec["start"]

    def stamp() -> str:
        nonlocal moment
        moment = moment + timedelta(seconds=rng.randint(4, 75))
        return iso_ms(moment)

    lines = []
    lines.append(compact_json({
        "timestamp": stamp(),
        "type": "session_meta",
        "payload": {"id": sid, "cwd": workspace},
    }))

    def message(role: str, block_type: str, text: str):
        lines.append(compact_json({
            "timestamp": stamp(),
            "type": "response_item",
            "payload": {
                "type": "message", "role": role,
                "content": [{"type": block_type, "text": text}],
            },
        }))
        book.messages += 1

    message(
        "user", "input_text",
        f"in {workspace}: {fill_sentence(rng)} "
        f"start with the {rng.choice(FILLER_WORDS)} module.",
    )

    plan_size = max(2, spec["size"]) - 1
    n_tools = min(max(1, plan_size // 4), max(1, plan_size // 2))
    planted = [token for token in ("common", "rare", "phrase") if spec[token]]
    allocation = allocate_slots(rng, plan_size, n_tools, spec["big"], planted)
    for slot in allocation["tools"]:
        allocation["tools"][slot] = rng.choice(CODEX_TOOLS)

    planted_text = {
        "common": f" The {COMMON_TERM} invariant must hold here.",
        "rare": f" Trace the {RARE_TERM} branch as well.",
        "phrase": f" This is the {PHRASE} scenario we discussed.",
    }

    for slot in range(plan_size):
        if slot in allocation["tools"]:
            tool = allocation["tools"][slot]
            call_id = "call_" + uuid.uuid5(BENCH_NAMESPACE, f"codex-call/{index}/{slot}").hex[:20]
            if tool == "shell":
                arguments = {"command": ["cargo", "test", rng.choice(FILLER_WORDS)]}
            else:
                arguments = {"patch": f"*** Update File: {file_path_token(rng, workspace)}"}
            lines.append(compact_json({
                "timestamp": stamp(),
                "type": "response_item",
                "payload": {
                    "type": "function_call", "name": tool,
                    "call_id": call_id, "arguments": compact_json(arguments),
                },
            }))
            book.messages += 1
            is_error = rng.random() < 0.15
            if is_error:
                output = "error: " + fill_sentence(rng) + f" (exit {rng.randint(1, 9)})"
            else:
                output = "\n".join(
                    rng.choice(TOOL_RESULT_LINES).format(
                        a=rng.choice(FILLER_WORDS), b=rng.randint(1, 9999)
                    )
                    for _ in range(rng.randint(3, 20))
                )
            lines.append(compact_json({
                "timestamp": stamp(),
                "type": "response_item",
                "payload": {"type": "function_call_output", "call_id": call_id, "output": output},
            }))
            book.messages += 1
            continue

        if slot == allocation["big"]:
            message("assistant", "output_text", big_text(rng))
            book.big_message_sessions += 1
            continue

        text = fill_paragraph(rng, rng.randint(2, 9))
        for token, message_slot in allocation["planted"].items():
            if message_slot == slot:
                text += planted_text[token]
        if rng.random() < 0.45:
            message("user", "input_text", text)
        else:
            message("assistant", "output_text", text)

    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as handle:
        handle.write("\n".join(lines) + "\n")
    book.codex_sessions += 1
    book.total_sessions += 1
    for token, field in (("common", "common_sessions"), ("rare", "rare_sessions"),
                         ("phrase", "phrase_sessions")):
        if spec[token]:
            setattr(book, field, getattr(book, field) + 1)


def generate_archive(archive_home: Path, *, seed: int, target_messages: int,
                     start_index: int, session_budget, big_every: int,
                     pin_mtime) -> "tuple[SessionBookkeeping, list[Path]]":
    """Write synthetic sessions into `archive_home` (the scratch $HOME).

    Deterministic given (seed, target_messages, start_index, session_budget,
    big_every). Returns the bookkeeping and the list of files written.
    """
    book = SessionBookkeeping()
    written: list[Path] = []
    index = start_index
    while True:
        if session_budget is not None and book.total_sessions >= session_budget:
            break
        if session_budget is None and book.messages >= target_messages:
            break
        if index - start_index > 200_000:  # unreachable safety valve
            break
        rng = random.Random(f"cass-ab-bench/{seed}/{index}")
        spec = session_spec(index, rng, big_every)
        if spec["format"] == "claude":
            sid = str(uuid.uuid5(BENCH_NAMESPACE, f"claude/{index}"))
            path = (archive_home / ".claude" / "projects"
                    / claude_workspace_slug(spec["workspace"]) / f"{sid}.jsonl")
            generate_claude_session(path, index, rng, book, spec)
        else:
            sid = str(uuid.uuid5(BENCH_NAMESPACE, f"codex/{index}"))
            date_dir = spec["start"].strftime("%Y/%m/%d")
            stamp = spec["start"].strftime("%Y%m%dT%H%M%S")
            path = (archive_home / ".codex" / "sessions" / date_dir
                    / f"rollout-{stamp}-{sid}.jsonl")
            generate_codex_session(path, index, rng, book, spec)
        written.append(path)
        index += 1
    if pin_mtime is not None:
        for path in written:
            os.utime(path, (pin_mtime, pin_mtime))
    return book, written


def bookkeeping_from_dict(raw: dict) -> SessionBookkeeping:
    book = SessionBookkeeping()
    for field in book.__dict__:
        setattr(book, field, raw[field])
    return book


def write_manifest(path: Path, *, seed: int, target_messages: int, big_every: int,
                   v1: SessionBookkeeping, incremental) -> dict:
    total = v1.merged(incremental) if incremental else v1
    manifest = {
        "generator_version": GENERATOR_VERSION,
        "seed": seed,
        "target_messages": target_messages,
        "big_every": big_every,
        "v1_mtime_epoch": V1_MTIME_EPOCH,
        "v1": v1.as_dict(),
        "incremental": incremental.as_dict() if incremental else None,
        "expected": {
            "total_sessions": total.total_sessions,
            "common": total.common_sessions,
            "rare": total.rare_sessions,
            "phrase": total.phrase_sessions,
            "not_common": total.total_sessions - total.common_sessions,
            "tool_bash": total.tool_target_sessions,
        },
        "vocabulary": {
            "common_term": COMMON_TERM,
            "rare_term": RARE_TERM,
            "phrase": PHRASE,
            "tool_target": TOOL_TARGET,
        },
    }
    path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return manifest


def load_manifest(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# Query set (viewer-bridge `search_sessions` wire ASTs; strict lane, no
# allow_partial). Shapes pinned by tests/viewer_bridge_search_sessions.rs and
# the reference probe.
# ---------------------------------------------------------------------------

def build_queries(manifest: dict):
    vocab = manifest["vocabulary"]

    def term(value, exact=False):
        return {"type": "term", "value": value, "exact": exact}

    return [
        ("common", term(vocab["common_term"])),
        ("rare", term(vocab["rare_term"])),
        ("phrase", term(vocab["phrase"], True)),
        ("not_common", {"type": "not", "expr": term(vocab["common_term"])}),
        ("tool_bash", {"type": "filter", "field": "tool", "op": "EQ",
                       "value": vocab["tool_target"]}),
    ]


# ---------------------------------------------------------------------------
# Child-process plumbing
# ---------------------------------------------------------------------------

class Build:
    def __init__(self, label: str, cass: Path, bridge: Path):
        self.label = label
        self.cass = cass
        self.bridge = bridge
        self.cass_sha256 = sha256_file(cass)
        self.bridge_sha256 = sha256_file(bridge)

    def as_dict(self) -> dict:
        return {
            "label": self.label,
            "cass": str(self.cass),
            "cass_sha256": self.cass_sha256,
            "bridge": str(self.bridge),
            "bridge_sha256": self.bridge_sha256,
        }


def resolve_build(label: str, directory, cass_override, bridge_override) -> Build:
    cass = Path(cass_override) if cass_override else (Path(directory) / "cass" if directory else None)
    bridge = (Path(bridge_override) if bridge_override
              else (Path(directory) / "cass-viewer-bridge" if directory else None))
    if cass is None or bridge is None:
        raise SystemExit(f"build {label}: pass --{label.lower()}-dir (with cass and "
                         f"cass-viewer-bridge inside) or both --{label.lower()}-cass "
                         f"and --{label.lower()}-bridge")
    for binary in (cass, bridge):
        if not binary.is_file():
            raise SystemExit(f"build {label}: missing binary {binary}")
        if not os.access(binary, os.X_OK):
            raise SystemExit(f"build {label}: binary not executable {binary}")
    return Build(label, cass, bridge)


class ScratchEnv:
    """Hermetic child environment: scratch HOME/XDG/TMPDIR + CASS knobs.

    Built from scratch — never inherits the ambient environment — so the
    real user's home, archives and stores cannot be reached and no stray
    CASS_*/CLAUDE_*/CODEX_* variables can redirect discovery.
    """

    def __init__(self, home: Path, data_dir: Path, work: Path, raw_mirror: bool):
        self.home = home
        self.data_dir = data_dir
        for extra in ("tmp", "xdg-data", "xdg-config"):
            (work / extra).mkdir(parents=True, exist_ok=True)
        self.env = {
            "PATH": "/usr/bin:/bin:/usr/sbin:/sbin:/usr/local/bin:/opt/homebrew/bin",
            "HOME": str(home),
            "TMPDIR": str(work / "tmp"),
            "USER": os.environ.get("USER", "cass-ab-bench"),
            "CASS_DATA_DIR": str(data_dir),
            "CASS_AUTO_REFRESH": "0",
            "XDG_DATA_HOME": str(work / "xdg-data"),
            "XDG_CONFIG_HOME": str(work / "xdg-config"),
        }
        if not raw_mirror:
            self.env["CASS_RAW_MIRROR"] = "0"

    def with_data_dir(self, data_dir: Path) -> "ScratchEnv":
        clone = ScratchEnv.__new__(ScratchEnv)
        clone.home = self.home
        clone.data_dir = data_dir
        clone.env = dict(self.env)
        clone.env["CASS_DATA_DIR"] = str(data_dir)
        return clone


def parse_last_json_object(text: str):
    """Return the index result object from `cass index --json` stdout.

    The payload may be pretty-printed (multi-line) or compact; either way we
    scan every `{` position, decode what starts there with raw_decode, and
    keep the outermost decoded dict that carries the payload's `success`
    key — nested look-alikes inside the payload lose to the outer object
    because the outer `{` is scanned first.
    """
    if not text:
        return None
    decoder = json.JSONDecoder()
    payload = None
    fallback = None
    for match in re.finditer(r"\{", text):
        try:
            value, _end = decoder.raw_decode(text, match.start())
        except json.JSONDecodeError:
            continue
        if not isinstance(value, dict):
            continue
        if fallback is None:
            fallback = value
        if "success" in value:
            payload = value
            break
    return payload if payload is not None else fallback


def run_index(build: Build, env: ScratchEnv, *, full: bool, args, log_dir: Path,
              tag: str) -> dict:
    data_dir = env.data_dir
    data_dir.mkdir(parents=True, exist_ok=True)
    command = ["index", "--json", "--no-progress-events", "--data-dir", str(data_dir)]
    if full:
        command.insert(1, "--full")
    wrapper = time_wrapper_args()
    argv = (wrapper + [str(build.cass)] + command) if wrapper else ([str(build.cass)] + command)
    started = time.monotonic()
    try:
        completed = subprocess.run(
            argv, env=env.env, cwd=str(env.data_dir.parent), capture_output=True,
            text=True, encoding="utf-8", errors="replace", timeout=args.index_timeout,
        )
        wall_s = time.monotonic() - started
        returncode = completed.returncode
        stdout, stderr = completed.stdout, completed.stderr
    except subprocess.TimeoutExpired:
        wall_s = time.monotonic() - started
        returncode = -1
        stdout, stderr = "", f"index timeout after {args.index_timeout}s"
    payload = parse_last_json_object(stdout) or {}
    log_path = log_dir / f"index-{build.label}-{tag}.log"
    log_path.write_text(
        f"$ {' '.join(argv)}\nexit={returncode}\n--- stdout ---\n{stdout[-20_000:]}\n"
        f"--- stderr tail ---\n{stderr[-20_000:]}\n",
        encoding="utf-8",
    )
    return {
        "build": build.label,
        "phase": "full" if full else "incremental",
        "tag": tag,
        "ok": returncode == 0 and payload.get("success") is True,
        "exit_code": returncode,
        "wall_s": round(wall_s, 3),
        "cass_elapsed_ms": payload.get("elapsed_ms"),
        "peak_rss_bytes": parse_peak_rss(stderr),
        "conversations": payload.get("conversations"),
        "messages": payload.get("messages"),
        "quarantined_conversations": payload.get("quarantined_conversations"),
        "log": str(log_path),
    }


class BridgeBatch:
    """One `cass-viewer-bridge --stdio` process serving one query batch."""

    def __init__(self, build: Build, env: ScratchEnv, log_dir: Path, tag: str):
        self.build = build
        self.tag = tag
        db_path = env.data_dir / "agent_search.db"
        if not db_path.is_file():
            raise SystemExit(f"bridge batch {tag}: missing db {db_path}")
        self.log_path = log_dir / f"bridge-{build.label}-{tag}.log"
        self._log_handle = open(self.log_path, "wb")
        argv = [str(build.bridge), "--stdio",
                "--data-dir", str(env.data_dir), "--db", str(db_path)]
        wrapper = time_wrapper_args()
        if wrapper:
            argv = wrapper + argv
        self.proc = subprocess.Popen(
            argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=self._log_handle, env=env.env, cwd=str(env.data_dir.parent),
        )
        self._lines = queue.Queue()
        self._eof = object()
        self._reader = threading.Thread(target=self._pump, daemon=True)
        self._reader.start()
        self._next_id = 1
        self.peak_rss_bytes = None

    def _pump(self):
        assert self.proc.stdout is not None
        for raw in self.proc.stdout:
            self._lines.put(raw)
        self._lines.put(self._eof)

    def exchange(self, request_body: dict, timeout_s: float) -> dict:
        request_id = self._next_id
        self._next_id += 1
        request = {"id": request_id}
        request.update(request_body)
        assert self.proc.stdin is not None
        self.proc.stdin.write((compact_json(request) + "\n").encode("utf-8"))
        self.proc.stdin.flush()
        deadline = time.monotonic() + timeout_s
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    f"bridge {self.tag}: no reply for id={request_id} in {timeout_s}s")
            try:
                raw = self._lines.get(timeout=remaining)
            except queue.Empty:
                raise TimeoutError(
                    f"bridge {self.tag}: no reply for id={request_id} in {timeout_s}s")
            if raw is self._eof:
                raise EOFError(f"bridge {self.tag}: exited before replying to id={request_id}")
            try:
                reply = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if reply.get("id") == request_id:
                return reply

    def close(self):
        try:
            if self.proc.stdin and not self.proc.stdin.closed:
                self.proc.stdin.close()
        except BrokenPipeError:
            pass
        try:
            self.proc.wait(timeout=60)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=10)
        self._reader.join(timeout=5)
        self._log_handle.close()
        self.peak_rss_bytes = parse_peak_rss(
            self.log_path.read_text(encoding="utf-8", errors="replace"))


def sample_from_reply(query_name: str, reply: dict) -> dict:
    result = reply.get("result") or {}
    error = reply.get("error") or {}
    items = result.get("items")
    return {
        "query": query_name,
        "ok": reply.get("ok") is True,
        "error_kind": error.get("kind"),
        "latency_s": None,
        "total_sessions": result.get("total_sessions"),
        "evaluated_sessions": result.get("evaluated_sessions"),
        "complete": result.get("complete"),
        "items": len(items) if isinstance(items, list) else None,
        "membership_source": result.get("membership_source"),
        "region_rows_loaded": (result.get("diagnostics") or {}).get("region_rows_loaded"),
    }


def run_search_round(build: Build, env: ScratchEnv, queries, *, round_index: int,
                     repeats: int, warmup: bool, args, log_dir: Path) -> dict:
    tag = f"r{round_index}{'-warmup' if warmup else ''}"
    batch = BridgeBatch(build, env, log_dir, tag)
    samples = []
    try:
        # One untimed warm-up exchange per batch: warms this build's mmap and
        # page cache before timing; identical treatment for both builds.
        batch.exchange({"op": "search_sessions", "ast": queries[0][1], "limit": 50},
                       timeout_s=args.search_timeout)
        for _repeat in range(1 if warmup else repeats):
            for name, ast in queries:
                started = time.monotonic()
                reply = batch.exchange(
                    {"op": "search_sessions", "ast": ast, "limit": 50},
                    timeout_s=args.search_timeout,
                )
                latency = time.monotonic() - started
                sample = sample_from_reply(name, reply)
                sample["latency_s"] = round(latency, 4)
                sample["build"] = build.label
                sample["round"] = round_index
                sample["warmup"] = warmup
                samples.append(sample)
    finally:
        batch.close()
    return {
        "build": build.label,
        "round": round_index,
        "warmup": warmup,
        "samples": samples,
        "bridge_peak_rss_bytes": batch.peak_rss_bytes,
        "log": str(batch.log_path),
        "error": None,
    }


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def aggregate_index_runs(runs):
    ok_runs = [run for run in runs if run["ok"]]
    if not ok_runs:
        return {"ok": False, "count": 0}
    walls = [run["wall_s"] for run in ok_runs]
    rss = [run["peak_rss_bytes"] for run in ok_runs if run["peak_rss_bytes"]]
    return {
        "ok": True,
        "count": len(ok_runs),
        "wall_s_min": min(walls),
        "wall_s_max": max(walls),
        "wall_s_mean": round(statistics.mean(walls), 3),
        "peak_rss_bytes_max": max(rss) if rss else None,
        "conversations": ok_runs[0]["conversations"],
        "messages": ok_runs[0]["messages"],
    }


def aggregate_search(samples):
    ok = [s for s in samples if s["ok"] and not s["warmup"] and s["latency_s"] is not None]
    latencies = [s["latency_s"] for s in ok]
    complete_flags = {str(s["complete"]) for s in ok}
    totals = {s["total_sessions"] for s in ok}
    return {
        "samples": len(ok),
        "refused": sum(1 for s in samples if not s["ok"] and not s["warmup"]),
        "median_s": median(latencies),
        "p90_s": percentile(latencies, 0.90),
        "mean_s": round(statistics.mean(latencies), 4) if latencies else None,
        "min_s": min(latencies) if latencies else None,
        "max_s": max(latencies) if latencies else None,
        "complete": sorted(complete_flags)[0] if complete_flags else None,
        "total_sessions_values": sorted(t for t in totals if t is not None),
        "evaluated_sessions_values": sorted(
            {s["evaluated_sessions"] for s in ok} - {None}
        ),
    }


def relative_delta(a, b):
    if a is None or b is None:
        return None
    base = max(abs(a), abs(b))
    if base == 0:
        return 0.0
    return round(abs(a - b) / base * 100.0, 2)


QUERY_EXPECTED_KEY = {
    "common": "common", "rare": "rare", "phrase": "phrase",
    "not_common": "not_common", "tool_bash": "tool_bash",
}


def build_report(args, *, build_a: Build, build_b: Build, manifest: dict,
                 archive_home: Path, full_runs, incremental_runs, search_rounds,
                 expected: dict) -> dict:
    index_full = {
        build.label: aggregate_index_runs([r for r in full_runs if r["build"] == build.label])
        for build in (build_a, build_b)
    }
    index_incr = {
        build.label: aggregate_index_runs([r for r in incremental_runs if r["build"] == build.label])
        for build in (build_a, build_b)
    }

    query_names = [name for name, _ast in build_queries(manifest)]
    search = {}
    count_verdicts = {}
    for name in query_names:
        per_build = {}
        for build in (build_a, build_b):
            samples = [
                sample
                for round_data in search_rounds
                for sample in round_data.get("samples", [])
                if sample["build"] == build.label and sample["query"] == name
            ]
            per_build[build.label] = aggregate_search(samples)
        a, b = per_build[build_a.label], per_build[build_b.label]
        # A query either completes or the bridge refuses it (a typed
        # deadline/verification refusal — the strict lane's 10 s ceiling).
        # Parity is what A vs B must hold: both refused, or both completed
        # with identical counts. One refusing where the other completes is a
        # behavioral mismatch. A build that wobbles across rounds (refused
        # some samples, completed others, or two different totals) fails too.
        def lane_state(agg):
            if agg["refused"] > 0 and agg["samples"] > 0:
                return "unstable"
            if agg["refused"] > 0:
                return "refused"
            if agg["samples"] == 0:
                return "no-samples"
            if len(agg["total_sessions_values"]) != 1:
                return "unstable"
            if agg["complete"] != "True":
                return "unstable"
            return "completed"

        state_a, state_b = lane_state(a), lane_state(b)
        if "unstable" in (state_a, state_b) or "no-samples" in (state_a, state_b):
            counts_match = False
        elif state_a != state_b:
            counts_match = False  # refusal parity broken: behavioral mismatch
        elif state_a == "refused":
            counts_match = True  # both refused identically: parity holds
        else:
            counts_match = a["total_sessions_values"] == b["total_sessions_values"]
        expected_total = expected.get(QUERY_EXPECTED_KEY[name])

        def measured(agg):
            return agg["total_sessions_values"][0] if len(agg["total_sessions_values"]) == 1 else None

        measured_a, measured_b = measured(a), measured(b)
        if state_a == "completed" and state_b == "completed":
            expected_match = measured_a == expected_total and measured_b == expected_total
        else:
            expected_match = None  # unmeasurable when the lane did not complete
        count_verdicts[name] = {
            "counts_match": counts_match,
            "lane": {"A": state_a, "B": state_b},
            "expected": expected_total,
            "measured": {"A": measured_a, "B": measured_b},
            "expected_match": expected_match,
            "refused": {"A": a["refused"], "B": b["refused"]},
            "complete": {"A": a["complete"], "B": b["complete"]},
        }
        search[name] = {"A": a, "B": b}

    deltas = {
        "index_full_wall_pct": relative_delta(
            index_full[build_a.label].get("wall_s_mean"),
            index_full[build_b.label].get("wall_s_mean"),
        ),
        "index_incr_wall_pct": relative_delta(
            index_incr[build_a.label].get("wall_s_mean"),
            index_incr[build_b.label].get("wall_s_mean"),
        ),
    }
    for name in query_names:
        a, b = search[name]["A"], search[name]["B"]
        deltas[f"search_{name}_median_pct"] = relative_delta(a["median_s"], b["median_s"])
        deltas[f"search_{name}_p90_pct"] = relative_delta(a["p90_s"], b["p90_s"])

    all_counts_match = all(verdict["counts_match"] for verdict in count_verdicts.values())
    # Run health is about infrastructure: index runs and bridge processes
    # must succeed end to end. Typed per-query refusals (deadline/verification
    # budget) are workload data with their own per-query verdict, not a run
    # failure — unless they break A/B parity, which all_counts_match catches.
    all_runs_ok = (
        all(run["ok"] for run in full_runs)
        and all(run["ok"] for run in incremental_runs)
        and all(not round_data.get("error") for round_data in search_rounds)
        and all(
            sample["latency_s"] is not None
            for round_data in search_rounds
            for sample in round_data.get("samples", [])
            if not sample["warmup"]
        )
    )

    return {
        "schema_version": 1,
        "runner_version": RUNNER_VERSION,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "label": args.label,
        "null_run": args.null_run,
        "platform": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "python": platform.python_version(),
        },
        "builds": {"A": build_a.as_dict(), "B": build_b.as_dict()},
        "binaries_identical": build_a.cass_sha256 == build_b.cass_sha256,
        "args": {
            "seed": args.seed,
            "messages": args.messages,
            "repeats": args.repeats,
            "search_rounds": args.search_rounds,
            "search_repeats": args.search_repeats,
            "search_warmup_rounds": args.search_warmup_rounds,
            "big_every": args.big_every,
            "incremental_sessions": args.incremental_sessions,
            "incremental_fraction": args.incremental_fraction,
            "raw_mirror": args.raw_mirror,
        },
        "archive": {"home": str(archive_home), "manifest": manifest},
        "index_full": {"runs": full_runs, "aggregate": index_full},
        "index_incremental": {"runs": incremental_runs, "aggregate": index_incr},
        "search": {"rounds": search_rounds, "aggregate": search, "counts": count_verdicts},
        "deltas_pct": deltas,
        "verdict": {"all_runs_ok": all_runs_ok, "all_counts_match": all_counts_match},
    }


def fmt_seconds(value):
    return f"{value:.3f}s" if isinstance(value, (int, float)) else "n/a"


def fmt_mib(bytes_value):
    if not bytes_value:
        return "n/a"
    return f"{bytes_value / (1024 * 1024):.1f} MiB"


def render_markdown(report: dict) -> str:
    lines = []
    lines.append(f"# Synthetic A/B bench — {report['label']}")
    lines.append("")
    lines.append(f"- generated: `{report['generated_at']}`")
    lines.append(f"- platform: {report['platform']['system']} {report['platform']['release']} "
                 f"({report['platform']['machine']}), python {report['platform']['python']}")
    lines.append(f"- A: `{report['builds']['A']['cass']}` "
                 f"(sha256 `{report['builds']['A']['cass_sha256'][:16]}…`)")
    lines.append(f"- B: `{report['builds']['B']['cass']}` "
                 f"(sha256 `{report['builds']['B']['cass_sha256'][:16]}…`)")
    lines.append(f"- binaries identical: {report['binaries_identical']}")
    manifest = report["archive"]["manifest"]
    incremental = manifest["incremental"] or {}
    v1 = manifest["v1"]
    lines.append(
        f"- archive: **{manifest['expected']['total_sessions']} sessions / "
        f"{v1['messages'] + incremental.get('messages', 0)} messages** "
        f"(claude {v1['claude_sessions'] + incremental.get('claude_sessions', 0)}, "
        f"codex {v1['codex_sessions'] + incremental.get('codex_sessions', 0)}; "
        f"v1 {v1['total_sessions']} sessions + incremental "
        f"{incremental.get('total_sessions', 0)}); seed {manifest['seed']}"
    )
    lines.append("")

    lines.append("## Index — `cass index --full` (fresh data dir)")
    lines.append("")
    lines.append("| build | ok | wall (mean) | wall (min…max) | peak RSS | conversations | messages |")
    lines.append("|---|---|---|---|---|---|---|")
    for side in ("A", "B"):
        agg = report["index_full"]["aggregate"][side]
        if not agg["ok"]:
            lines.append(f"| {side} | FAILED | — | — | — | — | — |")
            continue
        lines.append(
            f"| {side} | yes | {fmt_seconds(agg['wall_s_mean'])} "
            f"| {fmt_seconds(agg['wall_s_min'])}…{fmt_seconds(agg['wall_s_max'])} "
            f"| {fmt_mib(agg['peak_rss_bytes_max'])} | {agg['conversations']} | {agg['messages']} |"
        )
    lines.append("")

    lines.append("## Index — incremental `cass index` (after the appended batch)")
    lines.append("")
    lines.append("| build | ok | wall (mean) | wall (min…max) | peak RSS | conversations | messages |")
    lines.append("|---|---|---|---|---|---|---|")
    for side in ("A", "B"):
        agg = report["index_incremental"]["aggregate"][side]
        if not agg["ok"]:
            lines.append(f"| {side} | FAILED | — | — | — | — | — |")
            continue
        lines.append(
            f"| {side} | yes | {fmt_seconds(agg['wall_s_mean'])} "
            f"| {fmt_seconds(agg['wall_s_min'])}…{fmt_seconds(agg['wall_s_max'])} "
            f"| {fmt_mib(agg['peak_rss_bytes_max'])} | {agg['conversations']} | {agg['messages']} |"
        )
    lines.append("")

    lines.append("## Strict viewer search — `search_sessions` (per-query latency)")
    lines.append("")
    lines.append("| query | A median | A p90 | B median | B p90 | Δmed | Δp90 | "
                 "total (A/B) | complete (A/B) | verdict |")
    lines.append("|---|---|---|---|---|---|---|---|---|---|")
    for name, data in report["search"]["aggregate"].items():
        a, b = data["A"], data["B"]
        counts = report["search"]["counts"][name]
        totals_a = "/".join(str(v) for v in a["total_sessions_values"]) or "refused"
        totals_b = "/".join(str(v) for v in b["total_sessions_values"]) or "refused"
        delta_median = report["deltas_pct"].get(f"search_{name}_median_pct")
        delta_p90 = report["deltas_pct"].get(f"search_{name}_p90_pct")
        verdict_parts = []
        if not counts["counts_match"]:
            verdict_parts.append(
                f"LANE MISMATCH (A={counts['lane']['A']}, B={counts['lane']['B']})")
        if counts["expected_match"] is False:
            verdict_parts.append(f"expected {counts['expected']} not met")
        if not verdict_parts:
            if counts["lane"]["A"] == "refused":
                verdict_parts.append("refused on both (parity)")
            else:
                verdict_parts.append("OK")
        verdict = ", ".join(verdict_parts)
        lines.append(
            f"| `{name}` | {fmt_seconds(a['median_s'])} | {fmt_seconds(a['p90_s'])} "
            f"| {fmt_seconds(b['median_s'])} | {fmt_seconds(b['p90_s'])} "
            f"| {delta_median}% | {delta_p90}% "
            f"| {totals_a} / {totals_b} | {counts['complete']['A']}/{counts['complete']['B']} "
            f"| {verdict} |"
        )
    lines.append("")

    if report["null_run"]:
        lines.append("## A/A noise floor")
        lines.append("")
        deltas = report["deltas_pct"]
        lines.append("This run compares two builds of the same source, so the A–B deltas "
                     "above ARE the measured noise floor of bench + machine:")
        lines.append("")
        lines.append(f"- `index --full` wall: **{deltas['index_full_wall_pct']}%**")
        lines.append(f"- incremental `index` wall: **{deltas['index_incr_wall_pct']}%**")
        search_deltas = [v for k, v in deltas.items()
                         if k.startswith("search_") and k.endswith("_median_pct") and v is not None]
        p90_deltas = [v for k, v in deltas.items()
                      if k.startswith("search_") and k.endswith("_p90_pct") and v is not None]
        if search_deltas:
            lines.append(f"- search per-query medians: max **{max(search_deltas)}%** "
                         f"(median {statistics.median(search_deltas)}%)")
        if p90_deltas:
            lines.append(f"- search per-query p90: max **{max(p90_deltas)}%**")
        lines.append("")

    lines.append("## Verdict")
    lines.append("")
    verdict = report["verdict"]
    lines.append(f"- all runs ok: **{verdict['all_runs_ok']}**")
    lines.append(f"- all result counts match A vs B: **{verdict['all_counts_match']}**")
    for run in report["index_full"]["runs"] + report["index_incremental"]["runs"]:
        if not run["ok"]:
            lines.append(f"- FAILED index run: {run['build']} {run['phase']} {run['tag']} "
                         f"(exit {run['exit_code']}, log `{run['log']}`)")
    for round_data in report["search"]["rounds"]:
        if round_data.get("error"):
            lines.append(f"- FAILED search round: {round_data['build']} "
                         f"{round_data['round']}: {round_data['error']}")
    for name, counts in report["search"]["counts"].items():
        if not counts["counts_match"]:
            lines.append(f"- LANE/COUNT MISMATCH on `{name}`: A lane={counts['lane']['A']} "
                         f"(total {counts['measured']['A']}), B lane={counts['lane']['B']} "
                         f"(total {counts['measured']['B']}), generator expected "
                         f"{counts['expected']}")
        elif counts["expected_match"] is False:
            lines.append(f"- EXPECTED-COUNT mismatch on `{name}`: measured "
                         f"A {counts['measured']['A']}, B {counts['measured']['B']}, "
                         f"generator expected {counts['expected']}")
        elif counts["lane"]["A"] == "refused":
            lines.append(f"- `{name}` refused by the strict lane on both builds "
                         f"(parity; refused samples A={counts['refused']['A']}, "
                         f"B={counts['refused']['B']})")
    lines.append("")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def generate_initial_archive(args, archive_home: Path, manifest_path: Path) -> dict:
    log(f"[gen] building synthetic archive in {archive_home} "
        f"(target ~{args.messages} messages, seed {args.seed})")
    if archive_home.exists() and any(archive_home.iterdir()):
        raise SystemExit(
            f"{archive_home} already holds data; this bench never deletes or "
            f"overwrites an existing archive — use a fresh --work-dir")
    book, _paths = generate_archive(
        archive_home, seed=args.seed, target_messages=args.messages,
        start_index=0, session_budget=None, big_every=args.big_every,
        pin_mtime=V1_MTIME_EPOCH,
    )
    manifest = write_manifest(manifest_path, seed=args.seed,
                              target_messages=args.messages,
                              big_every=args.big_every, v1=book, incremental=None)
    log(f"[gen] v1: {book.total_sessions} sessions, {book.messages} messages "
        f"(claude {book.claude_sessions} / codex {book.codex_sessions}); "
        f"planted common={book.common_sessions} rare={book.rare_sessions} "
        f"phrase={book.phrase_sessions} tool:{TOOL_TARGET}={book.tool_target_sessions}")
    return manifest


def append_incremental_batch(args, archive_home: Path, manifest_path: Path) -> dict:
    manifest = load_manifest(manifest_path)
    if manifest.get("incremental"):
        raise SystemExit("incremental batch already appended; use a fresh --work-dir")
    count = args.incremental_sessions
    if count <= 0:
        if args.incremental_fraction > 0:
            count = max(3, round(manifest["v1"]["total_sessions"] * args.incremental_fraction))
        else:
            count = 3
    log(f"[gen] appending incremental batch of {count} new sessions "
        f"(mtimes pinned to now-10s so the incremental scan sees them)")
    start_index = manifest["v1"]["total_sessions"]
    batch, paths = generate_archive(
        archive_home, seed=args.seed + 1, target_messages=0,
        start_index=start_index, session_budget=count,
        big_every=args.big_every, pin_mtime=None,
    )
    # Pin the new files to now-10s: strictly after every full-index scan
    # watermark (the full runs happened minutes ago), strictly before the
    # incremental scans start, and far from the 1 s file_modified_since grace.
    stamp = time.time() - 10.0
    for path in paths:
        os.utime(path, (stamp, stamp))
    v1_book = bookkeeping_from_dict(manifest["v1"])
    merged = write_manifest(manifest_path, seed=args.seed,
                            target_messages=args.messages, big_every=args.big_every,
                            v1=v1_book, incremental=batch)
    log(f"[gen] incremental batch: {batch.total_sessions} sessions, "
        f"{batch.messages} messages")
    return merged


def guard_real_home(archive_home: Path) -> None:
    """Refuse to run when the scratch HOME would be (or contain) the real home."""
    real_home = os.environ.get("HOME")
    scratch = archive_home.resolve()
    if real_home:
        real = Path(real_home).resolve()
        if scratch == real:
            raise SystemExit(f"scratch HOME {scratch} IS the real HOME; refusing")
        if scratch in real.parents:
            raise SystemExit(f"scratch HOME {scratch} is a parent of the real HOME "
                             f"{real}; refusing")
    log(f"[guard] scratch HOME={scratch} (the real HOME is never read)")


def finish_failed(args, work, full_runs, incremental_runs, search_rounds,
                  build_a, build_b, manifest, archive_home, expected) -> int:
    report = build_report(args, build_a=build_a, build_b=build_b, manifest=manifest,
                          archive_home=archive_home, full_runs=full_runs,
                          incremental_runs=incremental_runs,
                          search_rounds=search_rounds, expected=expected)
    report_dir = Path(args.report_dir) if args.report_dir else work / "report"
    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / "ab_report.json").write_text(json.dumps(report, indent=2) + "\n",
                                               encoding="utf-8")
    (report_dir / "ab_report.md").write_text(render_markdown(report), encoding="utf-8")
    log(f"[report] failure report written to {report_dir}")
    return 1


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--a-dir", help="directory with build A's `cass` + `cass-viewer-bridge`")
    parser.add_argument("--b-dir", help="directory with build B's binaries")
    parser.add_argument("--a-cass", help="explicit path to build A's `cass`")
    parser.add_argument("--a-bridge", help="explicit path to build A's `cass-viewer-bridge`")
    parser.add_argument("--b-cass", help="explicit path to build B's `cass`")
    parser.add_argument("--b-bridge", help="explicit path to build B's `cass-viewer-bridge`")
    parser.add_argument("--work-dir",
                        help="scratch root for the archive, run data dirs and logs "
                             "(default: a fresh dir under the system temp)")
    parser.add_argument("--report-dir",
                        help="where to write ab_report.{json,md} (default <work-dir>/report)")
    parser.add_argument("--label", default="synthetic A/B", help="run label for the report")
    parser.add_argument("--null-run", action="store_true",
                        help="mark this run as an A/A null (same source): the report "
                             "frames A–B deltas as the noise floor")
    parser.add_argument("--messages", type=int, default=DEFAULT_MESSAGES_TARGET,
                        help=f"target message count for the archive "
                             f"(default {DEFAULT_MESSAGES_TARGET})")
    parser.add_argument("--seed", type=int, default=42, help="generator seed (default 42)")
    parser.add_argument("--big-every", type=int, default=20,
                        help="every Nth session gets a >64K-char assistant text "
                             "(default 20; 0 disables)")
    parser.add_argument("--incremental-sessions", type=int, default=0,
                        help="explicit incremental-batch session count "
                             "(default: max(3, incremental-fraction of sessions))")
    parser.add_argument("--incremental-fraction", type=float, default=0.01,
                        help="fraction of sessions appended for the incremental phase "
                             "(default 0.01)")
    parser.add_argument("--repeats", type=int, default=1,
                        help="interleaved repeats of the index phases (default 1; "
                             "A/A nulls want >=2)")
    parser.add_argument("--search-rounds", type=int, default=3,
                        help="timed search rounds, fresh bridge process per build "
                             "per round (default 3)")
    parser.add_argument("--search-repeats", type=int, default=3,
                        help="repeats of the whole query set per build per round (default 3)")
    parser.add_argument("--search-warmup-rounds", type=int, default=1,
                        help="untimed warm-up search rounds before the timed ones (default 1)")
    parser.add_argument("--search-timeout", type=float, default=120.0,
                        help="per-request bridge timeout in seconds (default 120)")
    parser.add_argument("--index-timeout", type=float, default=7200.0,
                        help="per-index-run timeout in seconds (default 7200)")
    parser.add_argument("--raw-mirror", choices=["on", "off"], default="on",
                        help="child CASS_RAW_MIRROR state (default on; `off` halves "
                             "run-dir disk use for large archives)")
    parser.add_argument("--generate-only", action="store_true",
                        help="generate the archive + manifest, print a summary, exit")
    parser.add_argument("--self-test", action="store_true",
                        help="run the bench's internal self-tests (no cass binaries) and exit")
    args = parser.parse_args(argv)

    if args.self_test:
        return run_self_test()

    work = Path(args.work_dir) if args.work_dir else Path(tempfile.mkdtemp(prefix="cass-ab-bench-"))
    work = work.resolve()
    work.mkdir(parents=True, exist_ok=True)
    archive_root = work / "archive"
    archive_home = archive_root / "home"
    manifest_path = archive_root / "manifest.json"
    log_dir = work / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    manifest = generate_initial_archive(args, archive_home, manifest_path)
    expected = dict(manifest["expected"])
    if args.generate_only:
        log(f"[gen] manifest at {manifest_path}")
        log("[gen] expected strict-search session counts: " + json.dumps(expected))
        return 0

    build_a = resolve_build("A", args.a_dir, args.a_cass, args.a_bridge)
    build_b = resolve_build("B", args.b_dir, args.b_cass, args.b_bridge)
    for build in (build_a, build_b):
        log(f"[build] {build.label}: cass={build.cass} sha256={build.cass_sha256[:16]}… "
            f"bridge={build.bridge}")

    guard_real_home(archive_home)

    runs_root = work / "runs"
    runs_root.mkdir(parents=True, exist_ok=True)
    base_env = ScratchEnv(archive_home, runs_root, work, args.raw_mirror == "on")

    # --- Phase 1: full index, interleaved A B (alternating who goes first) ---
    full_runs = []
    data_dirs = {build.label: [] for build in (build_a, build_b)}
    for repeat in range(args.repeats):
        order = [build_a, build_b] if repeat % 2 == 0 else [build_b, build_a]
        for build in order:
            data_dir = runs_root / build.label.lower() / f"r{repeat}" / "data"
            env = base_env.with_data_dir(data_dir)
            log(f"[full] {build.label} repeat {repeat} -> {data_dir}")
            run = run_index(build, env, full=True, args=args, log_dir=log_dir,
                            tag=f"full-r{repeat}")
            full_runs.append(run)
            data_dirs[build.label].append(data_dir)
            log(f"[full] {build.label} repeat {repeat}: ok={run['ok']} "
                f"wall={run['wall_s']}s rss={fmt_mib(run['peak_rss_bytes'])} "
                f"conversations={run['conversations']} messages={run['messages']}")
    if any(not run["ok"] for run in full_runs):
        log("[full] a full-index run failed (see logs); continuing is pointless")
        return finish_failed(args, work, full_runs, [], [], build_a, build_b,
                             manifest, archive_home, expected)

    # --- Phase 2: append the incremental batch ONCE (same inputs for A and B) ---
    manifest = append_incremental_batch(args, archive_home, manifest_path)
    expected = dict(manifest["expected"])
    log(f"[gen] merged expected strict-search counts: {json.dumps(expected)}")

    # --- Phase 3: incremental index on each repeat's data dir, interleaved ---
    incremental_runs = []
    for repeat in range(args.repeats):
        order = [build_a, build_b] if repeat % 2 == 0 else [build_b, build_a]
        for build in order:
            data_dir = data_dirs[build.label][repeat]
            env = base_env.with_data_dir(data_dir)
            log(f"[incr] {build.label} repeat {repeat} -> {data_dir}")
            run = run_index(build, env, full=False, args=args,
                            log_dir=log_dir, tag=f"incr-r{repeat}")
            incremental_runs.append(run)
            log(f"[incr] {build.label} repeat {repeat}: ok={run['ok']} "
                f"wall={run['wall_s']}s rss={fmt_mib(run['peak_rss_bytes'])} "
                f"conversations={run['conversations']} messages={run['messages']}")
    if any(not run["ok"] for run in incremental_runs):
        log("[incr] an incremental run failed (see logs)")
        return finish_failed(args, work, full_runs, incremental_runs, [],
                             build_a, build_b, manifest, archive_home, expected)

    # --- Phase 4: strict viewer search, interleaved batches, fresh bridge each ---
    queries = build_queries(manifest)
    search_rounds = []
    last_repeat = args.repeats - 1
    for round_index in range(-args.search_warmup_rounds, args.search_rounds):
        warmup = round_index < 0
        order = [build_a, build_b] if round_index % 2 == 0 else [build_b, build_a]
        for build in order:
            data_dir = data_dirs[build.label][last_repeat]
            env = base_env.with_data_dir(data_dir)
            label = f"round {round_index}" + (" (warmup)" if warmup else "")
            log(f"[search] {build.label} {label} -> bridge over {data_dir}")
            try:
                round_data = run_search_round(
                    build, env, queries, round_index=round_index,
                    repeats=args.search_repeats, warmup=warmup,
                    args=args, log_dir=log_dir)
            except (TimeoutError, EOFError) as error:
                log(f"[search] {build.label} {label}: BRIDGE FAILURE: {error}")
                search_rounds.append({
                    "build": build.label, "round": round_index, "warmup": warmup,
                    "samples": [], "bridge_peak_rss_bytes": None, "log": None,
                    "error": str(error),
                })
                continue
            search_rounds.append(round_data)
            timed = [s for s in round_data["samples"] if not s["warmup"]]
            if timed:
                common_latencies = [s["latency_s"] for s in timed if s["query"] == "common"]
                log(f"[search] {build.label} {label}: {len(timed)} samples, "
                    f"common median={fmt_seconds(median(common_latencies))}")

    report = build_report(args, build_a=build_a, build_b=build_b, manifest=manifest,
                          archive_home=archive_home, full_runs=full_runs,
                          incremental_runs=incremental_runs,
                          search_rounds=search_rounds, expected=expected)
    report_dir = Path(args.report_dir) if args.report_dir else work / "report"
    report_dir.mkdir(parents=True, exist_ok=True)
    json_path = report_dir / "ab_report.json"
    md_path = report_dir / "ab_report.md"
    json_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    md_path.write_text(render_markdown(report), encoding="utf-8")
    log(f"[report] json: {json_path}")
    log(f"[report] md:   {md_path}")

    verdict = report["verdict"]
    log(f"[verdict] all_runs_ok={verdict['all_runs_ok']} "
        f"all_counts_match={verdict['all_counts_match']}")
    if not verdict["all_runs_ok"] or not verdict["all_counts_match"]:
        return 1
    return 0


# ---------------------------------------------------------------------------
# Self-test (no cass binaries required)
# ---------------------------------------------------------------------------

def _scan_archive(archive_home: Path) -> dict:
    counts = {"sessions": 0, "common": 0, "rare": 0, "phrase": 0, "bash": 0,
              "big": 0, "claude": 0, "codex": 0, "files": 0, "parse_errors": 0}
    for root, _dirs, files in os.walk(archive_home):
        for name in files:
            path = Path(root) / name
            counts["files"] += 1
            body = path.read_text(encoding="utf-8")
            if ".claude" in path.parts:
                counts["claude"] += 1
            if ".codex" in path.parts:
                counts["codex"] += 1
            counts["sessions"] += 1
            long_text = False
            for line in body.splitlines():
                if not line.strip():
                    continue
                try:
                    parsed = json.loads(line)
                except json.JSONDecodeError:
                    counts["parse_errors"] += 1
                    continue
                if any(True for _ in iter_long_strings(parsed, 65_536)):
                    long_text = True
            if long_text:
                counts["big"] += 1
            if COMMON_TERM in body:
                counts["common"] += 1
            if RARE_TERM in body:
                counts["rare"] += 1
            if PHRASE in body:
                counts["phrase"] += 1
            if '"name":"Bash"' in body:
                counts["bash"] += 1
    return counts


def run_self_test() -> int:
    failures = []

    def check(name, condition, detail=""):
        if condition:
            log(f"  ok  {name}")
        else:
            failures.append(name)
            log(f"  FAIL {name} {detail}")

    log("[self-test] generator: determinism + planted-count honesty")
    tmp = Path(tempfile.mkdtemp(prefix="cass-ab-selftest-"))
    log(f"[self-test] scratch (kept; the bench never deletes files): {tmp}")
    digests = []
    books = []
    for run_index in range(2):
        home = tmp / f"home{run_index}"
        book, _paths = generate_archive(home, seed=7, target_messages=1500,
                                        start_index=0, session_budget=None,
                                        big_every=3, pin_mtime=V1_MTIME_EPOCH)
        manifest = write_manifest(tmp / f"manifest{run_index}.json", seed=7,
                                   target_messages=1500, big_every=3,
                                   v1=book, incremental=None)
        digest = {}
        for root, _dirs, files in os.walk(home):
            for name in sorted(files):
                path = Path(root) / name
                digest[str(path.relative_to(home))] = sha256_file(path)
        digests.append(digest)
        books.append((book, manifest, home))
    check("same seed → identical file set", set(digests[0]) == set(digests[1]))
    check("same seed → byte-identical files", digests[0] == digests[1])

    book, manifest, home = books[0]
    scanned = _scan_archive(home)
    check("manifest sessions == files on disk",
          book.total_sessions == scanned["sessions"] == len(digests[0]),
          f"book={book.total_sessions} scan={scanned['sessions']} files={len(digests[0])}")
    check("both formats generated", scanned["claude"] > 0 and scanned["codex"] > 0)
    check("every JSONL line parses", scanned["parse_errors"] == 0)
    check("planted COMMON count honest", scanned["common"] == book.common_sessions,
          f"scan={scanned['common']} book={book.common_sessions}")
    check("planted RARE count honest", scanned["rare"] == book.rare_sessions,
          f"scan={scanned['rare']} book={book.rare_sessions}")
    check("planted PHRASE count honest", scanned["phrase"] == book.phrase_sessions,
          f"scan={scanned['phrase']} book={book.phrase_sessions}")
    check("planted Bash tool count honest", scanned["bash"] == book.tool_target_sessions,
          f"scan={scanned['bash']} book={book.tool_target_sessions}")
    check("big (>64K chars) assistant texts exist", scanned["big"] >= 1,
          f"big={scanned['big']}")
    check("v1 mtimes pinned",
          abs(os.path.getmtime(home / next(iter(digests[0]))) - V1_MTIME_EPOCH) < 2.0)
    check("manifest expected arithmetic",
          manifest["expected"]["not_common"]
          == manifest["expected"]["total_sessions"] - manifest["expected"]["common"])

    log("[self-test] incremental batch + merge")
    batch, _batch_paths = generate_archive(
        home, seed=8, target_messages=0, start_index=book.total_sessions,
        session_budget=5, big_every=0, pin_mtime=None)
    v1_book = bookkeeping_from_dict(manifest["v1"])
    merged = write_manifest(tmp / "manifest-merged.json", seed=7, target_messages=1500,
                            big_every=3, v1=v1_book, incremental=batch)
    check("merge adds counts",
          merged["expected"]["total_sessions"]
          == manifest["expected"]["total_sessions"] + batch.total_sessions)
    scanned2 = _scan_archive(home)
    check("merged manifest counts match disk",
          scanned2["sessions"] == merged["expected"]["total_sessions"],
          f"scan={scanned2['sessions']} expected={merged['expected']['total_sessions']}")
    check("merged planted counts match disk",
          scanned2["common"] == merged["expected"]["common"]
          and scanned2["bash"] == merged["expected"]["tool_bash"],
          f"common scan={scanned2['common']} exp={merged['expected']['common']}; "
          f"bash scan={scanned2['bash']} exp={merged['expected']['tool_bash']}")

    log("[self-test] query wire shapes (viewer-bridge search_sessions AST)")
    queries = build_queries(merged)
    check("five queries", len(queries) == 5)
    check("common term shape",
          queries[0][1] == {"type": "term", "value": COMMON_TERM, "exact": False})
    check("phrase exact shape",
          queries[2][1] == {"type": "term", "value": PHRASE, "exact": True})
    check("not shape",
          queries[3][1] == {"type": "not",
                            "expr": {"type": "term", "value": COMMON_TERM, "exact": False}})
    check("tool filter shape",
          queries[4][1] == {"type": "filter", "field": "tool", "op": "EQ",
                            "value": TOOL_TARGET})

    log("[self-test] helpers")
    check("darwin rss parser",
          parse_peak_rss("  123456789  maximum resident set size\n") == 123456789)
    check("gnu rss parser",
          parse_peak_rss("Maximum resident set size (kbytes): 2048\n") == 2048 * 1024)
    check("no rss", parse_peak_rss("nothing here") is None)
    values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    check("median", median(values) == 5.5)
    check("p90 nearest-rank", percentile(values, 0.9) == 9)
    check("last-json parser (compact)",
          parse_last_json_object("noise\n{\"success\": true, \"conversations\": 3}")
          == {"success": True, "conversations": 3})
    check("last-json parser (pretty-printed)",
          parse_last_json_object(
              "note: something\n{\n  \"success\": true,\n  \"conversations\": 16,\n"
              "  \"stats\": {\"success\": false, \"inner\": 1}\n}")
          == {"success": True, "conversations": 16,
              "stats": {"success": False, "inner": 1}})

    if failures:
        log(f"[self-test] FAILED: {len(failures)} check(s): {failures}")
        return 1
    log("[self-test] all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
