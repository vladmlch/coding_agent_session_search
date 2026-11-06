#!/usr/bin/env python3
"""Rust item-boundary lexer for the lib.rs modularization tools.

Classifies every byte of a Rust source file as code / comment / string /
char / lifetime and extracts top-level (or module-nested) item spans with
their leading attribute and doc-comment blocks attached.

The lexer exists because line-based boundary detection breaks on raw strings
(`r#"…"#`), char literals and nested block comments — all present in
src/lib.rs. Anything the lexer cannot account for is a hard error, never a
silent skip: an unterminated literal, an unbalanced brace, or code outside
any item span aborts the run.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

# Byte classes.
CODE = 0
COMMENT = 1  # any comment (line or block, doc or plain)
STRING = 2  # "…" or r#"…"# contents including delimiters
CHAR = 3  # 'x' / '\n' / '\u{…}' literals (not lifetimes)


class LexError(Exception):
    """The source contains syntax this lexer refuses to guess at."""


def classify(src) -> list[int]:
    """Per-byte classification of Rust source.

    Covers: `//` and nested `/* */` comments, `"…"` strings with escapes,
    `r"…"`/`r#"…"#` raw strings with arbitrary hash counts, `'c'`/`'\\n'`/
    `'\\u{…}'` char literals (lifetimes stay CODE). Raises LexError on any
    unterminated construct or NUL byte. Accepts `str` (encoded utf-8) or
    `bytes` — the returned list always indexes raw bytes.
    """
    b = src if isinstance(src, bytes) else src.encode("utf-8")
    n = len(b)
    cls = [CODE] * n
    i = 0
    while i < n:
        c = b[i]
        if c == 0:
            raise LexError("NUL byte in source")
        if c == 0x2F and i + 1 < n and b[i + 1] == 0x2F:  # '//'
            s = i
            while i < n and b[i] != 0x0A:
                i += 1
            for j in range(s, i):
                cls[j] = COMMENT
            continue
        if c == 0x2F and i + 1 < n and b[i + 1] == 0x2A:  # '/*'
            s = i
            i += 2
            depth = 1
            while i < n and depth > 0:
                if b[i] == 0x2F and i + 1 < n and b[i + 1] == 0x2A:
                    depth += 1
                    i += 2
                elif b[i] == 0x2A and i + 1 < n and b[i + 1] == 0x2F:
                    depth -= 1
                    i += 2
                else:
                    i += 1
            if depth != 0:
                raise LexError(f"unterminated block comment at byte {s}")
            for j in range(s, i):
                cls[j] = COMMENT
            continue
        if c == 0x72:  # 'r' — maybe raw string r"…" / r#"…"#
            j = i + 1
            hashes = 0
            while j < n and b[j] == 0x23:
                hashes += 1
                j += 1
            if j < n and b[j] == 0x22:
                s = i
                i = j + 1
                closed = False
                while i < n:
                    if b[i] == 0x22:
                        ok = True
                        for h in range(hashes):
                            if i + 1 + h >= n or b[i + 1 + h] != 0x23:
                                ok = False
                                break
                        if ok:
                            i += 1 + hashes
                            closed = True
                            break
                    i += 1
                if not closed:
                    raise LexError(f"unterminated raw string at byte {s}")
                for j2 in range(s, i):
                    cls[j2] = STRING
                continue
            i += 1
            continue
        if c == 0x22:  # '"'
            s = i
            i += 1
            closed = False
            while i < n:
                if b[i] == 0x5C:
                    i += 2
                    continue
                if b[i] == 0x22:
                    i += 1
                    closed = True
                    break
                i += 1
            if not closed:
                raise LexError(f"unterminated string at byte {s}")
            for j in range(s, i):
                cls[j] = STRING
            continue
        if c == 0x27:  # '\'' — char literal vs lifetime
            j = i + 1
            is_char = False
            if j < n and b[j] == 0x5C:  # escape: '\n' '\x41' '\u{1F600}'
                j += 1
                if j < n and b[j] == 0x75 and j + 1 < n and b[j + 1] == 0x7B:  # \u{
                    j += 2
                    while j < n and b[j] != 0x7D:
                        j += 1
                    j = min(j + 1, n)
                elif j < n and b[j] == 0x78:  # \xNN
                    j = min(j + 3, n)
                else:
                    j = min(j + 1, n)
                if j < n and b[j] == 0x27:
                    is_char = True
            elif j + 1 < n and b[j] != 0x27 and b[j + 1] == 0x27:
                is_char = True
                j += 1
            if is_char:
                for k in range(i, min(j + 1, n)):
                    cls[k] = CHAR
                i = j + 1
            else:
                i += 1  # lifetime label
            continue
        i += 1
    return cls


def structure_view(src, cls: list[int], keep_strings: bool = False) -> str:
    """Source with comments (and optionally string/char contents) blanked.

    Newlines are preserved so line numbers stay exact. The result is
    decoded latin-1 so one character maps to one byte — every index in the
    returned view is a byte offset into the source bytes. Accepts `str`
    (encoded utf-8 internally) or `bytes` — pass bytes when `cls` was
    classified over pre-encoded data so indices stay aligned.
    """
    raw = src if isinstance(src, bytes) else src.encode("utf-8")
    out = bytearray(raw)
    for i, c in enumerate(cls):
        if c == COMMENT or (not keep_strings and c in (STRING, CHAR)):
            if raw[i] != 0x0A:
                out[i] = 0x20
    return bytes(out).decode("latin-1")


def line_starts(raw: bytes) -> list[int]:
    starts = [0]
    i = 0
    while True:
        i = raw.find(b"\n", i)
        if i == -1:
            break
        starts.append(i + 1)
        i += 1
    return starts


def line_of(starts: list[int], pos: int) -> int:
    """1-based line number of byte offset `pos` given `line_starts`."""
    import bisect

    return bisect.bisect_right(starts, pos)


@dataclass
class Attr:
    """One outer `#[…]` attribute attached to an item."""

    text: str  # contents inside #[ … ]
    start: int
    end: int


@dataclass
class Item:
    """A top-level (or module-nested) Rust item with attached trivia."""

    kind: str  # fn|struct|enum|union|impl|trait|const|static|type|mod_decl|mod_inline|use|macro|extern|macro_rules
    name: str
    vis: str  # "", "pub", "pub(crate)", "pub(super)", "pub(in …)"
    start: int  # byte offset of first attached attr/doc comment
    end: int  # byte offset just past the item
    head_start: int  # byte offset of the item keyword (after attrs)
    attrs: list[Attr] = field(default_factory=list)
    doc_start: int | None = None  # start of contiguous doc/attr block
    ordinal: int = 0  # index among same (kind, name) items, 0-based
    inner: list["Item"] = field(default_factory=list)  # items inside mod { }

    def signature(self) -> str:
        vis = f"{self.vis} " if self.vis else ""
        return f"{vis}{self.kind} {self.name}".strip()


_ITEM_KEYWORDS = {
    "fn": "fn",
    "struct": "struct",
    "enum": "enum",
    "union": "union",
    "impl": "impl",
    "trait": "trait",
    "const": "const",
    "static": "static",
    "type": "type",
    "mod": "mod",
    "use": "use",
    "extern": "extern",
}
_QUALIFIERS = {"pub", "async", "unsafe", "default", "auto"}
_SEMI_ITEMS = {"use", "const", "static", "type", "extern"}

_IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _skip_ws(view: str, i: int, n: int) -> int:
    while i < n and view[i] in " \t\n\r":
        i += 1
    return i


def _read_ident(view: str, i: int) -> tuple[str, int]:
    m = _IDENT.match(view, i)
    if not m:
        return "", i
    return m.group(0), m.end()


def _match_attr(view: str, i: int, n: int) -> int | None:
    """If `view[i]` opens `#`/`#!` attr, return index just past `]`."""
    if i + 1 >= n or view[i] != "#":
        return None
    j = i + 1
    if view[j] == "!":
        j += 1
    if j >= n or view[j] != "[":
        return None
    depth = 0
    while j < n:
        c = view[j]
        if c in "[{(":
            depth += 1
        elif c in "]})":
            depth -= 1
            if depth == 0:
                return j + 1
            if depth < 0:
                raise LexError(f"unbalanced attr brackets near byte {i}")
        j += 1
    raise LexError(f"unterminated attribute at byte {i}")


def _find_matching_brace(view: str, open_i: int, n: int) -> int:
    depth = 0
    i = open_i
    while i < n:
        c = view[i]
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    raise LexError(f"unbalanced brace opened at byte {open_i}")


# Bytes after which `<` opens a generic/qualified-path context rather than
# a comparison. Digits and `'` are deliberately absent so `3 < 5` in a
# const expression stays a comparison.
_ANGLE_PREV = set("abcdefghijklmnopqrstuvwxyz" "ABCDEFGHIJKLMNOPQRSTUVWXYZ" "_" "')]:,&|+?*!=#>")


def _find_item_end(view: str, i: int, n: int, kind: str) -> tuple[int, int | None]:
    """Return (end_offset, brace_open_or_None) for an item head at `i`.

    Terminators are depth-aware: `;` ends the item only at paren/bracket/
    generic depth 0, so `fn f(…) -> Option<[Vec<i64>; 3]>` and
    `const X: [i32; 4]` do not truncate early. The first `{` at depth 0
    opens the body (matched by `_find_matching_brace`); `<` counts as an
    opening generic only after an ident-like or operator byte, so
    `const X: bool = 3 < 5` still ends at its `;` — anything genuinely
    unbalanced raises LexError rather than mis-splitting.
    """
    paren = 0
    bracket = 0
    angle = 0
    j = i
    while j < n:
        c = view[j]
        if c == "(":
            paren += 1
        elif c == ")":
            paren -= 1
        elif c == "[":
            bracket += 1
        elif c == "]":
            bracket -= 1
        elif c == "<":
            if angle > 0 or (j > i and view[j - 1] in _ANGLE_PREV):
                angle += 1
            elif j > i and view[j - 1] in " \t\n\r":
                k = j - 1
                while k > i and view[k] in " \t\n\r":
                    k -= 1
                if k > i and view[k] in _ANGLE_PREV:
                    angle += 1
        elif c == ">":
            if view[j - 1] == "-":
                pass  # arrow '->', not a generic close
            elif angle > 0:
                angle -= 1
        elif c == ";" and paren == 0 and bracket == 0 and angle == 0:
            return j + 1, None
        elif c == "{" and paren == 0 and bracket == 0 and angle == 0:
            if kind in _SEMI_ITEMS:
                # const/static/type/use brace bodies stay inside the item;
                # skip the group and keep looking for the `;`.
                j = _find_matching_brace(view, j, n)
                continue
            close = _find_matching_brace(view, j, n)
            return close + 1, j
        elif c == "{" and kind in _SEMI_ITEMS:
            j = _find_matching_brace(view, j, n)
            continue
        j += 1
    raise LexError(f"item at byte {i} has no terminating token")


def _name_for(view: str, kind: str, head_end: int, n: int) -> str:
    i = _skip_ws(view, head_end, n)
    if kind == "impl":
        # `impl Foo` / `impl Trait for Foo` / `impl<T> Foo<T>` — keep the
        # trimmed header up to `{`/`where`/`;` for identity.
        j = i
        depth = 0
        while j < n:
            c = view[j]
            if c in "<(":
                depth += 1
            elif c in ">)":
                depth -= 1
            elif depth == 0 and c in "{;":
                break
            j += 1
        header = view[i:j].strip()
        header = re.sub(r"\bwhere\b.*$", "", header, flags=re.S).strip()
        return re.sub(r"\s+", " ", header)
    name, _ = _read_ident(view, i)
    return name


def _vis_of(view: str, i: int, item_start: int) -> tuple[str, int]:
    """Parse a leading `pub`/`pub(…)` qualifier at `i`; return (vis, next)."""
    j = _skip_ws(view, i, len(view))
    word, wend = _read_ident(view, j)
    if word != "pub":
        return "", item_start
    k = _skip_ws(view, wend, len(view))
    if k < len(view) and view[k] == "(":
        close = view.find(")", k)
        if close == -1:
            raise LexError(f"unterminated pub(…) at byte {k}")
        return view[j : close + 1].strip(), _skip_ws(view, close + 1, len(view))
    return "pub", k


def _items_in_view(
    src: str, view: str, cls: list[int], lo: int, hi: int, depth: int
) -> tuple[list[Item], list[tuple[int, int]]]:
    """Parse items whose heads sit in [lo, hi) at a given brace depth.

    Returns (items, code_gaps): `code_gaps` lists (start, end) spans of
    non-whitespace, non-comment bytes in the range that no item covers —
    callers treat a non-empty list as a hard error (lost code).
    """
    n = len(view)
    items: list[Item] = []
    gaps: list[tuple[int, int]] = []
    i = lo
    while i < hi:
        c = view[i]
        if c == "}":
            # A `}` at this level belongs to the enclosing block, not us.
            break
        if c in " \t\n\r":
            i += 1
            continue
        if c == "#":
            # `#![…]` inner attributes are preamble of the enclosing
            # module/file, never attached to an item. Skip them, then
            # collect any following outer `#[…]` attributes onto the item.
            j = i
            while True:
                j = _skip_ws(view, j, n)
                if j + 1 < n and view[j] == "#" and view[j + 1] == "!":
                    end = _match_attr(view, j, n)
                    if end is None:
                        raise LexError(f"malformed inner attribute at {j}")
                    j = end
                    continue
                break
            attr_start = j
            attrs: list[Attr] = []
            while True:
                j = _skip_ws(view, j, n)
                if j + 1 < n and view[j] == "#" and view[j + 1] == "[":
                    end = _match_attr(view, j, n)
                    attrs.append(Attr(view[j:end], j, end))
                    j = end
                    continue
                if j + 1 < n and view[j] == "#" and view[j + 1] == "!":
                    # an inner attr appearing after outer attrs — still
                    # preamble, not part of the item
                    end = _match_attr(view, j, n)
                    j = end
                    continue
                break
            if not attrs:
                # pure preamble (e.g. file-top #![allow]); keep going
                i = j
                continue
            # After attrs there must be an item head (or nothing, for
            # `#![…]` at file scope — handled by caller via gaps).
            k = _skip_ws(view, j, n)
            if k >= hi:
                break
            item, next_i = _parse_item(src, view, cls, k, hi, attrs, attr_start, depth)
            if item is None:
                # Not an item head — attr applied to a non-item? report gap.
                gaps.append((attr_start, j))
                i = j
                continue
            items.append(item)
            i = next_i
            continue
        # No leading attr: try an item head directly.
        word, wend = _read_ident(view, i)
        if word or (i < n and _IDENT.match(view, i)):
            item, next_i = _parse_item(src, view, cls, i, hi, [], i, depth)
            if item is not None:
                items.append(item)
                i = next_i
                continue
        if c == ";":
            # Tolerated stray semicolons are still reported as gaps by the
            # caller if it wants strictness; we skip them silently here.
            i += 1
            continue
        # Anything else at item level is code we cannot attribute: record
        # it as a gap and advance one byte (the caller fails on gaps).
        j = i + 1
        while j < hi and view[j] not in " \t\n\r#{};":
            word2, w2 = _read_ident(view, j)
            if word2 in _ITEM_KEYWORDS or word2 in _QUALIFIERS:
                break
            j += 1
        gaps.append((i, j))
        i = j
    return items, gaps


def _parse_item(
    src: str,
    view: str,
    cls: list[int],
    i: int,
    hi: int,
    attrs: list[Attr],
    attr_start: int,
    depth: int,
) -> tuple[Item | None, int]:
    """Parse one item starting at `i` (a keyword/qualifier position).

    Returns (item, next_offset) or (None, attr_start) when `i` does not
    begin an item head.
    """
    n = len(view)
    j = _skip_ws(view, i, n)
    vis = ""
    word, wend = _read_ident(view, j)
    if word == "pub":
        vis, j = _vis_of(view, j, j)
        word, wend = _read_ident(view, j)
    quals: list[str] = []
    while word in ("async", "unsafe", "const", "default", "auto"):
        # A qualifier word is an item keyword too (`const X`, `static`,
        # `async` block…): it only qualifies when a real item keyword —
        # in practice `fn`/`trait`/`impl` — follows it.
        k2 = _skip_ws(view, wend, n)
        word2, _w2end = _read_ident(view, k2)
        if word2 not in _ITEM_KEYWORDS and word2 != "extern":
            break
        quals.append(word)
        j = k2
        word, wend = word2, _w2end
    if word in _ITEM_KEYWORDS:
        kind = _ITEM_KEYWORDS[word]
        if word == "extern":
            # `extern crate self as x;` / `extern "C" fn` — the latter is
            # handled as a fn qualifier, the former as a semi item.
            k = _skip_ws(view, wend, n)
            w2, _w2end = _read_ident(view, k)
            if w2 in ("crate", "mod"):
                end, _ = _find_item_end(view, wend, n, "extern")
                name = re.sub(r"\s+", " ", view[i:end].rstrip(";")).strip()
                return (
                    Item(
                        kind="extern",
                        name=name,
                        vis=vis,
                        start=attr_start,
                        end=end,
                        head_start=i,
                        attrs=attrs,
                    ),
                    end,
                )
            kind = "fn"  # extern "C" fn … — treat as fn
        if word == "mod":
            end, brace = _find_item_end(view, wend, n, "mod")
            name = _name_for(view, "mod", wend, n)
            kind = "mod_inline" if brace is not None else "mod_decl"
            item = Item(kind, name, vis, attr_start, end, i, attrs)
            if brace is not None:
                inner, _gaps = _items_in_view(src, view, cls, brace + 1, end - 1, depth + 1)
                item.inner = inner
            return item, end
        if word == "use":
            end, _ = _find_item_end(view, wend, n, "use")
            name = re.sub(r"\s+", " ", view[wend:end].rstrip(";").strip())
            return Item("use", name, vis, attr_start, end, i, attrs), end
        if word == "macro_rules":
            # handled by the macro branch below via '!' — unreachable here
            pass
        end, brace = _find_item_end(view, wend, n, kind)
        name = _name_for(view, kind, wend, n)
        return Item(kind, name, vis, attr_start, end, i, attrs), end
    if word == "macro_rules":
        k = _skip_ws(view, wend, n)
        if k < n and view[k] == "!":
            name, nend = _read_ident(view, _skip_ws(view, k + 1, n))
            end, brace = _find_item_end(view, nend, n, "macro_rules")
            return (
                Item("macro_rules", name, vis, attr_start, end, i, attrs),
                end,
            )
        return None, attr_start
    # Macro invocation: ident ! or path::ident ! followed by a delimiter.
    if word:
        k = _skip_ws(view, wend, n)
        while k < n and view[k] == ":" and k + 1 < n and view[k + 1] == ":":
            k = _skip_ws(view, k + 2, n)
            word2, w2end = _read_ident(view, k)
            if not word2:
                return None, attr_start
            k = _skip_ws(view, w2end, n)
        if k < n and view[k] == "!":
            k2 = _skip_ws(view, k + 1, n)
            name = view[i : k].strip()
            if k2 < n and view[k2] in "{[":
                close = _find_matching_brace(view, k2, n)
                end = close + 1
                item = Item("macro", name, vis, attr_start, end, i, attrs)
                if view[k2] == "{":
                    inner, _gaps = _items_in_view(
                        src, view, cls, k2 + 1, end - 1, depth + 1
                    )
                    item.inner = inner
                return item, end
            if k2 < n and view[k2] == "(":
                # parenthesized macro call: match ( )
                depth2 = 0
                m = k2
                while m < n:
                    if view[m] == "(":
                        depth2 += 1
                    elif view[m] == ")":
                        depth2 -= 1
                        if depth2 == 0:
                            break
                    m += 1
                if m >= n:
                    raise LexError(f"unbalanced paren at byte {k2}")
                end = m + 1
                k3 = _skip_ws(view, end, n)
                if k3 < n and view[k3] == ";":
                    end = k3 + 1
                return Item("macro", name, vis, attr_start, end, i, attrs), end
        return None, attr_start
    return None, attr_start


def attach_trivia(src: str, cls: list[int], items: list[Item]) -> None:
    """Extend each item's `start` over its preceding doc/plain-comment
    block (outer `#[…]` attrs are already inside `start`).

    Doc comments (`///`, `/**`, `//!`) attach across any whitespace; plain
    comments attach only when directly adjacent (no blank line) to the
    comment/attr block below them.
    """
    import bisect

    raw = src.encode("utf-8")
    comments: list[tuple[int, int, bool]] = []  # start,end,is_doc
    i = 0
    n = len(raw)
    while i < n:
        if cls[i] == COMMENT:
            s = i
            while i < n and cls[i] == COMMENT:
                i += 1
            text = raw[s:i].decode("utf-8", "replace")
            is_doc = (
                text.startswith("///")
                or text.startswith("/**")
                or text.startswith("//!")
            )
            comments.append((s, i, is_doc))
        else:
            i += 1
    ends = [c[1] for c in comments]

    def attach(item: Item) -> None:
        s = item.start
        while True:
            k = bisect.bisect_right(ends, s) - 1
            if k < 0:
                break
            cs, ce, is_doc = comments[k]
            gap = raw[ce:s]
            if gap.strip(b" \t\n\r"):
                break
            if is_doc:
                item.start = cs
                s = cs
                continue
            # plain comment: attach only when directly adjacent — the gap
            # must contain no blank line and no other comment boundary
            if b"\n\n" not in gap.replace(b"\r", b""):
                item.start = cs
                s = cs
                continue
            break

    def attach_tree(item: Item) -> None:
        attach(item)
        for sub in item.inner:
            attach_tree(sub)

    for it in items:
        attach_tree(it)


def parse_items(src: str) -> tuple[list[Item], list[int], list[tuple[int, int]]]:
    """Parse top-level items of `src`.

    Returns (items, cls, gaps). `gaps` lists non-whitespace byte spans not
    covered by any item or `#![…]` inner attribute — callers must treat a
    non-empty result as an error (unattributed code would be lost).
    """
    cls = classify(src)
    view = structure_view(src, cls)
    items, gaps = _items_in_view(src, view, cls, 0, len(view), 0)
    # Filter gaps: `#![…]` inner attrs at top level are preamble, not gaps.
    real_gaps = []
    for s, e in gaps:
        frag = view[s:e]
        if frag.strip().startswith("#!"):
            continue
        # stray '{' or '}' fragments and other code
        if frag.strip():
            real_gaps.append((s, e))
    # Attach leading doc comments/plain comments to each item.
    attach_trivia(src, cls, items)
    # Ordinals per (kind, name).
    seen: dict[tuple[str, str], int] = {}
    for it in items:
        key = (it.kind, it.name)
        it.ordinal = seen.get(key, 0)
        seen[key] = it.ordinal + 1
    return items, cls, real_gaps


def item_text(src: str | bytes, item: Item) -> str:
    raw = src if isinstance(src, bytes) else src.encode("utf-8")
    return raw[item.start : item.end].decode("utf-8")


def cfg_chain(item: Item, parents: list[Item]) -> list[str]:
    """Effective #[cfg] stack: own cfg attrs plus all parents', outermost
    first. Returns normalized cfg expression strings."""
    chain: list[str] = []
    for it in [*parents, item]:
        for a in it.attrs:
            m = re.match(r"#\s*\[\s*cfg\s*\((.*)\)\s*\]\s*$", a.text, re.S)
            if m:
                chain.append(re.sub(r"\s+", " ", m.group(1)).strip())
    return chain


def is_test_cfg(cfg: str) -> bool:
    """Does this cfg expression force `test` compilation?"""
    return not cfg_can_hold(cfg, test=False)


def item_is_test_item(item: Item) -> bool:
    """Item carries a #[test] attribute (fn-level)."""
    return any(re.match(r"#\s*\[\s*test\s*(?:\([^)]*\))?\s*\]", a.text) for a in item.attrs)


def mod_has_test_cfg(item: Item) -> bool:
    """Any cfg attr on this item requires `test` compilation."""
    return test_only(item)


def cfg_exprs(item: Item) -> list[str]:
    """Normalized expressions of every `#[cfg(…)]` gate on `item`.

    `cfg_attr` is not a gate — the item compiles regardless — so it is
    deliberately excluded.
    """
    out: list[str] = []
    for a in item.attrs:
        m = re.match(r"#\s*\[\s*cfg\s*\((.*)\)\s*\]\s*$", a.text, re.S)
        if m:
            out.append(re.sub(r"\s+", " ", m.group(1)).strip())
    return out


_CFG_TOK = re.compile(r"[A-Za-z_][A-Za-z0-9_]*|[()!,=]|\"[^\"]*\"")


def _eval_cfg(tokens: list[str], env: dict[str, bool]) -> bool:
    pos = 0

    def parse_or() -> bool:
        nonlocal pos
        return parse_term()

    def parse_term() -> bool:
        nonlocal pos
        tok = tokens[pos]
        if tok == "(":
            pos += 1
            v = parse_or()
            if pos >= len(tokens) or tokens[pos] != ")":
                raise LexError(f"unbalanced cfg parens: {tokens}")
            pos += 1
            return v
        if tok == "!":
            pos += 1
            return not parse_term()
        if pos + 1 < len(tokens) and tokens[pos + 1] == "(" and tok in ("all", "any", "not"):
            pos += 2
            vals = []
            while pos < len(tokens) and tokens[pos] != ")":
                vals.append(parse_or())
                if pos < len(tokens) and tokens[pos] == ",":
                    pos += 1
            if pos >= len(tokens):
                raise LexError("unterminated cfg call")
            pos += 1
            if tok == "all":
                return all(vals)
            if tok == "any":
                return any(vals)
            return not (len(vals) == 1 and vals[0])
        # predicate: ident or ident = "value"
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", tok or ""):
            name = tok
            pos += 1
            if pos + 1 < len(tokens) and tokens[pos] == "=":
                pos += 2  # consume ="value" — predicate name still drives truth
            return env.get(name, True)
        raise LexError(f"unparseable cfg token {tok!r}")

    val = parse_or()
    if pos != len(tokens):
        raise LexError(f"trailing cfg tokens: {tokens[pos:]}")
    return val


def cfg_can_hold(expr: str, test: bool) -> bool:
    """Can `expr` evaluate true under `test=test` for some assignment of
    all other predicates? Unknown atoms are enumerated (cfg exprs in this
    codebase are small); anything unparseable is a hard error."""
    expr = expr.strip()
    if not expr:
        return True
    tokens = _CFG_TOK.findall(expr)
    atoms = sorted({t for t in tokens if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", t)
                    and t not in ("all", "any", "not", "test")})
    if len(atoms) > 12:
        raise LexError(f"cfg expression too complex to verify: {expr}")
    from itertools import product
    for combo in product((False, True), repeat=len(atoms)):
        env = dict(zip(atoms, combo))
        env["test"] = test
        if _eval_cfg(tokens, env):
            return True
    return False


def test_only(item: Item) -> bool:
    """Item can never compile with `--cfg test` unset (test-only)."""
    return any(not cfg_can_hold(e, test=False) for e in cfg_exprs(item))


def field_insert_offsets(item_text: str, item: Item) -> list[int]:
    """Byte offsets (relative to `item.start`) where `pub(crate) ` must be
    inserted to promote every private field of a struct/union.

    Handles braced (`struct S { a: i32 }`) and tuple (`struct S(i32)`)
    field lists, skips already-visible fields, and ignores items that are
    not structs/unions (enum variants carry no visibility).
    """
    if item.kind not in ("struct", "union"):
        return []
    raw = item_text.encode("utf-8")
    cls = classify(item_text)
    view = structure_view(item_text, cls)
    n = len(view)
    # locate the field-list opener: first '{' or '(' at depth 0 after the
    # head, skipping generic `<…>` (e.g. `struct S<F: Fn()>`).
    head = item.head_start - item.start
    # the field list opens only after the `struct`/`union` keyword itself —
    # a `pub(crate)` qualifier before it has parens that must not be read
    # as a tuple-field group.
    kw = re.search(rf"\b{item.kind}\b", view[head:])
    scan_from = head + kw.end() if kw else head
    open_i = None
    angle = 0
    for j in range(scan_from, n):
        c = view[j]
        if c == "<" and angle == 0 and j > head and view[j - 1] in _ANGLE_PREV:
            angle += 1
            continue
        if c == "<" and angle > 0:
            angle += 1
            continue
        if c == ">" and angle > 0 and view[j - 1] != "-":
            angle -= 1
            continue
        if angle > 0:
            continue
        if c == ";":
            return []  # unit struct
        if c in "{(":
            open_i = j
            break
    if open_i is None:
        return []
    depth = 0
    close_i = None
    for j in range(open_i, n):
        if view[j] in "{(":
            depth += 1
        elif view[j] in "})":
            depth -= 1
            if depth == 0:
                close_i = j
                break
    if close_i is None:
        raise LexError(f"unbalanced field list in {item.name}")
    braced = view[open_i] == "{"
    # split the interior at depth-1 commas
    segs: list[tuple[int, int]] = []
    seg_start = open_i + 1
    depth = 0
    for j in range(open_i + 1, close_i):
        c = view[j]
        if c in "{(<[":
            depth += 1
        elif c in "})>]" and not (c == ">" and view[j - 1] == "-"):
            # '->' in fn-pointer/closure field types is not a generic close
            depth -= 1
        elif c == "," and depth == 0:
            segs.append((seg_start, j))
            seg_start = j + 1
    segs.append((seg_start, close_i))
    offsets: list[int] = []
    for s, e in segs:
        i = _skip_ws(view, s, e)
        # skip field attrs #[…]
        while i < e and view[i] == "#":
            end = _match_attr(view, i, e)
            if end is None:
                break
            i = _skip_ws(view, end, e)
        if i >= e:
            continue
        # already visible?
        word, wend = _read_ident(view, i)
        if word == "pub":
            continue
        if braced:
            if not word:
                continue
            offsets.append(i)  # before field name
        else:
            offsets.append(i)  # before tuple field type
    return offsets


def ownership(
    raw: bytes, cls: list[int], items: list[Item]
) -> tuple[list[tuple[int, int]], tuple[int, int]]:
    """Assign every byte between items an owner.

    The gap before items[i] belongs to items[i] (it travels with that
    item); the tail after the last item belongs to the file remainder.
    Returns (leads, tail) where leads[i] = (start, end) — possibly an
    empty span. Raises LexError when a gap contains anything but
    whitespace, comments, `#![…]` inner attrs or stray `;`.
    """
    leads: list[tuple[int, int]] = []
    prev_end = 0
    n = len(raw)
    for it in items:
        gap = (prev_end, it.start)
        validate_gap_bytes(raw, cls, gap)
        leads.append(gap)
        prev_end = it.end
    tail = (prev_end, n)
    validate_gap_bytes(raw, cls, tail)
    return leads, tail


def validate_gap_bytes(raw: bytes, cls: list[int], span: tuple[int, int]) -> None:
    """Gap bytes must be whitespace/comment/`#![…]`/stray `;` only."""
    s, e = span
    i = s
    while i < e:
        if cls[i] == COMMENT or raw[i] in b" \t\n\r":
            i += 1
            continue
        if raw[i : i + 2] == b"#!":
            view = raw[i:e].decode("latin-1")
            end = _match_attr(view, 0, len(view))
            if end is None:
                raise LexError(f"malformed inner attribute at byte {i}")
            i += end
            continue
        if raw[i : i + 1] == b";":
            i += 1
            continue
        raise LexError(
            f"unowned non-whitespace byte {raw[i:i+1]!r} at offset {i}"
        )


def code_regions(cls: list[int]) -> list[tuple[int, int]]:
    """Maximal [s, e) spans classified as CODE."""
    out: list[tuple[int, int]] = []
    i = 0
    n = len(cls)
    while i < n:
        if cls[i] == CODE:
            s = i
            while i < n and cls[i] == CODE:
                i += 1
            out.append((s, i))
        else:
            i += 1
    return out


def find_in_code(
    raw: bytes, cls: list[int], needle: bytes
) -> list[tuple[int, int]]:
    """All (start, end) hits of `needle` fully inside CODE-classified
    bytes — hits inside strings/comments/char literals do not count."""
    hits: list[tuple[int, int]] = []
    ln = len(needle)
    for s, e in code_regions(cls):
        i = raw.find(needle, s, e)
        while i != -1 and i + ln <= e:
            hits.append((i, i + ln))
            i = raw.find(needle, i + 1, e)
    return hits


def find_in_syntax(
    raw: bytes, cls: list[int], needle: bytes
) -> list[tuple[int, int]]:
    """All (start, end) hits of `needle` with no COMMENT-classified byte.

    Unlike find_in_code, string/char literals count as matchable syntax:
    retarget patterns such as `include_str!("lib.rs")` legitimately span
    code tokens and a string literal. Comment bytes are blanked in the
    match view so hits can never originate inside comments.
    """
    view = structure_view(raw, cls, keep_strings=True)
    n = needle.decode("latin-1")
    hits: list[tuple[int, int]] = []
    i = view.find(n)
    while i != -1:
        hits.append((i, i + len(n)))
        i = view.find(n, i + 1)
    return hits


def syntax_regions(cls: list[int]) -> list[tuple[int, int]]:
    """Maximal [s, e) spans containing no COMMENT-classified byte."""
    out: list[tuple[int, int]] = []
    i = 0
    n = len(cls)
    while i < n:
        if cls[i] != COMMENT:
            s = i
            while i < n and cls[i] != COMMENT:
                i += 1
            out.append((s, i))
        else:
            i += 1
    return out


def rewrite_syntax(raw: bytes, cls: list[int], pat: "re.Pattern[bytes]",
                   repl: bytes) -> bytes:
    """Regex-replace `pat`→`repl` inside non-comment spans only.

    Like rewrite_code but string/char literals are matchable syntax:
    `include!("search/x.rs")` spans code+string and the path must be
    editable. Matches can never consume comment bytes because every
    region ends where a comment begins."""
    out = bytearray()
    prev = 0
    for s, e in syntax_regions(cls):
        out += raw[prev:s]
        out += pat.sub(repl, raw[s:e])
        prev = e
    out += raw[prev:]
    return bytes(out)


def rewrite_code(raw: bytes, cls: list[int], pat: "re.Pattern[bytes]",
                 repl: bytes) -> bytes:
    """Regex-replace `pat`→`repl` inside CODE-classified spans only.

    Matches spanning a code/comment boundary are rejected (a pattern that
    would need bytes inside a string or comment to match is a bug in the
    rewrite rule, not a rewrite)."""
    out = bytearray()
    prev = 0
    for s, e in code_regions(cls):
        seg = raw[s:e]
        out += raw[prev:s]
        out += pat.sub(repl, seg)
        prev = e
    out += raw[prev:]
    return bytes(out)


def summarize(src: str) -> dict:
    """Quick stats used by reports: counts per item kind."""
    items, _cls, gaps = parse_items(src)
    kinds: dict[str, int] = {}
    for it in items:
        kinds[it.kind] = kinds.get(it.kind, 0) + 1
    return {"items": len(items), "gaps": len(gaps), "kinds": kinds}
