//! Task 17 — Final local-only audit.
//!
//! Scans the surfaces that could reintroduce remote reach and fails on any
//! unmarked violation:
//!
//! * `src/**` production code (excluding `#[cfg(test)]` items and the
//!   explicitly named old-parameter diagnostics allowlist) must not use
//!   `reqwest`/`ssh2`, spawn `ssh`/`scp`/`sftp`/`rsync`/`tailscale`/`gh`/
//!   `wrangler`/`curl`/`wget`, or bind/connect a socket anywhere but
//!   loopback.
//! * `src/pages_assets/**` (including `vendor/**`, minus `vendor/manifest.json`
//!   and comments) and the export templates under `src/html_export/` must not
//!   load remote resources: no absolute `http(s)://` or protocol-relative
//!   `//` argument to `src=`, `rel=stylesheet|preconnect|preload|modulepreload`
//!   `href`, `@import`, CSS `url(`, `fetch(`, `importScripts(`, `new Worker(`,
//!   or `import(`.
//! * `scripts/**` and `docs/**` (plus `AGENTS.md`, `README.md`, `SKILL.md`,
//!   `.config/nextest.toml`, `.cargo/config.toml`, `Cargo.toml`) must not
//!   instruct remote operations: no `rch exec`, `dsr`, `ssh `, `scp `,
//!   `sftp `, `rsync `, `tailscale `, `curl|wget … |sh`,
//!   `cass sources sync|setup|discover|reingest|artifact-manifest|schedule`,
//!   `cass swarm|lessons|fleet|upgrade|release-verify`,
//!   `cass models check-update`, `models install` without `--from-file`, or
//!   the removed `pages` deploy flags / `--embedder external` /
//!   `--allow-rch` / `--no-cdns` — unless the enclosing paragraph is
//!   explicitly marked as a removal ledger / historical note.
//! * `Cargo.toml` + `Cargo.lock` carry none of the dropped crates
//!   (`reqwest`, `ssh2`, `libssh2-sys`, `openssl*`, `rustls*`,
//!   `webpki-roots`, `hyper*`, `tokio`, `tailscale*`, `isahc`, `ureq`,
//!   `minreq`, `attohttpc`, `curl*`, `surf`, `git2`/`libgit2-sys`); `url`
//!   may exist only as a franken-agent-detection transitive dep and
//!   `semver` only under `[dev-dependencies]`.
//! * The removed-command matrix stays fully active: every row in
//!   `tests/cli_dispatch_coverage.rs` has `pending: false` and the
//!   `REMOVED_COMMANDS`/`REMOVED_FLAGS` tables live in `src/lib.rs`.
//!
//! Exceptions are named and narrow: `docs/LOCAL_ONLY.md` is the audit
//! journal itself; the planning doc listed below is a banner-marked
//! historical design document; `tests/fixtures/**`, `fuzz/corpus/**`,
//! `docs/artifacts/**`, and `tests/cli_dispatch_coverage.rs` hold
//! removed-command fixtures and recorded session content that must never
//! be rewritten.

use regex::Regex;
use std::fs;
use std::path::{Path, PathBuf};

// ---------------------------------------------------------------------------
// Filesystem helpers
// ---------------------------------------------------------------------------

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn read_text(path: &Path) -> String {
    fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("audit: cannot read {}: {e}", path.display()))
}

fn rel(path: &Path) -> String {
    path.strip_prefix(repo_root())
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn collect_files(dir: &Path, out: &mut Vec<PathBuf>) {
    if !dir.is_dir() {
        return;
    }
    let mut entries: Vec<_> = fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("audit: cannot list {}: {e}", dir.display()))
        .filter_map(|e| e.ok())
        .collect();
    entries.sort_by_key(|e| e.file_name());
    for entry in entries {
        let path = entry.path();
        let ft = entry.file_type().expect("audit: file_type");
        if ft.is_dir() {
            collect_files(&path, out);
        } else if ft.is_file() {
            out.push(path);
        }
    }
}

// ---------------------------------------------------------------------------
// Comment/string classification (Rust and JS) + #[cfg(test)] item stripping
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum Cls {
    Code,
    Comment,
    Str,
}

/// Heuristic: `/` opens a JS regex when the previous significant byte is an
/// operator/open-punctuation position.
fn looks_like_regex_start(prefix: &[u8]) -> bool {
    let mut j = prefix.len();
    while j > 0 && prefix[j - 1].is_ascii_whitespace() {
        j -= 1;
    }
    if j == 0 {
        return true;
    }
    matches!(
        prefix[j - 1],
        b'(' | b','
            | b'='
            | b':'
            | b'['
            | b'!'
            | b'&'
            | b'|'
            | b'?'
            | b'{'
            | b'}'
            | b';'
            | b'*'
            | b'+'
            | b'-'
            | b'<'
            | b'>'
            | b'^'
            | b'~'
            | b'%'
    )
}

/// Classify every byte as code / comment / string. Handles `//` and nested
/// `/* */` comments, `"` and `'` strings, `r#"…"#` raw strings, `'c'` char
/// literals vs lifetimes, and — when `js` is set — `` ` `` template strings
/// and `/regex/` literals.
fn classify(src: &str, js: bool) -> Vec<Cls> {
    let b = src.as_bytes();
    let n = b.len();
    let mut cls = vec![Cls::Code; n];
    let mark = |cls: &mut Vec<Cls>, s: usize, e: usize, k: Cls| {
        for c in cls.iter_mut().take(e.min(n)).skip(s) {
            *c = k;
        }
    };
    let mut i = 0usize;
    while i < n {
        match b[i] {
            b'/' if i + 1 < n && b[i + 1] == b'/' => {
                let s = i;
                while i < n && b[i] != b'\n' {
                    i += 1;
                }
                mark(&mut cls, s, i, Cls::Comment);
            }
            b'/' if i + 1 < n && b[i + 1] == b'*' => {
                let s = i;
                i += 2;
                let mut depth = 1u32;
                while i < n && depth > 0 {
                    if b[i] == b'/' && i + 1 < n && b[i + 1] == b'*' {
                        depth += 1;
                        i += 2;
                    } else if b[i] == b'*' && i + 1 < n && b[i + 1] == b'/' {
                        depth -= 1;
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                mark(&mut cls, s, i, Cls::Comment);
            }
            b'/' if js && looks_like_regex_start(&b[..i]) => {
                // JS regex literal — scan to the unescaped closing `/`.
                let s = i;
                i += 1;
                let mut in_class = false;
                while i < n {
                    if b[i] == b'\\' {
                        i += 2;
                        continue;
                    }
                    if in_class {
                        if b[i] == b']' {
                            in_class = false;
                        }
                        i += 1;
                        continue;
                    }
                    if b[i] == b'[' {
                        in_class = true;
                    } else if b[i] == b'/' {
                        i += 1;
                        break;
                    } else if b[i] == b'\n' {
                        break;
                    }
                    i += 1;
                }
                mark(&mut cls, s, i, Cls::Str);
            }
            b'"' => {
                let s = i;
                i += 1;
                while i < n {
                    if b[i] == b'\\' {
                        i += 2;
                        continue;
                    }
                    if b[i] == b'"' {
                        i += 1;
                        break;
                    }
                    i += 1;
                }
                mark(&mut cls, s, i, Cls::Str);
            }
            b'`' if js => {
                let s = i;
                i += 1;
                while i < n {
                    if b[i] == b'\\' {
                        i += 2;
                        continue;
                    }
                    if b[i] == b'`' {
                        i += 1;
                        break;
                    }
                    i += 1;
                }
                mark(&mut cls, s, i, Cls::Str);
            }
            b'r' if !js => {
                // Rust raw string r"…" / r#"…"# — but not raw ident r#foo.
                let mut j = i + 1;
                let mut hashes = 0usize;
                while j < n && b[j] == b'#' {
                    hashes += 1;
                    j += 1;
                }
                if j < n && b[j] == b'"' {
                    let s = i;
                    i = j + 1;
                    while i < n {
                        if b[i] == b'"' {
                            let mut ok = true;
                            for h in 0..hashes {
                                if i + 1 + h >= n || b[i + 1 + h] != b'#' {
                                    ok = false;
                                    break;
                                }
                            }
                            if ok {
                                i += 1 + hashes;
                                break;
                            }
                        }
                        i += 1;
                    }
                    mark(&mut cls, s, i, Cls::Str);
                } else {
                    i += 1;
                }
            }
            b'\'' => {
                if js {
                    let s = i;
                    i += 1;
                    while i < n {
                        if b[i] == b'\\' {
                            i += 2;
                            continue;
                        }
                        if b[i] == b'\'' {
                            i += 1;
                            break;
                        }
                        i += 1;
                    }
                    mark(&mut cls, s, i, Cls::Str);
                } else {
                    // Rust char literal `'x'`/`'\n'`/`'\u{1F600}'` vs lifetime `'a`.
                    let mut j = i + 1;
                    let mut is_char = false;
                    if j < n && b[j] == b'\\' {
                        j += 1;
                        if j < n && b[j] == b'u' && j + 1 < n && b[j + 1] == b'{' {
                            j += 2;
                            while j < n && b[j] != b'}' {
                                j += 1;
                            }
                            j = (j + 1).min(n);
                        } else if j < n && b[j] == b'x' {
                            j = (j + 3).min(n);
                        } else {
                            j = (j + 1).min(n);
                        }
                        if j < n && b[j] == b'\'' {
                            is_char = true;
                        }
                    } else if j + 1 < n && b[j] != b'\'' && b[j + 1] == b'\'' {
                        is_char = true;
                        j += 1;
                    }
                    if is_char {
                        mark(&mut cls, i, j + 1, Cls::Str);
                        i = j + 1;
                    } else {
                        i += 1; // lifetime
                    }
                }
            }
            _ => i += 1,
        }
    }
    cls
}

/// Replace masked bytes with ASCII spaces (newlines preserved so line
/// numbers stay accurate). Every replacement is ASCII, so the result is
/// valid UTF-8 whenever `src` was.
fn mask_with(src: &str, cls: &[Cls], mask_comments: bool, mask_strings: bool) -> String {
    let bytes = src.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    for (i, &b) in bytes.iter().enumerate() {
        let masked =
            (cls[i] == Cls::Comment && mask_comments) || (cls[i] == Cls::Str && mask_strings);
        if masked && b != b'\n' {
            out.push(b' ');
        } else {
            out.push(b);
        }
    }
    String::from_utf8(out).expect("audit: masked view must stay UTF-8")
}

fn blank_region(view: &mut String, s: usize, e: usize) {
    let mut out = view.clone().into_bytes();
    let end = e.min(out.len());
    for b in out.iter_mut().take(end).skip(s.min(end)) {
        if *b != b'\n' {
            *b = b' ';
        }
    }
    *view = String::from_utf8(out).expect("audit: blanked view must stay UTF-8");
}

/// Produce the "production code" view of a Rust file: comments masked,
/// string contents kept (they are `Command::new` arguments), and every
/// `#[cfg(test)]` item — module, fn, impl, use — blanked out.
fn production_view(src: &str) -> String {
    let cls = classify(src, false);
    // Structure view (comments + strings masked) for cfg(test) detection.
    let structure = mask_with(src, &cls, true, true);
    let mut regions: Vec<(usize, usize)> = Vec::new();
    let mut off = 0usize;
    while let Some(pos) = structure[off..].find("#[cfg(test)]") {
        let attr_start = off + pos;
        let after_attr = attr_start + "#[cfg(test)]".len();
        let end = find_item_end(&structure, after_attr);
        regions.push((attr_start, end));
        off = end.max(after_attr);
    }
    // Scan view: comments masked, strings kept, cfg(test) regions blanked.
    let mut scan = mask_with(src, &cls, true, false);
    for (s, e) in regions {
        blank_region(&mut scan, s, e);
    }
    scan
}

/// Skip whitespace + outer attributes after a `#[cfg(test)]`, then find the
/// end of the item it decorates (brace-matched `{…}` or `;`).
fn find_item_end(masked: &str, start: usize) -> usize {
    let b = masked.as_bytes();
    let n = b.len();
    let mut i = start;
    loop {
        while i < n && b[i].is_ascii_whitespace() {
            i += 1;
        }
        if i + 1 < n && b[i] == b'#' && b[i + 1] == b'[' {
            let mut depth = 0i32;
            while i < n {
                if b[i] == b'[' {
                    depth += 1;
                } else if b[i] == b']' {
                    depth -= 1;
                    if depth == 0 {
                        i += 1;
                        break;
                    }
                }
                i += 1;
            }
            continue;
        }
        break;
    }
    let scan_start = i;
    while i < n && b[i] != b'{' && b[i] != b';' {
        if i - scan_start > 8192 {
            return i;
        }
        i += 1;
    }
    if i >= n {
        return n;
    }
    if b[i] == b';' {
        return i + 1;
    }
    let mut depth = 0i32;
    while i < n {
        if b[i] == b'{' {
            depth += 1;
        } else if b[i] == b'}' {
            depth -= 1;
            if depth == 0 {
                return i + 1;
            }
        }
        i += 1;
    }
    n
}

// ---------------------------------------------------------------------------
// Scope definitions
// ---------------------------------------------------------------------------

/// src/ files allowed to carry old-parameter diagnostics in production
/// code. Each exists only to keep legacy config parseable/readable or to
/// print an inert-record diagnostic — never to open a connection.
const SRC_DIAGNOSTIC_ALLOWLIST: &[&str] = &[
    "src/sources/config.rs",
    "src/sources/config_validation.rs",
    "src/schedule/discovery.rs",
    "src/sources/provenance.rs",
    "src/search/semantic_manifest.rs",
    "src/search/model_acquisition.rs",
    "src/pages/config_input.rs",
];

/// src/ path suffixes that are test-only modules (compiled under
/// `#[cfg(test)]` via a parent `mod` declaration).
fn is_src_test_only(path: &str) -> bool {
    path.ends_with("_tests.rs")
        || path.ends_with(".tests.rs")
        || path.contains("/tests/")
        || path.ends_with("/tests.rs")
}

fn src_files() -> Vec<PathBuf> {
    let mut v = Vec::new();
    collect_files(&repo_root().join("src"), &mut v);
    v.retain(|p| p.extension().map(|e| e == "rs").unwrap_or(false));
    v
}

fn asset_files() -> Vec<PathBuf> {
    let mut v = Vec::new();
    collect_files(&repo_root().join("src/pages_assets"), &mut v);
    v.retain(|p| {
        let r = rel(p);
        if r.ends_with("vendor/manifest.json") {
            return false;
        }
        matches!(
            p.extension().and_then(|e| e.to_str()),
            Some("html" | "css" | "js" | "mjs" | "json" | "txt" | "map")
        )
    });
    v
}

/// Strip comments from an asset file by extension.
fn asset_view(path: &Path, src: &str) -> String {
    match path.extension().and_then(|e| e.to_str()) {
        Some("html") => {
            // Mask <!-- --> comments only.
            let b = src.as_bytes();
            let mut out = Vec::with_capacity(b.len());
            let mut i = 0usize;
            while i < b.len() {
                if i + 3 < b.len() && &b[i..i + 4] == b"<!--" {
                    i += 4;
                    while i + 2 < b.len() && &b[i..i + 3] != b"-->" {
                        i += 1;
                    }
                    i = (i + 3).min(b.len());
                    while out.len() < i {
                        let k = out.len();
                        out.push(if b[k] == b'\n' { b'\n' } else { b' ' });
                    }
                } else {
                    out.push(b[i]);
                    i += 1;
                }
            }
            String::from_utf8(out).expect("audit: html view must stay UTF-8")
        }
        Some("css") => {
            // CSS has only /* */ comments; strings stay (they are
            // url()/import arguments).
            let mut cls = vec![Cls::Code; src.len()];
            let b = src.as_bytes();
            let mut i = 0usize;
            while i + 1 < b.len() {
                if b[i] == b'/' && b[i + 1] == b'*' {
                    let s = i;
                    i += 2;
                    while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') {
                        i += 1;
                    }
                    i = (i + 2).min(b.len());
                    for c in cls.iter_mut().take(i).skip(s) {
                        *c = Cls::Comment;
                    }
                } else {
                    i += 1;
                }
            }
            mask_with(src, &cls, true, false)
        }
        Some("js" | "mjs" | "map" | "json") => {
            let cls = classify(src, true);
            mask_with(src, &cls, true, false)
        }
        _ => src.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Violation reporting
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct Violation {
    file: String,
    line: usize,
    rule: &'static str,
    text: String,
}

fn line_of(text: &str, offset: usize) -> usize {
    text[..offset.min(text.len())].matches('\n').count() + 1
}

fn snippet(text: &str, offset: usize) -> String {
    let start = text[..offset.min(text.len())]
        .rfind('\n')
        .map(|i| i + 1)
        .unwrap_or(0);
    let end = text[offset.min(text.len())..]
        .find('\n')
        .map(|i| offset.min(text.len()) + i)
        .unwrap_or(text.len());
    text[start..end].trim().chars().take(160).collect()
}

fn report(violations: &[Violation], suite: &str) {
    if violations.is_empty() {
        return;
    }
    let mut msg = format!("{suite}: {} violation(s)\n", violations.len());
    for v in violations.iter().take(40) {
        msg.push_str(&format!(
            "  {}:{} [{}] {}\n",
            v.file, v.line, v.rule, v.text
        ));
    }
    panic!("{msg}");
}

// ---------------------------------------------------------------------------
// Audit 1 — production src carries no remote client / remote tool / remote bind
// ---------------------------------------------------------------------------

const REMOTE_CLIENT_PATTERNS: &[&str] = &[
    "reqwest::",
    "ssh2::",
    "extern crate reqwest",
    "extern crate ssh2",
];

const REMOTE_TOOLS: &[&str] = &[
    "ssh",
    "scp",
    "sftp",
    "rsync",
    "tailscale",
    "gh",
    "wrangler",
    "curl",
    "wget",
];

const SHELL_WRAPPERS: &[&str] = &["sh", "bash", "zsh", "cmd", "powershell", "pwsh"];

const LOOPBACK_MARKERS: &[&str] = &[
    "127.0.0.1",
    "127, 0, 0, 1",
    "::1",
    "0, 0, 0, 0, 0, 0, 0, 1",
    "localhost",
    "LOCALHOST",
    "loopback",
    "Ipv4Addr::LOCALHOST",
    "Ipv6Addr::LOCALHOST",
    "preview_bind_addr",
    "preview_bind_socket",
];

fn literal_after(view: &str, start: usize) -> Option<String> {
    // `start` points at `(`; skip ws then expect a `"` literal.
    let b = view.as_bytes();
    let mut i = start + 1;
    while i < b.len() && b[i].is_ascii_whitespace() {
        i += 1;
    }
    if i >= b.len() || b[i] != b'"' {
        return None;
    }
    i += 1;
    let s = i;
    while i < b.len() && b[i] != b'"' {
        if b[i] == b'\\' {
            i += 1;
        }
        i += 1;
    }
    Some(view[s..i].to_string())
}

fn basename_of(cmd: &str) -> &str {
    cmd.rsplit(['/', '\\'])
        .next()
        .unwrap_or(cmd)
        .trim_end_matches(".exe")
}

fn word_in(text: &str, word: &str) -> bool {
    let mut start = 0usize;
    while let Some(pos) = text[start..].find(word) {
        let abs = start + pos;
        let prev_ok = abs == 0
            || (!text.as_bytes()[abs - 1].is_ascii_alphanumeric()
                && text.as_bytes()[abs - 1] != b'_');
        let after = abs + word.len();
        let next_ok = after >= text.len()
            || (!text.as_bytes()[after].is_ascii_alphanumeric() && text.as_bytes()[after] != b'_');
        if prev_ok && next_ok {
            return true;
        }
        start = abs + word.len().max(1);
    }
    false
}

#[test]
fn src_code_has_no_remote_client_or_remote_tool() {
    let mut violations = Vec::new();
    for file in src_files() {
        let r = rel(&file);
        if is_src_test_only(&r) {
            continue;
        }
        let allow = SRC_DIAGNOSTIC_ALLOWLIST.iter().any(|a| r == *a);
        let src = read_text(&file);
        let view = production_view(&src);

        // 1) remote client crates
        for pat in REMOTE_CLIENT_PATTERNS {
            let mut off = 0;
            while let Some(pos) = view[off..].find(pat) {
                let abs = off + pos;
                if !allow {
                    violations.push(Violation {
                        file: r.clone(),
                        line: line_of(&view, abs),
                        rule: "remote-client",
                        text: snippet(&view, abs),
                    });
                }
                off = abs + pat.len();
            }
        }

        // 2) Command::new("<literal>") — remote tools + shell wrappers
        let needle = "Command::new";
        let mut off = 0;
        while let Some(pos) = view[off..].find(needle) {
            let abs = off + pos;
            let mut i = abs + needle.len();
            let b = view.as_bytes();
            while i < b.len() && b[i].is_ascii_whitespace() {
                i += 1;
            }
            if i < b.len()
                && b[i] == b'('
                && let Some(lit) = literal_after(&view, i)
            {
                let base = basename_of(&lit).to_ascii_lowercase();
                let first_tok = lit.split_whitespace().next().unwrap_or("");
                let first_base = basename_of(first_tok).to_ascii_lowercase();
                let remote = REMOTE_TOOLS.iter().any(|t| base == *t || first_base == *t);
                if remote && !allow {
                    violations.push(Violation {
                        file: r.clone(),
                        line: line_of(&view, abs),
                        rule: "remote-tool-spawn",
                        text: snippet(&view, abs),
                    });
                }
                if SHELL_WRAPPERS.iter().any(|s| base == *s) {
                    let ctx_end = (i + 400).min(view.len());
                    let ctx = &view[i..ctx_end];
                    for t in REMOTE_TOOLS {
                        if word_in(ctx, t) && !allow {
                            violations.push(Violation {
                                file: r.clone(),
                                line: line_of(&view, abs),
                                rule: "remote-tool-via-shell",
                                text: snippet(&view, abs),
                            });
                            break;
                        }
                    }
                }
            }
            off = abs + needle.len();
        }

        // 3) outbound socket connect helpers — allowed only on loopback.
        for pat in [
            "TcpStream::connect",
            "UdpSocket::connect",
            "std::net::TcpStream::connect",
            "tokio::",
        ] {
            let mut o = 0;
            while let Some(pos) = view[o..].find(pat) {
                let abs = o + pos;
                let stmt_start = view[..abs].rfind(';').map(|i| i + 1).unwrap_or(0);
                let stmt_end = view[abs..]
                    .find(';')
                    .map(|i| abs + i)
                    .unwrap_or(view.len().min(abs + 300));
                let stmt = &view[stmt_start..stmt_end.min(view.len())];
                if !LOOPBACK_MARKERS.iter().any(|m| stmt.contains(m)) && !allow {
                    violations.push(Violation {
                        file: r.clone(),
                        line: line_of(&view, abs),
                        rule: "non-loopback-net",
                        text: snippet(&view, abs),
                    });
                }
                o = abs + pat.len();
            }
        }

        // 4) wildcard bind literals are banned outright in production.
        for pat in ["0.0.0.0", "INADDR_ANY"] {
            let mut o = 0;
            while let Some(pos) = view[o..].find(pat) {
                let abs = o + pos;
                if !allow {
                    violations.push(Violation {
                        file: r.clone(),
                        line: line_of(&view, abs),
                        rule: "wildcard-bind",
                        text: snippet(&view, abs),
                    });
                }
                o = abs + pat.len();
            }
        }

        // 5) socket ::bind( must name a loopback producer in its argument.
        for pat in ["TcpListener::bind", "UdpSocket::bind", "TcpSocket::bind"] {
            let mut o = 0;
            while let Some(pos) = view[o..].find(pat) {
                let abs = o + pos;
                let arg_end = view[abs..]
                    .find(')')
                    .map(|i| abs + i)
                    .unwrap_or(view.len().min(abs + 200));
                let arg = &view[abs..arg_end.min(view.len())];
                if !LOOPBACK_MARKERS.iter().any(|m| arg.contains(m)) && !allow {
                    violations.push(Violation {
                        file: r.clone(),
                        line: line_of(&view, abs),
                        rule: "bind-not-loopback",
                        text: snippet(&view, abs),
                    });
                }
                o = abs + pat.len();
            }
        }
    }
    report(&violations, "src-remote-reach");
}

// ---------------------------------------------------------------------------
// Audit 2 — export templates & pages assets load only archive-local resources
// ---------------------------------------------------------------------------

fn remote_resource_patterns() -> Vec<(&'static str, Regex)> {
    vec![
        (
            "src-remote",
            Regex::new(r#"(?i)\bsrc\s*=\s*["']?\s*(?:https?:|//)"#).unwrap(),
        ),
        (
            "js-src-remote",
            Regex::new(r#"(?i)\.\s*src\s*=\s*["']\s*(?:https?:|//)"#).unwrap(),
        ),
        (
            "css-url-remote",
            Regex::new(r#"(?i)(?:^|[^A-Za-z0-9_])url\s*\(\s*["']?\s*(?:https?:|//)"#).unwrap(),
        ),
        (
            "css-import-remote",
            Regex::new(r#"(?i)@import\s+(?:url\s*\()?\s*["']?\s*(?:https?:|//)"#).unwrap(),
        ),
        (
            "fetch-remote",
            Regex::new(r#"(?i)\bfetch\s*\(\s*["']\s*(?:https?:|//)"#).unwrap(),
        ),
        (
            "importscripts-remote",
            Regex::new(r#"(?i)\bimportScripts\s*\(\s*["']\s*(?:https?:|//)"#).unwrap(),
        ),
        (
            "worker-remote",
            Regex::new(r#"(?i)\bnew\s+Worker\s*\(\s*["']\s*(?:https?:|//)"#).unwrap(),
        ),
        (
            "import-remote",
            Regex::new(r#"(?i)(?:^|[^A-Za-z0-9_.])import\s*\(\s*["']\s*(?:https?:|//)"#).unwrap(),
        ),
        (
            "navigate-remote",
            Regex::new(r#"(?i)\b(?:window\.)?location(?:\.href)?\s*=\s*["']\s*(?:https?:|//)"#)
                .unwrap(),
        ),
    ]
}

fn scan_remote_resources(
    view: &str,
    file: &str,
    violations: &mut Vec<Violation>,
    patterns: &[(&'static str, Regex)],
) {
    for (rule, re) in patterns {
        for m in re.find_iter(view) {
            violations.push(Violation {
                file: file.to_string(),
                line: line_of(view, m.start()),
                rule,
                text: snippet(view, m.start()),
            });
        }
    }
    // <link rel=stylesheet|preconnect|preload|modulepreload href="http…">
    let link_re = Regex::new(r"(?is)<link\b[^>]*>").unwrap();
    let attr_re =
        |name: &str| Regex::new(&format!(r#"(?i)\b{name}\s*=\s*["']([^"']*)["']"#)).unwrap();
    for m in link_re.find_iter(view) {
        let tag = m.as_str();
        let relv = attr_re("rel")
            .captures(tag)
            .and_then(|c| c.get(1).map(|x| x.as_str().to_ascii_lowercase()))
            .unwrap_or_default();
        let restricted = ["stylesheet", "preconnect", "preload", "modulepreload"]
            .iter()
            .any(|r| relv.split_whitespace().any(|w| w == *r));
        if !restricted {
            continue;
        }
        if let Some(href) = attr_re("href")
            .captures(tag)
            .and_then(|c| c.get(1).map(|x| x.as_str().to_string()))
        {
            let h = href.trim_start();
            if h.starts_with("http://") || h.starts_with("https://") || h.starts_with("//") {
                violations.push(Violation {
                    file: file.to_string(),
                    line: line_of(view, m.start()),
                    rule: "link-rel-remote",
                    text: snippet(view, m.start()),
                });
            }
        }
    }
}

#[test]
fn pages_assets_load_only_archive_local_resources() {
    let patterns = remote_resource_patterns();
    let files = asset_files();
    assert!(
        files.len() >= 10,
        "audit: expected >=10 pages asset files, found {}",
        files.len()
    );
    let mut violations = Vec::new();
    for file in &files {
        let r = rel(file);
        let src = read_text(file);
        let view = asset_view(file, &src);
        scan_remote_resources(&view, &r, &mut violations, &patterns);
    }
    report(&violations, "pages-assets-remote");
}

#[test]
fn export_templates_reference_no_external_resources() {
    let patterns = remote_resource_patterns();
    let mut files = Vec::new();
    collect_files(&repo_root().join("src/html_export"), &mut files);
    files.retain(|p| p.extension().map(|e| e == "rs").unwrap_or(false));
    assert!(
        !files.is_empty(),
        "audit: src/html_export/*.rs scan set is empty"
    );
    let mut violations = Vec::new();
    for file in &files {
        let r = rel(file);
        let src = read_text(file);
        let view = production_view(&src);
        scan_remote_resources(&view, &r, &mut violations, &patterns);
    }
    report(&violations, "export-template-remote");
}

// ---------------------------------------------------------------------------
// Audit 3 — scripts and docs carry no remote instructions
// ---------------------------------------------------------------------------

/// Annotation words marking a paragraph as a removal ledger / historical
/// note rather than an instruction.
const ANNOTATION: &[&str] = &[
    "remov", // removed / removal / removing
    "no longer",
    "superseded",
    "supersede",
    "historical",
    "inert",
    "legacy",
    "deprecat",
    "never download",
    "never connect",
    "obsolete",
];

fn paragraph_annotated(text: &str, offset: usize) -> bool {
    let bytes = text.as_bytes();
    let mut s = offset.min(bytes.len());
    while s > 0 {
        if bytes[s - 1] == b'\n' && (s == 1 || bytes[s - 2] == b'\n') {
            break;
        }
        s -= 1;
    }
    let mut e = offset.min(bytes.len());
    while e < bytes.len() {
        if bytes[e] == b'\n' && e + 1 < bytes.len() && bytes[e + 1] == b'\n' {
            break;
        }
        e += 1;
    }
    let para = &text[s..e.min(bytes.len())];
    let lower = para.to_ascii_lowercase();
    ANNOTATION.iter().any(|a| lower.contains(a))
}

fn instruction_patterns() -> Vec<(&'static str, Regex)> {
    vec![
        ("rch-exec", Regex::new(r"\brch\s+exec\b").unwrap()),
        ("dsr", Regex::new(r"\bdsr\w*\b").unwrap()),
        ("ssh", Regex::new(r"\bssh\s").unwrap()),
        ("scp", Regex::new(r"\bscp\s").unwrap()),
        ("sftp", Regex::new(r"\bsftp\s").unwrap()),
        ("rsync", Regex::new(r"\brsync\s").unwrap()),
        ("tailscale", Regex::new(r"\btailscale\s").unwrap()),
        (
            "curl-pipe-sh",
            Regex::new(r"\bcurl\b[^|\n]*\|\s*(?:sudo\s+)?(?:ba|z|fi)?sh\b").unwrap(),
        ),
        (
            "wget-pipe-sh",
            Regex::new(r"\bwget\b[^|\n]*\|\s*(?:sudo\s+)?(?:ba|z|fi)?sh\b").unwrap(),
        ),
        (
            "removed-sources-cmd",
            Regex::new(
                r"\bcass\s+sources\s+(?:sync|setup|discover|reingest|artifact-manifest|schedule)\b",
            )
            .unwrap(),
        ),
        (
            "removed-cmd",
            Regex::new(r"\bcass\s+(?:swarm|lessons|fleet|upgrade|release-verify)\b").unwrap(),
        ),
        (
            "models-check-update",
            Regex::new(r"\bmodels\s+check-update\b").unwrap(),
        ),
        (
            // Violation only when the same line lacks `--from-file` —
            // enforced in the scan loop below (the regex crate has no
            // look-around).
            "models-install-remote",
            Regex::new(r"\bmodels\s+install\b").unwrap(),
        ),
        (
            "pages-deploy-flag",
            Regex::new(
                r"\bcass\s+pages\b[^\n]*--(?:target|project|repo|branch|account-id|api-token)\b",
            )
            .unwrap(),
        ),
        (
            "embedder-external",
            Regex::new(r"--embedder[=\s]+external\b").unwrap(),
        ),
        ("allow-rch", Regex::new(r"--allow-rch\b").unwrap()),
        ("no-cdns", Regex::new(r"--no-cdns\b").unwrap()),
    ]
}

/// Files whose *whole content* is audit-authored (the journal itself) or a
/// banner-marked historical design doc. `banner` must be present for the
/// exemption to hold — remove the banner and the file is scanned.
const DOC_FILE_EXEMPTIONS: &[(&str, &str)] = &[
    ("docs/LOCAL_ONLY.md", "local-only"),
    (
        "docs/planning/PLAN_TO_ADD_LIGHTWEIGHT_SEMANTIC_AND_HYBRID_SEARCH_TO_CASS.md",
        "historical design",
    ),
];

fn docs_scripts_files() -> Vec<PathBuf> {
    let mut v = Vec::new();
    collect_files(&repo_root().join("scripts"), &mut v);
    let mut docs = Vec::new();
    collect_files(&repo_root().join("docs"), &mut docs);
    docs.retain(|p| {
        matches!(
            p.extension().and_then(|e| e.to_str()),
            Some("md" | "txt" | "sh" | "py" | "ps1" | "toml")
        )
    });
    v.extend(docs);
    for extra in [
        "AGENTS.md",
        "README.md",
        "SKILL.md",
        ".config/nextest.toml",
        ".cargo/config.toml",
        "Cargo.toml",
    ] {
        let p = repo_root().join(extra);
        if p.is_file() {
            v.push(p);
        }
    }
    v
}

#[test]
fn scripts_and_docs_carry_no_remote_instructions() {
    let patterns = instruction_patterns();
    let mut violations = Vec::new();
    for file in docs_scripts_files() {
        let r = rel(&file);
        let src = match fs::read_to_string(&file) {
            Ok(s) => s,
            Err(_) => continue, // binary artifacts are not instructions
        };
        let exempt = DOC_FILE_EXEMPTIONS
            .iter()
            .any(|(p, banner)| r == *p && src.contains(banner));
        if exempt {
            continue;
        }
        for (rule, re) in &patterns {
            for m in re.find_iter(&src) {
                if *rule == "models-install-remote" {
                    // Only a violation if this line lacks `--from-file`.
                    let ls = src[..m.start()].rfind('\n').map(|i| i + 1).unwrap_or(0);
                    let le = src[m.end()..]
                        .find('\n')
                        .map(|i| m.end() + i)
                        .unwrap_or(src.len());
                    if src[ls..le].contains("--from-file") {
                        continue;
                    }
                }
                if paragraph_annotated(&src, m.start()) {
                    continue;
                }
                violations.push(Violation {
                    file: r.clone(),
                    line: line_of(&src, m.start()),
                    rule,
                    text: snippet(&src, m.start()),
                });
            }
        }
    }
    report(&violations, "docs-scripts-remote");
}

// ---------------------------------------------------------------------------
// Audit 4 — Cargo manifest & lockfile carry no removed crates
// ---------------------------------------------------------------------------

const BANNED_CRATES: &[&str] = &[
    "reqwest",
    "ssh2",
    "libssh2-sys",
    "openssl",
    "openssl-sys",
    "openssl-probe",
    "rustls",
    "rustls-pemfile",
    "rustls-native-certs",
    "webpki-roots",
    "hyper",
    "hyper-util",
    "tokio",
    "tokio-util",
    "tailscale",
    "tailscale-localapi",
    "isahc",
    "ureq",
    "minreq",
    "attohttpc",
    "curl",
    "curl-sys",
    "surf",
    "git2",
    "libgit2-sys",
];

fn banned_name_re(name: &str) -> Regex {
    Regex::new(&format!(r#"(?m)^name = "{}"$"#, regex::escape(name))).unwrap()
}

fn dep_key_re(name: &str) -> Regex {
    Regex::new(&format!(
        r#"(?m)^[\s"']*{}[\s"']*\s*="#,
        regex::escape(name)
    ))
    .unwrap()
}

#[test]
fn cargo_manifest_and_lockfile_carry_no_remote_deps() {
    let root = repo_root();
    let toml = read_text(&root.join("Cargo.toml"));
    let lock = read_text(&root.join("Cargo.lock"));

    let mut violations = Vec::new();
    for name in BANNED_CRATES {
        for m in dep_key_re(name).find_iter(&toml) {
            violations.push(Violation {
                file: "Cargo.toml".into(),
                line: line_of(&toml, m.start()),
                rule: "banned-dep",
                text: snippet(&toml, m.start()),
            });
        }
        for m in banned_name_re(name).find_iter(&lock) {
            violations.push(Violation {
                file: "Cargo.lock".into(),
                line: line_of(&lock, m.start()),
                rule: "banned-lock-package",
                text: snippet(&lock, m.start()),
            });
        }
    }
    // `url` may exist in the lockfile only via franken-agent-detection; it
    // must never be a direct dep.
    for m in dep_key_re("url").find_iter(&toml) {
        violations.push(Violation {
            file: "Cargo.toml".into(),
            line: line_of(&toml, m.start()),
            rule: "direct-url-dep",
            text: snippet(&toml, m.start()),
        });
    }
    // `semver` is dev-only.
    let deps_start = toml.find("[dependencies]").expect("audit: [dependencies]");
    let deps_end = toml[deps_start..]
        .find("\n[")
        .map(|i| deps_start + i)
        .unwrap_or(toml.len());
    let deps = &toml[deps_start..deps_end];
    for m in dep_key_re("semver").find_iter(deps) {
        violations.push(Violation {
            file: "Cargo.toml".into(),
            line: line_of(&toml, deps_start + m.start()),
            rule: "semver-in-dependencies",
            text: snippet(&toml, deps_start + m.start()),
        });
    }
    report(&violations, "cargo-remote-deps");

    // Prove the only `url` consumer in the lockfile is
    // franken-agent-detection.
    if banned_name_re("url").is_match(&lock) {
        let fad_re = Regex::new(
            r#"(?ms)^\[\[package\]\]\nname = "franken-agent-detection".*?^dependencies = \[(.*?)\]"#,
        )
        .unwrap();
        let fad_deps = fad_re
            .captures(&lock)
            .and_then(|c| c.get(1).map(|m| m.as_str().to_string()))
            .unwrap_or_default();
        assert!(
            fad_deps.contains("\"url\""),
            "audit: `url` is locked but not a franken-agent-detection dep — \
             find what reintroduced it"
        );
    }
}

// ---------------------------------------------------------------------------
// Audit 5 — removed-command matrix stays fully active
// ---------------------------------------------------------------------------

#[test]
fn removed_command_matrix_is_fully_active() {
    let root = repo_root();
    let cov = read_text(&root.join("tests/cli_dispatch_coverage.rs"));
    let lib = read_text(&root.join("src/lib.rs"));

    assert!(
        lib.contains("REMOVED_COMMANDS"),
        "audit: src/lib.rs no longer defines REMOVED_COMMANDS"
    );
    assert!(
        lib.contains("REMOVED_FLAGS"),
        "audit: src/lib.rs no longer defines REMOVED_FLAGS"
    );
    assert!(
        lib.contains("was removed"),
        "audit: src/lib.rs no longer emits the removed-command diagnostic"
    );

    let pending_true = cov.matches("pending: true").count();
    let pending_false = cov.matches("pending: false").count();
    assert_eq!(
        pending_true, 0,
        "audit: {pending_true} removed-command rows still pending — flip them \
         only together with the REMOVED_* entry"
    );
    assert_eq!(
        pending_false, 26,
        "audit: expected 26 active removed-command rows, found {pending_false}; \
         if the matrix grows, update this count deliberately"
    );
}

// ---------------------------------------------------------------------------
// Audit 6 — the scanner itself has teeth (anti-tautology self-check)
// ---------------------------------------------------------------------------

#[test]
fn scanner_detects_synthetic_violations() {
    // production_view must blank #[cfg(test)] items but keep strings in code.
    let fake = r#"
        fn live() {
            let c = Command::new("ssh");
            let _req = reqwest::Client::new();
        }
        #[cfg(test)]
        mod tests {
            fn t() {
                let _ok = Command::new("ssh"); // test fixture — invisible
            }
        }
        fn after() {
            let _x = TcpListener::bind("0.0.0.0:80".parse().unwrap());
        }
        // a trailing comment containing ssh2:: must stay invisible
        // ssh2::Session reqwest::blocking Command::new("curl")
    "#;
    let view = production_view(fake);
    assert!(
        view.contains("Command::new(\"ssh\")"),
        "scanner must keep live Command::new literals visible"
    );
    assert!(
        view.matches("Command::new(\"ssh\")").count() == 1,
        "cfg(test) Command::new must be blanked: {view}"
    );
    assert!(view.contains("reqwest::Client"), "live reqwest:: must show");
    assert!(
        !view.contains("ssh2::Session"),
        "trailing comment must stay masked"
    );

    // Remote-resource patterns must fire on synthetic HTML/JS.
    let patterns = remote_resource_patterns();
    let mut v = Vec::new();
    scan_remote_resources(
        r#"<script src="https://cdn.example.com/x.js"></script>
           <link rel="stylesheet" href="https://cdn.example.com/y.css">
           <a href="https://example.com/docs">ok user link</a>
           <script src="/archive/x.js"></script>
           fetch("./config.json"); fetch("https://evil.example/x")"#,
        "synthetic.html",
        &mut v,
        &patterns,
    );
    let rules: Vec<_> = v.iter().map(|x| x.rule).collect();
    assert!(
        rules.contains(&"src-remote"),
        "must catch remote <script src>: {rules:?}"
    );
    assert!(
        rules.contains(&"link-rel-remote"),
        "must catch remote stylesheet link: {rules:?}"
    );
    assert!(
        rules.contains(&"fetch-remote"),
        "must catch remote fetch literal: {rules:?}"
    );
    assert_eq!(
        v.iter().filter(|x| x.rule == "src-remote").count(),
        1,
        "relative src must not flag; <a href> must not flag: {:?}",
        v
    );

    // Doc paragraph annotation: unannotated removal-command mention flags,
    // annotated one does not.
    let bad = "Run `cass swarm status` to see the board.\n";
    let good = "**Removed commands:** `cass swarm`, `cass fleet` fail fast.\n";
    let pats = instruction_patterns();
    let swarm = pats.iter().find(|(n, _)| *n == "removed-cmd").unwrap();
    assert!(swarm.1.is_match(bad));
    assert!(!paragraph_annotated(bad, 5));
    assert!(paragraph_annotated(good, 40));
}

// ---------------------------------------------------------------------------
// Audit 7 — exception surface stays narrow
// ---------------------------------------------------------------------------

#[test]
fn audit_exception_surface_is_narrow() {
    let root = repo_root();
    for a in SRC_DIAGNOSTIC_ALLOWLIST {
        assert!(
            root.join(a).is_file(),
            "audit: allowlisted file {a} is missing"
        );
    }
    for (p, banner) in DOC_FILE_EXEMPTIONS {
        let path = root.join(p);
        if path.is_file() {
            let src = read_text(&path);
            assert!(
                src.contains(banner),
                "audit: {p} lost its '{banner}' marker — rescan it instead of \
                 exempting it"
            );
        }
    }
    assert!(
        !root.join(".github/workflows").exists(),
        "audit: .github/workflows reappeared"
    );
}
