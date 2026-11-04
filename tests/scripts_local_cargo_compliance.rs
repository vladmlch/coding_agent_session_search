//! Scanner that asserts the ported development scripts keep Cargo fully local:
//! no remote-build offload (`rch`, `dsr`) and no remote-copy / network-fetch
//! (`ssh`, `scp`, `rsync`, `curl`, `wget`) invocations — `cargo` runs directly
//! or through `run_cargo` — plus the `set -e` + `((VAR++))` arithmetic-abort
//! idiom stays banned in every shell script under `scripts/`.
//!
//! ## Compliance rule 1: no remote execution plumbing in any script
//!
//! For every `*.sh`, `*.ps1` and `*.py` file under `scripts/` (Task 3 widened
//! the scan from a hand-maintained allowlist to the whole scripts tree):
//!   - Any leftover offload-plumbing token — a word starting with `rch`
//!     (`rch`, `RCH_BIN`, `RCH_TARGET_DIR`, `rch_exec`, ...), a word starting
//!     with `dsr`, or `ensure_rch` — is a violation anywhere in the file,
//!     including comments and help text. These tokens are deleted by the
//!     port, not documented.
//!   - `ssh`, `scp`, `rsync`, `curl`, `wget` are forbidden in every script,
//!     with a per-extension rule (the invocation syntax differs too much for
//!     one model):
//!       * Shell (`*.sh`, and any other extension) uses the command-position
//!         model: after `;`, `|`, `&`, `(`, `` ` ``, or at the start of a
//!         command segment, past leading shell keywords, `VAR=...`
//!         assignments and `command`/`env`/`sudo`-style modifiers — is a
//!         violation. Arguments are not invocations: `run_cargo test ssh`
//!         and a `--include-ssh` case pattern stay clean. Quoted string
//!         literals are masked except for live `$(...)` / `` `...` ``
//!         substitutions inside double quotes.
//!       * Non-shell scripts (`*.py`, `*.ps1`) have no shell command
//!         position the model can parse — `subprocess.run(["ssh", ...])`,
//!         `Start-Process ssh`, `& $tool`, `iex "curl …"` would all evade —
//!         so they get the stricter raw scan: ANY standalone occurrence of
//!         a forbidden tool name (string literal, comment or call site) is
//!         a violation. A name fused into a larger token stays clean:
//!         `~/.ssh/config`, `--include-ssh`, `ssh-keygen`, `use_ssh`,
//!         `SSH_KEY`, `asupersync`. `ssh.exe`-style Windows binary suffixes
//!         still flag.
//!     `rch`/`dsr` need no entry in either model: the plumbing rule above
//!     already scans every extension raw.
//!   - Bare `cargo build|test|...` is NOT a violation anymore: local Cargo is
//!     the point of the port. `cargo` may be invoked directly or via
//!     `run_cargo`.
//!
//! ## Compliance rule 2: avoid `set -e + ((VAR++))`
//!
//! Bash's `((VAR++))` evaluates to 0 when VAR is initially 0; under `set -e`,
//! the shell aborts. This caused the zlzpk bug. The scanner flags any
//! `((VAR++))` or `((VAR--))` pattern in a script that also has `set -e` (or
//! `set -euo pipefail`) earlier in the file. Rule 2 scans every `*.sh` file
//! under `scripts/` (PowerShell and Python have no such idiom).
//!
//! ## Logging
//!
//! Every match emits `tracing::info!(target: "scripts_local_cargo_compliance", ...)`
//! so failure context lands in the test harness output.

use std::path::{Path, PathBuf};

/// File extensions that count as scripts for compliance rule 1. Task 3
/// widened the scan from a hand-maintained allowlist to every `*.sh`,
/// `*.ps1` and `*.py` file under `scripts/`.
const SCRIPT_EXTENSIONS: &[&str] = &["sh", "ps1", "py"];

/// Remote-build offload plumbing that must not survive the port, anywhere in
/// the file (comments and help text included).
fn plumbing_re() -> regex::Regex {
    regex::Regex::new(r"(?i)\brch\w*|\bdsr\w*|\bensure_rch\b").expect("regex compiles")
}

/// Tools that must never appear in command position in a ported script.
const FORBIDDEN_COMMANDS: &[&str] = &["rch", "dsr", "ssh", "scp", "rsync", "curl", "wget"];

/// Leading tokens that are never the invoked command itself: shell keywords,
/// `VAR=...` assignments, redirections, option flags, and command modifiers
/// (`command`, `env`, `sudo`, ...).
fn is_prefix_token(token: &str) -> bool {
    const KEYWORDS: &[&str] = &[
        "if", "then", "else", "elif", "fi", "for", "while", "until", "do", "done", "case", "esac",
        "select", "function", "in", "time", "coproc", "{", "}", "!",
    ];
    const MODIFIERS: &[&str] = &[
        "command", "builtin", "exec", "eval", "sudo", "doas", "env", "nohup", "nice", "ionice",
        "stdbuf", "timeout", "xargs",
    ];
    if KEYWORDS.contains(&token) || MODIFIERS.contains(&token) {
        return true;
    }
    // Tokens are whitespace-split so they are never empty. Flags, redirections,
    // and numeric args (e.g. `timeout 10 ...`, `2>file`) are never the command.
    let first = token.as_bytes()[0];
    if first == b'-' || first == b'<' || first == b'>' || first.is_ascii_digit() {
        return true;
    }
    // `VAR=value` / `VAR+=value` assignment prefix is never the command.
    if let Some(eq) = token.find('=') {
        let name = token[..eq].strip_suffix('+').unwrap_or(&token[..eq]);
        let mut chars = name.bytes();
        if matches!(chars.next(), Some(b) if b.is_ascii_alphabetic() || b == b'_')
            && chars.all(|b| b.is_ascii_alphanumeric() || b == b'_')
        {
            return true;
        }
    }
    false
}

fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn local_script_paths() -> Vec<PathBuf> {
    list_scripts(&["scripts"], SCRIPT_EXTENSIONS)
}

fn list_scripts(roots: &[&str], extensions: &[&str]) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for root in roots {
        let root_path = project_root().join(root);
        if !root_path.is_dir() {
            continue;
        }
        walk_dir(&root_path, extensions, &mut out);
    }
    out.sort();
    out
}

fn walk_dir(dir: &Path, extensions: &[&str], out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        // Skip hidden dirs (.git, etc.) and known-noise dirs.
        if let Some(name) = path.file_name().and_then(|n| n.to_str())
            && (name.starts_with('.') || name == "target" || name == "node_modules")
        {
            continue;
        }
        if path.is_dir() {
            walk_dir(&path, extensions, out);
        } else if let Some(ext) = path.extension().and_then(|s| s.to_str())
            && extensions.contains(&ext)
        {
            out.push(path);
        }
    }
}

/// Strip trailing comment from a line (after the first unquoted `#`).
/// This is a heuristic — full bash parsing is out of scope; we mostly need
/// to ignore "# cargo ..." which is overwhelming the dominant FP.
fn strip_trailing_comment(line: &str) -> &str {
    // Single-quoted and double-quoted regions hide #s; we punt on those and
    // accept the FP risk. The conservative approach: split on " # " (space-#)
    // which excludes the one common case where # appears in identifiers.
    if let Some(idx) = line.find(" # ") {
        return &line[..idx];
    }
    if line.trim_start().starts_with('#') {
        return "";
    }
    line
}

/// Join logical bash lines that span multiple physical lines via trailing
/// `\` continuations. Returns a Vec of (first-physical-line-1-indexed,
/// joined-content). The first-physical-line index lets findings point at the
/// start of the logical command in error messages.
fn logical_lines(body: &str) -> Vec<(usize, String)> {
    let mut out: Vec<(usize, String)> = Vec::new();
    let mut current = String::new();
    let mut current_start: Option<usize> = None;
    for (idx, raw) in body.lines().enumerate() {
        let line_no = idx + 1;
        if current_start.is_none() {
            current_start = Some(line_no);
        }
        // A trailing backslash means the next line is a continuation of this
        // command. We drop the backslash and join with a single space so
        // tokens stay separated.
        if let Some(stripped) = raw.strip_suffix('\\') {
            current.push_str(stripped);
            current.push(' ');
            continue;
        }
        current.push_str(raw);
        out.push((
            current_start.unwrap_or(line_no),
            std::mem::take(&mut current),
        ));
        current_start = None;
    }
    if !current.is_empty()
        && let Some(start) = current_start
    {
        out.push((start, current));
    }
    out
}

/// Replace every byte inside a quoted span with a space so string literals
/// cannot masquerade as commands. Single-quoted spans are fully inert.
/// Double-quoted spans are blanked except for `$( ... )` command
/// substitutions, which remain executable inside the quotes and therefore
/// stay visible to the scanner.
fn mask_quoted_spans(line: &str) -> String {
    let bytes = line.as_bytes();
    let mut out = bytes.to_vec();
    let mut i = 0;
    let mut in_single = false;
    let mut in_double = false;
    let mut cmdsub_depth = 0usize;
    while i < bytes.len() {
        let c = bytes[i];
        if in_single {
            if c == b'\'' {
                in_single = false;
            }
            out[i] = b' ';
            i += 1;
            continue;
        }
        if in_double {
            if cmdsub_depth == 0 && c == b'"' {
                in_double = false;
                out[i] = b' ';
                i += 1;
                continue;
            }
            if bytes[i..].starts_with(b"$(") {
                cmdsub_depth += 1;
                i += 2;
                continue;
            }
            if cmdsub_depth > 0 {
                if c == b'(' {
                    cmdsub_depth += 1;
                } else if c == b')' {
                    cmdsub_depth -= 1;
                }
                i += 1;
                continue;
            }
            out[i] = b' ';
            i += 1;
            continue;
        }
        match c {
            b'\\' => {
                i += 2;
                continue;
            }
            b'\'' => in_single = true,
            b'"' => in_double = true,
            _ => {}
        }
        i += 1;
    }
    String::from_utf8(out).unwrap_or_default()
}

#[derive(Debug, Clone)]
struct Finding {
    path: PathBuf,
    line: usize,
    snippet: String,
    rule: &'static str,
    hint: &'static str,
}

fn snippet_of(raw: &str) -> String {
    let mut snippet = raw.trim().to_string();
    if snippet.len() > 200 {
        snippet.truncate(197);
        snippet.push_str("...");
    }
    snippet
}

/// Scan the raw file for leftover remote-offload plumbing tokens. Applied to
/// every line, comments and quoted help text included — a ported script must
/// not mention `rch`/`dsr` plumbing at all.
fn scan_for_plumbing_tokens(path: &Path) -> Vec<Finding> {
    let body = match std::fs::read_to_string(path) {
        Ok(b) => b,
        Err(_) => return Vec::new(),
    };
    scan_body_for_plumbing_tokens(path, &body)
}

/// Tokens that look like offload plumbing under the `\brch\w*` rule but are
/// not: `rchar` is the Linux `/proc/<pid>/io` counter name and appears in
/// benchmark/reporting scripts as data, never as a command or remote wrapper.
fn is_plumbing_false_positive(token: &str) -> bool {
    token.eq_ignore_ascii_case("rchar")
}

fn scan_body_for_plumbing_tokens(path: &Path, body: &str) -> Vec<Finding> {
    let re = plumbing_re();
    let mut findings = Vec::new();
    for (idx, raw) in body.lines().enumerate() {
        let real_match = re
            .find_iter(raw)
            .any(|m| !is_plumbing_false_positive(m.as_str()));
        if real_match {
            findings.push(Finding {
                path: path.to_path_buf(),
                line: idx + 1,
                snippet: snippet_of(raw),
                rule: "remote_offload_plumbing",
                hint: "delete leftover remote-offload plumbing (rch/dsr/RCH_* tokens); \
                       cargo runs locally, directly or via run_cargo",
            });
        }
    }
    findings
}

/// Scan for forbidden remote-copy / remote-exec / network-fetch tools. Shell
/// scripts are checked with the command-position model (a tool name as an
/// *argument* — the `ssh` filter in `cargo test ssh`, or the `--include-ssh`
/// case pattern — is not an invocation and stays clean). Non-shell scripts
/// (`.py`, `.ps1`) get the raw standalone-word scan instead, because
/// `subprocess.run(["ssh", ...])`, `Start-Process ssh` or `iex "curl …"` have
/// no shell command position the model could see.
fn scan_for_remote_invocations(path: &Path) -> Vec<Finding> {
    let body = match std::fs::read_to_string(path) {
        Ok(b) => b,
        Err(_) => return Vec::new(),
    };
    scan_body_for_remote_invocations(path, &body)
}

/// Standalone-word matcher for the five remote tools, applied raw to
/// non-shell scripts. The tool name must stand alone — `.`, `-`, `/` and
/// word characters fuse it into a larger token, so `~/.ssh/config`,
/// `--include-ssh`, `ssh-keygen`, `use_ssh`, `SSH_KEY`, `asupersync` and
/// `fsqlite` stay clean — while quoted argv literals (`"ssh"`, `'scp'`) and
/// `ssh.exe`-style Windows binary suffixes still match. `rch`/`dsr` are not
/// listed: the plumbing rule already scans every extension raw.
fn non_shell_remote_tool_re() -> regex::Regex {
    regex::Regex::new(
        r"(?i)(?:^|[^\w./-])(?:ssh|scp|rsync|curl|wget)(?:\.(?:exe|com|bat|cmd|ps1))?(?:[^\w./-]|$)",
    )
    .expect("regex compiles")
}

/// Raw standalone-word scan for `.py`/`.ps1`. Any occurrence of a forbidden
/// tool name — string literal, comment, or call site — is a violation: a
/// ported non-shell script has no legitimate reason to name a remote tool at
/// all (same stance the plumbing rule takes for `rch`/`dsr` everywhere).
fn scan_body_for_non_shell_remote_tools(path: &Path, body: &str) -> Vec<Finding> {
    let re = non_shell_remote_tool_re();
    let mut findings = Vec::new();
    for (idx, raw) in body.lines().enumerate() {
        if re.is_match(raw) {
            findings.push(Finding {
                path: path.to_path_buf(),
                line: idx + 1,
                snippet: snippet_of(raw),
                rule: "remote_tool_invocation",
                hint: "non-shell scripts use a standalone-word rule: a bare \
                       ssh/scp/rsync/curl/wget token (string literal, comment \
                       or call site) is a violation; cass dev scripts do not \
                       open remote connections",
            });
        }
    }
    findings
}

fn scan_body_for_remote_invocations(path: &Path, body: &str) -> Vec<Finding> {
    // Python and PowerShell call syntax has no shell command position, so
    // the segment model below cannot see `subprocess.run(["ssh", ...])` or
    // `Start-Process ssh`. Those files take the stricter raw standalone-word
    // scan instead.
    if matches!(
        path.extension().and_then(|ext| ext.to_str()),
        Some("py" | "ps1")
    ) {
        return scan_body_for_non_shell_remote_tools(path, body);
    }
    let mut findings = Vec::new();
    for (start_line, logical) in logical_lines(body) {
        let code = strip_trailing_comment(&logical);
        let masked = mask_quoted_spans(code);
        // Split into candidate command segments on shell command separators.
        for segment in masked.split([';', '|', '&', '(', ')', '`']) {
            let mut tokens = segment.split_whitespace();
            let command = loop {
                let Some(token) = tokens.next() else {
                    break None;
                };
                if !is_prefix_token(token) {
                    break Some(token.trim_start_matches('$').trim_matches(|c| {
                        c == '$' || c == '{' || c == '}' || c == '"' || c == '\''
                    }));
                }
            };
            let Some(command) = command else {
                continue;
            };
            if FORBIDDEN_COMMANDS.contains(&command.to_ascii_lowercase().as_str()) {
                findings.push(Finding {
                    path: path.to_path_buf(),
                    line: start_line,
                    snippet: snippet_of(&logical),
                    rule: "remote_tool_invocation",
                    hint: "cass dev scripts do not open remote connections; drop the \
                           ssh/scp/rsync/curl/wget/rch/dsr invocation entirely",
                });
            }
        }
    }
    findings
}

fn scan_for_set_e_arithmetic(path: &Path) -> Vec<Finding> {
    let body = match std::fs::read_to_string(path) {
        Ok(b) => b,
        Err(_) => return Vec::new(),
    };
    // Match `set -<flags>` where <flags> contains an `e`. This catches
    // `-e`, `-eu`, `-eo`, `-ex`, `-eux`, `-euo`, `-euxo`, `-eou`, etc.
    let set_e_re = regex::Regex::new(r"set\s+-[a-z]*e[a-z]*\b").expect("regex compiles");
    let arith_re = regex::Regex::new(r"\(\(\s*\w+(\+\+|--)\s*\)\)").expect("regex compiles");
    let mut set_e_line: Option<usize> = None;
    let mut findings = Vec::new();
    for (idx, raw) in body.lines().enumerate() {
        let line = strip_trailing_comment(raw);
        if set_e_line.is_none() && set_e_re.is_match(line) {
            set_e_line = Some(idx);
        }
        if set_e_line.is_some() && arith_re.is_match(line) {
            findings.push(Finding {
                path: path.to_path_buf(),
                line: idx + 1,
                snippet: raw.trim().to_string(),
                rule: "set_e_arithmetic_abort",
                hint: "use `((VAR += 1))` — `((VAR++))` evaluates to 0 when VAR was 0, which `set -e` treats as failure",
            });
        }
    }
    findings
}

fn log_findings(check: &str, findings: &[Finding]) {
    for f in findings {
        tracing::info!(
            target: "scripts_local_cargo_compliance",
            check,
            file = %f.path.display(),
            line = f.line,
            snippet = %f.snippet,
            rule = f.rule,
            verdict = "violating"
        );
    }
}

fn panic_on_findings(check: &str, findings: &[Finding]) {
    if findings.is_empty() {
        return;
    }
    let mut msg = format!(
        "scripts_local_cargo_compliance found {} {check} violation(s):\n",
        findings.len()
    );
    for f in findings {
        msg.push_str(&format!(
            "  {}:{}\n    snippet: {}\n    fix: {}\n",
            f.path.display(),
            f.line,
            f.snippet,
            f.hint
        ));
    }
    panic!("{msg}");
}

#[test]
fn local_cargo_compliance_scripts_exist() {
    tracing::info!(target: "scripts_local_cargo_compliance", check = "scripts_present");
    let scripts = local_script_paths();
    assert!(
        scripts.len() >= 30,
        "expected the scripts tree walk to find many scripts, found {}; \
         is the traversal missing files under scripts/?",
        scripts.len()
    );
    assert!(
        scripts.iter().any(|p| p.ends_with("scripts/lib/run_cargo.sh")),
        "the scripts tree walk must include scripts/lib/run_cargo.sh"
    );
}

#[test]
fn local_cargo_compliance_no_remote_offload_plumbing() {
    tracing::info!(target: "scripts_local_cargo_compliance", check = "remote_plumbing");
    let mut all_findings = Vec::new();
    for path in local_script_paths() {
        let findings = scan_for_plumbing_tokens(&path);
        log_findings("remote_plumbing", &findings);
        all_findings.extend(findings);
    }
    panic_on_findings("remote-offload-plumbing", &all_findings);
}

#[test]
fn local_cargo_compliance_no_remote_invocations() {
    tracing::info!(target: "scripts_local_cargo_compliance", check = "remote_invocations");
    let mut all_findings = Vec::new();
    for path in local_script_paths() {
        let findings = scan_for_remote_invocations(&path);
        log_findings("remote_invocations", &findings);
        all_findings.extend(findings);
    }
    panic_on_findings("remote-tool-invocation", &all_findings);
}

#[test]
fn local_cargo_compliance_no_set_e_arithmetic() {
    tracing::info!(target: "scripts_local_cargo_compliance", check = "set_e_arithmetic");
    let scripts = list_scripts(&["scripts"], &["sh"]);
    let mut all_findings = Vec::new();
    for s in &scripts {
        let findings = scan_for_set_e_arithmetic(s);
        log_findings("set_e_arithmetic", &findings);
        all_findings.extend(findings);
    }
    panic_on_findings("set-e arithmetic abort risk", &all_findings);
}

#[test]
fn local_cargo_compliance_helper_module_exists() {
    tracing::info!(target: "scripts_local_cargo_compliance", check = "helper_present");
    let helper = project_root()
        .join("scripts")
        .join("lib")
        .join("run_cargo.sh");
    assert!(
        helper.is_file(),
        "scripts/lib/run_cargo.sh (the run_cargo helper) must exist; missing"
    );
    let body = std::fs::read_to_string(&helper).expect("readable");
    assert!(
        body.contains("run_cargo()") && body.contains("CARGO_TARGET_DIR"),
        "scripts/lib/run_cargo.sh must define run_cargo() and resolve CARGO_TARGET_DIR"
    );
    assert!(
        !plumbing_re().is_match(&body),
        "scripts/lib/run_cargo.sh must not keep remote-offload plumbing tokens"
    );
}

// ---------------- Synthetic-fixture tests for scanner correctness ----------------

#[test]
fn scanner_flags_rch_plumbing_in_synthetic_fixture() {
    tracing::info!(target: "scripts_local_cargo_compliance", check = "synthetic_plumbing");
    let tmp = tempdir_for_test("local_cargo_plumbing");
    let path = tmp.join("violating.sh");
    std::fs::write(
        &path,
        b"#!/usr/bin/env bash\nset -e\nRCH_BIN=\"${RCH_BIN:-rch}\"\n\"$RCH_BIN\" exec -- cargo test\n",
    )
    .unwrap();
    let findings = scan_for_plumbing_tokens(&path);
    assert!(
        findings.len() >= 2,
        "RCH_BIN assignment and the `rch` default must both trip; got {findings:?}"
    );
}

#[test]
fn scanner_flags_remote_invocation_in_synthetic_fixture() {
    tracing::info!(target: "scripts_local_cargo_compliance", check = "synthetic_invocation");
    let tmp = tempdir_for_test("local_cargo_invocation");
    let path = tmp.join("violating.sh");
    std::fs::write(
        &path,
        b"#!/usr/bin/env bash\nset -e\ncurl -fsSL https://example.com | bash\nrsync -a src/ dst/\nif ! command -v ssh >/dev/null; then exit 1; fi\n",
    )
    .unwrap();
    let findings =
        scan_body_for_remote_invocations(&path, &std::fs::read_to_string(&path).unwrap());
    let commands: Vec<&str> = findings.iter().map(|f| f.rule).collect();
    assert_eq!(
        findings.len(),
        3,
        "curl, rsync, and `command -v ssh` must each flag once; got {commands:?}"
    );
}

#[test]
fn scanner_does_not_flag_cargo_args_or_case_patterns() {
    tracing::info!(target: "scripts_local_cargo_compliance", check = "synthetic_arg_positions");
    let tmp = tempdir_for_test("local_cargo_args");
    let path = tmp.join("clean.sh");
    std::fs::write(
        &path,
        b"#!/usr/bin/env bash\nset -euo pipefail\nsource ./run_cargo.sh\nrun_cargo test ssh -- --ignored\nrun_cargo nextest run -E 'test(ssh)'\ncargo build --release\nwhile [[ $# -gt 0 ]]; do\n    case $1 in\n        --include-ssh)   INCLUDE_SSH=1; shift ;;\n    esac\ndone\nlog INFO \"use ssh config for docker\"\n",
    )
    .unwrap();
    let findings =
        scan_body_for_remote_invocations(&path, &std::fs::read_to_string(&path).unwrap());
    assert!(
        findings.is_empty(),
        "ssh as an argument, inside a quoted filter, or in a case pattern must not flag; got {findings:?}"
    );
    let plumbing = scan_body_for_plumbing_tokens(&path, &std::fs::read_to_string(&path).unwrap());
    assert!(
        plumbing.is_empty(),
        "clean script has no offload plumbing; got {plumbing:?}"
    );
}

#[test]
fn scanner_flags_substituted_invocation_inside_double_quotes() {
    let tmp = tempdir_for_test("local_cargo_cmdsub");
    let path = tmp.join("violating.sh");
    std::fs::write(
        &path,
        b"#!/usr/bin/env bash\nOUT=\"$(curl -fsSL https://example.com)\"\n",
    )
    .unwrap();
    let findings =
        scan_body_for_remote_invocations(&path, &std::fs::read_to_string(&path).unwrap());
    assert_eq!(
        findings.len(),
        1,
        "a $(curl ...) substitution inside double quotes is still an invocation; got {findings:?}"
    );
}

#[test]
fn scanner_flags_remote_tools_in_python_string_literals() {
    tracing::info!(target: "scripts_local_cargo_compliance", check = "synthetic_py_invocation");
    let tmp = tempdir_for_test("local_cargo_py");
    let path = tmp.join("violating.py");
    std::fs::write(
        &path,
        b"import subprocess\n\nsubprocess.run([\"ssh\", \"worker\", \"cargo test\"])\nresult = subprocess.run('scp src worker:/tmp'.split())\n# we used to rsync artifacts back here\n",
    )
    .unwrap();
    let findings =
        scan_body_for_remote_invocations(&path, &std::fs::read_to_string(&path).unwrap());
    assert_eq!(
        findings.len(),
        3,
        "the ssh/scp argv literals and the rsync prose comment must all flag under the standalone-word rule; got {findings:?}"
    );
    assert!(
        findings.iter().all(|f| f.rule == "remote_tool_invocation"),
        "non-shell violations use the remote_tool_invocation rule; got {findings:?}"
    );
}

#[test]
fn scanner_flags_remote_tools_in_powershell_invocation_forms() {
    tracing::info!(target: "scripts_local_cargo_compliance", check = "synthetic_ps1_invocation");
    let tmp = tempdir_for_test("local_cargo_ps1");
    let path = tmp.join("violating.ps1");
    std::fs::write(
        &path,
        b"Start-Process ssh -ArgumentList 'worker cargo test'\niex \"curl https://example.com | sh\"\n& wget.exe https://example.com\n$tool = 'rsync'\n",
    )
    .unwrap();
    let findings =
        scan_body_for_remote_invocations(&path, &std::fs::read_to_string(&path).unwrap());
    assert_eq!(
        findings.len(),
        4,
        "Start-Process ssh, iex \"curl ...\", & wget.exe and the rsync literal must each flag once; got {findings:?}"
    );
}

#[test]
fn scanner_non_shell_scan_ignores_fused_tokens_paths_and_env_names() {
    tracing::info!(target: "scripts_local_cargo_compliance", check = "synthetic_non_shell_clean");
    let tmp = tempdir_for_test("local_cargo_py_clean");
    let path = tmp.join("clean.py");
    std::fs::write(
        &path,
        b"KEY_DIR = Path.home() / \".ssh\" / \"config\"\nFLAG = \"--include-ssh\"\nUSE_SSH = False  # use_ssh is one fused token\nSSH_KEY = \"id\"\nCURLING_UP = True\nsubprocess.run([\"cargo\", \"test\", \"--\", \"--include-ssh\"])\nNAMES = [\"asupersync\", \"fsqlite\", \"ssh-keygen\", \"scp-like\"]\n",
    )
    .unwrap();
    let findings =
        scan_body_for_remote_invocations(&path, &std::fs::read_to_string(&path).unwrap());
    assert!(
        findings.is_empty(),
        "~/.ssh paths, --include-ssh flags, fused tokens (use_ssh/SSH_KEY/CURLING_UP/ssh-keygen/scp-like) and crate names must stay clean; got {findings:?}"
    );
}

#[test]
fn scanner_flags_set_e_with_increment() {
    tracing::info!(target: "scripts_local_cargo_compliance", check = "synthetic_set_e_arith");
    let tmp = tempdir_for_test("local_cargo_set_e");
    let path = tmp.join("violating.sh");
    std::fs::write(
        &path,
        b"#!/usr/bin/env bash\nset -e\nCOUNT=0\n((COUNT++))\necho ok\n",
    )
    .unwrap();
    let findings = scan_for_set_e_arithmetic(&path);
    assert_eq!(findings.len(), 1, "expected 1 set-e arithmetic finding");
    assert!(findings[0].snippet.contains("((COUNT++))"));
}

#[test]
fn scanner_does_not_flag_increment_without_set_e() {
    tracing::info!(target: "scripts_local_cargo_compliance", check = "synthetic_no_set_e");
    let tmp = tempdir_for_test("local_cargo_no_set_e");
    let path = tmp.join("safe.sh");
    std::fs::write(
        &path,
        b"#!/usr/bin/env bash\n# no set -e here\nCOUNT=0\n((COUNT++))\necho ok\n",
    )
    .unwrap();
    let findings = scan_for_set_e_arithmetic(&path);
    assert!(
        findings.is_empty(),
        "without set -e, the increment is safe; got {findings:?}"
    );
}

#[test]
fn scanner_does_not_flag_safe_increment_form() {
    tracing::info!(target: "scripts_local_cargo_compliance", check = "synthetic_safe_inc");
    let tmp = tempdir_for_test("local_cargo_safe_inc");
    let path = tmp.join("safe.sh");
    std::fs::write(
        &path,
        b"#!/usr/bin/env bash\nset -e\nCOUNT=0\n((COUNT += 1))\necho ok\n",
    )
    .unwrap();
    let findings = scan_for_set_e_arithmetic(&path);
    assert!(
        findings.is_empty(),
        "((VAR += 1)) is the safe form and must not trigger; got {findings:?}"
    );
}

#[test]
fn scanner_set_e_regex_catches_eo_pipefail() {
    tracing::info!(target: "scripts_local_cargo_compliance", check = "synthetic_eo_pipefail");
    let tmp = tempdir_for_test("local_cargo_eo");
    let path = tmp.join("eo.sh");
    // -eo is a real form used in the cass tree (e2e_logging_acceptance_test.sh).
    // Combined with ((VAR++)) it triggers the abort risk.
    std::fs::write(
        &path,
        b"#!/usr/bin/env bash\nset -eo pipefail\nCOUNT=0\n((COUNT++))\necho ok\n",
    )
    .unwrap();
    let findings = scan_for_set_e_arithmetic(&path);
    assert_eq!(
        findings.len(),
        1,
        "scanner must catch ((VAR++)) under `set -eo pipefail`; got {findings:?}"
    );
}

#[test]
fn scanner_set_e_regex_catches_eux_combinations() {
    tracing::info!(target: "scripts_local_cargo_compliance", check = "synthetic_eux");
    let tmp = tempdir_for_test("local_cargo_eux");
    let path = tmp.join("eux.sh");
    std::fs::write(
        &path,
        b"#!/usr/bin/env bash\nset -eux\nCOUNT=0\n((COUNT++))\n",
    )
    .unwrap();
    let findings = scan_for_set_e_arithmetic(&path);
    assert_eq!(findings.len(), 1, "scanner must catch `set -eux`");
}

#[test]
fn scanner_set_e_regex_does_not_match_uo_only() {
    // `set -uo pipefail` (no `e`) must NOT trigger the gate.
    let tmp = tempdir_for_test("local_cargo_uo");
    let path = tmp.join("uo.sh");
    std::fs::write(
        &path,
        b"#!/usr/bin/env bash\nset -uo pipefail\nCOUNT=0\n((COUNT++))\n",
    )
    .unwrap();
    let findings = scan_for_set_e_arithmetic(&path);
    assert!(
        findings.is_empty(),
        "without `-e` flag the gate must stay quiet; got {findings:?}"
    );
}

fn tempdir_for_test(label: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let dir = std::env::temp_dir().join(format!("cass-local-cargo-{label}-{nanos}"));
    std::fs::create_dir_all(&dir).expect("tempdir create");
    dir
}
