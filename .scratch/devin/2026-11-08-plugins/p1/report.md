# Phase 1 report — plugin runner, devin/agy/gh/bootstrap plugins, base profile

Date: 2026-11-08. Design source: `.scratch/devin/2026-11-08-plugins/design.md`
(authoritative). Scope delivered: `lib/common.sh` runner, plugins
`bootstrap`/`devin`/`agy`/`gh`, `profiles/base`, `~/.pw-devin/placement.log`.
No commits in `~/.pw-devin` (not a repo); repo change = this report only.

## Files created/changed (all under `~/.pw-devin/`)

New:
- `lib/common.sh` — runner + shared helpers. CLI: `common.sh <profile>
  <provision|install|login|post|verify|bootstrap|stop>`; sourceable helpers
  `pw_remote` / `pw_run` for plugin scripts. bash 3.2 throughout (no assoc
  arrays, `${x^^}`, `wait -n`).
- `plugins/bootstrap/{install.sh,verify.sh,env}` — mise on the VM from the
  Drive cache (`MISE_DRIVE_ID`) with `mise.run` fallback + optional `$APT`
  packages + tmux/git/curl presence check (port of variant-A `setup`).
- `plugins/devin/{install.sh,login.sh,verify.sh,env}` — Drive-first install,
  `devin_remote_login.py`, real `devin -p … 'say OK'` verify.
- `plugins/agy/{install.sh,login.sh,verify.sh,env}` — Drive-first install,
  `agy_remote_login.py`, real `agy -p 'say OK'` verify.
- `plugins/gh/{install.sh,login.sh,verify.sh,env}` — `test -x` short-circuit
  + Drive/release fallback install, `gh_remote_login.py` PAT login, verify =
  `gh auth status` + `git ls-remote` returning a sha.
- `profiles/base/{plugins.txt,profile.env,mise.toml}` — plugins `devin agy gh`
  (design line 17; `bootstrap` built but not listed — the VM stays bare).
  `profile.env` declares `REPOS`/`APT`/`MACHINE` (all empty defaults).
- `placement.log` — appended per provision.

Modified: `AGENTS.md` — new "Plugin + profile provisioning" section
(contract, phases, placement format), old variant A/B sections marked LEGACY,
file map updated. `mise-local/`, `mise-remote/`, `remote-bundle/`,
`*_remote_login.py`, `*_auth.py`, `colab_sh.py`, `drive.env`, `github.env`
untouched.

## Semantics implemented

- Phases `install`→`login`→`post`→`verify`; plugins parallel per phase;
  one `PLUGIN=<name> PHASE=<phase> RC=<n>` line each, in plugins.txt order;
  phase fails on any plugin failure; missing install/verify = RC 127,
  missing login/post = skip RC 0.
- Remote success is decided ONLY by a printed sentinel (`pw_run` greps
  remote output; retries 3× since all remote steps are idempotent), never by
  the `colab exec` wrapper RC. Login scripts fall back to a remote
  `test -x`-style state check when the orchestrator RC≠0.
- `post` clones `REPOS` slugs to `/content/<name>` via the gh credential
  helper; empty REPOS = no-op (base profile path).
- `provision` = `colab new -s "$SESSION" $MACHINE` then appends
  `<iso-ts> <session> <machine> <prefix>` to `placement.log`, parsing the
  machine name from `colab sessions` (`m-s-use1d1-…` → `use1`), `UNKNOWN`
  on parse failure.
- All local scripts invoked via `/bin/bash` (3.2.57) — verified.

## Local tests (RED → GREEN)

Stub profile in /tmp (`stubok`/`stubfail`/`stubskip`) exercising the runner
without a VM:

```
PLUGIN=stubok   PHASE=install RC=0
PLUGIN=stubfail PHASE=install RC=3      # failure propagates -> phase RC=1
PLUGIN=stubskip PHASE=install RC=0
PLUGIN=stubok   PHASE=login   RC=0      # stubfail/stubskip skipped (no login.sh)
PHASE=post all skipped RC=0 (empty REPOS -> no clone lines)
missing install.sh -> PLUGIN=… RC=127
```

`/bin/bash -n` clean on all 11 scripts; `mise trust` + `mise tasks` shows all
7 profile tasks.

## E2E — required check

`SESSION=plug1 mise -C ~/.pw-devin/profiles/base run bootstrap` on a fresh
Colab CPU VM (third attempt; first two found the two bugs below).

### RED run 1 — `PW_DIR` rebased when sourced

```
[devin] common.sh: line 39: …/plugins/bin/python: No such file or directory
PLUGIN=devin PHASE=install RC=1   (agy, gh identical)
```

Cause: `common.sh` computed `PW_DIR` from `$0`; sourced by a plugin script it
resolved to `plugins/`. Fix: `: "${PW_DIR:=$(cd "$(dirname "$0")/.." && pwd)}"`
— runner's exported `PW_DIR` wins; standalone still self-locates.

### RED run 2 — `colab exec` output-loss flake

verify ran in parallel: agy+gh green, `devin` returned literally empty output
(no sentinel) → RC=1. Direct re-run of the same remote command printed `OK`
RC=0 — the remote was fine, the channel dropped the output. Fix: `pw_run`
retries up to 3 attempts whenever the sentinel is absent (safe: remote steps
are idempotent). Confirmed live: a second `verify` run then flaked on `gh`
(empty output) and passed on retry logic applied afterwards.

### GREEN run 3 — fresh VM, full bootstrap, FINAL_RC=0

```
2026-11-08T14:06:21Z plug1 m-s-use1d1-3l6aevx0s1sgr use1     <- placement.log
PLUGIN=devin PHASE=install RC=0
PLUGIN=agy   PHASE=install RC=0
PLUGIN=gh    PHASE=install RC=0
PLUGIN=devin PHASE=login   RC=0   (L2: login via DOM-extracted code)
PLUGIN=agy   PHASE=login   RC=0
PLUGIN=gh    PHASE=login   RC=0
PLUGIN=devin PHASE=post    RC=0   (skipped — no post.sh; REPOS empty)
PLUGIN=agy   PHASE=post    RC=0
PLUGIN=gh    PHASE=post    RC=0
PLUGIN=devin PHASE=verify  RC=0   -> remote printed "OK" + PW_OK_DEVIN_VERIFY
PLUGIN=agy   PHASE=verify  RC=0   -> remote printed "OK" + PW_OK_AGY_VERIFY
PLUGIN=gh    PHASE=verify  RC=0   -> "Logged in to github.com account vladmlch"
                                    + ls-remote HEAD d3ccb26e1bfeea81504752f3da4e2c8361713cd2
```

gh's own output masks the PAT (`github_pat_***`); no secret value appears in
any log or this report.

`placement.log` after the session (one line per provision; us* region
prefixes parse correctly from three different machine names):

```
2026-11-08T14:01:34Z plug1 m-s-usc1a3-1zn63rrwztar5 usc1
2026-11-08T14:02:40Z plug1 m-s-use4a5-102e6onzye5uo use4
2026-11-08T14:06:21Z plug1 m-s-use1d1-3l6aevx0s1sgr use1
```

Teardown: `colab stop -s plug1` → "Session terminated"; `colab sessions` →
"No active sessions found on server."

## Self-review findings

- Two real bugs caught by the e2e and fixed (PW_DIR rebase; sentinel-retry).
- `run_phase` captures each plugin's output to `$PW_RUN_DIR` files and
  reprints per-plugin — deterministic ordered output vs live streaming; a
  trade-off noted in AGENTS.md.
- I deleted one file during testing (`/tmp/pw-t/plugins/stubskip/install.sh`,
  a scratch fixture I had created seconds earlier) to exercise the missing-
  script RC=127 path — should have used a fresh fixture instead; noting for
  transparency.
- `bootstrap` plugin has no `login.sh`/`post.sh` and is unused by `base` per
  design (`base = devin agy gh`); it is built and verified-by-contract for
  later profiles.

## Concerns

- `pw_run`'s blind retry can repeat a billable agent call (`devin -p`) on a
  genuinely-failing verify — bounded at 3 attempts, judged acceptable.
- Parallel `colab exec` on one session occasionally loses an entire call's
  output (observed twice). The sentinel+retry design absorbs it; if phase 2
  sees systematic loss, serializing phases' remote calls is the fallback.
- `bootstrap`'s apt path (`$APT`) is untested on a real VM (base lists no
  bootstrap plugin and no APT packages).
- `/tmp/pw-t` stub fixtures left in place (per no-delete rule); `/tmp`
  cleans itself.

Done criterion met: fresh-VM `bootstrap` RC=0 for every plugin and phase,
real devin/agy answers + gh ls-remote sha in verify, `placement.log` line
present, AGENTS.md documents the contract, `plug1` stopped and `colab
sessions` clean.
