# cass doctor fixture suite — pass-1

> Phase 9 deliverable. One fixture per representative failure mode plus the
> round-trip regression test asserting `corrupt → doctor --fix → undo → byte-identical`.
>
> Pass-1 ships **a representative slice** of fixtures (5 of the 136 catalogued
> FMs). Each fixture is self-contained and can be invoked from
> `tests/doctor_fixtures_round_trip.rs` (added in this pass).
>
> Pass-2 extends to all P0/P1 FMs (~30 fixtures); pass-3 covers P2/P3.

## Layout

```
tests/doctor_fixtures/
├── README.md                                    (this file)
├── fm-pass1-foundation/
│   ├── corrupt.rs                              corruption helper (writes a known-bad state)
│   └── expected_after_fix.txt                  text describing the post-fix state
├── fm-storage-stale-wal-shm/
│   ├── corrupt.rs
│   └── expected_after_fix.txt
├── fm-tui-state-json-corrupt/
│   ├── corrupt.rs
│   └── expected_after_fix.txt
└── fm-cli_robot-schema-version-missing/
    └── README.md                               (regression spec; no on-disk fixture needed)
```

## Round-trip contract

Every fixture must satisfy this round-trip in the regression test:

```
1. corrupt(data_dir)                        → on-disk state matches expected corruption
2. cass doctor                              → finding emitted with the FM's stable id
3. cass doctor --fix                        → mutation applied; report.json::operation_outcome.kind == "fixed"
4. cass doctor                              → exit 0, no findings
5. cass doctor undo <run-id>                → byte-identical to step 1's corrupted state
```

Step 5 is the strongest invariant: undo must be a true inverse, not a
"best-effort restore."

## Pass-1 scope

In pass-1 the round-trip is exercised against the new pass-1 modules
(doctor_runs, doctor_chokepoint, doctor_undo, doctor_robot_docs) directly
because the dispatch wiring into `run_doctor_impl()` is deferred to pass-2.
The fixture infrastructure is in place; pass-2 adds `cass doctor` invocation
in steps 2-5.

For pass-1, the fixtures mainly serve to:

1. Document what "corruption" looks like for each FM (so the chokepoint
   integration knows what state to repair from).
2. Provide a smoke target for `cargo test --test doctor_fixtures_round_trip`.
3. Pin the fixture layout for future passes.
