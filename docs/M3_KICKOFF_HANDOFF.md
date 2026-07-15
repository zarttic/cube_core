# M3 Kickoff Handoff

> Historical handoff. M3 passed at `cf449555b9ec68dcfb620b8fd023d38ac138a204`
> and the M1-M3 integration gate passed at `8646c1fa6c0aaa36f6d5c60c81d171ed14af6456`.
> This document is retained for provenance only. Current contracts are defined
> by code, tests, `AGENTS.md`, and the coordination ledger.

## Historical Status

- M1 is passed at `fe6a2cd6d9d73e136453f921b593656bc1bab326`.
- M2 is passed at `8df4269fce051787546a68c2dc6aa9a6f71cef4b`.
- M3 was `READY` when this handoff was written. It is now passed; do not use
  this document to restart or redefine M3 work.

Start every M3 worktree from the exact M2 integration hash above. Do not use an
older M2 acceptance worktree, the coordination checkout's dirty state, or an
unreviewed worker branch as the base.

## M2 Handoff

M2 provides the versioned dataset partition domain, OpenGauss structural DDL,
atomic output-version/current-pointer update, outbox operations, supplied-COG
Ray execution, and MinIO object lifecycle. Its real acceptance used actual
OpenGauss, MinIO, and Ray. The final M2 real gate reported `8 passed, 1 warning`
with no skipped M2-real scenario.

The M3 plan consumes, without renaming or redefining, these M2 modules:

- `cube_web.services.partition_contracts`
- `cube_web.services.partition_domain_schema`
- `cube_web.services.partition_domain_store`
- `cube_web.services.partition_object_store`

Before implementing M3 business behavior, add focused compatibility tests that
exercise the M3 plan's required M2 read/list/count methods, transaction context,
outbox lease methods, quality/publication DDL constraints, and current-pointer
fields against the `8df4269` base. M3 must not silently modify M2-owned domain
schema, store, or contracts to compensate for a failed prerequisite: record the
gap as an M2 defect and repair it in an M2-scoped worktree first.

## M3 Scope

Implement only the work assigned to
`docs/superpowers/plans/2026-07-14-m3-dataset-quality-publication.md`:

1. Normalized, dataset-scoped read APIs with stable pagination and sorting.
2. Durable quality allocation, leased execution, result/error persistence, and
   outbox-driven automatic runs.
3. Complete parameterized CSV/JSON error exports, including filtered-count
   equality with OpenGauss queries.
4. Run-scoped immutable Warn approvals.
5. Immutable publication snapshots and exact-ID withdrawal through a real
   publication gateway.
6. The M3-owned legacy quality route/store/coupling removal after normalized
   consumers are switched.

Publication states are frozen as
`publishing|active|withdrawing|failed|withdrawn`. Dataset `publish_status` is
`unpublished|publishing|active|withdrawing|failed|withdrawn`; `published` is
forbidden.

M4 frontend implementation and M5 cleanup/acceptance are explicitly out of
scope. Stop after M3 passes; do not start M4 automatically.

## Required Process

- Create isolated, non-overlapping worker worktrees from the M2 hash.
- Preserve the M3 plan's file ownership: Opus owns transactional quality and
  publication behavior; Sonnet owns bounded API/rules/export slices; Haiku only
  owns the planned mechanical legacy removal under review.
- Complete self-review, independent review, adversarial verification, and final
  integration review. Assemble reviewed changes without an integration commit
  until all gates pass, then create exactly one local commit:
  `feat: add dataset quality and publication`.
- Do not push, open a PR, or put credentials, DSNs, source object URLs, or local
  paths in commits, evidence, or documentation.

## Gate Requirements

L1 covers M3 contracts and failure paths. L2 runs the canonical encoder, split,
and Web regressions. L3 runs Ruff, Mypy, all three Python package builds, and
the frontend production build. L4 is only:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 \
  cube_web/scripts/run_m3_quality_publication_gate.py
```

The L4 runner must execute non-skipping `m3_real` scenarios using actual
OpenGauss, MinIO, Ray, and the publication gateway. It must prove full and
filtered CSV/JSON export counts equal the parameterized OpenGauss count,
snapshot locking, Warn approval validity, and exact-ID withdrawal. A missing
environment variable, unreachable service, skip, deselection, xfail, mock, or
local substitute is a blocking failure.

## Runtime Notes

`CUBE_WEB_ENV_FILE` is interpreted by Python configuration code; scripts that
need shell environment variables must load the local file in a controlled shell
without printing it. The local OpenGauss database name is validated by reset
tools, and destructive domain reset requires `CUBE_WEB_ENV=development` plus
the explicit dangerous-reset flag. Read the current root `AGENTS.md` before
running a real gate for the worktree, MinIO, Ray, cache, and secret-handling
rules.
