# Milestone Coordination Ledger

This record contains milestone gate evidence only. It does not contain runtime
configuration, credentials, source-object paths, or generated data.

| milestone | status | predecessor_integration_hash | integration_hash | l1_status | l2_status | l3_status | l4_status | review_status | worker_branch | worktree_root | file_ownership | worker_checkpoint_hash | evidence_paths | owner | timestamp_utc | blockers |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| M1 | PASSED | - | `fe6a2cd6d9d73e136453f921b593656bc1bab326` | PASSED | PASSED | PASSED | PASSED | PASSED | `m1-acceptance-final` | isolated M1 worktrees | Frozen grid SDK, ISEA4H conformance, split/Web callers, gate scripts | `fe6a2cd6d9d73e136453f921b593656bc1bab326` | Focused SDK/caller tests, encoder/split and Web regression, static/build, performance smoke, and real AOI evidence recorded in the M1 acceptance ledger | Codex | 2026-07-15T00:00:00Z | - |
| M2 | PASSED | `fe6a2cd6d9d73e136453f921b593656bc1bab326` | `8df4269fce051787546a68c2dc6aa9a6f71cef4b` | PASSED | PASSED | PASSED | PASSED | PASSED | reviewed M2 worker branches | `.claude/worktrees/m2-final-clean2` | Partition domain contracts/DDL/store, supplied-COG Ray runner, object store, partition route, M2 gates | `8df4269fce051787546a68c2dc6aa9a6f71cef4b` | Focused tests: 65 passed; Web non-real regression: 305 passed, 8 deselected; Ruff/scoped Mypy/frontend build: exit 0; real OpenGauss/MinIO/Ray gate: 8 passed, 1 warning, no skipped scenario | Codex | 2026-07-15T18:35:00+08:00 | - |
| M3 | READY | `8df4269fce051787546a68c2dc6aa9a6f71cef4b` | - | PENDING | PENDING | PENDING | PENDING | PENDING | - | create new isolated worktrees from the M2 hash | Quality/publication workflows, normalized dataset/quality APIs, export, legacy removal, M3 real gate | - | `docs/M3_KICKOFF_HANDOFF.md` | Codex | 2026-07-15T18:35:00+08:00 | M3 implementation has not started. |
| M4 | BLOCKED | - | - | - | - | - | - | - | - | - | - | - | Requires M3 PASSED row and integration hash | Codex | 2026-07-15T05:00:00Z | M3 not passed |
| M5 | BLOCKED | - | - | - | - | - | - | - | - | - | - | - | Requires M4 PASSED row and integration hash | Codex | 2026-07-15T05:00:00Z | M4 not passed |
