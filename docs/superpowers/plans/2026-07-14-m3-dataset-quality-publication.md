# Dataset Quality and Publication Milestone 3 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build dataset-scoped read APIs, durable normalized quality runs, complete paginated/error export APIs, run-specific Warn approval, and immutable publication snapshots on top of Milestone 2's versioned partition domain.

**Architecture:** Milestone 3 consumes Milestone 2's `PartitionDomainStore`, immutable output versions, current pointers, transaction context, and outbox lease operations without redefining them. Focused M3 repositories execute quality and publication SQL inside the M2 transaction; a durable outbox dispatcher allocates automatic runs, a separately leased worker executes snapshotted rules and persists errors in batches, FastAPI routes expose one normalized dataset/quality contract, and publication validates the exact current `(dataset_id, output_version, quality_run_id)` snapshot under lock before activation.

**Tech Stack:** Python 3.11, FastAPI 0.115+, Pydantic 2, psycopg 3, OpenGauss PostgreSQL-compatible SQL, MinIO 7.2+, Ray 2.55.x, pytest 8, Ruff, mypy

## Current Contract Amendment (2026-07-15)

There is no external data-service publication platform. For this repository,
publication means that a quality-authorized dataset output is queryable and
usable through the normalized OpenGauss-backed dataset APIs. This amendment
overrides every earlier reference in this plan to a publication gateway,
gateway configuration, gateway reconciliation, or gateway idempotency.

- `publish_dataset` validates the same immutable current
  `(dataset_id, output_version, quality_run_id)` tuple described below and
  commits one `partition_publications` row with `status='active'` in that
  transaction. `service_version_id` is the immutable `output_version` used by
  query consumers; no external side effect is required.
- `withdraw_publication` changes only the exact owned publication row to
  `status='withdrawn'`, records actor/time/reason, and retains all history.
  A withdrawn publication is not queryable as an active dataset output.
- M3 code produces `active` and `withdrawn` directly. Legacy lifecycle values
  remain only as schema-compatible historical values and must not introduce a
  worker or external gateway.
- The M3 real gate must prove actual MinIO/Ray/OpenGauss partitioning,
  normalized quality/error export counts, and exact OpenGauss
  `active -> withdrawn` state transitions. It must not require a gateway URL
  or gateway probe.

## Global Constraints

- This is a development-stage destructive refactor: do not migrate old quality reports and do not preserve old quality routes, response structures, PDF/TXT exports, run-directory identity, or compatibility adapters.
- Canonical milestone order is strict `M1 -> M2 -> M3 -> M4 -> M5`; do not start M3 until M2 is merged and all four M2 quality levels pass.
- M1 owns `requested_grid_level`, `GridCell.grid_level`, and MGRS `topology_code`; M2 owns `partition_contracts.py`, domain DDL, `PartitionDomainStore`, transaction creation, outbox leasing, immutable output versions, and dataset current pointers. M3 consumes those interfaces unchanged.
- M3 owns the normalized dataset, quality, export, Warn approval, and publication service/API contracts. M4 must consume these API shapes unchanged and owns all frontend/router integration; M5 is verification and historical cleanup only and adds no product behavior.
- Every quality run binds exactly one immutable output version. A manual request without a selector resolves the current version while holding the dataset lock; explicit historical-version selection is admin-only and accepts only a completed version belonging to that dataset.
- Output-version switching remains an M2 transaction: it atomically sets dataset `quality_status='pending'`, clears `current_quality_run_id`, resets `quality_error_count`/`quality_warning_count`, and inserts unique `(dataset_id, output_version, event_type)` outbox event `output-version.completed`.
- Automatic dispatch always creates exactly one automatic run per completion event, even if a manual run for that version already exists. `trigger_event_id UNIQUE` makes event redelivery idempotent; manual and automatic runs remain separate monotonic sequences, and the higher same-version sequence is authoritative.
- Manual and automatic allocation lock the dataset, increment the dataset-wide `quality_sequence`, and create the run in one transaction. Only a run for the current output may become `current_quality_run_id`; historical-version runs remain history.
- Completion always terminalizes the requested run. It updates dataset current-quality fields only when the run's output is still current and no greater `quality_sequence` exists for that same output version.
- Quality terminal semantics are exact: findings from any mandatory rule produce `fail`; findings only from optional rules produce `warn`; no findings produce `pass`; any unhandled rule, engine, database, MinIO, Ray, or other dependency exception makes the whole run `error` regardless of rule optionality.
- On execution error, preserve every error already written, set `results_complete=false`, persist the full execution error, and finalize rule/run summaries only after all successful error batches are committed.
- Each run snapshots `rule_set_version` plus every rule's code, name, applicability, mandatory flag, parameters, and implementation version. Later rule configuration changes cannot reinterpret history.
- Persist every quality error without truncation. Pagination limits only HTTP responses; they never cap storage, summaries, database counts, or exports.
- CSV/JSON exports stream every database match, never the visible page. No filters means all run errors; filters are a fixed whitelist and use parameterized SQL; filenames include dataset code, quality time, and run ID, with `filtered` for filtered exports.
- Warn approval is immutable, admin-only, run-specific, and records approver, time, reason, and rule-set version. Approval is valid only while that exact Warn run is the dataset's current run for the same current output.
- Publication authorization locks and validates the exact immutable `(dataset_id, output_version, quality_run_id)` tuple in one transaction. The output must be completed and current, the run must equal `current_quality_run_id`, bind that output, have the highest same-version sequence, and be `pass` or `warn` with that run's approval. `pending`, `running`, `fail`, `error`, and `cancelled` never publish.
- A prior pass or approved Warn run cannot authorize publication after a newer run becomes current. Explicit request IDs never bypass current-pointer checks.
- Publication records are immutable snapshots. A new output or quality run cannot alter an in-progress, active, failed, or withdrawn publication. Repeating publish for an existing in-progress/active tuple returns that record; publishing after withdrawal creates a new `publication_id`.
- Withdrawal requires the exact dataset-owned `publication_id`, locks that record, deactivates its exact service version, and writes actor/time/reason without deleting output, quality, approval, or publication history.
- Delete `quality_reports`, split data-type history/latest/report routes, PDF/TXT report exports, `QualityReportStore`, run-directory quality execution, and batch/ingest quality coupling in the same milestone after normalized consumers switch; provide no compatibility layer.
- API pagination is `page >= 1`, `1 <= page_size <= 500`, `sort_order in {'asc','desc'}`, with a resource-specific `sort_by` whitelist. Responses are exactly `{"items": [...], "total": int, "page": int, "page_size": int}` and append a stable primary-key tie-breaker.
- Logs and errors include applicable `dataset_id`, `output_version`, `quality_run_id`, and `publication_id`; never expose DSNs, credentials, access keys, or internal object-store secrets.
- Use Python 3.11 and type annotations on public functions. Do not add another ORM, queue, cache, or export library.
- Each implementation slice uses an isolated worktree and passes implementer self-test, independent Sonnet/Opus review, adversarial verification, and final Opus execution before integration.
- M3 ends in one local commit only, `feat: add dataset quality and publication`; do not make per-task commits, push, create a PR, or stage files outside M3 ownership.
- Real gates must use actual OpenGauss, MinIO, and Ray—not mocks, SQLite, or in-memory substitutes. Missing infrastructure, a skipped scenario, or a blocked scenario fails M3. All real quality/export/publication scenarios must pass, and streamed export counts must exactly equal OpenGauss counts for the same filters.

---

## Frozen Milestone 2 Preconditions

M3 imports these repo-relative M2 modules and does not move or rename them:

```python
from contextlib import AbstractContextManager
from typing import Any

import psycopg

from cube_web.services.partition_contracts import (
    BandInput,
    DatasetInput,
    OutputIdentity,
    PartitionDatasetResult,
    SourceAssetInput,
    StrictPartitionRequest,
    make_output_id,
    make_output_version,
)
from cube_web.services.partition_domain_store import (
    InMemoryPartitionDomainStore,
    OpenGaussPartitionDomainStore,
    PartitionDomainStore,
    get_partition_domain_store,
    set_partition_domain_store,
)

domain_store: PartitionDomainStore = get_partition_domain_store()
if not isinstance(domain_store, OpenGaussPartitionDomainStore):
    raise RuntimeError("M3 transactional quality/publication requires OpenGaussPartitionDomainStore")
transaction: AbstractContextManager[psycopg.Connection[Any]] = domain_store.transaction()
events: list[dict] = domain_store.claim_outbox("quality-dispatcher-1", limit=100)
domain_store.acknowledge_outbox(event_id)
domain_store.retry_outbox(event_id, error, available_at=retry_at)
```

M2 owns the transaction implementation and outbox claim/ack/retry SQL. Because only `OpenGaussPartitionDomainStore` exposes `transaction()`, M3 obtains the configured store through `require_open_gauss_domain_store() -> OpenGaussPartitionDomainStore`, which performs the `isinstance` check shown above. M3 defines only a type alias for the supplied connection; it does not introduce a `PartitionDomainTransaction` class:

```python
from typing import Any, TypeAlias
import psycopg

PartitionDomainTransaction: TypeAlias = psycopg.Connection[Any]
```

### Required M2 amendment before M3 starts

Authoritative M2 commit `dc64c31` freezes module/class/outbox names, but its prose at the quality/publication DDL boundary and read surface is not yet executable enough for M3. Before M2 is considered merged/gated, revise the M2 plan and its implementation to produce the exact consumed surface below. This is an M2 prerequisite, not permission for M3 to edit `partition_domain_schema.py` or append methods silently.

The revised `PartitionDomainStore`, `InMemoryPartitionDomainStore`, and `OpenGaussPartitionDomainStore` must expose matching behavior for these reads; OpenGauss applies parameterized SQL and the in-memory store provides parity:

```python
def get_dataset(self, dataset_id: str) -> dict[str, Any] | None: ...
def list_datasets(
    self, *, keyword: str | None, data_type: str | None, product_type: str | None,
    batch_id: str | None, grid_type: str | None, partition_status: str | None,
    quality_status: str | None, publish_status: str | None,
    time_start: datetime | None, time_end: datetime | None,
    limit: int, offset: int, sort_by: str, sort_order: Literal["asc", "desc"],
) -> list[dict[str, Any]]: ...
def count_datasets(
    self, *, keyword: str | None, data_type: str | None, product_type: str | None,
    batch_id: str | None, grid_type: str | None, partition_status: str | None,
    quality_status: str | None, publish_status: str | None,
    time_start: datetime | None, time_end: datetime | None,
) -> int: ...
def get_output_version(self, dataset_id: str, output_version: str) -> dict[str, Any] | None: ...
def list_assets(self, dataset_id: str, output_version: str | None = None, *, limit: int, offset: int, sort_by: str, sort_order: Literal["asc", "desc"]) -> list[dict[str, Any]]: ...
def count_assets(self, dataset_id: str, output_version: str | None = None) -> int: ...
def list_bands(self, dataset_id: str, output_version: str | None = None, *, limit: int, offset: int, sort_by: str, sort_order: Literal["asc", "desc"]) -> list[dict[str, Any]]: ...
def count_bands(self, dataset_id: str, output_version: str | None = None) -> int: ...
def list_tiles(self, dataset_id: str, output_version: str | None = None, *, limit: int, offset: int, sort_by: str, sort_order: Literal["asc", "desc"]) -> list[dict[str, Any]]: ...
def count_tiles(self, dataset_id: str, output_version: str | None = None) -> int: ...
def list_indexes(self, dataset_id: str, output_version: str | None = None, *, limit: int, offset: int, sort_by: str, sort_order: Literal["asc", "desc"]) -> list[dict[str, Any]]: ...
def count_indexes(self, dataset_id: str, output_version: str | None = None) -> int: ...
def list_grid_cells(self, dataset_id: str, output_version: str | None = None, *, limit: int, offset: int, sort_by: str, sort_order: Literal["asc", "desc"]) -> list[dict[str, Any]]: ...
def count_grid_cells(self, dataset_id: str, output_version: str | None = None) -> int: ...
def list_publications(self, dataset_id: str, *, limit: int, offset: int, sort_by: str, sort_order: Literal["asc", "desc"]) -> list[dict[str, Any]]: ...
def count_publications(self, dataset_id: str) -> int: ...
def output_has_publication_reference(self, dataset_id: str, output_version: str) -> bool: ...
```

`sort_by` is still validated against M3's route-specific constant maps before these methods are called; the M2 OpenGauss implementation maps the already-validated symbolic value to a fixed SQL expression and always appends the resource primary key. `resolve_output_version()` remains M2's ownership validator/default-current resolver. The existing unpaginated signatures in `dc64c31` must be revised rather than overloaded ambiguously.

The revised M2 `partition_domain_schema.py` must render these exact foundational columns and constraints, which M3 consumes without DDL edits:

```sql
CREATE TABLE partition_quality_results (
  quality_run_id UUID NOT NULL REFERENCES partition_quality_runs(quality_run_id) ON DELETE CASCADE,
  dataset_id TEXT NOT NULL,
  output_version TEXT NOT NULL,
  rule_code TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('pass','warn','fail','error')),
  finding_count BIGINT NOT NULL DEFAULT 0 CHECK (finding_count >= 0),
  error_count BIGINT NOT NULL DEFAULT 0 CHECK (error_count >= 0),
  warning_count BIGINT NOT NULL DEFAULT 0 CHECK (warning_count >= 0),
  metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
  execution_error TEXT,
  started_at TIMESTAMPTZ NOT NULL,
  completed_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (quality_run_id, rule_code),
  FOREIGN KEY (dataset_id, output_version, quality_run_id)
    REFERENCES partition_quality_runs(dataset_id, output_version, quality_run_id)
);

CREATE TABLE partition_quality_errors (
  quality_error_id UUID PRIMARY KEY,
  quality_run_id UUID NOT NULL,
  dataset_id TEXT NOT NULL,
  output_version TEXT NOT NULL,
  rule_code TEXT NOT NULL,
  source_asset_id TEXT,
  tile_id TEXT,
  index_id TEXT,
  output_id TEXT,
  row_number BIGINT,
  field_name TEXT,
  error_code TEXT NOT NULL,
  message TEXT NOT NULL,
  context JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (dataset_id, output_version, quality_run_id)
    REFERENCES partition_quality_runs(dataset_id, output_version, quality_run_id)
);

CREATE TABLE partition_quality_warn_approvals (
  approval_id UUID PRIMARY KEY,
  dataset_id TEXT NOT NULL,
  output_version TEXT NOT NULL,
  quality_run_id UUID NOT NULL UNIQUE,
  rule_set_version TEXT NOT NULL,
  approved_by TEXT NOT NULL,
  approved_at TIMESTAMPTZ NOT NULL,
  reason TEXT NOT NULL CHECK (length(btrim(reason)) BETWEEN 1 AND 2000),
  FOREIGN KEY (dataset_id, output_version, quality_run_id)
    REFERENCES partition_quality_runs(dataset_id, output_version, quality_run_id)
);

CREATE TABLE partition_publications (
  publication_id UUID PRIMARY KEY,
  dataset_id TEXT NOT NULL,
  output_version TEXT NOT NULL,
  quality_run_id UUID NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('publishing','active','withdrawing','withdrawn','failed')),
  desired_action TEXT NOT NULL CHECK (desired_action IN ('activate','withdraw')),
  service_version_id TEXT,
  requested_by TEXT NOT NULL,
  requested_at TIMESTAMPTZ NOT NULL,
  activated_at TIMESTAMPTZ,
  failure TEXT,
  withdrawn_by TEXT,
  withdrawn_at TIMESTAMPTZ,
  withdrawal_reason TEXT,
  attempt_count INT NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  claimed_at TIMESTAMPTZ,
  claimed_by TEXT,
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (dataset_id, output_version, quality_run_id)
    REFERENCES partition_quality_runs(dataset_id, output_version, quality_run_id)
);
CREATE UNIQUE INDEX uq_partition_publication_live_snapshot
  ON partition_publications(dataset_id, output_version, quality_run_id)
  WHERE status IN ('publishing','active','withdrawing');
CREATE INDEX idx_partition_publication_claim
  ON partition_publications(status, available_at, claimed_at, created_at);
```

M2 must also add `UNIQUE(dataset_id, output_version, quality_run_id)` to `partition_quality_runs`, retain `trigger_event_id UUID NULL UNIQUE` without an outbox FK, and retain `trigger`, `requested_by`, `attempt_count`, `available_at`, `claimed_at`, `claimed_by`, `last_error`, `result_complete`, timestamps, and the run claim index. M2 schema tests must assert every column, FK, check, unique/partial index, and drop order; M2 real tests must bootstrap this DDL in actual OpenGauss before M3 begins.

Only after this amendment is merged and all four M2 levels pass does the following statement become true: M2 supplies immutable output-version read/list methods, paginated dataset/detail/publication/cleanup reads, and dataset fields `current_output_version`, `current_quality_run_id`, `quality_status`, `quality_sequence`, `quality_error_count`, and `quality_warning_count`. M3 then owns quality allocation/completion SQL through the provided transaction, rules, execution, export, approval, and publication behavior.

## M3-Owned Interface Catalog

All tasks use these names and types exactly.

```python
from collections.abc import Iterable, Iterator, Mapping, Sequence
from datetime import datetime
from typing import Any, Literal, Protocol
from uuid import UUID

QualityStatus = Literal["pending", "running", "pass", "warn", "fail", "error", "cancelled"]
TerminalQualityStatus = Literal["pass", "warn", "fail", "error", "cancelled"]
TriggerKind = Literal["automatic", "manual"]
SortOrder = Literal["asc", "desc"]
ExportFormat = Literal["csv", "json"]

The concrete immutable definitions of `RuleSnapshot`, `QualityRun`, `QualityResult`, `QualityError`, `QualityErrorFilter`, `WarnApproval`, `Publication`, and `Page[T]` are frozen in Task 1 below.


def allocate_quality_run(
    tx: PartitionDomainTransaction,
    *,
    dataset_id: str,
    output_version: str,
    expected_current_output_version: str | None,
    quality_run_id: UUID,
    trigger_event_id: UUID | None,
    trigger: Literal["automatic", "manual"],
    requested_by: str,
    rule_set_version: str,
    rule_snapshot: Sequence[RuleSnapshot],
) -> QualityRun: ...


def complete_quality_run_if_current(
    tx: PartitionDomainTransaction,
    *,
    quality_run_id: UUID,
    terminal_status: QualityStatus,
    error_count: int,
    warning_count: int,
    results_complete: bool,
    execution_error: str | None,
    completed_at: datetime,
) -> bool: ...
```

`allocate_quality_run` locks the dataset, verifies the expected current pointer when non-`None`, verifies the selected version is completed and belongs to the dataset, increments the dataset-wide sequence, and inserts a pending run. A repeated non-null `trigger_event_id` returns the existing run only after verifying its dataset, output version, trigger `automatic`, and rule-set snapshot identity; any mismatch raises `QualityTriggerConflict`. When the selected version is current, allocation sets dataset `current_quality_run_id` to the new ID, keeps `quality_status='pending'`, and resets counts to zero. Only `start_quality_run` after a successful worker lease changes both run and current-dataset quality status to `running`.

`complete_quality_run_if_current` requires a running run plus the caller's valid lease/fence supplied through the transaction's worker context; pending runs cannot complete through this primitive. It locks and terminalizes that run exactly once and returns whether that transaction updated the dataset aggregate. Cancellation is a separate `cancel_quality_run(tx, *, quality_run_id, requested_by, cancelled_at) -> bool` operation that locks a pending/running run, invalidates its lease generation, terminalizes `cancelled`, and applies the same current-output/higher-sequence aggregate guard. A request for an already-terminal run raises `QualityCompletionConflict`; callers recover an unknown commit by reading the terminal run rather than mutating it again.

## Agent, Model, and File Ownership

| Slice | Model | Exclusive files |
|---|---|---|
| Contracts and SQL repository | Opus | `cube_web/cube_web/services/quality_contracts.py`, `cube_web/cube_web/services/quality_repository.py`, `cube_web/tests/test_quality_repository.py`, `cube_web/tests/test_quality_repository_real.py` |
| Rules and durable execution | Sonnet | `cube_web/cube_web/services/quality_rules.py`, `cube_web/cube_web/services/quality_worker.py`, `cube_web/tests/test_quality_rules.py`, `cube_web/tests/test_quality_worker.py` |
| Dataset/read service | Sonnet | `cube_web/cube_web/services/dataset_service.py`, `cube_web/cube_web/routes/partition_datasets.py`, `cube_web/tests/test_dataset_api.py` |
| Export | Sonnet | `cube_web/cube_web/services/quality_export.py`, `cube_web/tests/test_quality_export.py` |
| Warn approval/publication | Opus | `cube_web/cube_web/services/publication_service.py`, `cube_web/cube_web/services/publication_gateway.py`, `cube_web/tests/test_publication_service.py` |
| HTTP/auth integration | Sonnet | `cube_web/cube_web/routes/quality.py`, `cube_web/cube_web/schemas.py`, `cube_web/cube_web/routes/auth.py`, `cube_web/cube_web/services/auth_service.py`, `cube_web/cube_web/app.py`, `cube_web/tests/test_quality_api.py` |
| Legacy removal | Haiku, reviewed by Sonnet | deletion files listed in Task 10, `cube_split/cube_split/quality/*.py`, `cube_split/tests/test_quality_check.py`, quality cases in `cube_split/tests/test_product_workflow.py`, plus `cube_web/cube_web/services/ingest_service.py`, `cube_web/cube_web/services/partition_runners.py`, `cube_web/cube_web/services/partition_workflow.py`, `cube_web/cube_web/services/partition_job_store.py`, `cube_web/tests/test_app.py` |
| Real gate | Sonnet implementer, Opus operator | `cube_web/tests/real/test_m3_quality_publication_real.py`, `cube_web/scripts/run_m3_quality_publication_gate.py`, `pytest.ini` |

No two agents edit `quality_repository.py`, `publication_service.py`, `app.py`, `routes/quality.py`, or the legacy coupling files concurrently. M3 does not edit M2-owned `partition_contracts.py`, `partition_domain_store.py`, or `partition_domain_schema.py`; repository-real tests assert their frozen schema supports M3 SQL.

## Merge Order and Commit Boundary

1. Merge and gate M1.
2. Merge M2 in its frozen order: contracts/DDL, Ray and object work, dataset workflow/store, then all four M2 levels.
3. Merge M3 contracts/repository and transaction tests.
4. Merge rules, outbox dispatcher, and durable worker.
5. Merge dataset reads and normalized quality APIs.
6. Merge streaming exports.
7. Merge Warn approval and publication.
8. Switch all normalized consumers, then delete legacy quality routes/store/coupling in the same integration branch.
9. Run M3 Levels 1–4 and all four review roles. Any failure blocks the single M3 commit.
10. Stage only M3-owned files and create one local `feat: add dataset quality and publication` commit. M4 starts only after that commit is merged; M5 follows M4.

---

### Task 1: Freeze Quality, Pagination, Approval, and Publication Contracts

**Files:**
- Create: `cube_web/cube_web/services/quality_contracts.py`
- Create: `cube_web/tests/test_quality_contracts.py`

**Interfaces:**
- Consumes: M2 dataset/output identifiers and Python `UUID`/UTC `datetime` values.
- Produces: every model and literal in the M3-Owned Interface Catalog; `DEFAULT_PAGE_SIZE=20`, `MAX_PAGE_SIZE=500`, `ERROR_BATCH_SIZE=1000`.
- Produces: `page_offset(page: int, page_size: int) -> int`, `validate_sort(sort_by, sort_order, allowed)`, and canonical `QualityErrorFilter.active()`.

- [ ] **Step 1: Write failing frozen-model tests**

```python
from datetime import datetime, timezone
from uuid import UUID

import pytest
from pydantic import ValidationError

from cube_web.services.quality_contracts import (
    QualityErrorFilter,
    RuleSnapshot,
    page_offset,
    validate_sort,
)


def test_rule_snapshot_is_complete_and_frozen() -> None:
    snapshot = RuleSnapshot(
        code="output_count_consistency",
        name="Output count consistency",
        applicability={"data_types": ["optical", "radar", "product", "carbon"]},
        mandatory=True,
        parameters={"batch_size": 1000},
        implementation_version="1.0.0",
    )
    assert snapshot.model_dump(mode="json")["parameters"] == {"batch_size": 1000}
    with pytest.raises(ValidationError):
        snapshot.code = "changed"


def test_filter_and_sort_contracts_are_closed() -> None:
    assert QualityErrorFilter().active() is False
    assert QualityErrorFilter(rule_code="bounds").active() is True
    assert page_offset(2, 20) == 20
    assert validate_sort("completed_at", "desc", {"completed_at", "quality_run_id"}) == ("completed_at", "desc")
    with pytest.raises(ValueError, match="sort_by"):
        validate_sort("raw_sql", "desc", {"completed_at"})
```

- [ ] **Step 2: Run the contract test and verify module absence**

Run from repository root:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_web/tests/test_quality_contracts.py -q
```

Expected: collection fails with `ModuleNotFoundError: cube_web.services.quality_contracts`.

- [ ] **Step 3: Implement the exact immutable contracts**

```python
from __future__ import annotations

from datetime import datetime
from typing import Any, Generic, Literal, TypeVar
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

QualityStatus = Literal["pending", "running", "pass", "warn", "fail", "error", "cancelled"]
TerminalQualityStatus = Literal["pass", "warn", "fail", "error", "cancelled"]
TriggerKind = Literal["automatic", "manual"]
SortOrder = Literal["asc", "desc"]
ExportFormat = Literal["csv", "json"]
DEFAULT_PAGE_SIZE = 20
MAX_PAGE_SIZE = 500
ERROR_BATCH_SIZE = 1000
T = TypeVar("T")


class FrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class RuleSnapshot(FrozenModel):
    code: str = Field(min_length=1, max_length=128)
    name: str = Field(min_length=1, max_length=256)
    applicability: dict[str, Any]
    mandatory: bool
    parameters: dict[str, Any]
    implementation_version: str = Field(min_length=1, max_length=64)


class QualityRun(FrozenModel):
    quality_run_id: UUID
    dataset_id: str
    dataset_code: str
    output_version: str
    quality_sequence: int
    trigger_event_id: UUID | None
    trigger: TriggerKind
    requested_by: str
    rule_set_version: str
    rule_snapshot: tuple[RuleSnapshot, ...]
    status: QualityStatus
    results_complete: bool
    error_count: int
    warning_count: int
    execution_error: str | None
    started_at: datetime | None
    completed_at: datetime | None
    created_at: datetime
    is_current: bool


class QualityResult(FrozenModel):
    quality_run_id: UUID
    rule_code: str
    status: Literal["pass", "warn", "fail", "error"]
    finding_count: int
    error_count: int
    warning_count: int
    metrics: dict[str, Any]
    execution_error: str | None
    started_at: datetime
    completed_at: datetime


class QualityError(FrozenModel):
    quality_error_id: UUID
    quality_run_id: UUID
    rule_code: str
    source_asset_id: str | None
    tile_id: str | None
    index_id: str | None
    output_id: str | None
    row_number: int | None
    field: str | None
    error_code: str
    message: str
    context: dict[str, Any]
    created_at: datetime


class QualityErrorFilter(FrozenModel):
    rule_code: str | None = None
    error_code: str | None = None
    source_asset_id: str | None = None
    output_id: str | None = None
    field: str | None = None

    def active(self) -> bool:
        return any(value is not None for value in self.model_dump().values())


class WarnApproval(FrozenModel):
    approval_id: UUID
    dataset_id: str
    output_version: str
    quality_run_id: UUID
    rule_set_version: str
    approved_by: str
    approved_at: datetime
    reason: str


class Publication(FrozenModel):
    publication_id: UUID
    dataset_id: str
    output_version: str
    quality_run_id: UUID
    status: Literal["publishing", "active", "withdrawing", "failed", "withdrawn"]
    service_version_id: str | None
    requested_by: str
    requested_at: datetime
    activated_at: datetime | None
    failure: str | None
    withdrawn_by: str | None
    withdrawn_at: datetime | None
    withdrawal_reason: str | None


class Page(FrozenModel, Generic[T]):
    items: tuple[T, ...]
    total: int
    page: int
    page_size: int


def page_offset(page: int, page_size: int) -> int:
    if page < 1 or page_size < 1 or page_size > MAX_PAGE_SIZE:
        raise ValueError("page must be >= 1 and page_size must be between 1 and 500")
    return (page - 1) * page_size


def validate_sort(sort_by: str, sort_order: str, allowed: set[str]) -> tuple[str, SortOrder]:
    if sort_by not in allowed:
        raise ValueError(f"sort_by must be one of: {', '.join(sorted(allowed))}")
    if sort_order not in {"asc", "desc"}:
        raise ValueError("sort_order must be asc or desc")
    return sort_by, sort_order
```

- [ ] **Step 4: Run the contract tests**

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_web/tests/test_quality_contracts.py -q
```

Expected: all tests pass.

---

### Task 2: Implement Atomic Run Allocation and Completion SQL

**Files:**
- Create: `cube_web/cube_web/services/quality_repository.py`
- Create: `cube_web/tests/test_quality_repository.py`
- Create: `cube_web/tests/test_quality_repository_real.py`

**Interfaces:**
- Consumes: `OpenGaussPartitionDomainStore.transaction()` and Task 1 contracts.
- Produces: the exact `allocate_quality_run(...) -> QualityRun` and `complete_quality_run_if_current(...) -> bool` signatures from the catalog.
- Produces: `QualityTriggerConflict`, `QualityCompletionConflict`, `DatasetNotFound`, `OutputVersionNotFound`, `OutputVersionNotCompleted`.
- Produces: `require_open_gauss_domain_store() -> OpenGaussPartitionDomainStore`; all production transaction callers use it rather than calling `transaction()` on the base `PartitionDomainStore` type.
- Produces: `QualityRepository` quality/publication mutation and quality read methods used in Tasks 3–9; all SQL placeholders are `%s`, never interpolated values. Dataset/detail/publication collection reads consume the amended M2 store surface above.

- [ ] **Step 1: Write failing transaction-behavior tests**

```python
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from cube_web.services.quality_repository import (
    QualityCompletionConflict,
    allocate_quality_run,
    complete_quality_run_if_current,
)


def test_concurrent_sequences_and_late_completion_do_not_overwrite_current(open_gauss_tx, seeded_current_output, snapshots) -> None:
    first_id, second_id = uuid4(), uuid4()
    with open_gauss_tx() as tx:
        first = allocate_quality_run(
            tx,
            dataset_id="dataset-a",
            output_version="ov-current",
            expected_current_output_version="ov-current",
            quality_run_id=first_id,
            trigger_event_id=None,
            trigger="manual",
            requested_by="alice",
            rule_set_version="2026.07.14-v1",
            rule_snapshot=snapshots,
        )
    with open_gauss_tx() as tx:
        second = allocate_quality_run(
            tx,
            dataset_id="dataset-a",
            output_version="ov-current",
            expected_current_output_version="ov-current",
            quality_run_id=second_id,
            trigger_event_id=None,
            trigger="manual",
            requested_by="bob",
            rule_set_version="2026.07.14-v1",
            rule_snapshot=snapshots,
        )
    assert second.quality_sequence == first.quality_sequence + 1
    with open_gauss_tx() as tx:
        assert complete_quality_run_if_current(
            tx,
            quality_run_id=first_id,
            terminal_status="fail",
            error_count=3,
            warning_count=0,
            results_complete=True,
            execution_error=None,
            completed_at=datetime.now(timezone.utc),
        ) is False
```

Add exact tests named `test_automatic_redelivery_returns_same_run_after_identity_verification`, `test_automatic_redelivery_with_different_output_raises_trigger_conflict`, `test_explicit_completed_historical_output_does_not_change_current_fields`, `test_expected_current_pointer_change_aborts_allocation`, `test_old_output_completion_terminalizes_history_only`, `test_second_completion_raises_quality_completion_conflict`, and `test_nonterminal_completion_status_is_rejected`. Reuse the `open_gauss_tx`, `seeded_current_output`, and `snapshots` fixtures shown above; each test invokes the public primitive with concrete IDs `dataset-a`, `ov-current`/`ov-old`, fixed `uuid4()` values held in local variables, and asserts the behavior encoded by its full name. Use no mocks for transaction locking behavior.

- [ ] **Step 2: Run the repository tests and verify failure**

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_web/tests/test_quality_repository.py -q
```

Expected: collection fails because `quality_repository.py` does not exist.

- [ ] **Step 3: Implement allocation with a locked dataset and idempotent event identity**

```python
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Literal, Sequence, TypeAlias
from uuid import UUID

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from cube_web.services.quality_contracts import QualityRun, QualityStatus, RuleSnapshot

PartitionDomainTransaction: TypeAlias = psycopg.Connection[Any]


class QualityTriggerConflict(RuntimeError):
    pass


class QualityCompletionConflict(RuntimeError):
    pass


def _snapshot_json(snapshot: Sequence[RuleSnapshot]) -> list[dict[str, Any]]:
    return [item.model_dump(mode="json") for item in snapshot]


def allocate_quality_run(
    tx: PartitionDomainTransaction,
    *,
    dataset_id: str,
    output_version: str,
    expected_current_output_version: str | None,
    quality_run_id: UUID,
    trigger_event_id: UUID | None,
    trigger: Literal["automatic", "manual"],
    requested_by: str,
    rule_set_version: str,
    rule_snapshot: Sequence[RuleSnapshot],
) -> QualityRun:
    snapshot = _snapshot_json(rule_snapshot)
    with tx.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT dataset_id, dataset_code, current_output_version, current_quality_run_id, quality_sequence "
            "FROM partition_datasets WHERE dataset_id = %s FOR UPDATE",
            (dataset_id,),
        )
        dataset = cur.fetchone()
        if dataset is None:
            raise DatasetNotFound(dataset_id)
        if expected_current_output_version is not None and dataset["current_output_version"] != expected_current_output_version:
            raise QualityTriggerConflict("current output version changed before quality allocation")
        cur.execute(
            "SELECT status FROM partition_output_versions WHERE dataset_id = %s AND output_version = %s",
            (dataset_id, output_version),
        )
        output = cur.fetchone()
        if output is None:
            raise OutputVersionNotFound(output_version)
        if output["status"] != "completed":
            raise OutputVersionNotCompleted(output_version)
        if trigger_event_id is not None:
            cur.execute(
                "SELECT * FROM partition_quality_runs WHERE trigger_event_id = %s",
                (trigger_event_id,),
            )
            existing = cur.fetchone()
            if existing is not None:
                if (
                    existing["dataset_id"] != dataset_id
                    or existing["output_version"] != output_version
                    or existing["trigger"] != "automatic"
                    or existing["rule_set_version"] != rule_set_version
                    or existing["rule_snapshot"] != snapshot
                ):
                    raise QualityTriggerConflict("trigger_event_id is already bound to another quality identity")
                return _quality_run(existing, dataset_code=dataset["dataset_code"], is_current=existing["quality_run_id"] == dataset.get("current_quality_run_id"))
        sequence = int(dataset["quality_sequence"]) + 1
        cur.execute(
            "UPDATE partition_datasets SET quality_sequence = %s, updated_at = now() WHERE dataset_id = %s",
            (sequence, dataset_id),
        )
        cur.execute(
            "INSERT INTO partition_quality_runs "
            "(quality_run_id, dataset_id, output_version, quality_sequence, trigger_event_id, trigger, requested_by, "
            "rule_set_version, rule_snapshot, status, result_complete, available_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending', FALSE, now()) RETURNING *",
            (quality_run_id, dataset_id, output_version, sequence, trigger_event_id, trigger, requested_by, rule_set_version, Jsonb(snapshot)),
        )
        row = cur.fetchone()
        is_current = output_version == dataset["current_output_version"]
        if is_current:
            cur.execute(
                "UPDATE partition_datasets SET current_quality_run_id = %s, quality_status = 'pending', "
                "quality_error_count = 0, quality_warning_count = 0, updated_at = now() WHERE dataset_id = %s",
                (quality_run_id, dataset_id),
            )
        return _quality_run(row, dataset_code=dataset["dataset_code"], is_current=is_current)
```

Use named exceptions defined in the same module. `_quality_run` must map every Task 1 field and compute `is_current` from the locked dataset, never from client input.

- [ ] **Step 4: Implement terminal completion with same-version sequence protection**

```python
def complete_quality_run_if_current(
    tx: PartitionDomainTransaction,
    *,
    quality_run_id: UUID,
    terminal_status: QualityStatus,
    error_count: int,
    warning_count: int,
    results_complete: bool,
    execution_error: str | None,
    completed_at: datetime,
) -> bool:
    if terminal_status not in {"pass", "warn", "fail", "error", "cancelled"}:
        raise ValueError("terminal_status must be pass, warn, fail, error, or cancelled")
    with tx.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT * FROM partition_quality_runs WHERE quality_run_id = %s FOR UPDATE", (quality_run_id,))
        run = cur.fetchone()
        if run is None:
            raise QualityRunNotFound(str(quality_run_id))
        if run["status"] != "running":
            raise QualityCompletionConflict("quality run must be running")
        fence = quality_lease_from_transaction(tx, quality_run_id)
        if fence is None:
            raise StaleQualityLease(str(quality_run_id))
        cur.execute(
            "UPDATE partition_quality_runs SET status = %s, error_count = %s, warning_count = %s, "
            "result_complete = %s, last_error = %s, completed_at = %s, updated_at = %s, claimed_at = NULL, claimed_by = NULL "
            "WHERE quality_run_id = %s AND claimed_by = %s AND attempt_count = %s AND status = 'running' RETURNING dataset_id",
            (
                terminal_status, error_count, warning_count, results_complete, execution_error, completed_at, completed_at,
                quality_run_id, fence.claimed_by, fence.attempt_count,
            ),
        )
        if cur.fetchone() is None:
            raise StaleQualityLease(str(quality_run_id))
        cur.execute(
            "UPDATE partition_datasets d SET current_quality_run_id = %s, quality_status = %s, quality_error_count = %s, "
            "quality_warning_count = %s, updated_at = %s WHERE d.dataset_id = %s AND d.current_output_version = %s "
            "AND NOT EXISTS (SELECT 1 FROM partition_quality_runs newer WHERE newer.dataset_id = d.dataset_id "
            "AND newer.output_version = %s AND newer.quality_sequence > %s) RETURNING dataset_id",
            (quality_run_id, terminal_status, error_count, warning_count, completed_at, run["dataset_id"], run["output_version"], run["output_version"], run["quality_sequence"]),
        )
        return cur.fetchone() is not None
```

`quality_lease_from_transaction` reads a transaction-local worker context set only by `execute_quality_run`; direct API callers cannot fabricate it. `write_quality_error_batch` and `finish_quality_result` take the same `QualityLease` explicitly and use its fence. The frozen M2 run row uses `result_complete` and `last_error`; the Task 1 model maps them to public `results_complete` and `execution_error`. The update's returned boolean is the exact result of this completion transaction; a repeat completion is a conflict, so no extra M2 column or schema edit is required.

- [ ] **Step 5: Run focused fake and actual OpenGauss transaction tests**

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest \
  cube_web/tests/test_quality_repository.py \
  cube_web/tests/test_quality_repository_real.py -q
```

Expected: all tests pass against the in-memory contract fixture and configured OpenGauss test database; the real transaction cases use OpenGauss and no SQLite path exists.

---

### Task 3: Persist Complete Rule Results and Unbounded Batched Errors

**Files:**
- Modify: `cube_web/cube_web/services/quality_repository.py`
- Modify: `cube_web/tests/test_quality_repository.py`
- Modify: `cube_web/tests/test_quality_repository_real.py`

**Interfaces:**
- Produces: `start_quality_run(tx, *, quality_run_id, worker_id, started_at) -> QualityRun`.
- Produces: `write_quality_error_batch(tx, *, quality_run_id, errors: Sequence[NewQualityError]) -> int`.
- Produces: `finish_quality_result(tx, *, result: QualityResult) -> QualityResult`.
- Produces: `get/list/count` methods for runs, results, errors, datasets, and publications; list methods always take explicit `limit`, `offset`, `sort_by`, and `sort_order`.
- Produces: `iter_quality_errors(tx, *, quality_run_id, filters, fetch_size=1000) -> Iterator[QualityError]` and `count_quality_errors(...) -> int` using the same predicate builder.

- [ ] **Step 1: Add failing full-storage and shared-filter tests**

```python
def test_error_batches_store_every_row_and_page_does_not_cap_count(repository, running_run) -> None:
    errors = [new_error(running_run.quality_run_id, row_number=index) for index in range(2505)]
    for start in range(0, len(errors), 1000):
        repository.write_quality_error_batch(errors[start:start + 1000])
    page = repository.list_quality_errors(running_run.quality_run_id, limit=20, offset=0)
    assert len(page) == 20
    assert repository.count_quality_errors(running_run.quality_run_id) == 2505
    assert len(list(repository.iter_quality_errors(running_run.quality_run_id, fetch_size=257))) == 2505


def test_filtered_iterator_and_count_use_identical_predicates(repository, running_run) -> None:
    filters = QualityErrorFilter(rule_code="bounds", error_code="outside_extent", field="bbox")
    assert len(list(repository.iter_quality_errors(running_run.quality_run_id, filters=filters))) == repository.count_quality_errors(
        running_run.quality_run_id, filters=filters
    )
```

- [ ] **Step 2: Run tests and verify missing methods**

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_web/tests/test_quality_repository.py -q
```

Expected: failures report the missing result/error repository methods.

- [ ] **Step 3: Implement one whitelisted predicate builder and server-side streaming cursor**

```python
_ERROR_FILTER_COLUMNS = {
    "rule_code": "rule_code",
    "error_code": "error_code",
    "source_asset_id": "source_asset_id",
    "output_id": "output_id",
    "field": "field_name",
}


def _error_predicate(quality_run_id: UUID, filters: QualityErrorFilter) -> tuple[str, list[Any]]:
    clauses = ["quality_run_id = %s"]
    params: list[Any] = [quality_run_id]
    for field, column in _ERROR_FILTER_COLUMNS.items():
        value = getattr(filters, field)
        if value is not None:
            clauses.append(f"{column} = %s")
            params.append(value)
    return " AND ".join(clauses), params


def iter_quality_errors(
    tx: PartitionDomainTransaction,
    *,
    quality_run_id: UUID,
    filters: QualityErrorFilter,
    fetch_size: int = 1000,
):
    where, params = _error_predicate(quality_run_id, filters)
    cursor_name = f"quality_export_{quality_run_id.hex}"
    with tx.cursor(name=cursor_name, row_factory=dict_row) as cur:
        cur.itersize = fetch_size
        cur.execute(
            "SELECT quality_error_id, quality_run_id, rule_code, source_asset_id, tile_id, index_id, output_id, "
            "row_number, field_name, error_code, message, context, created_at "
            f"FROM partition_quality_errors WHERE {where} ORDER BY quality_error_id",
            params,
        )
        for row in cur:
            yield _quality_error(row)
```

The only SQL interpolation is a column/expression selected from the module constants. Values remain `%s` parameters. `write_quality_error_batch` uses `executemany` with all fields, `Jsonb(error.context)`, no slicing of message/context, and rejects errors whose run/rule identity differs from the target batch.

- [ ] **Step 4: Finalize results after batches and assert run totals equal result sums**

`finish_quality_result` upserts exactly one row per `(quality_run_id, rule_code)` after that rule's error batches commit. Before run completion, query normalized result sums and compare them to the supplied run totals; mismatch raises `QualitySummaryMismatch` and terminalizes the run as `error` with `results_complete=false`.

```sql
SELECT COALESCE(sum(error_count), 0), COALESCE(sum(warning_count), 0)
FROM partition_quality_results
WHERE quality_run_id = %s
```

- [ ] **Step 5: Run repository tests**

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest \
  cube_web/tests/test_quality_repository.py \
  cube_web/tests/test_quality_repository_real.py -q
```

Expected: all storage, count, filter, and streaming tests pass.

---

### Task 4: Implement a Version-Bound Rule Engine and Exact Status Reduction

**Files:**
- Create: `cube_web/cube_web/services/quality_rules.py`
- Create: `cube_web/tests/test_quality_rules.py`

**Interfaces:**
- Consumes: M2 immutable output reads and Task 3 batch persistence.
- Produces: `RuleContext`, `QualityFinding`, `QualityRule` protocol, `RuleRegistry`, `DEFAULT_RULE_SET_VERSION="2026.07.14-v1"`, `default_rule_registry()`.
- Produces: `snapshot_rules(registry, *, data_type, product_type) -> tuple[RuleSnapshot, ...]` and `reduce_quality_status(results, execution_error) -> TerminalQualityStatus`.
- Produces: `RuleContext`, `QualityFinding`, `QualityRule` protocol, `RuleRegistry`, `DEFAULT_RULE_SET_VERSION="2026.07.14-v1"`, `default_rule_registry()`.
- The default registry replaces the deleted legacy engines only after parity coverage. Shared mandatory rules are `index_schema`, `output_count_consistency`, `output_reference_integrity`, `grid_method_agreement`, `cell_bbox_validity`, `time_bucket_consistency`, `asset_readability`, `asset_crs`, and `window_bounds`; shared optional rules include `pixel_sample` and `metadata_completeness`. Product adds mandatory `product_year_consistency`. Carbon adds mandatory `carbon_schema`, `carbon_coordinates`, `carbon_xco2_range`, `carbon_quality_flags`, `carbon_observation_duplicates`, and `carbon_footprints`. Radar/optical applicability is explicit even where they share an implementation; no data type silently receives only the four generic checks.

- [ ] **Step 1: Write failing status and snapshot tests**

```python
import pytest

from cube_web.services.quality_rules import default_rule_registry, reduce_quality_status, snapshot_rules


def test_status_reduction_is_normative() -> None:
    assert reduce_quality_status([], None) == "pass"
    assert reduce_quality_status([result("warn")], None) == "warn"
    assert reduce_quality_status([result("warn"), result("fail")], None) == "fail"
    assert reduce_quality_status([result("fail")], "minio unavailable") == "error"


def test_snapshot_contains_every_interpretive_field() -> None:
    registry = default_rule_registry()
    optical = snapshot_rules(registry, data_type="optical", product_type="surface-reflectance")
    radar = snapshot_rules(registry, data_type="radar", product_type="sar")
    product = snapshot_rules(registry, data_type="product", product_type="annual")
    carbon = snapshot_rules(registry, data_type="carbon", product_type="xco2")
    assert {"index_schema", "asset_readability", "asset_crs", "window_bounds", "cell_bbox_validity", "time_bucket_consistency", "grid_method_agreement"} <= {item.code for item in optical}
    assert {"index_schema", "asset_readability", "asset_crs", "window_bounds", "grid_method_agreement"} <= {item.code for item in radar}
    assert "product_year_consistency" in {item.code for item in product}
    assert {"carbon_schema", "carbon_coordinates", "carbon_xco2_range", "carbon_quality_flags", "carbon_observation_duplicates", "carbon_footprints"} <= {item.code for item in carbon}
    assert all(item.name and item.applicability and item.implementation_version for item in (*optical, *radar, *product, *carbon))
```

- [ ] **Step 2: Run tests and verify module absence**

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_web/tests/test_quality_rules.py -q
```

Expected: collection fails because `quality_rules.py` does not exist.

- [ ] **Step 3: Implement the protocol, immutable context, and reduction**

```python
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Literal, Protocol

from cube_web.services.quality_contracts import QualityResult, RuleSnapshot, TerminalQualityStatus

DEFAULT_RULE_SET_VERSION = "2026.07.14-v1"


@dataclass(frozen=True)
class RuleContext:
    dataset_id: str
    output_version: str
    data_type: str
    product_type: str | None
    repository: "QualityRepository"
    object_reader: "ObjectReader"


@dataclass(frozen=True)
class QualityFinding:
    error_code: str
    message: str
    source_asset_id: str | None = None
    tile_id: str | None = None
    index_id: str | None = None
    output_id: str | None = None
    row_number: int | None = None
    field: str | None = None
    context: Mapping[str, Any] | None = None


class QualityRule(Protocol):
    code: str
    name: str
    applicability: Mapping[str, Any]
    mandatory: bool
    parameters: Mapping[str, Any]
    implementation_version: str

    def applies(self, *, data_type: str, product_type: str | None) -> bool: ...
    def evaluate(self, context: RuleContext) -> Iterable[QualityFinding]: ...


def reduce_quality_status(results: list[QualityResult], execution_error: str | None) -> TerminalQualityStatus:
    if execution_error is not None or any(item.status == "error" for item in results):
        return "error"
    if any(item.status == "fail" for item in results):
        return "fail"
    if any(item.status == "warn" for item in results):
        return "warn"
    return "pass"
```

Each rule converts domain findings only; it does not catch unexpected exceptions. The engine catches exceptions at the rule boundary so optional-rule exceptions still become run `error`. Shared rules query normalized OpenGauss rows rather than run-directory JSON: schema/identity/count rules validate required index/tile/grid columns and immutable counts; grid-method agreement requires logical windows/source references for Geohash/MGRS and entity tile references for ISEA4H; bbox/time rules validate finite ordered bounds and acquisition/time-bucket agreement; asset rules stream actual MinIO COG/entity objects, validate readability/recorded checksum/CRS, and verify logical windows are inside raster dimensions. Product year consistency compares acquisition year/time bucket and configured expected years. Carbon rules preserve legacy required fields, finite coordinate ranges, XCO2/value ranges, quality flags, observation uniqueness, and footprint geometry. `pixel_sample` and descriptive metadata remain optional. Task 10 deletes old engines only after all four data-type parity tests and real rule scenarios pass.

- [ ] **Step 4: Test mandatory/optional findings and exceptions**

Add exact tests asserting: mandatory finding result is `fail`; optional-only finding result is `warn`; zero findings is `pass`; a `RuntimeError` from either a mandatory or optional rule is a result/run `error`; errors from prior rules remain stored; and the run reports `results_complete=false` after the exception.

- [ ] **Step 5: Run rule tests**

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_web/tests/test_quality_rules.py -q
```

Expected: all tests pass.

---

### Task 5: Add Idempotent Outbox Dispatch and Durable Quality Workers

**Files:**
- Create: `cube_web/cube_web/services/quality_worker.py`
- Create: `cube_web/tests/test_quality_worker.py`
- Modify: `cube_web/cube_web/app.py`

**Interfaces:**
- Consumes: M2 `claim_outbox/acknowledge_outbox/retry_outbox`, M2 run lease columns, Tasks 2–4.
- Produces: `dispatch_quality_events(*, worker_id: str, limit: int=100, now: datetime) -> DispatchSummary`.
- Produces: `claim_quality_runs(tx, *, worker_id, limit=10, lease_seconds=300) -> list[QualityLease]`, `heartbeat_quality_run(tx, *, lease: QualityLease, now: datetime) -> bool`, `start_quality_run(tx, *, lease: QualityLease, started_at: datetime) -> QualityRun`, `execute_quality_run(quality_run_id, *, worker_id) -> QualityRun`, and `QualityRuntime.start()/stop()`.
- `QualityLease` contains `quality_run_id`, `claimed_by`, and fencing `attempt_count`. Every error/result/heartbeat/completion mutation includes `WHERE quality_run_id=%s AND claimed_by=%s AND attempt_count=%s AND status='running'`; zero affected rows raises `StaleQualityLease` and the stale worker writes nothing further.
- Outbox dispatch allocates and acknowledges only; rule execution is a separate durable lease so ack-before-execution cannot lose work.

- [ ] **Step 1: Write failing dispatch/lease tests**

```python
def test_dispatch_redelivery_allocates_one_automatic_run(domain_store, completed_event, worker) -> None:
    first = worker.dispatch_quality_events(worker_id="dispatcher-a", limit=100, now=utc_now())
    domain_store.redeliver(completed_event.event_id)
    second = worker.dispatch_quality_events(worker_id="dispatcher-b", limit=100, now=utc_now())
    assert first.allocated == 1
    assert second.allocated == 0
    assert repository.count_runs(trigger_event_id=completed_event.event_id) == 1


def test_manual_before_outbox_keeps_two_sequences_and_later_auto_is_current(seed_current_output, worker) -> None:
    manual = service.request_manual_run("dataset-a", actor("alice"))
    worker.dispatch_quality_events(worker_id="dispatcher", limit=100, now=utc_now())
    runs = repository.list_runs(dataset_id="dataset-a", sort_by="quality_sequence", sort_order="asc")
    assert [run.trigger for run in runs] == ["manual", "automatic"]
    assert runs[1].quality_sequence == manual.quality_sequence + 1
    assert runs[1].is_current is True
```

Also add the following exact tests; each seeds the identities in its name and asserts the shown invariant:

```python
def test_dispatch_failure_retries_without_ack(...):
    assert event.status == "pending" and event.attempt_count == 1

def test_ack_occurs_only_after_successful_allocation(...):
    assert repository.get_by_trigger_event(event.event_id) is not None

def test_stale_running_lease_is_reclaimed(...):
    assert claimed[0].quality_run_id == stale_run.quality_run_id

def test_live_running_lease_is_not_reclaimed(...):
    assert claimed == []

def test_second_worker_cannot_complete_terminal_run(...):
    with pytest.raises(QualityCompletionConflict): ...

def test_rule_exception_preserves_prior_error_rows(...):
    assert repository.count_quality_errors(run_id, filters=QualityErrorFilter()) == prior_count
```

- [ ] **Step 2: Run tests and verify module absence**

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_web/tests/test_quality_worker.py -q
```

Expected: collection fails because `quality_worker.py` does not exist.

- [ ] **Step 3: Implement event dispatch with bounded exponential retry**

```python
def dispatch_quality_events(*, worker_id: str, limit: int = 100, now: datetime) -> DispatchSummary:
    allocated = delivered = retried = 0
    base_store = get_partition_domain_store()
    store = require_open_gauss_domain_store()
    for event in base_store.claim_outbox(worker_id, limit=limit):
        try:
            snapshot = snapshot_rules_for_event(event)
            with store.transaction() as tx:
                run = allocate_quality_run(
                    tx,
                    dataset_id=event["dataset_id"],
                    output_version=event["output_version"],
                    expected_current_output_version=None,
                    quality_run_id=uuid5(NAMESPACE_URL, f"cube-quality:{event['event_id']}"),
                    trigger_event_id=UUID(str(event["event_id"])),
                    trigger="automatic",
                    requested_by="system:partition-outbox",
                    rule_set_version=DEFAULT_RULE_SET_VERSION,
                    rule_snapshot=snapshot,
                )
            base_store.acknowledge_outbox(event["event_id"])
            allocated += int(run.created_at >= now)
            delivered += 1
        except Exception as exc:
            attempt = int(event["attempt_count"]) + 1
            delay = min(300, 2 ** min(attempt, 8))
            base_store.retry_outbox(event["event_id"], safe_error(exc), available_at=(now + timedelta(seconds=delay)).isoformat())
            retried += 1
    return DispatchSummary(allocated=allocated, delivered=delivered, retried=retried)
```

Do not infer `allocated` from timestamps in final code. Return `(run, created: bool)` from the repository's internal helper while preserving the public exact `allocate_quality_run(...) -> QualityRun` signature; the dispatcher uses the internal helper. Automatic completion events deliberately pass `expected_current_output_version=None`: a delayed event still allocates exactly one historical automatic run after the dataset pointer advances, while `allocate_quality_run` sets current fields only if its locked selected output is still current. Manual default-current triggers continue to pass the expected pointer and fail rather than silently changing selection.

- [ ] **Step 4: Implement leased run execution**

Claim with one OpenGauss transaction using `FOR UPDATE SKIP LOCKED`, selecting `pending` due runs and `running` rows whose `claimed_at` is older than the lease. Increment `attempt_count` as the fencing generation, assign worker/time, and return `QualityLease`; `start_quality_run` uses that fence to set run `running` and sets dataset quality to `running` only when the run is still the current run for the current output. During execution heartbeat `claimed_at` at least every `lease_seconds / 3` and before every batch/result/completion write. Each mutation predicates on the full fence and aborts on zero rows. A reclaimed generation can proceed; the original stale generation cannot heartbeat or write. Execute the snapshot by resolving rule code and exact `implementation_version`; missing/mismatched implementations terminalize `error`.

For each rule: write error batches of exactly at most 1000; finish one normalized result; accumulate counts. On any exception, finish the affected result as `error`, preserve prior rows, set `results_complete=false`, and call `complete_quality_run_if_current(... terminal_status="error")`. Otherwise reduce status and complete with exact sums.

- [ ] **Step 5: Start and stop runtime without destructive startup work**

`QualityRuntime.start()` launches one dispatcher loop and one execution loop with `threading.Event` shutdown, no daemon-only durability assumptions, and no database reset. `QualityRuntime.stop()` signals and joins both threads. Wire it into `app.py` lifespan after M2 task reconciliation and stop it in `finally`.

- [ ] **Step 6: Run worker and app lifespan tests**

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest \
  cube_web/tests/test_quality_worker.py \
  cube_web/tests/test_app.py -q
```

Expected: dispatch, lease recovery, execution, and clean shutdown tests pass.

---

### Task 6: Add Paginated and Sorted Dataset Read APIs

**Files:**
- Create: `cube_web/cube_web/services/dataset_service.py`
- Create: `cube_web/cube_web/routes/partition_datasets.py`
- Create: `cube_web/tests/test_dataset_api.py`
- Modify: `cube_web/cube_web/routes/partition.py`
- Modify: `cube_web/cube_web/app.py`

**Interfaces:**
- Consumes: amended M2 `get_dataset`, `list/count_datasets`, `resolve_output_version`, paginated asset/band/tile/index/grid/publication reads, and publication-reference cleanup read; Task 1 `Page`.
- Produces all dataset routes from normative section 8.2, including quality/publications routes delegated to Tasks 8–9.
- Sort whitelists: datasets `updated_at|created_at|dataset_code|partition_completed_at|quality_status`; assets `source_asset_id|created_at`; bands `display_order|band_code`; tiles/indexes/grid `created_at|output_id|space_code|grid_level`; quality `created_at|completed_at|generated_at|quality_sequence|status`; publications `requested_at|activated_at|status`.
- Dataset-list filters consumed unchanged by M4 are `keyword`, `data_type`, `product_type`, `batch_id`, `grid_type`, `partition_status`, `quality_status`, `publish_status`, `time_start`, and `time_end`. `time_start/time_end` bound `updated_at`; `publish_status` is derived by an `EXISTS`/latest-publication predicate without multiplying dataset rows and has exact literals `unpublished|publishing|active|withdrawing|failed|withdrawn`. No publication history yields `unpublished`; otherwise the latest publication by `(requested_at, publication_id)` supplies its lifecycle status unchanged. M4 must display/filter `active`, not invent `published`.

- [ ] **Step 1: Write failing dataset list/detail/version tests**

```python
def test_dataset_list_is_stably_paginated(client, seeded_datasets) -> None:
    response = client.get(
        "/v1/partition/datasets",
        params={"page": 2, "page_size": 2, "sort_by": "updated_at", "sort_order": "desc"},
    )
    assert response.status_code == 200
    assert set(response.json()) == {"items", "total", "page", "page_size"}
    assert response.json()["page"] == 2


def test_tiles_default_to_current_and_reject_foreign_version(client, seeded_datasets) -> None:
    current = client.get("/v1/partition/datasets/dataset-a/tiles")
    assert {item["output_version"] for item in current.json()["items"]} == {"ov-a-current"}
    foreign = client.get("/v1/partition/datasets/dataset-a/tiles", params={"output_version": "ov-b-current"})
    assert foreign.status_code == 404
```

- [ ] **Step 2: Run tests and verify missing routes**

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_web/tests/test_dataset_api.py -q
```

Expected: requests return 404 because dataset routes do not exist.

- [ ] **Step 3: Implement service-side pagination and stable sort mapping**

```python
_DATASET_SORT = {
    "updated_at": "updated_at",
    "created_at": "created_at",
    "dataset_code": "dataset_code",
    "partition_completed_at": "partition_completed_at",
    "quality_status": "quality_status",
}


def list_datasets(self, query: DatasetQuery) -> Page[dict[str, Any]]:
    sort_by, sort_order = validate_sort(query.sort_by, query.sort_order, set(_DATASET_SORT))
    limit = query.page_size
    offset = page_offset(query.page, query.page_size)
    items = self.store.list_datasets(
        data_type=query.data_type,
        product_type=query.product_type,
        batch_id=query.batch_id,
        grid_type=query.grid_type,
        partition_status=query.partition_status,
        quality_status=query.quality_status,
        publish_status=query.publish_status,
        time_start=query.time_start,
        time_end=query.time_end,
        keyword=query.keyword,
        limit=limit,
        offset=offset,
        sort_by=_DATASET_SORT[sort_by],
        sort_order=sort_order,
        stable_tiebreaker="dataset_id",
    )
    total = self.store.count_datasets(
        data_type=query.data_type,
        product_type=query.product_type,
        batch_id=query.batch_id,
        grid_type=query.grid_type,
        partition_status=query.partition_status,
        quality_status=query.quality_status,
        publish_status=query.publish_status,
        time_start=query.time_start,
        time_end=query.time_end,
        keyword=query.keyword,
    )
    return Page(items=tuple(items), total=total, page=query.page, page_size=query.page_size)
```

No route accepts arbitrary column names. Every child read applies `dataset_id`; tiles/indexes/grid first call M2's version resolver, defaulting to current and rejecting foreign versions, then the amended M2 store executes the paginated query/count for that resolved tuple. Assets and bands remain dataset-scoped; an optional selector is not added because they are dataset identity records, not output detail.

- [ ] **Step 4: Implement M3 service use of the amended M2 read surface**

`DatasetService` calls the amended M2 `get_dataset`, `list/count_datasets`, paginated child reads, publication reads, and `output_has_publication_reference`; M3 does not define a parallel read repository, edit `partition_domain_store.py`, or issue ad-hoc dataset/publication/cleanup SQL. Route tests inject `InMemoryPartitionDomainStore` and real tests use `OpenGaussPartitionDomainStore`, proving surface parity.

- [ ] **Step 5: Register exact routes**

Register:

```text
GET /v1/partition/datasets
GET /v1/partition/datasets/{dataset_id}
GET /v1/partition/datasets/{dataset_id}/assets
GET /v1/partition/datasets/{dataset_id}/bands
GET /v1/partition/datasets/{dataset_id}/tiles
GET /v1/partition/datasets/{dataset_id}/indexes
GET /v1/partition/datasets/{dataset_id}/grid
GET /v1/partition/datasets/{dataset_id}/quality
GET /v1/partition/datasets/{dataset_id}/publications
```

Use FastAPI `Query(ge=1, le=500)` and return `Page.model_dump(mode="json")`. A missing dataset or foreign version returns 404 with stable code `partition_dataset_not_found` or `partition_output_version_not_found`; a bad sort returns 422 `invalid_sort`.

- [ ] **Step 6: Run dataset API tests**

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_web/tests/test_dataset_api.py -q
```

Expected: all dataset, child-scope, sorting, pagination, and version tests pass.

---

### Task 7: Expose Normalized Quality Run, Result, and Error APIs

**Files:**
- Rewrite: `cube_web/cube_web/routes/quality.py`
- Modify: `cube_web/cube_web/schemas.py`
- Modify: `cube_web/cube_web/routes/partition_datasets.py`
- Modify: `cube_web/cube_web/services/auth_service.py`
- Modify: `cube_web/cube_web/routes/auth.py`
- Create: `cube_web/tests/test_quality_api.py`

**Interfaces:**
- Produces `Actor(username: str, role: str)`, `current_actor(request) -> Actor`, `require_admin(actor) -> Actor`.
- Produces `ManualQualityRunRequest(dataset_id: str, output_version: str | None)` and dataset route body `DatasetQualityRunRequest(output_version: str | None)`.
- Produces exact normalized routes `/v1/quality/records`, `/records/{id}`, `/results`, `/errors`, `POST /v1/quality/runs`, and dataset-scoped POST quality-runs.

- [ ] **Step 1: Write failing route/auth tests**

```python
def test_manual_current_run_and_global_records(client, current_dataset, user_token) -> None:
    created = client.post(
        "/v1/partition/datasets/dataset-a/quality-runs",
        json={},
        headers={"Authorization": f"Bearer {user_token}"},
    )
    assert created.status_code == 202
    assert created.json()["output_version"] == "ov-current"
    records = client.get("/v1/quality/records", params={"dataset_id": "dataset-a", "page": 1, "page_size": 20})
    assert records.status_code == 200
    assert records.json()["items"][0]["quality_run_id"] == created.json()["quality_run_id"]


def test_explicit_historical_version_requires_admin(client, current_dataset, user_token, admin_token) -> None:
    user = client.post(
        "/v1/quality/runs",
        json={"dataset_id": "dataset-a", "output_version": "ov-old"},
        headers={"Authorization": f"Bearer {user_token}"},
    )
    assert user.status_code == 403
    admin = client.post(
        "/v1/quality/runs",
        json={"dataset_id": "dataset-a", "output_version": "ov-old"},
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert admin.status_code == 202
    assert admin.json()["is_current"] is False
```

- [ ] **Step 2: Run tests and verify missing normalized routes**

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_web/tests/test_quality_api.py -q
```

Expected: normalized requests return 404 or method-not-allowed.

- [ ] **Step 3: Persist actor identity in auth middleware**

When auth is required, verify once and set `request.state.actor = Actor(username, role)`. When auth is disabled, set deterministic local actor `Actor(username="local-development", role="admin")`. Preserve `PUBLIC_V1_PATHS = {"/v1/partition/schemas/import"}` exactly; the public import path still receives a non-admin system actor so downstream logs have identity but auth is not newly required. `current_actor(request)` fails closed with HTTP 401 if middleware did not set state; it never re-decodes a token or trusts JSON. `require_admin` accepts normalized roles `admin`, `administrator`, and `管理员`; all other roles raise 403. Never trust an actor supplied in request JSON.

- [ ] **Step 4: Implement manual allocation under one transaction**

```python
def request_manual_quality_run(dataset_id: str, output_version: str | None, actor: Actor) -> QualityRun:
    store = require_open_gauss_domain_store()
    with store.transaction() as tx:
        dataset = lock_dataset(tx, dataset_id)
        selected = output_version or dataset.current_output_version
        if selected is None:
            raise OutputVersionNotFound("dataset has no current output")
        if output_version is not None and output_version != dataset.current_output_version:
            require_admin(actor)
        snapshot = snapshot_rules_for_output(tx, dataset_id=dataset_id, output_version=selected)
        return allocate_quality_run(
            tx,
            dataset_id=dataset_id,
            output_version=selected,
            expected_current_output_version=dataset.current_output_version if output_version is None else None,
            quality_run_id=uuid4(),
            trigger_event_id=None,
            trigger="manual",
            requested_by=actor.username,
            rule_set_version=DEFAULT_RULE_SET_VERSION,
            rule_snapshot=snapshot,
        )
```

The explicit historical branch still validates ownership/completed status inside `allocate_quality_run`; `None` for expected pointer permits a concurrent current switch without reclassifying the historical run as current.

- [ ] **Step 5: Register exact normalized quality routes and filters**

```text
GET  /v1/quality/records
GET  /v1/quality/records/{quality_run_id}
GET  /v1/quality/records/{quality_run_id}/results
GET  /v1/quality/records/{quality_run_id}/errors
POST /v1/quality/runs
POST /v1/partition/datasets/{dataset_id}/quality-runs
```

Records filters are `keyword`, `dataset_id`, `output_version`, `data_type`, `status`, `trigger`, `requested_by`, `current_only`, `started_from`, and `started_to`. The default sort alias `generated_at` maps to `COALESCE(completed_at, started_at, created_at)` and appends `quality_run_id`; `current_only=true` compares to the dataset current pointer. Error filters are exactly Task 1 `QualityErrorFilter`. Every list uses the standard Page shape; record details include rule snapshot, counts, execution error, `results_complete`, sequence, and `is_current`.

- [ ] **Step 6: Add concurrency and late-completion API tests**

Use a barrier and two threads against actual transaction fixtures to assert concurrent POSTs get distinct sequences. Add tests that a newer run wins if an older run completes later, a prior-version run never changes current fields, a version switch clears current quality before outbox dispatch, and `fail/error/pending/running` remain visible as history.

- [ ] **Step 7: Run API tests**

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest \
  cube_web/tests/test_quality_api.py \
  cube_web/tests/test_dataset_api.py -q
```

Expected: all normalized route and concurrency tests pass.

---

### Task 8: Stream Complete or Whitelist-Filtered CSV and JSON Exports

**Files:**
- Create: `cube_web/cube_web/services/quality_export.py`
- Create: `cube_web/tests/test_quality_export.py`
- Modify: `cube_web/cube_web/routes/quality.py`

**Interfaces:**
- Consumes: Task 3 `iter_quality_errors` and `count_quality_errors` with the same filter object.
- Produces: `quality_export_filename(run, format, filtered) -> str` and `stream_quality_errors(run_id, filters, format) -> tuple[Iterator[bytes], int, str, str]`.
- `stream_quality_errors` obtains the count in one short transaction, but its returned generator opens a new `require_open_gauss_domain_store().transaction()` and named cursor on first iteration, owns both for the iterator's entire lifetime, and closes cursor/transaction in `finally` on exhaustion, encoder error, cancellation, or client disconnect. It never returns an iterator backed by an already-exited transaction.
- CSV columns are exactly `quality_error_id,quality_run_id,rule_code,source_asset_id,tile_id,index_id,output_id,row_number,field,error_code,message,context,created_at`. JSON format is a streamed JSON array of complete objects.

- [ ] **Step 1: Write failing count, filename, and no-materialization tests**

```python
def test_csv_full_and_filtered_counts_equal_repository(client, seeded_2505_errors, repository) -> None:
    full = client.get(f"/v1/quality/records/{seeded_2505_errors}/errors/export", params={"format": "csv"})
    assert full.status_code == 200
    assert len(full.text.splitlines()) - 1 == repository.count_quality_errors(seeded_2505_errors, filters=QualityErrorFilter())
    filtered = client.get(
        f"/v1/quality/records/{seeded_2505_errors}/errors/export",
        params={"format": "csv", "rule_code": "bounds", "error_code": "outside_extent"},
    )
    filters = QualityErrorFilter(rule_code="bounds", error_code="outside_extent")
    assert len(filtered.text.splitlines()) - 1 == repository.count_quality_errors(seeded_2505_errors, filters=filters)
    assert "filtered" in filtered.headers["content-disposition"]
```

Add equivalent JSON tests, a page-size-independence test, Unicode/context round-trip, CSV formula-safe quoting (prefix values beginning `=`, `+`, `-`, or `@` with a single quote), an iterator spy proving the service never calls `list()` or fetches all rows, and `test_early_client_disconnect_closes_export_cursor_and_transaction` that consumes one chunk, closes the response iterator, and asserts both tracked resources close exactly once.

- [ ] **Step 2: Run tests and verify export route absence**

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_web/tests/test_quality_export.py -q
```

Expected: export requests return 404.

- [ ] **Step 3: Implement bounded streaming encoders**

```python
CSV_COLUMNS = (
    "quality_error_id", "quality_run_id", "rule_code", "source_asset_id", "tile_id", "index_id",
    "output_id", "row_number", "field", "error_code", "message", "context", "created_at",
)


def csv_chunks(rows: Iterator[QualityError]) -> Iterator[bytes]:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=CSV_COLUMNS)
    writer.writeheader()
    yield buffer.getvalue().encode("utf-8-sig")
    buffer.seek(0); buffer.truncate(0)
    for row in rows:
        payload = row.model_dump(mode="json")
        payload["context"] = json.dumps(payload["context"], ensure_ascii=False, separators=(",", ":"))
        for key, value in payload.items():
            if isinstance(value, str) and value.startswith(("=", "+", "-", "@")):
                payload[key] = "'" + value
        writer.writerow(payload)
        yield buffer.getvalue().encode("utf-8")
        buffer.seek(0); buffer.truncate(0)


def json_chunks(rows: Iterator[QualityError]) -> Iterator[bytes]:
    yield b"["
    first = True
    for row in rows:
        if not first:
            yield b","
        first = False
        yield json.dumps(row.model_dump(mode="json"), ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    yield b"]"
```

- [ ] **Step 4: Generate exact safe filenames**

Normalize dataset code to ASCII-safe `[A-Za-z0-9._-]`, convert quality time to UTC `YYYYMMDDTHHMMSSZ`, and return:

```text
<dataset-code>_<quality-time>_<quality-run-id>_errors.csv
<dataset-code>_<quality-time>_<quality-run-id>_errors.json
<dataset-code>_<quality-time>_<quality-run-id>_errors_filtered.csv
<dataset-code>_<quality-time>_<quality-run-id>_errors_filtered.json
```

- [ ] **Step 5: Register one route with `format=csv|json`**

```text
GET /v1/quality/records/{quality_run_id}/errors/export
```

Use `StreamingResponse`, `Content-Disposition: attachment`, `X-Export-Count` from `count_quality_errors`, and media types `text/csv; charset=utf-8` or `application/json`. Do not accept page/page_size on export.

- [ ] **Step 6: Run export tests**

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_web/tests/test_quality_export.py -q
```

Expected: full and every filtered CSV/JSON count equals the repository count.

---

### Task 9: Add Immutable Warn Approval, Locked Publication, and Exact Withdrawal

**Files:**
- Create: `cube_web/cube_web/services/publication_gateway.py`
- Create: `cube_web/cube_web/services/publication_service.py`
- Create: `cube_web/tests/test_publication_service.py`
- Modify: `cube_web/cube_web/routes/partition_datasets.py`
- Modify: `cube_web/cube_web/schemas.py`

**Interfaces:**
- Produces `PublicationGateway.activate(snapshot: PublicationSnapshot, *, idempotency_key: str) -> str`, `deactivate(service_version_id: str, *, idempotency_key: str) -> None`, and `inspect(idempotency_key: str) -> GatewayOperation`; production configuration must provide a server-side gateway with idempotent operations, while tests inject `RecordingPublicationGateway`.
- Produces `claim_publication_actions(tx, *, worker_id, limit=10, lease_seconds=300) -> list[PublicationLease]`, `reconcile_publication_action(lease) -> Publication`, and startup recovery for stale `publishing|withdrawing` rows. `publication_id` plus desired action is the stable gateway idempotency key; no request thread performs an untracked external side effect.
- Produces `approve_warn(dataset_id, quality_run_id, reason, actor) -> WarnApproval`, `publish_dataset(dataset_id, request, actor) -> Publication`, and `withdraw_publication(dataset_id, publication_id, reason, actor) -> Publication`.
- Request bodies: `WarnApprovalRequest(reason: str min_length=1 max_length=2000)`, `PublishRequest(output_version: str | None, quality_run_id: UUID | None)`, `WithdrawPublicationRequest(reason: str min_length=1 max_length=2000)`.

- [ ] **Step 1: Write failing approval and publication policy tests**

```python
def test_warn_approval_is_exact_current_run_and_immutable(service, admin, current_warn_run) -> None:
    approval = service.approve_warn("dataset-a", current_warn_run.quality_run_id, "Reviewed domain warnings", admin)
    assert approval.rule_set_version == current_warn_run.rule_set_version
    assert service.approve_warn("dataset-a", current_warn_run.quality_run_id, "Reviewed domain warnings", admin) == approval
    with pytest.raises(WarnApprovalConflict):
        service.approve_warn("dataset-a", current_warn_run.quality_run_id, "Different reason", admin)


def test_old_approved_warn_cannot_publish_after_new_run(service, admin, approved_warn_run, newer_pass_run) -> None:
    with pytest.raises(PublicationPolicyRejected, match="current quality run"):
        service.publish_dataset(
            "dataset-a",
            PublishRequest(output_version=approved_warn_run.output_version, quality_run_id=approved_warn_run.quality_run_id),
            admin,
        )
```

Add these exact policy/race tests:

```python
def test_pass_current_run_publishes(...):
    assert service.publish_dataset("dataset-a", PublishRequest(), admin).status in {"publishing", "active"}

def test_warn_without_approval_is_rejected(...):
    with pytest.raises(PublicationPolicyRejected, match="approval"): ...

@pytest.mark.parametrize("status", ["fail", "error", "pending", "running", "cancelled"])
def test_non_authorizing_status_is_rejected(status, ...):
    with pytest.raises(PublicationPolicyRejected): ...

def test_output_run_dataset_mismatch_is_rejected(...): ...
def test_noncurrent_output_is_rejected(...): ...
def test_approval_racing_new_allocation_rechecks_current_run(...): ...
def test_publication_racing_output_or_qc_switch_rechecks_locked_pointers(...): ...
def test_repeat_publish_returns_same_active_or_publishing_record(...): ...
def test_failed_activation_preserves_snapshot_tuple(...): ...
def test_republish_after_withdrawal_uses_new_publication_id(...): ...
def test_withdraw_deactivates_requested_nonlatest_publication(...): ...
```

The two publication race tests coordinate threads with barriers immediately before lock acquisition, then assert either a valid snapshot of the pre-switch tuple or a policy conflict; they never permit a mixed tuple. Add `test_activation_crash_after_gateway_call_is_reconciled`, `test_activation_unknown_database_commit_is_reconciled`, `test_withdrawal_crash_after_gateway_call_is_reconciled`, and `test_withdrawal_unknown_database_commit_is_reconciled`; each resumes the leased worker and asserts one idempotency key, the correct final DB lifecycle, and the exact external service version.

- [ ] **Step 2: Run tests and verify modules are absent**

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_web/tests/test_publication_service.py -q
```

Expected: collection fails because publication modules do not exist.

- [ ] **Step 3: Implement locked Warn approval**

```python
def approve_warn(dataset_id: str, quality_run_id: UUID, reason: str, actor: Actor) -> WarnApproval:
    require_admin(actor)
    with require_open_gauss_domain_store().transaction() as tx:
        dataset = lock_dataset(tx, dataset_id)
        run = lock_quality_run(tx, quality_run_id)
        if run.dataset_id != dataset_id or run.output_version != dataset.current_output_version:
            raise WarnApprovalRejected("quality run does not bind the current dataset output")
        if run.quality_run_id != dataset.current_quality_run_id or run.status != "warn" or not run.results_complete:
            raise WarnApprovalRejected("only the exact complete current Warn run can be approved")
        existing = get_warn_approval(tx, quality_run_id)
        if existing is not None:
            if existing.approved_by == actor.username and existing.reason == reason and existing.rule_set_version == run.rule_set_version:
                return existing
            raise WarnApprovalConflict(str(quality_run_id))
        return insert_warn_approval(
            tx,
            approval_id=uuid4(),
            dataset_id=dataset_id,
            output_version=run.output_version,
            quality_run_id=quality_run_id,
            rule_set_version=run.rule_set_version,
            approved_by=actor.username,
            approved_at=utc_now(),
            reason=reason,
        )
```

The unique key is `quality_run_id`; approval rows are never updated or deleted.

- [ ] **Step 4: Validate and reserve the immutable publication snapshot under lock**

Inside one transaction, lock dataset, selected output, selected quality run, and any existing publication in that order. Resolve omitted request IDs from current pointers. Require both requested/resolved IDs to equal the locked current pointers; require completed output; require highest same-version sequence; require complete `pass`, or complete `warn` plus matching approval `quality_run_id` and `rule_set_version`. Insert status `publishing` before releasing locks. This tuple remains immutable.

The request transaction inserts status `publishing`, `desired_action='activate'`, and due lease fields before releasing locks; it returns that immutable record without calling the gateway. The publication worker claims due/stale actions with `FOR UPDATE SKIP LOCKED`, increments a fencing `attempt_count`, and calls `gateway.inspect(f"{publication_id}:activate")`: if already active, persist the inspected service version; otherwise call idempotent `activate(snapshot, idempotency_key=...)`. A new transaction fenced by `(publication_id, claimed_by, attempt_count, status='publishing')` sets `active`, or schedules retry with sanitized `last_error`. Startup invokes the same reconciler for stale rows. A crash before/after the external call and an unknown database commit therefore converge instead of leaving permanent `publishing`. Never re-resolve current pointers after reservation.

- [ ] **Step 5: Implement exact-ID withdrawal**

Lock the requested `(dataset_id, publication_id)` record. Reject foreign IDs, already failed records, and missing `service_version_id`. Repeated withdrawal with the same actor/reason returns the withdrawn/withdrawing record; a conflicting repeat raises `PublicationWithdrawalConflict`. The request transaction sets `status='withdrawing'`, `desired_action='withdraw'`, actor/reason and due lease fields without calling the gateway. The publication worker reconciles `f"{publication_id}:withdraw"` through gateway `inspect`/idempotent `deactivate`, then a fenced transaction sets `withdrawn_at`; unknown external/DB commit is retried safely. Do not delete rows or modify snapshot IDs.

- [ ] **Step 6: Register exact mutation routes**

```text
POST /v1/partition/datasets/{dataset_id}/quality-runs/{quality_run_id}/warn-approvals
POST /v1/partition/datasets/{dataset_id}/publish
POST /v1/partition/datasets/{dataset_id}/publications/{publication_id}/withdraw
```

Warn approval requires admin. Publication and withdrawal require authenticated actor; policy remains server-side. Return 201 for new approval/publication, 200 for idempotent existing records, 409 for races/policy changes, and 404 for foreign IDs.

- [ ] **Step 7: Run publication tests**

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_web/tests/test_publication_service.py -q
```

Expected: all approval, lock/race, idempotency, snapshot, and exact withdrawal tests pass.

---

### Task 10: Switch Consumers and Delete Every Legacy Quality Route, Store, and Coupling

**Files:**
- Delete: `cube_web/cube_web/routes/quality_adapters.py`
- Delete: `cube_web/cube_web/services/quality_report_store.py`
- Delete: `cube_web/cube_web/services/quality_pdf.py`
- Delete: `cube_web/cube_web/services/quality_checks.py`
- Delete: `cube_split/cube_split/quality/__init__.py`
- Delete: `cube_split/cube_split/quality/optical_quality.py`
- Delete: `cube_split/cube_split/quality/radar_quality.py`
- Delete: `cube_split/cube_split/quality/product_quality.py`
- Delete: `cube_split/cube_split/quality/carbon_quality.py`
- Delete: `cube_split/tests/test_quality_check.py`
- Modify: `cube_split/tests/test_product_workflow.py`
- Modify: `cube_split/tests/test_partition_e2e_smoke.py`
- Modify: `cube_web/tests/test_partition_entity_coverage.py`
- Rewrite: `cube_web/cube_web/services/quality_service.py`
- Modify: `cube_web/cube_web/services/ingest_service.py`
- Modify: `cube_web/cube_web/services/partition_runners.py`
- Modify: `cube_web/cube_web/services/partition_workflow.py`
- Modify: `cube_web/cube_web/services/partition_job_store.py`
- Modify: `cube_web/cube_web/schemas.py`
- Modify: `cube_web/tests/test_app.py`
- Test: `cube_web/tests/test_quality_api.py`

**Interfaces:**
- Consumes: normalized Tasks 2–9 only.
- Removes `QualityRunRequest(run_dir)`, `QualityLatestRequest`, `QualityReportRequest`, `QualityHistoryRequest`, `QualityReportStore`, `get/set_quality_report_store`, PDF/TXT responses, and all data-type-specific quality functions.
- Removes all batch fields/coupling `quality_report_id`, `quality_report`, and `quality_failure_reason`; dataset current quality is the only aggregate.

- [ ] **Step 1: Add failing absence tests before deleting code**

```python
def test_only_normalized_quality_routes_exist(app) -> None:
    paths = {route.path for route in app.routes}
    forbidden_suffixes = {"/latest", "/report", "/report/pdf", "/report/txt", "/history"}
    assert not any(path.startswith("/v1/quality/optical") for path in paths)
    assert not any(path.startswith("/v1/quality/radar") for path in paths)
    assert not any(path.startswith("/v1/quality/product") for path in paths)
    assert not any(path.startswith("/v1/quality/carbon") for path in paths)
    assert "/v1/quality/records" in paths


def test_legacy_quality_modules_are_absent() -> None:
    for name in (
        "cube_web.routes.quality_adapters",
        "cube_web.services.quality_report_store",
        "cube_web.services.quality_pdf",
        "cube_web.services.quality_checks",
    ):
        assert importlib.util.find_spec(name) is None
```

- [ ] **Step 2: Run absence tests and verify they fail on legacy modules/routes**

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest \
  cube_web/tests/test_quality_api.py::test_only_normalized_quality_routes_exist \
  cube_web/tests/test_quality_api.py::test_legacy_quality_modules_are_absent -q
```

Expected: failures list the still-registered routes and modules.

- [ ] **Step 3: Remove legacy routing and storage atomically with consumer switch**

Delete the four `cube_web` legacy files and the five-file `cube_split/cube_split/quality` package. Delete `cube_split/tests/test_quality_check.py`; remove only deleted quality-runner cases/imports from `cube_split/tests/test_product_workflow.py`, `cube_split/tests/test_partition_e2e_smoke.py`, and `cube_web/tests/test_partition_entity_coverage.py`, retaining their partition/ingest/entity coverage against normalized M3 quality behavior. Replace `quality_service.py` with normalized manual-trigger/read orchestration used by `routes/quality.py`. Remove legacy schema models and imports. Remove ingest lookup by `report_id`; ingest/partition flows now operate on M2 dataset/output records and automatic quality outbox only. Remove partition runner execution of run-directory checks and batch persistence of report blobs/IDs/status; completed output causes only the M2 outbox event.

M3 owns this deletion because M5 is verification-only. After Task 10, no production or test import of `cube_split.quality`, no run-directory quality CLI, and no old report generator remains.

M2's canonical scheduling DDL must never recreate `quality_report_id` or `quality_failure_reason`; the required M2 amendment adds an assertion for their absence. M3 Task 10 removes only residual `partition_job_store.py` methods/serialization and tests, not scheduling schema columns. The only schema deletion in M3 remains `quality_reports` through the guarded reset/bootstrap.

- [ ] **Step 4: Drop `quality_reports` only through the explicit M2 reset/bootstrap path**

M2's destructive development reset already lists `quality_reports` in its legacy allowlist and drop order. M3 adds a real-schema assertion that after the guarded reset/bootstrap and M3 start, `to_regclass('quality_reports') IS NULL`; normal application startup never issues `DROP TABLE`.

- [ ] **Step 5: Run static legacy-reference scan**

```bash
! git grep -n -E \
  'quality_report_id|quality_failure_reason|QualityReportStore|get_quality_report_store|set_quality_report_store|quality_adapters|quality_pdf|cube_split\.quality|/quality/(optical|radar|product|carbon)/(latest|report|history)' \
  -- cube_web/cube_web cube_web/tests cube_split/cube_split cube_split/tests
```

Expected: command succeeds because `git grep` finds no matches. Tests may use the phrase `quality_reports` only in the guarded reset/schema assertion and may mention forbidden route strings in absence tests; keep those two test cases outside this broad scan or narrow the scan to production for that assertion.

- [ ] **Step 6: Run web regressions**

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_web/tests -q
```

Expected: the complete web suite passes with normalized quality behavior and no compatibility routes.

---

### Task 11: Add Mandatory Actual OpenGauss/MinIO/Ray M3 Acceptance

**Files:**
- Create: `cube_web/tests/real/test_m3_quality_publication_real.py`
- Create: `cube_web/scripts/run_m3_quality_publication_gate.py`
- Modify: `pytest.ini`

**Interfaces:**
- Adds marker `m3_real: actual OpenGauss, MinIO, Ray dataset-quality-publication acceptance; never skipped`.
- Gate script validates required runtime configuration, probes actual services, invokes pytest, and exits nonzero on missing/unreachable infrastructure, skipped tests, count mismatch, or scenario failure.
- Exact real input contract: `CUBE_M3_REAL_INPUT_MANIFEST` names a UTF-8 JSON file outside the repository with one strict M2 `StrictPartitionRequest` payload; it contains non-empty `batch_id`, grid `geohash`, positive `requested_grid_level`, `cover_mode`, and exactly one dataset with stable identity, normalized bands, and at least one source asset whose `cog_uri` is `s3://<CUBE_WEB_MINIO_BUCKET>/...`, checksum is a 64-character lowercase SHA-256, CRS is `EPSG:4326`, bbox/time fields are valid, and the object exists in the configured actual MinIO bucket. `CUBE_M3_REAL_DEFECT_MANIFEST` names a second strict payload with at least 501 deterministic metadata defects so pagination/full export is observable. The gate rejects paths inside the repository, malformed/extra fields, bucket mismatch, missing objects, checksum mismatch, or fewer than 501 expected findings.
- Uses those manifests to create M2-completed real dataset/output versions through actual Ray and MinIO; no direct fixture insertion may substitute for the real output path.

- [ ] **Step 1: Add a failing infrastructure preflight test**

```python
@pytest.mark.m3_real
def test_real_dependencies_are_required(real_runtime) -> None:
    required = {
        "CUBE_WEB_POSTGRES_DSN",
        "CUBE_WEB_RAY_ADDRESS",
        "CUBE_WEB_MINIO_ENDPOINT",
        "CUBE_WEB_MINIO_ACCESS_KEY",
        "CUBE_WEB_MINIO_SECRET_KEY",
        "CUBE_WEB_MINIO_BUCKET",
        "CUBE_M3_REAL_INPUT_MANIFEST",
        "CUBE_M3_REAL_DEFECT_MANIFEST",
    }
    assert required.issubset(real_runtime.present_environment_names)
    assert real_runtime.open_gauss.select_one("SELECT 1") == 1
    assert real_runtime.minio.bucket_exists(real_runtime.bucket)
    ray.init(address=real_runtime.ray_address, ignore_reinit_error=True)
    assert ray.cluster_resources().get("CPU", 0) > 0
```

`real_runtime` calls `pytest.fail`, never `pytest.skip`, when `CUBE_WEB_POSTGRES_DSN`, `CUBE_WEB_RAY_ADDRESS`, MinIO endpoint/access/secret/bucket, `CUBE_M3_REAL_INPUT_MANIFEST`, `CUBE_M3_REAL_DEFECT_MANIFEST`, or publication gateway configuration is absent or unreachable. It loads both files with `StrictPartitionRequest.model_validate_json`, verifies paths are outside the repository, verifies every manifest COG by actual MinIO stat/download and SHA-256, and verifies the defect manifest declares at least 501 deterministic optional/mandatory findings before submitting either request.

- [ ] **Step 2: Register marker and run the preflight**

Add to `pytest.ini`:

```ini
markers =
    m3_real: actual OpenGauss, MinIO, Ray dataset quality and publication acceptance
```

Run:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest \
  cube_web/tests/real/test_m3_quality_publication_real.py::test_real_dependencies_are_required -v -m m3_real
```

Expected: PASS only with all actual services reachable; otherwise FAIL with the missing/unreachable dependency.

- [ ] **Step 3: Exercise real automatic/manual concurrency and complete storage**

Create one real M2 output through Ray/MinIO, confirm its outbox event, dispatch automatic quality, concurrently allocate two manual runs, execute them, and assert unique monotonic sequences. Seed a controlled real-data defect producing more than one HTTP page of normalized errors; assert OpenGauss stores every error and result totals equal run totals. Force one dependency failure after an earlier batch and assert terminal `error`, preserved rows, full execution error, and `results_complete=false`.

- [ ] **Step 4: Exercise exact OpenGauss export counts**

For each of four cases—unfiltered CSV, unfiltered JSON, filtered CSV, filtered JSON—execute the exact parameterized OpenGauss count predicate, stream the HTTP export, parse every record, and assert:

```python
assert response.headers["X-Export-Count"] == str(open_gauss_count)
assert parsed_export_count == open_gauss_count
```

Use at least `rule_code + error_code` in filtered cases and verify `filtered` filename. Any mismatch fails the real gate.

- [ ] **Step 5: Exercise publication authorization, immutable snapshots, and exact withdrawal**

On actual persisted records, execute: Pass publication; Warn rejection before approval; admin approval of exact current Warn then successful publication; old approved Warn rejection after newer run; race publication against current output/QC switch; repeated publish idempotency; publish snapshot stability after a later output; withdrawal by exact non-latest `publication_id`; and republish after withdrawal with a new ID. Assert OpenGauss history remains and the configured server-side gateway deactivates the exact `service_version_id`.

- [ ] **Step 6: Implement the canonical gate runner**

`run_m3_quality_publication_gate.py` performs probes, then invokes exactly:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest \
  cube_web/tests/real/test_m3_quality_publication_real.py -v -m m3_real -rs
```

It parses pytest's exit status and fails if output contains ` skipped`, `SKIPPED`, or ` deselected` for an M3 real scenario. It prints sanitized service endpoints and scenario/count summaries, never credentials.

- [ ] **Step 7: Run the complete real gate from repository root**

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 cube_web/scripts/run_m3_quality_publication_gate.py
```

Expected: all actual OpenGauss/MinIO/Ray quality, export, approval, publication, race, and withdrawal scenarios pass; zero skips; every export count exactly equals the corresponding OpenGauss count.

---

### Task 12: Execute Four Quality Levels, Four Review Roles, and One M3 Commit

**Files:**
- Verify only: all M3-owned files listed above
- Do not modify: M2-owned `cube_web/cube_web/services/partition_contracts.py`, `partition_domain_store.py`, or `partition_domain_schema.py`

**Interfaces:**
- Produces one integrated M3 commit for M4.
- Records command/result summaries and unresolved findings in the commit body or implementation handoff; unresolved high-severity findings are forbidden.

- [ ] **Step 1: Level 1 — run task-focused TDD suites**

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest \
  cube_web/tests/test_quality_contracts.py \
  cube_web/tests/test_quality_repository.py \
  cube_web/tests/test_quality_rules.py \
  cube_web/tests/test_quality_worker.py \
  cube_web/tests/test_dataset_api.py \
  cube_web/tests/test_quality_api.py \
  cube_web/tests/test_quality_export.py \
  cube_web/tests/test_publication_service.py -q
```

Expected: all focused tests pass.

- [ ] **Step 2: Level 2 — run package and complete web regressions**

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_encoder/tests cube_split/tests
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_web/tests
```

Expected: both suites pass with no quality compatibility route restored.

- [ ] **Step 3: Level 3 — run static checks and all builds**

```bash
python3.11 -m ruff check cube_encoder cube_split cube_web
python3.11 -m mypy cube_encoder/grid_core cube_split/cube_split cube_web/cube_web
python3.11 -m build cube_encoder
python3.11 -m build cube_split
python3.11 -m build cube_web
npm --prefix cube_web/frontend ci
npm --prefix cube_web/frontend run build
```

Expected: Ruff and mypy report no errors; all three Python distributions and the current frontend build succeed. M4, not M3, owns new frontend unit/Playwright behavior.

- [ ] **Step 4: Level 4 — run actual infrastructure and real-data acceptance**

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 cube_web/scripts/run_m3_quality_publication_gate.py
```

Expected: actual OpenGauss/MinIO/Ray and configured publication gateway pass every scenario with zero skip/block and exact export/database count equality.

- [ ] **Step 5: Complete the four review roles**

1. Implementer self-review checks every task and attaches Level 1 evidence.
2. Independent Sonnet or Opus review checks transaction ordering, SQL parameterization, unbounded storage/streaming, auth, and deletion; any high-severity finding blocks.
3. Adversarial verifier attempts concurrent triggers, outbox/manual duplication, event redelivery, stale leases, late completion, approval race, publication/output/QC race, repeated publish, export count mismatch, and wrong-publication withdrawal.
4. Main Opus resolves every finding and personally reruns Levels 1–4. Missing real infrastructure is a blocker, not a waiver.

- [ ] **Step 6: Verify only M3 files are changed**

```bash
git status --short
git diff --check
git diff --name-status
```

Expected: no whitespace errors; only M3-owned files appear. Preserve unrelated user changes and do not stage them.

- [ ] **Step 7: Create the single coherent M3 commit**

```bash
git add \
  cube_web/cube_web/app.py \
  cube_web/cube_web/routes/auth.py \
  cube_web/cube_web/routes/partition.py \
  cube_web/cube_web/routes/partition_datasets.py \
  cube_web/cube_web/routes/quality.py \
  cube_web/cube_web/schemas.py \
  cube_web/cube_web/services/auth_service.py \
  cube_web/cube_web/services/dataset_service.py \
  cube_web/cube_web/services/ingest_service.py \
  cube_web/cube_web/services/partition_job_store.py \
  cube_web/cube_web/services/partition_runners.py \
  cube_web/cube_web/services/partition_workflow.py \
  cube_web/cube_web/services/publication_gateway.py \
  cube_web/cube_web/services/publication_service.py \
  cube_web/cube_web/services/quality_contracts.py \
  cube_web/cube_web/services/quality_export.py \
  cube_web/cube_web/services/quality_repository.py \
  cube_web/cube_web/services/quality_rules.py \
  cube_web/cube_web/services/quality_service.py \
  cube_web/cube_web/services/quality_worker.py \
  cube_web/cube_web/routes/quality_adapters.py \
  cube_web/cube_web/services/quality_checks.py \
  cube_web/cube_web/services/quality_pdf.py \
  cube_web/cube_web/services/quality_report_store.py \
  cube_split/cube_split/quality/__init__.py \
  cube_split/cube_split/quality/optical_quality.py \
  cube_split/cube_split/quality/radar_quality.py \
  cube_split/cube_split/quality/product_quality.py \
  cube_split/cube_split/quality/carbon_quality.py \
  cube_split/tests/test_quality_check.py \
  cube_split/tests/test_product_workflow.py \
  cube_split/tests/test_partition_e2e_smoke.py \
  cube_web/tests/test_partition_entity_coverage.py \
  cube_web/scripts/run_m3_quality_publication_gate.py \
  cube_web/tests/test_app.py \
  cube_web/tests/test_dataset_api.py \
  cube_web/tests/real/test_m3_quality_publication_real.py \
  cube_web/tests/test_publication_service.py \
  cube_web/tests/test_quality_api.py \
  cube_web/tests/test_quality_contracts.py \
  cube_web/tests/test_quality_export.py \
  cube_web/tests/test_quality_repository.py \
  cube_web/tests/test_quality_repository_real.py \
  cube_web/tests/test_quality_rules.py \
  cube_web/tests/test_quality_worker.py \
  pytest.ini
git diff --cached --check
git commit -m "feat: add dataset quality and publication"
```

Expected: one local M3 commit; no push, remote branch, or PR.

## M4 Handoff Contract

M4 consumes the exact routes and Page shape in Tasks 6–9 without backend renames. Dataset rows expose derived `publish_status` as exactly `unpublished|publishing|active|withdrawing|failed|withdrawn`; `Publication.status` uses the same lifecycle values except it never uses `unpublished`. M4 must map/display `active` directly and remove its draft-only `published` literal.

M3 does not own, wrap, or adapt partition submission. M4 must consume the exact M2 partition route and complete `StrictPartitionRequest` payload (`batch_id`, grid/request fields, and normalized dataset/assets/bands), not submit an IDs-only surrogate and not ask M3 to reconstruct loader data.

M4-facing quality/data routes are:

```text
Dataset detail:
GET /v1/partition/datasets/{dataset_id}
GET /v1/partition/datasets/{dataset_id}/{assets|bands|tiles|indexes|grid|quality|publications}

Quality drawer:
GET /v1/quality/records/{quality_run_id}
GET /v1/quality/records/{quality_run_id}/results
GET /v1/quality/records/{quality_run_id}/errors

Export:
GET /v1/quality/records/{quality_run_id}/errors/export?format=csv|json
GET /v1/quality/records/{quality_run_id}/errors/export?format=csv|json&rule_code=...&error_code=...&field=...

Rerun:
POST /v1/quality/runs
{"dataset_id": "dataset-a", "output_version": "ov-completed"}
```

Every collection above returns exactly `{"items": [...], "total": int, "page": int, "page_size": int}` except streaming export. The rerun response is the Task 1 `QualityRun` JSON and returns HTTP 202. M4 may add frontend request cancellation, Pinia stores, drawers, download controls, and router guards, but it must not reinterpret current quality, relax explicit historical-run admin rules, send visible-page limits to export, or publish an older run. The two export actions map to no filters (all) and the current whitelist filters (filtered), respectively.

## Completion Checklist

- [ ] M1 and M2 are merged and passed all four levels before M3 begins.
- [ ] Every run binds a completed dataset-owned output and snapshots the full applicable rule set.
- [ ] Concurrent/manual/automatic sequences are monotonic and event redelivery is idempotent.
- [ ] Late old-version or lower-sequence completion cannot overwrite current quality.
- [ ] Mandatory/optional/exception status semantics and incomplete-result behavior are exact.
- [ ] Every error is stored; pagination never caps storage or export.
- [ ] Full/filtered CSV and JSON stream and exactly match OpenGauss counts.
- [ ] Warn approval is immutable, admin-only, exact-current-run-specific, and cannot authorize a newer run.
- [ ] Publication locks current output/current run/highest sequence and preserves an immutable tuple.
- [ ] Withdrawal targets exact `publication_id` and exact service version without deleting history.
- [ ] Legacy report table/routes/store/adapters/coupling are absent with no compatibility layer.
- [ ] Levels 1–4 and all four review roles pass, including actual OpenGauss/MinIO/Ray with zero skips.
- [ ] One M3 local commit exists; no push or implementation outside M3 ownership occurred.
