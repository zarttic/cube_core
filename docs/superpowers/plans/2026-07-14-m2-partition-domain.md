# Partition Dataset Domain Milestone 2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make partitioning accept only the normalized loader COG contract and commit dataset-scoped, immutable output versions atomically across OpenGauss and MinIO.

**Architecture:** Validate typed datasets before scheduling, derive the partition method from the Milestone 1 grid contract, and run each dataset independently. Ray workers only cache loader-owned COGs locally; Geohash/MGRS return logical windows while ISEA4H writes entity tiles beneath immutable version prefixes. OpenGauss is the source of truth: one dataset transaction validates the active attempt, writes all versioned records, switches the current pointer, resets current quality state, and emits one unique completion outbox event.

**Tech Stack:** Python 3.11, Pydantic 2, FastAPI, psycopg 3.2+, OpenGauss, Ray 2.55.x, MinIO 7.2+, Rasterio 1.4+, pytest 8+, Ruff, mypy

## Global Constraints

- M2 kickoff is an executable progress-ledger gate, not a prose dependency: read the coordination row with schema `milestone | status | predecessor_integration_hash | integration_hash | l1_status | l2_status | l3_status | l4_status | review_status | ...`; require exactly one row with `milestone=M1`, `status=PASSED`, `integration_hash` matching `^[0-9a-f]{40}$`, and `l1_status=l2_status=l3_status=l4_status=review_status=PASSED`. Then create/update the M2 row and copy that exact M1 `integration_hash` into M2 `predecessor_integration_hash` before any M2 file edit or worker dispatch. Documentation-plan commits `d667e25e0d2597219554e206d8bd91876c83802e` and `3da8c5f1fc4148c670a33214144ed451df9b3254` are invalid substitutes; no implementation hash exists during planning and none may be invented. Any missing/duplicate/failed checkpoint, malformed hash, or copy mismatch blocks M2.
- The same M1 row must cover the passing three-grid SDK contract, including `PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_encoder/tests/integration/test_grid_real_aoi.py -m real_aoi -vv`; a missing real AOI asset is BLOCKED, not skipped.
- Production grids are exactly `geohash`, `mgrs`, and `isea4h`; Geohash/MGRS derive `logical`, and ISEA4H derives `entity`.
- Import the exported Milestone 1 `grid_core.app.models.request.validate_requested_grid_level`; it accepts exactly Geohash `1..12`, MGRS `0..5`, and ISEA4H `0..15` (inclusive). `StrictPartitionRequest` calls it from an after-validator. Never duplicate the ranges or reintroduce request field `grid_level`.
- Consume each Milestone 1 `GridCell.grid_level` and `GridCell.topology_code` property; never read an old `level` field or infer topology from `metadata`.
- `minimal` cover may return mixed cell levels, so every grid/index/tile row uses the result cell's `grid_level`, while the dataset and output version retain `requested_grid_level`.
- The strict loader request is named `StrictPartitionRequest`; do not rename or retain it as the legacy `PartitionDemoRequest`.
- A dataset-level normalized `bands` array is required and legitimate. Reject legacy scalar or asset aliases such as `band`, `polarization`, asset-level `bands`, `selected_assets`, `source_uri`, and `observations`. Carbon is not a special observation shape: `data_type=carbon` uses the same non-empty `DatasetInput.assets` COG records and dataset-level `bands` records as optical/radar/product.
- Loader assets are already COGs. Partition code must not convert, reproject, or re-upload source COGs; workers may only copy `s3://` objects into a node-local cache.
- Do not add zero-valued COG conversion timing fields. Conversion/config/timing fields are absent from requests, runtime results, persisted payloads, and UI-facing task results.
- One batch may contain many datasets, but execution, version commit, retry, failure, and cleanup are isolated by `dataset_id`.
- OpenGauss `partition_datasets.current_output_version` is the only mutable current-output pointer; do not create a MinIO current manifest.
- Output entity tiles use `partition/<dataset_id>/versions/<output_version>/tiles/<deterministic-tile-name>.tif` in the configured bucket.
- A version switch atomically sets `quality_status='pending'`, `current_quality_run_id=NULL`, `quality_error_count=0`, and `quality_warning_count=0`, without resetting the monotonic `quality_sequence`.
- The same switch inserts exactly one `output-version.completed` outbox event per `(dataset_id, output_version)`.
- Milestone 2 owns complete executable structural DDL for quality results, quality errors, Warn approvals, publications, publication leases/reconciliation, and every FK/check/unique/index consumed by M3. M2 owns dataset list/count/detail, publication-reference, output cleanup-state, and paginated asset/band/tile/index/grid/publication reads with in-memory/OpenGauss parity. Milestone 3 consumes this frozen structure/read surface and owns quality/publication business mutations, workflows, APIs, dispatch, approval, and publication behavior.
- `partition_quality_runs.trigger_event_id` is nullable and unique, and intentionally has no foreign key to `partition_domain_outbox`; outbox retention must not invalidate quality history.
- `partition_publications.status` is exactly `publishing | active | withdrawing | failed | withdrawn`; never use `published`. Dataset read rows derive `publish_status` as the latest publication's same lifecycle literal, with `unpublished` added only when no publication record exists.
- M3 cannot start until M2's complete paginated store amendment, exact DDL/catalog tests, and all four M2 review levels pass.
- Development reset has no migration path and may rebuild only the named scheduling/result objects after schema inventory succeeds. It never touches `cube_web_configs`, `ard_*`, or `rs_*`.
- Reset execution requires all three confirmations: `CUBE_WEB_ENV=development`, `--dangerously-reset-partition-domain`, and `--database-name` equal to `SELECT current_database()`.
- No real-dependency test may skip. Missing OpenGauss, MinIO, Ray, or source COG configuration is a blocking failure.
- All commands in this plan run from the repository root. Do not use directory-changing command variants.
- Package dependency direction remains `cube_web -> cube_split -> cube_encoder`; grid logic is consumed only through `grid_core.sdk.CubeEncoderSDK` and Milestone 1 public models.
- Public functions use Python 3.11 type annotations and the repository's 140-column Ruff policy.
- No remote branch, push, or pull request is permitted. Slice commits remain in worker worktrees; the main integrator produces one coherent local Milestone 2 commit after all four gates pass.

---

## File Map, Ownership, and Frozen Interfaces

### File ownership

| Owner/model | Files | Responsibility |
|---|---|---|
| Contract worker — Sonnet | `cube_web/cube_web/services/partition_contracts.py`, `cube_web/cube_web/schemas.py`, `cube_web/cube_web/app.py`, `cube_web/cube_web/routes/partition.py`, `cube_web/cube_web/routes/partition_adapters.py`, `cube_web/tests/test_partition_contracts.py`, `cube_web/tests/test_app.py` | Strict loader models, exact production route/body, method derivation, grouping, version/output identities |
| Ray worker — Sonnet | `cube_split/cube_split/jobs/ray_partition_core.py`, `cube_split/cube_split/jobs/ray_logical_partition_job.py`, `cube_split/cube_split/jobs/entity_partition_job.py`, related narrow tests | Cache-only COG reads and normalized versioned result rows |
| Mechanical cleanup worker — Haiku, mandatory Sonnet review | `cube_web/cube_web/services/config_store.py`, `cube_web/cube_web/services/partition_defaults.py`, `cube_web/cube_web/services/partition_loaded_schemas.py`, `cube_split/pyproject.toml`, `cube_split/tests/test_production_imports.py`, `cube_web/tests/test_app.py` | Delete conversion/reprojection/upload/config/timing, legacy selector paths, and the deferred H3 runtime dependency |
| Schema/store integrator — Opus | `cube_web/cube_web/services/partition_domain_schema.py`, `cube_web/cube_web/services/partition_domain_store.py`, `cube_web/scripts/reset_partition_domain.py`, schema/store tests | Complete DDL, inventory/reset, deterministic transactional persistence |
| Object lifecycle worker — Sonnet | `cube_web/cube_web/services/partition_object_store.py`, `cube_web/tests/test_partition_object_store.py` | Immutable prefixes, manifest verification, guarded cleanup |
| Workflow integrator — Opus | `cube_web/cube_web/services/partition_workflow.py`, `cube_web/cube_web/services/partition_job_store.py`, `cube_web/cube_web/services/partition_runners.py`, workflow tests | Dataset fan-out, active-attempt validation, partial failure, atomic completion |
| Real-gate worker — Sonnet; gate sign-off — Opus | `cube_web/scripts/run_m2_partition_domain_gate.py`, `cube_web/tests/test_partition_domain_real.py`, `cube_web/docs/M2_PARTITION_DOMAIN_GATE.md` | Non-skipping OpenGauss/MinIO/Ray proof and evidence instructions |

`cube_web/tests/test_app.py` is shared sequentially: the contract worker commits first; the mechanical cleanup worker starts from that reviewed commit and edits it only after Task 1 ownership ends. No two workers edit the same file concurrently. All other shared files are integrated by Opus after owning workers have committed and passed their narrow tests.

### Executable M1 predecessor ledger gate

The coordination runner executes this gate against the persisted M1/M2 rows before Task 1 or any worker dispatch. It is the exact gate semantics for the roadmap schema, independent of whether the coordination record is stored as Markdown, CSV, or a service row:

```python
from collections.abc import Mapping
import re

PASSED = "PASSED"
SHA40 = re.compile(r"^[0-9a-f]{40}$")
DOCUMENTATION_ONLY_HASHES = {
    "d667e25e0d2597219554e206d8bd91876c83802e",
    "3da8c5f1fc4148c670a33214144ed451df9b3254",
}
CHECKPOINT_COLUMNS = ("l1_status", "l2_status", "l3_status", "l4_status", "review_status")


def require_m2_kickoff(m1_rows: list[Mapping[str, str]], m2_row: dict[str, str]) -> dict[str, str]:
    if len(m1_rows) != 1 or m1_rows[0].get("milestone") != "M1":
        raise RuntimeError("M2 requires exactly one milestone=M1 coordination row")
    m1 = m1_rows[0]
    integration_hash = m1.get("integration_hash", "")
    if m1.get("status") != PASSED:
        raise RuntimeError("M2 requires M1 status=PASSED")
    if not SHA40.fullmatch(integration_hash) or integration_hash in DOCUMENTATION_ONLY_HASHES:
        raise RuntimeError("M2 requires the 40-hex M1 implementation integration_hash; documentation-plan commits are invalid")
    failed = [name for name in CHECKPOINT_COLUMNS if m1.get(name) != PASSED]
    if failed:
        raise RuntimeError(f"M2 requires passed M1 checkpoints: {failed}")
    if m2_row.get("milestone") != "M2":
        raise RuntimeError("coordination target row must be milestone=M2")
    existing = m2_row.get("predecessor_integration_hash", "")
    if existing not in {"", integration_hash}:
        raise RuntimeError("M2 predecessor_integration_hash conflicts with M1 integration_hash")
    m2_row["predecessor_integration_hash"] = integration_hash
    if m2_row["predecessor_integration_hash"] != m1["integration_hash"]:
        raise RuntimeError("M2 predecessor hash copy failed")
    return m2_row
```

The coordination runner unit-tests these exact cases with `pytest.mark.parametrize`: zero/two M1 rows; `status` equal to `PLANNED`, `RUNNING`, or `FAILED`; integration hashes `"abcdef0"`, `"a" * 39`, `"a" * 41`, and `"g" * 40`; each one of `l1_status` through `review_status` changed independently to `FAILED`; each member of `DOCUMENTATION_ONLY_HASHES`; `m2_row={"milestone": "M3"}`; an existing different 40-hex predecessor; and a valid row `integration_hash="a" * 40` whose exact value is copied. Every invalid case raises `RuntimeError`; the valid case persists the returned M2 row, re-reads it, and asserts `predecessor_integration_hash == "a" * 40` before dispatching Task 1. Planning deliberately supplies no real M1 implementation SHA because none exists yet.

### Frozen contract models

Create these names in `cube_web/cube_web/services/partition_contracts.py`; later tasks import them rather than creating parallel dictionaries or aliases:

```python
from __future__ import annotations

from hashlib import sha256
from typing import Any, Literal

from pydantic import AnyUrl, BaseModel, ConfigDict, Field, field_validator, model_validator

from grid_core.app.models.request import validate_requested_grid_level

GridType = Literal["geohash", "mgrs", "isea4h"]
PartitionMethod = Literal["logical", "entity"]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class SourceAssetInput(StrictModel):
    source_asset_id: str = Field(min_length=1)
    cog_uri: AnyUrl
    checksum: str = Field(pattern=r"^[0-9a-f]{64}$")
    bbox: tuple[float, float, float, float]
    crs: str = Field(min_length=1)
    time_start: str
    time_end: str
    attributes: dict[str, Any] = Field(default_factory=dict)

    @field_validator("cog_uri")
    @classmethod
    def require_s3_cog(cls, value: AnyUrl) -> AnyUrl:
        if value.scheme != "s3":
            raise ValueError("cog_uri must use s3://")
        return value


class BandInput(StrictModel):
    source_asset_id: str = Field(min_length=1)
    band_code: str = Field(min_length=1)
    band_name: str = Field(min_length=1)
    band_type: Literal["spectral", "polarization", "variable"]
    unit: str | None = None
    display_order: int = Field(ge=0)
    attributes: dict[str, Any] = Field(default_factory=dict)


class DatasetInput(StrictModel):
    dataset_id: str = Field(min_length=1)
    dataset_code: str = Field(min_length=1)
    dataset_title: str = Field(min_length=1)
    data_type: Literal["optical", "radar", "product", "carbon"]
    product_type: str | None = None
    assets: tuple[SourceAssetInput, ...] = Field(min_length=1)
    bands: tuple[BandInput, ...] = Field(min_length=1)
    attributes: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def bands_reference_assets(self) -> "DatasetInput":
        asset_ids = {asset.source_asset_id for asset in self.assets}
        unknown = sorted({band.source_asset_id for band in self.bands} - asset_ids)
        if unknown:
            raise ValueError(f"bands reference unknown source assets: {unknown}")
        return self


class StrictPartitionRequest(StrictModel):
    batch_id: str = Field(min_length=1)
    grid_type: GridType
    requested_grid_level: int
    partition_method: PartitionMethod
    cover_mode: Literal["intersect", "contain", "minimal"] = "intersect"
    time_granularity: Literal["second", "minute", "hour", "day", "month"] = "day"
    max_cells_per_asset: int = Field(default=0, ge=0)
    datasets: tuple[DatasetInput, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_grid_contract(self) -> "StrictPartitionRequest":
        validate_requested_grid_level(self.grid_type, self.requested_grid_level)
        validate_partition_method(self.grid_type, self.partition_method)
        return self


class OutputIdentity(StrictModel):
    dataset_id: str
    output_version: str
    source_asset_id: str
    band_code: str
    grid_type: GridType
    grid_level: int
    space_code: str
    topology_code: str | None = None
    time_bucket: str
    window_identity: str


class PartitionDatasetResult(StrictModel):
    dataset_id: str
    task_id: str
    output_version: str
    grid_type: GridType
    requested_grid_level: int
    partition_method: PartitionMethod
    object_prefix: str
    tiles: tuple[dict[str, Any], ...]
    indexes: tuple[dict[str, Any], ...]
    grid_cells: tuple[dict[str, Any], ...]


def derive_partition_method(grid_type: GridType) -> PartitionMethod:
    return "entity" if grid_type == "isea4h" else "logical"


def validate_partition_method(grid_type: GridType, supplied: PartitionMethod) -> PartitionMethod:
    derived = derive_partition_method(grid_type)
    if supplied != derived:
        raise ValueError(f"partition_method must be {derived} for grid_type={grid_type}")
    return derived


def group_datasets(request: StrictPartitionRequest) -> dict[str, DatasetInput]:
    grouped: dict[str, DatasetInput] = {}
    for dataset in request.datasets:
        if dataset.dataset_id in grouped:
            raise ValueError(f"duplicate dataset_id: {dataset.dataset_id}")
        grouped[dataset.dataset_id] = dataset
    return grouped


def make_output_version(dataset_id: str, task_id: str) -> str:
    return sha256(f"{dataset_id}\0{task_id}".encode("utf-8")).hexdigest()[:32]


def make_output_id(identity: OutputIdentity) -> str:
    canonical = identity.model_dump_json(exclude_none=False)
    return sha256(canonical.encode("utf-8")).hexdigest()
```

The contract is frozen exactly as shown. Do not weaken, rename, relocate, or append request fields. `StrictPartitionRequest.validate_grid_contract` invokes the exported M1 validator in the after-validation phase; accepted inclusive ranges are exactly Geohash `1..12`, MGRS `0..5`, and ISEA4H `0..15`. There is no `grid_level_mode` field and no duplicated M2 range table.

### Frozen store surface consumed by Milestone 3

`cube_web/cube_web/services/partition_domain_store.py` exports exactly these classes/accessors:

```python
class PartitionDomainStore:
    """Dataset-result persistence boundary."""


class InMemoryPartitionDomainStore(PartitionDomainStore):
    """Behavioral test double with OpenGauss parity."""


class OpenGaussPartitionDomainStore(PartitionDomainStore):
    """psycopg-backed production implementation."""


def get_partition_domain_store() -> PartitionDomainStore:
    return _partition_domain_store


def set_partition_domain_store(store: PartitionDomainStore) -> None:
    global _partition_domain_store
    _partition_domain_store = store
```

The abstract surface is complete and frozen; `InMemoryPartitionDomainStore` and `OpenGaussPartitionDomainStore` implement every method with matching results and ownership validation:

```python
from datetime import datetime
from typing import Any, Literal

SortOrder = Literal["asc", "desc"]


class PartitionDomainStore:
    def ensure_schema(self) -> None: ...
    def start_output(self, request: StrictPartitionRequest, dataset: DatasetInput, task_id: str) -> str: ...
    def complete_output(self, result: PartitionDatasetResult) -> dict[str, Any]: ...
    def fail_output(self, dataset_id: str, output_version: str, *, error_code: str, error_message: str) -> None: ...
    def resolve_output_version(self, dataset_id: str, output_version: str | None = None) -> str: ...
    def get_dataset(self, dataset_id: str) -> dict[str, Any] | None: ...
    def list_datasets(
        self, *, keyword: str | None, data_type: str | None, product_type: str | None,
        batch_id: str | None, grid_type: str | None, partition_status: str | None,
        quality_status: str | None, publish_status: str | None,
        time_start: datetime | None, time_end: datetime | None,
        limit: int, offset: int, sort_by: str, sort_order: SortOrder,
    ) -> list[dict[str, Any]]: ...
    def count_datasets(
        self, *, keyword: str | None, data_type: str | None, product_type: str | None,
        batch_id: str | None, grid_type: str | None, partition_status: str | None,
        quality_status: str | None, publish_status: str | None,
        time_start: datetime | None, time_end: datetime | None,
    ) -> int: ...
    def get_output_version(self, dataset_id: str, output_version: str) -> dict[str, Any] | None: ...
    def list_assets(self, dataset_id: str, output_version: str | None = None, *, limit: int, offset: int, sort_by: str, sort_order: SortOrder) -> list[dict[str, Any]]: ...
    def count_assets(self, dataset_id: str, output_version: str | None = None) -> int: ...
    def list_bands(self, dataset_id: str, output_version: str | None = None, *, limit: int, offset: int, sort_by: str, sort_order: SortOrder) -> list[dict[str, Any]]: ...
    def count_bands(self, dataset_id: str, output_version: str | None = None) -> int: ...
    def list_tiles(self, dataset_id: str, output_version: str | None = None, *, limit: int, offset: int, sort_by: str, sort_order: SortOrder) -> list[dict[str, Any]]: ...
    def count_tiles(self, dataset_id: str, output_version: str | None = None) -> int: ...
    def list_indexes(self, dataset_id: str, output_version: str | None = None, *, limit: int, offset: int, sort_by: str, sort_order: SortOrder) -> list[dict[str, Any]]: ...
    def count_indexes(self, dataset_id: str, output_version: str | None = None) -> int: ...
    def list_grid_cells(self, dataset_id: str, output_version: str | None = None, *, limit: int, offset: int, sort_by: str, sort_order: SortOrder) -> list[dict[str, Any]]: ...
    def count_grid_cells(self, dataset_id: str, output_version: str | None = None) -> int: ...
    def list_publications(self, dataset_id: str, *, limit: int, offset: int, sort_by: str, sort_order: SortOrder) -> list[dict[str, Any]]: ...
    def count_publications(self, dataset_id: str) -> int: ...
    def output_has_publication_reference(self, dataset_id: str, output_version: str) -> bool: ...
    def get_output_cleanup_state(self, dataset_id: str, output_version: str) -> dict[str, Any] | None: ...
    def claim_outbox(self, worker_id: str, *, limit: int = 100) -> list[dict[str, Any]]: ...
    def acknowledge_outbox(self, event_id: str) -> None: ...
    def retry_outbox(self, event_id: str, error: str, *, available_at: str) -> None: ...
```

`list_datasets` and `count_datasets` share one predicate builder. `keyword` matches dataset code/title, `time_start/time_end` bound `updated_at`, and `publish_status` is derived without multiplying rows: no publication row gives `unpublished`; otherwise the latest row ordered by `(requested_at DESC, publication_id DESC)` gives exactly `publishing|active|withdrawing|failed|withdrawn`. Sort names are mapped through these exact fixed whitelists: datasets `updated_at|created_at|dataset_code|partition_completed_at|quality_status`; assets `source_asset_id|created_at`; bands `display_order|band_code`; tiles/indexes/grid cells `created_at|output_id|space_code|grid_level`; publications `requested_at|activated_at|status`. Every page requires `1 <= limit <= 200`, `offset >= 0`, appends its primary key as a deterministic tie-breaker, and rejects invalid `sort_by` or `sort_order` before SQL. Count methods accept no page arguments. Detail readers resolve/validate dataset ownership and default to the current output version; the in-memory implementation applies the same filters, ordering, offsets, and limits.

`OpenGaussPartitionDomainStore` additionally exposes:

```python
from contextlib import AbstractContextManager
import psycopg

class OpenGaussPartitionDomainStore(PartitionDomainStore):
    def transaction(self) -> AbstractContextManager[psycopg.Connection[Any]]:
        return self._pool.connection()
```

M2 does not add `allocate_quality_run`, `complete_quality_run`, publication mutation, approval mutation, or quality-result mutation methods. M3 implements those business operations through `OpenGaussPartitionDomainStore.transaction()` but must not silently alter M2 DDL or replace the frozen read surface. M3 kickoff is blocked until every listed paginated/read method has in-memory/OpenGauss parity tests and the real M2 catalog gate passes.

### Merge order

1. Merge and gate Milestone 1.
2. Integrate strict contracts and exact breaking request schema.
3. Integrate the complete DDL and frozen store interface.
4. Integrate Ray cache-only source handling and conversion/config removal.
5. Integrate immutable MinIO object lifecycle.
6. Integrate dataset fan-out, atomic completion, current pointer, and outbox.
7. Integrate unit/static gates, then the real OpenGauss/MinIO/Ray gate.
8. Run four-level review, squash the M2 implementation slices, and create one local M2 milestone commit.
9. Only then begin M3 quality allocation/completion, outbox dispatcher, APIs, approval, and publication.

---

### Task 1: Enforce the Strict Loader Contract and Derived Method

**Owner:** Sonnet contract worker

**Files:**
- Create: `cube_web/cube_web/services/partition_contracts.py`
- Modify: `cube_web/cube_web/schemas.py`
- Modify: `cube_web/cube_web/app.py`
- Modify: `cube_web/cube_web/routes/partition.py`
- Modify: `cube_web/cube_web/routes/partition_adapters.py`
- Create: `cube_web/tests/test_partition_contracts.py`
- Modify: `cube_web/tests/test_app.py`

**Interfaces:**
- Consumes: Milestone 1 request models in `cube_encoder/grid_core/app/models/request.py`, `GridType = Literal["geohash", "mgrs", "isea4h"]`, and request field `requested_grid_level`.
- Produces: `StrictPartitionRequest`, `DatasetInput`, `SourceAssetInput`, `BandInput`, `PartitionDatasetResult`, `OutputIdentity`, `derive_partition_method`, `validate_partition_method`, `group_datasets`, `make_output_version`, and `make_output_id` exactly as frozen above.

- [ ] **Step 1: Write focused failing contract tests**

Create `cube_web/tests/test_partition_contracts.py` with a reusable exact normalized request and assertions that the dataset-level `bands` array is accepted while legacy aliases are rejected:

```python
from copy import deepcopy

import pytest
from pydantic import ValidationError

from cube_web.services.partition_contracts import (
    OutputIdentity,
    StrictPartitionRequest,
    derive_partition_method,
    group_datasets,
    make_output_id,
    make_output_version,
    validate_partition_method,
)


def normalized_request() -> dict:
    return {
        "batch_id": "batch-01",
        "grid_type": "geohash",
        "requested_grid_level": 7,
        "partition_method": "logical",
        "cover_mode": "minimal",
        "time_granularity": "day",
        "max_cells_per_asset": 0,
        "datasets": [{
            "dataset_id": "dataset-a",
            "dataset_code": "DS-A",
            "dataset_title": "Dataset A",
            "data_type": "optical",
            "product_type": "L2A",
            "assets": [{
                "source_asset_id": "asset-a",
                "cog_uri": "s3://cube/loader/dataset-a/asset-a.tif",
                "checksum": "a" * 64,
                "bbox": [100.0, 20.0, 101.0, 21.0],
                "crs": "EPSG:4326",
                "time_start": "2026-07-01T00:00:00Z",
                "time_end": "2026-07-01T00:05:00Z",
                "attributes": {"scene_id": "scene-a"},
            }],
            "bands": [{
                "source_asset_id": "asset-a",
                "band_code": "B04",
                "band_name": "Red",
                "band_type": "spectral",
                "unit": None,
                "display_order": 4,
                "attributes": {"wavelength_nm": 665},
            }],
            "attributes": {},
        }],
    }


def test_accepts_exact_dataset_level_normalized_bands() -> None:
    request = StrictPartitionRequest.model_validate(normalized_request())
    assert request.datasets[0].bands[0].band_code == "B04"
    assert validate_partition_method(request.grid_type, request.partition_method) == "logical"


@pytest.mark.parametrize(
    ("grid_type", "minimum", "maximum"),
    [("geohash", 1, 12), ("mgrs", 0, 5), ("isea4h", 0, 15)],
)
def test_strict_request_uses_exact_m1_level_ranges(grid_type: str, minimum: int, maximum: int) -> None:
    for level in (minimum, maximum):
        payload = normalized_request()
        payload["grid_type"] = grid_type
        payload["requested_grid_level"] = level
        payload["partition_method"] = derive_partition_method(grid_type)
        assert StrictPartitionRequest.model_validate(payload).requested_grid_level == level
    for level in (minimum - 1, maximum + 1):
        payload = normalized_request()
        payload["grid_type"] = grid_type
        payload["requested_grid_level"] = level
        payload["partition_method"] = derive_partition_method(grid_type)
        with pytest.raises(ValidationError):
            StrictPartitionRequest.model_validate(payload)


def test_carbon_uses_cog_dataset_input_not_observations() -> None:
    payload = normalized_request()
    payload["datasets"][0]["data_type"] = "carbon"
    assert StrictPartitionRequest.model_validate(payload).datasets[0].assets[0].cog_uri.scheme == "s3"
    payload["datasets"][0]["observations"] = [{"latitude": 20.0, "longitude": 100.0}]
    with pytest.raises(ValidationError):
        StrictPartitionRequest.model_validate(payload)


@pytest.mark.parametrize(
    ("target", "field", "value"),
    [
        ("request", "grid_level", 7),
        ("request", "cog_workers", 0),
        ("dataset", "selected_assets", []),
        ("asset", "source_uri", "s3://cube/raw.tif"),
        ("asset", "band", "B04"),
        ("asset", "bands", ["B04"]),
        ("asset", "polarization", "VV"),
    ],
)
def test_rejects_legacy_aliases(target: str, field: str, value: object) -> None:
    payload = normalized_request()
    node = payload if target == "request" else payload["datasets"][0]
    if target == "asset":
        node = payload["datasets"][0]["assets"][0]
    node[field] = value
    with pytest.raises(ValidationError):
        StrictPartitionRequest.model_validate(payload)


def test_rejects_mismatched_method_and_duplicate_dataset() -> None:
    with pytest.raises(ValueError, match="must be logical"):
        validate_partition_method("mgrs", "entity")
    payload = normalized_request()
    payload["datasets"].append(deepcopy(payload["datasets"][0]))
    request = StrictPartitionRequest.model_validate(payload)
    with pytest.raises(ValueError, match="duplicate dataset_id"):
        group_datasets(request)


def test_version_and_output_ids_are_deterministic_and_level_sensitive() -> None:
    assert make_output_version("dataset-a", "task-a") == make_output_version("dataset-a", "task-a")
    base = OutputIdentity(
        dataset_id="dataset-a", output_version="v1", source_asset_id="asset-a", band_code="B04",
        grid_type="mgrs", grid_level=2, space_code="50QKK1234", topology_code="mgrs-topo-v1:utm-50n:2:50QKK1234",
        time_bucket="20260701", window_identity="0:0:512:512",
    )
    changed = base.model_copy(update={"grid_level": 1})
    assert make_output_id(base) != make_output_id(changed)
    assert derive_partition_method("isea4h") == "entity"


def test_partition_method_is_required_and_must_match_derived_value() -> None:
    missing = normalized_request()
    missing.pop("partition_method")
    with pytest.raises(ValidationError):
        StrictPartitionRequest.model_validate(missing)
    mismatched = normalized_request()
    mismatched["partition_method"] = "entity"
    with pytest.raises(ValidationError, match="must be logical"):
        StrictPartitionRequest.model_validate(mismatched)
```

Freeze the only M4 submission contract as `POST /v1/partition/{data_type}/tasks/run` with body exactly `StrictPartitionRequest` as shown above: required `batch_id`; full normalized `datasets[]`, each with all `assets[]` and dataset-level `bands[]`; `grid_type`; `requested_grid_level`; required `partition_method`, accepted only when it equals the server-derived method; `cover_mode`; `time_granularity`; and `max_cells_per_asset`. The path `data_type` is required and is exactly one of `optical|radar|product|carbon`; every `datasets[*].data_type` must equal it or the route returns 422 before scheduling. There is no `dataset_ids`, `grid_level`, or `grid_level_mode`. The route synchronously validates the full body, path/body type agreement, and method before returning 202/scheduling.

In `cube_web/tests/test_app.py`, parameterize the strict-route mutations exactly as follows:

```python
@pytest.mark.parametrize(
    "mutate",
    [
        lambda p: p.pop("batch_id"),
        lambda p: p.pop("partition_method"),
        lambda p: p["datasets"][0].pop("assets"),
        lambda p: p["datasets"][0].pop("bands"),
        lambda p: p.update(grid_level=7),
        lambda p: p.update(grid_level_mode="manual"),
        lambda p: p.update(dataset_ids=["dataset-a"]),
        lambda p: p.update(partition_method="entity"),
        lambda p: p["datasets"][0].update(observations=[]),
    ],
)
def test_partition_tasks_run_rejects_non_contract_bodies(route_client, scheduler, mutate) -> None:
    payload = normalized_request()
    mutate(payload)
    assert route_client.post(f"/v1/partition/{payload['datasets'][0]['data_type']}/tasks/run", json=payload).status_code == 422
    scheduler.assert_not_called()


def test_partition_tasks_run_schedules_exact_normalized_body(route_client, scheduler) -> None:
    payload = normalized_request()
    response = route_client.post(f"/v1/partition/{payload['datasets'][0]['data_type']}/tasks/run", json=payload)
    assert response.status_code == 202
    scheduler.assert_called_once_with(StrictPartitionRequest.model_validate(payload).model_dump(mode="json"))
```

Include the six underflow/overflow payloads from `test_strict_request_uses_exact_m1_level_ranges`, unsupported path `/v1/partition/unknown/tasks/run`, and path/body mismatches (`/radar/` carrying optical datasets, plus a mixed optical/radar dataset array) in this route parameterization; each returns 422 and leaves the scheduler untouched.

- [ ] **Step 2: Run the tests and prove the old permissive schema fails**

Run:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_web/tests/test_partition_contracts.py cube_web/tests/test_app.py -k 'normalized_bands or legacy_aliases or mismatched_method or version_and_output or requested_grid_level' -vv
```

Expected: FAIL during collection because `cube_web.services.partition_contracts` does not exist, and the existing permissive request accepts legacy fields.

- [ ] **Step 3: Implement the frozen models and replace the route request type**

Create `partition_contracts.py` from the complete frozen contract block above. In `schemas.py`, delete `OpticalAssetSelection` and `PartitionDemoRequest`, import and expose `StrictPartitionRequest`. In `routes/partition.py`, replace the existing `@router.post("/{data_type}/tasks/run", response_model=PartitionTaskCreateResponse, status_code=202)` handler's body type with `StrictPartitionRequest`; validate `data_type` against `optical|radar|product|carbon` and require `{dataset.data_type for dataset in payload.datasets} == {data_type}` before calling the strict scheduler adapter. Update `routes/partition_adapters.py` so the adapter takes `StrictPartitionRequest` rather than a permissive dict/demo model. `app.py` continues mounting this router at both `/v1/partition` and the compatibility prefix as applicable, but M4's frozen submission path is `/v1/partition/{data_type}/tasks/run`. Validate the derived method synchronously before creating a task:

```python
request = StrictPartitionRequest.model_validate(payload)
partition_method = validate_partition_method(request.grid_type, request.partition_method)
normalized = request.model_dump(mode="json")
normalized["partition_method"] = partition_method
schedule_partition(normalized)
```

Register exactly `POST /v1/partition/{data_type}/tasks/run` for this production contract. The `data_type` path uses `Literal["optical", "radar", "product", "carbon"]`, so FastAPI returns 422 for unsupported values; the explicit equality check returns 422 for homogeneous mismatch or mixed dataset types. The dynamic path selects/validates one supported data type but never replaces the full normalized body. Do not preserve old request aliases, IDs-only submission payloads, `dataset_ids`, `grid_level_mode`, or extra-allow behavior. Carbon follows the same `DatasetInput` COG shape.

- [ ] **Step 4: Run contract and route tests**

Run:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_web/tests/test_partition_contracts.py cube_web/tests/test_app.py -k 'partition or requested_grid_level' -vv
```

Expected: PASS; normalized dataset-level `bands` is accepted, every listed legacy alias returns validation failure, and mismatch returns HTTP 422 before scheduling.

- [ ] **Step 5: Create the worker slice commit**

```bash
git add cube_web/cube_web/services/partition_contracts.py cube_web/cube_web/schemas.py cube_web/cube_web/app.py cube_web/cube_web/routes/partition.py cube_web/cube_web/routes/partition_adapters.py cube_web/tests/test_partition_contracts.py cube_web/tests/test_app.py
git commit -m "feat(partition): enforce loader COG contract"
```

Expected: one Sonnet worktree commit containing only contract/schema tests and implementation.

---

### Task 2: Remove Source COG Conversion, Reprojection, Upload, Configuration, and Timings

**Owner:** Sonnet Ray worker for job files; Haiku for mechanical config deletion; mandatory Sonnet review before integration

**Files:**
- Modify: `cube_split/cube_split/jobs/ray_partition_core.py`
- Modify: `cube_split/cube_split/jobs/ray_logical_partition_job.py`
- Modify: `cube_split/cube_split/jobs/entity_partition_job.py`
- Modify: `cube_split/tests/test_ray_partition_core.py`
- Modify: `cube_split/tests/test_ray_logical_partition_job.py`
- Modify: `cube_split/tests/test_entity_partition_job.py`
- Modify: `cube_web/cube_web/services/config_store.py`
- Modify: `cube_web/cube_web/services/partition_defaults.py`
- Modify: `cube_web/cube_web/services/partition_loaded_schemas.py`
- Modify: `cube_web/tests/test_app.py`
- Modify: `cube_split/pyproject.toml`
- Create: `cube_split/tests/test_production_imports.py`

**Interfaces:**
- Consumes: `SourceAssetInput.cog_uri`, checksum, CRS, bbox, time range; MinIO runtime credentials remain environment-only.
- Produces: a cache-only helper `cache_source_cog(cog_uri: str, cache_dir: Path, minio_client: Minio, bucket: str) -> Path`; runtime result dictionaries with no COG conversion keys; a production `cube_split` import graph and declared runtime dependencies with no H3.

- [ ] **Step 1: Replace conversion tests with cache-only and absence tests**

In `cube_split/tests/test_ray_partition_core.py`, delete tests whose desired behavior is conversion or source upload and add:

```python
from pathlib import Path

from cube_split.jobs.ray_partition_core import cache_source_cog


class RecordingMinio:
    def __init__(self) -> None:
        self.downloads: list[tuple[str, str, str]] = []
        self.uploads: list[tuple[object, ...]] = []

    def fget_object(self, bucket: str, key: str, target: str) -> None:
        self.downloads.append((bucket, key, target))
        Path(target).write_bytes(b"loader-owned-cog")

    def fput_object(self, *args: object) -> None:
        self.uploads.append(args)


def test_cache_source_cog_downloads_without_conversion_or_upload(tmp_path: Path) -> None:
    client = RecordingMinio()
    path = cache_source_cog("s3://cube/loader/ds/a.tif", tmp_path, client, "cube")
    assert path.read_bytes() == b"loader-owned-cog"
    assert client.downloads == [("cube", "loader/ds/a.tif", str(path))]
    assert client.uploads == []
    assert path.suffix == ".tif"
```

In logical/entity job tests, assert the complete result key sets and explicit absence:

```python
for forbidden in (
    "cog_seconds", "cog_conversion_seconds", "cog_upload_seconds", "converted_cog_uri",
    "source_upload_uri", "cog_workers", "cog_compress", "cog_predictor", "target_crs",
):
    assert forbidden not in result
```

In `cube_web/tests/test_app.py`, assert config defaults and normalized task results contain none of those keys rather than asserting zero values. Add `cube_split/tests/test_production_imports.py` with exact package/import assertions:

```python
import os
from pathlib import Path
import re
import subprocess
import sys
import tomllib


def test_cube_split_declares_no_h3_runtime_dependency() -> None:
    project = tomllib.loads(Path("cube_split/pyproject.toml").read_text())
    names = {re.split(r"[<>=!~\[]", item, maxsplit=1)[0].strip().lower() for item in project["project"]["dependencies"]}
    assert "h3" not in names


def test_cube_split_production_imports_without_h3_available() -> None:
    code = r"""
from importlib.abc import MetaPathFinder
import sys
class RejectH3Finder(MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'h3' or fullname.startswith('h3.'):
            raise ModuleNotFoundError('h3 is intentionally unavailable')
        return None
sys.meta_path.insert(0, RejectH3Finder())
import cube_split.jobs.entity_partition_job
import cube_split.jobs.ray_logical_partition_job
import cube_split.jobs.ray_partition_core
import cube_split.partition.registry
"""
    root = Path.cwd()
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(str(root / name) for name in ("cube_encoder", "cube_split", "cube_web"))
    completed = subprocess.run([sys.executable, "-c", code], env=env, text=True, capture_output=True, check=False)
    assert completed.returncode == 0, completed.stderr
```

- [ ] **Step 2: Run tests and prove conversion remains reachable**

Run:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_split/tests/test_production_imports.py cube_split/tests/test_ray_partition_core.py cube_split/tests/test_ray_logical_partition_job.py cube_split/tests/test_entity_partition_job.py cube_web/tests/test_app.py -k 'production_imports or cache_source_cog or conversion or upload or cog_workers or timing' -vv
```

Expected: FAIL because `cache_source_cog` is missing and current job/config outputs still contain conversion, upload, or timing fields.

- [ ] **Step 3: Implement cache-only reads and mechanically delete conversion paths**

Implement only URI parsing, deterministic cache naming, atomic local download, and checksum/readability validation:

```python
def cache_source_cog(cog_uri: str, cache_dir: Path, minio_client: Any, bucket: str) -> Path:
    parsed = urlparse(cog_uri)
    if parsed.scheme != "s3" or parsed.netloc != bucket or not parsed.path.lstrip("/"):
        raise ValueError(f"invalid source COG URI for bucket {bucket}: {cog_uri}")
    key = parsed.path.lstrip("/")
    target = cache_dir / sha256(cog_uri.encode("utf-8")).hexdigest() / Path(key).name
    if target.exists():
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".part")
    minio_client.fget_object(bucket, key, str(temporary))
    temporary.replace(target)
    return target
```

Delete `convert_assets_to_cog`, `convert_asset_to_cog`, `upload_source_assets_to_minio`, old ISEA/H3 selector/partition branches, direct `import h3` use, GDAL/Rasterio conversion profiles, source reprojection, source upload branches, `cog_*` request/config defaults, `target_crs` partition behavior, and all conversion timing writes. Remove `h3>=4.4.0` from `cube_split/pyproject.toml`; M1 intentionally deferred only this split-package runtime declaration to M2. Preserve Rasterio reads of cached loader COGs and preserve output uploads for ISEA4H entity tiles only through the M1 SDK.

- [ ] **Step 4: Run narrow tests and static removal checks**

Run:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_split/tests/test_production_imports.py cube_split/tests/test_ray_partition_core.py cube_split/tests/test_ray_logical_partition_job.py cube_split/tests/test_entity_partition_job.py cube_web/tests/test_app.py -k 'production_imports or cache_source_cog or partition or config' -vv
```

Expected: PASS.

Run:

```bash
rg -n 'convert_assets_to_cog|convert_asset_to_cog|upload_source_assets_to_minio|cog_workers|cog_compress|cog_predictor|cog_conversion_seconds|cog_upload_seconds|converted_cog_uri|import h3|from h3' cube_split/cube_split cube_web/cube_web/services cube_web/cube_web/schemas.py
```

Expected: exit status 1 and no output. Any hit in those production paths blocks integration; do not replace removed timings with zero fields.

- [ ] **Step 5: Create reviewed worker slice commits**

```bash
git add cube_split/cube_split/jobs/ray_partition_core.py cube_split/cube_split/jobs/ray_logical_partition_job.py cube_split/cube_split/jobs/entity_partition_job.py cube_split/tests/test_ray_partition_core.py cube_split/tests/test_ray_logical_partition_job.py cube_split/tests/test_entity_partition_job.py
git commit -m "refactor(partition): consume loader COGs directly"
```

```bash
git add cube_web/cube_web/services/config_store.py cube_web/cube_web/services/partition_defaults.py cube_web/cube_web/services/partition_loaded_schemas.py cube_web/tests/test_app.py cube_split/pyproject.toml cube_split/tests/test_production_imports.py
git commit -m "refactor(partition): remove conversion configuration"
```

Expected: Ray and mechanical-cleanup commits remain separate until Sonnet review; neither includes domain schema or orchestration edits.

---

### Task 3: Create Canonical Scheduling and Partition Domain DDL

**Owner:** Opus schema/store integrator

**Files:**
- Create: `cube_web/cube_web/services/partition_domain_schema.py`
- Create: `cube_web/tests/test_partition_domain_schema.py`
- Modify: `cube_web/cube_web/services/partition_job_store.py`

**Interfaces:**
- Consumes: existing scheduling names `partition_batches`, `partition_assets`, `partition_job_attempts`.
- Produces: `PARTITION_DOMAIN_SCHEMA_VERSION = "2026-07-14-m2-v1"`, `NEW_DOMAIN_OBJECTS`, `LEGACY_ALLOWLIST`, `schema_statements()`, `inventory_partition_objects(connection)`, and complete scheduling/result DDL.

- [ ] **Step 1: Write schema contract and inventory tests**

Create `cube_web/tests/test_partition_domain_schema.py`:

```python
from cube_web.services.partition_domain_schema import (
    LEGACY_ALLOWLIST,
    NEW_DOMAIN_TABLES,
    PARTITION_DOMAIN_SCHEMA_VERSION,
    schema_statements,
)


def test_domain_schema_contains_exact_tables_constraints_and_quality_handoff() -> None:
    sql = "\n".join(schema_statements()).lower()
    assert PARTITION_DOMAIN_SCHEMA_VERSION == "2026-07-14-m2-v1"
    assert NEW_DOMAIN_TABLES == {
        "partition_datasets", "partition_dataset_assets", "partition_dataset_bands",
        "partition_output_versions", "partition_tiles", "partition_indexes", "partition_grid_cells",
        "partition_quality_runs", "partition_quality_results", "partition_quality_errors",
        "partition_quality_warn_approvals", "partition_publications", "partition_domain_outbox",
        "partition_domain_schema_version",
    }
    assert LEGACY_ALLOWLIST == {"quality_reports", "partition_batches", "partition_assets", "partition_job_attempts"}
    for column in (
        "current_quality_run_id", "quality_status", "quality_sequence", "quality_error_count", "quality_warning_count",
    ):
        assert column in sql
    assert "trigger_event_id uuid null unique" in sql
    assert "foreign key (trigger_event_id)" not in sql
    assert "check (trigger in ('automatic', 'manual'))" in sql
    assert "unique (dataset_id, output_version, event_type)" in sql
    assert "deferrable initially deferred" in sql
    assert "unique(dataset_id,output_version,quality_run_id)" in sql.replace(" ", "")
    assert "status in ('publishing','active','withdrawing','failed','withdrawn')" in sql
    assert "desired_action in ('activate','withdraw')" in sql
    assert "uq_partition_publication_live_snapshot" in sql
    assert "where status in ('publishing','active','withdrawing')" in sql
    assert "idx_partition_publication_claim" in sql
    assert "on partition_publications(status, available_at, claimed_at, created_at)" in sql
    assert "idx_partition_publication_dataset_latest" in sql
    publication_statement = next(item.lower() for item in schema_statements() if "create table partition_publications" in item.lower())
    assert "'published'" not in publication_statement
    assert "'unpublished'" not in publication_statement


def test_quality_handoff_tables_have_every_m3_column() -> None:
    sql = "\n".join(schema_statements()).lower()
    expected = {
        "partition_quality_results": {"quality_run_id", "dataset_id", "output_version", "rule_code", "status", "finding_count", "error_count", "warning_count", "metrics", "execution_error", "started_at", "completed_at"},
        "partition_quality_errors": {"quality_error_id", "quality_run_id", "dataset_id", "output_version", "rule_code", "source_asset_id", "tile_id", "index_id", "output_id", "row_number", "field_name", "error_code", "message", "context", "created_at"},
        "partition_quality_warn_approvals": {"approval_id", "dataset_id", "output_version", "quality_run_id", "rule_set_version", "approved_by", "approved_at", "reason"},
        "partition_publications": {"publication_id", "dataset_id", "output_version", "quality_run_id", "status", "desired_action", "service_version_id", "requested_by", "requested_at", "activated_at", "failure", "withdrawn_by", "withdrawn_at", "withdrawal_reason", "attempt_count", "available_at", "claimed_at", "claimed_by", "last_error", "created_at", "updated_at"},
    }
    for table, columns in expected.items():
        statement = next(item.lower() for item in schema_statements() if f"create table {table}" in item.lower())
        assert all(column in statement for column in columns), table


def test_schema_has_no_destructive_objects_outside_partition_domain() -> None:
    sql = "\n".join(schema_statements()).lower()
    assert "cube_web_configs" not in sql
    assert "ard_" not in sql
    assert "rs_" not in sql
```

Create exact catalog fixtures and assertions:

```python
EXPECTED_HANDOFF_INDEXES = {
    ("uq_partition_publication_live_snapshot", ("dataset_id", "output_version", "quality_run_id"),
     "status = any (array['publishing'::text, 'active'::text, 'withdrawing'::text])"),
    ("idx_partition_publication_claim", ("status", "available_at", "claimed_at", "created_at"), None),
    ("idx_partition_publication_dataset_latest", ("dataset_id", "requested_at desc", "publication_id desc"), None),
    ("idx_partition_quality_errors_page", ("quality_run_id", "created_at", "quality_error_id"), None),
    ("idx_partition_quality_errors_filter",
     ("quality_run_id", "rule_code", "error_code", "source_asset_id", "output_id", "field_name"), None),
}
EXPECTED_HANDOFF_CONSTRAINT_NAMES = {
    "partition_quality_results_pkey", "partition_quality_results_dataset_output_run_fkey",
    "partition_quality_errors_pkey", "partition_quality_errors_dataset_output_run_fkey",
    "partition_quality_warn_approvals_pkey", "partition_quality_warn_approvals_quality_run_id_key",
    "partition_quality_warn_approvals_dataset_output_run_fkey", "partition_quality_warn_approvals_reason_check",
    "partition_publications_pkey", "partition_publications_dataset_output_run_fkey",
    "partition_publications_status_check", "partition_publications_desired_action_check",
    "partition_publications_claim_check", "partition_publications_lifecycle_check",
    "partition_quality_runs_dataset_output_run_key",
}


def test_catalog_inventory_reports_unknown_kind_and_exact_handoff_objects(fake_catalog_connection) -> None:
    fake_catalog_connection.add_object(kind="view", schema="public", table=None, name="partition_old_tiles")
    fake_catalog_connection.seed_expected_handoff(EXPECTED_HANDOFF_INDEXES, EXPECTED_HANDOFF_CONSTRAINT_NAMES)
    inventory = inventory_partition_objects(fake_catalog_connection)
    assert ("view", "public", None, "partition_old_tiles") in inventory.objects
    assert inventory.indexes == EXPECTED_HANDOFF_INDEXES
    assert {row.name for row in inventory.constraints} == EXPECTED_HANDOFF_CONSTRAINT_NAMES
```

`inventory_partition_objects()` must report each object's kind, schema, table, name, ordered columns/predicate, and normalized check/FK/unique definition. The real OpenGauss catalog gate in Task 10 compares the same exact sets; a missing withdrawal lease column, status check, composite run unique, partial predicate, or claim order fails.

- [ ] **Step 2: Run tests and verify the schema module is absent**

Run:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_web/tests/test_partition_domain_schema.py -vv
```

Expected: FAIL during collection because `partition_domain_schema.py` does not exist.

- [ ] **Step 3: Implement complete forward DDL and catalog inventory**

Create explicit executable SQL for all three scheduling tables and all fourteen result-domain tables. Do not summarize multiple tables on one combined line. Scheduling and partition-output tables have these exact structural requirements (render every named column, FK, check, and unique constraint):

```text
partition_datasets:
  dataset_id TEXT PRIMARY KEY; batch_id TEXT NOT NULL FK partition_batches(batch_id);
  dataset_code TEXT NOT NULL UNIQUE; dataset_title TEXT NOT NULL; data_type TEXT NOT NULL CHECK optical|radar|product|carbon;
  product_type TEXT NULL; attributes JSONB NOT NULL DEFAULT '{}'; grid_type TEXT NOT NULL CHECK geohash|mgrs|isea4h;
  requested_grid_level INT NOT NULL; requested_grid_level_name TEXT NOT NULL; partition_method TEXT NOT NULL CHECK logical|entity;
  cover_mode TEXT NOT NULL CHECK intersect|contain|minimal; partition_status TEXT NOT NULL CHECK pending|queued|running|completed|failed|cancelled;
  current_output_version TEXT NULL; current_quality_run_id UUID NULL; quality_status TEXT NOT NULL DEFAULT 'pending'
    CHECK pending|running|pass|warn|fail|error|cancelled;
  quality_sequence BIGINT NOT NULL DEFAULT 0 CHECK >= 0; quality_error_count BIGINT NOT NULL DEFAULT 0 CHECK >= 0;
  quality_warning_count BIGINT NOT NULL DEFAULT 0 CHECK >= 0; partition_completed_at TIMESTAMPTZ NULL;
  created_at/updated_at TIMESTAMPTZ NOT NULL; completed status requires partition_completed_at; every other status forbids it.

partition_dataset_assets:
  PK(dataset_id, source_asset_id); dataset FK; cog_uri TEXT NOT NULL CHECK starts s3://; checksum CHAR(64) lowercase-hex CHECK;
  bbox JSONB; crs TEXT; time_start/time_end TIMESTAMPTZ with time_end >= time_start; attributes JSONB; created_at.

partition_dataset_bands:
  PK(dataset_id, source_asset_id, band_code); composite FK to dataset asset; band_name; band_type CHECK spectral|polarization|variable;
  unit; display_order >= 0; attributes JSONB; created_at.

partition_output_versions:
  PK(dataset_id, output_version); output_version TEXT UNIQUE; task_id TEXT NOT NULL FK partition_job_attempts(task_id);
  grid_type CHECK geohash|mgrs|isea4h; requested_grid_level INT; requested_grid_level_name; partition_method CHECK logical|entity;
  status CHECK staging|completed|failed|superseded; object_prefix TEXT; tile_count/index_count/grid_cell_count BIGINT >= 0;
  counts JSONB NOT NULL; error_code/error_message; created_at/completed_at/failed_at;
  completed requires completed_at and null failure fields; failed requires failed_at and nonempty error_code; staging/superseded obey timestamp checks.

partition_grid_cells:
  output_id TEXT PRIMARY KEY; dataset/output composite FK; grid_type; grid_level INT; grid_level_name;
  space_code TEXT; topology_code TEXT NULL; normalized_topology_code TEXT GENERATED ALWAYS AS (coalesce(topology_code, '')) STORED;
  bbox JSONB; geometry JSONB; tile_count/index_count BIGINT NOT NULL CHECK >= 0;
  UNIQUE(dataset_id, output_version, grid_type, grid_level, normalized_topology_code, space_code).

partition_tiles:
  output_id TEXT PRIMARY KEY; dataset/output FK; source asset/band composite FK; grid_type/grid_level/grid_level_name/space_code/topology_code;
  time_bucket TEXT; tile_uri TEXT NOT NULL CHECK starts s3://; tile_kind CHECK logical_reference|entity_file; bbox JSONB;
  width/height BIGINT CHECK > 0; byte_size BIGINT CHECK >= 0; checksum CHAR(64) lowercase-hex CHECK; status CHECK ready|failed; created_at;
  UNIQUE(dataset_id, output_version, source_asset_id, band_code, grid_type, grid_level, space_code, time_bucket, tile_kind).

partition_indexes:
  output_id TEXT PRIMARY KEY; dataset/output FK; tile_output_id nullable FK; source asset/band composite FK; acquisition_time TIMESTAMPTZ;
  time_bucket; grid_type/grid_level/grid_level_name/topology_code; space_code/st_code; window_col_off/window_row_off/window_width/window_height;
  all four window integers all-null or all-nonnull, offsets >= 0, dimensions > 0; value_ref_uri TEXT NOT NULL CHECK starts s3://; created_at;
  UNIQUE(dataset_id, output_version, source_asset_id, band_code, grid_type, grid_level, space_code, time_bucket, st_code).

partition_quality_runs:
  quality_run_id UUID PRIMARY KEY; dataset_id/output_version; quality_sequence BIGINT NOT NULL CHECK > 0; trigger TEXT CHECK automatic|manual;
  trigger_event_id UUID NULL UNIQUE with NO outbox FK; requested_by TEXT NOT NULL; rule_set_version TEXT NOT NULL; rule_snapshot JSONB NOT NULL;
  status CHECK pending|running|pass|warn|fail|error|cancelled; error_count/warning_count BIGINT NOT NULL DEFAULT 0 CHECK >= 0;
  result_complete BOOLEAN NOT NULL DEFAULT FALSE; attempt_count INT NOT NULL DEFAULT 0 CHECK >= 0;
  available_at TIMESTAMPTZ NOT NULL DEFAULT now(); claimed_at/claimed_by/last_error; started_at/completed_at/created_at/updated_at;
  UNIQUE(dataset_id, output_version, quality_sequence); UNIQUE(dataset_id, output_version, quality_run_id);
  composite dataset/output FK and claim index(status, available_at, claimed_at, created_at).
```

Render these four handoff tables exactly, including all withdrawal lease/reconciliation fields:

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
  row_number BIGINT CHECK (row_number IS NULL OR row_number >= 0),
  field_name TEXT,
  error_code TEXT NOT NULL,
  message TEXT NOT NULL,
  context JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (dataset_id, output_version, quality_run_id)
    REFERENCES partition_quality_runs(dataset_id, output_version, quality_run_id)
);
CREATE INDEX idx_partition_quality_errors_page
  ON partition_quality_errors(quality_run_id, created_at, quality_error_id);
CREATE INDEX idx_partition_quality_errors_filter
  ON partition_quality_errors(quality_run_id, rule_code, error_code, source_asset_id, output_id, field_name);

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
  status TEXT NOT NULL CHECK (status IN ('publishing','active','withdrawing','failed','withdrawn')),
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
    REFERENCES partition_quality_runs(dataset_id, output_version, quality_run_id),
  CHECK ((claimed_at IS NULL) = (claimed_by IS NULL)),
  CHECK ((status = 'publishing' AND desired_action = 'activate') OR
         (status = 'active' AND desired_action = 'activate' AND service_version_id IS NOT NULL AND activated_at IS NOT NULL) OR
         (status = 'withdrawing' AND desired_action = 'withdraw' AND service_version_id IS NOT NULL AND
          withdrawn_by IS NOT NULL AND withdrawal_reason IS NOT NULL AND length(btrim(withdrawal_reason)) BETWEEN 1 AND 2000) OR
         (status = 'withdrawn' AND desired_action = 'withdraw' AND service_version_id IS NOT NULL AND
          withdrawn_by IS NOT NULL AND withdrawn_at IS NOT NULL AND withdrawal_reason IS NOT NULL AND
          length(btrim(withdrawal_reason)) BETWEEN 1 AND 2000) OR
         (status = 'failed' AND failure IS NOT NULL))
);
CREATE UNIQUE INDEX uq_partition_publication_live_snapshot
  ON partition_publications(dataset_id, output_version, quality_run_id)
  WHERE status IN ('publishing','active','withdrawing');
CREATE INDEX idx_partition_publication_claim
  ON partition_publications(status, available_at, claimed_at, created_at);
CREATE INDEX idx_partition_publication_dataset_latest
  ON partition_publications(dataset_id, requested_at DESC, publication_id DESC);
```

The publication lifecycle never contains `published` or `unpublished`; `unpublished` is a dataset-read-only derived literal when no publication row exists. `withdrawing` is durable work: `desired_action`, `attempt_count`, `available_at`, `claimed_at`, `claimed_by`, `last_error`, service version, actor, and reason allow lease recovery and reconciliation without re-resolving a dataset pointer.

```text
partition_domain_outbox:
  event_id UUID PK; dataset/output FK; event_type TEXT CHECK exactly output-version.completed; payload JSONB;
  status CHECK pending|processing|delivered; attempt_count INT CHECK >= 0; available_at/claimed_at/claimed_by/last_error/created_at/delivered_at;
  claimed_at/claimed_by nullability parity; delivered status requires delivered_at; UNIQUE(dataset_id, output_version, event_type);
  claim index(status, available_at, claimed_at, created_at).

partition_domain_schema_version:
  singleton BOOLEAN PRIMARY KEY CHECK singleton; schema_version TEXT NOT NULL; installed_at TIMESTAMPTZ NOT NULL.
```

Render the deferred circular constraints exactly:

```sql
ALTER TABLE partition_datasets ADD CONSTRAINT partition_datasets_current_output_fkey
  FOREIGN KEY (dataset_id, current_output_version)
  REFERENCES partition_output_versions(dataset_id, output_version)
  DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE partition_datasets ADD CONSTRAINT partition_datasets_current_quality_run_fkey
  FOREIGN KEY (dataset_id, current_output_version, current_quality_run_id)
  REFERENCES partition_quality_runs(dataset_id, output_version, quality_run_id)
  DEFERRABLE INITIALLY DEFERRED;
```

Use parameterized catalog queries over `pg_class`, `pg_namespace`, `pg_attribute`, `pg_index`, `pg_constraint`, `pg_get_constraintdef`, and `pg_get_expr` to inventory tables, partitioned tables, views, materialized views, sequences, indexes, ordered index columns/predicates, and normalized constraints matching `partition_%` or `quality_%`.

- [ ] **Step 4: Run schema tests**

Run:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_web/tests/test_partition_domain_schema.py -vv
```

Expected: PASS; exact object sets match; every quality/publication column, composite FK, check, unique, partial/claim/latest/error index is catalog-asserted; publication status excludes `published`; no outbox FK is attached to `trigger_event_id`; and inventory identifies unknown object kinds.

- [ ] **Step 5: Create the schema slice commit**

```bash
git add cube_web/cube_web/services/partition_domain_schema.py cube_web/cube_web/services/partition_job_store.py cube_web/tests/test_partition_domain_schema.py
git commit -m "feat(partition): add versioned domain schema"
```

Expected: one Opus worktree commit; no reset execution or runtime store mutation yet.

---

### Task 4: Add Triple-Guarded Reset and Bootstrap

**Owner:** Opus schema/store integrator

**Files:**
- Create: `cube_web/scripts/reset_partition_domain.py`
- Create: `cube_web/tests/test_reset_partition_domain.py`
- Modify: `cube_web/cube_web/services/partition_domain_schema.py`

**Interfaces:**
- Consumes: exact new/legacy object sets and forward DDL from Task 3.
- Produces: `ResetPlan`, `build_reset_plan(connection)`, `validate_reset_guards(connection, database_name, dangerous, environment)`, preview-by-default CLI, and explicit `--execute`.

- [ ] **Step 1: Write guard, unknown-object, ordering, and scope tests**

Create `cube_web/tests/test_reset_partition_domain.py` with a fake connection and these cases:

```python
import pytest

from cube_web.scripts.reset_partition_domain import build_reset_plan, validate_reset_guards


def test_reset_requires_all_three_guards(fake_connection) -> None:
    fake_connection.current_database = "cube_dev"
    with pytest.raises(RuntimeError, match="CUBE_WEB_ENV=development"):
        validate_reset_guards(fake_connection, "cube_dev", True, "production")
    with pytest.raises(RuntimeError, match="dangerously-reset-partition-domain"):
        validate_reset_guards(fake_connection, "cube_dev", False, "development")
    with pytest.raises(RuntimeError, match="actual database cube_dev"):
        validate_reset_guards(fake_connection, "wrong", True, "development")


def test_unknown_partition_or_quality_object_refuses_reset(fake_connection) -> None:
    fake_connection.inventory = [{"kind": "view", "schema": "public", "name": "partition_old_view"}]
    with pytest.raises(RuntimeError, match="partition_old_view"):
        build_reset_plan(fake_connection)


def test_reset_order_and_scope(fake_connection) -> None:
    plan = build_reset_plan(fake_connection)
    drops = "\n".join(plan.drop_statements)
    assert drops.index("partition_domain_outbox") < drops.index("partition_datasets")
    assert drops.index("partition_datasets") < drops.index("partition_job_attempts")
    for forbidden in ("cube_web_configs", "ard_", "rs_"):
        assert forbidden not in drops
```

Add these CLI tests with the fake executor:

```python
def test_cli_preview_prints_ordered_sql_without_execution(runner, fake_connection) -> None:
    result = runner.invoke(["--database-name", "cube_dev", "--dangerously-reset-partition-domain"])
    assert result.exit_code == 0
    assert "execution=false" in result.output
    assert "DROP TABLE" in result.output
    assert fake_connection.executed == []


def test_cli_execute_runs_only_after_all_guards(runner, fake_connection) -> None:
    result = runner.invoke(["--database-name", "cube_dev", "--dangerously-reset-partition-domain", "--execute"], env={"CUBE_WEB_ENV": "development"})
    assert result.exit_code == 0
    assert "execution=true" in result.output
    assert fake_connection.executed
```

Retain the wrong-environment, missing-dangerous-flag, and wrong-database tests above; each asserts `fake_connection.executed == []`.

- [ ] **Step 2: Run reset tests and verify failure**

Run:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_web/tests/test_reset_partition_domain.py -vv
```

Expected: FAIL because the reset script and guard functions do not exist.

- [ ] **Step 3: Implement deterministic preview, reset, bootstrap, and optional MinIO purge**

Drop in exact dependency order:

```python
DROP_ORDER = (
    "partition_domain_outbox",
    "partition_quality_warn_approvals",
    "partition_publications",
    "partition_quality_errors",
    "partition_quality_results",
    "partition_quality_runs",
    "partition_indexes",
    "partition_tiles",
    "partition_grid_cells",
    "partition_output_versions",
    "partition_dataset_bands",
    "partition_dataset_assets",
    "partition_datasets",
    "quality_reports",
    "partition_job_attempts",
    "partition_assets",
    "partition_batches",
)
```

For each inventoried index/constraint owned by a known table, let its owning `DROP TABLE <known_table> CASCADE` statement remove it; explicitly reject independently named unknown objects before producing a plan. Recreate scheduling tables first, then datasets/assets/bands/versions, result tables, quality/publication tables, outbox, deferred circular constraints, indexes, and schema version row.

CLI arguments are exact:

```text
--database-name NAME                         required
--dangerously-reset-partition-domain         required for execute
--execute                                    otherwise preview only
--purge-partition-objects                    optional separate destructive action
```

`--purge-partition-objects` previews object keys and, only with `--execute` plus all three guards, deletes keys strictly under `partition/` in the configured bucket. Reject empty prefix, `/`, keys outside `partition/`, and bucket mismatch. Normal reset never purges MinIO.

- [ ] **Step 4: Run unit tests and both safe CLI proofs**

Run:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_web/tests/test_reset_partition_domain.py cube_web/tests/test_partition_domain_schema.py -vv
```

Expected: PASS.

Run without `--execute` against the configured development DSN:

```bash
CUBE_WEB_ENV=development PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 cube_web/scripts/reset_partition_domain.py --database-name "$CUBE_WEB_M2_DATABASE_NAME" --dangerously-reset-partition-domain
```

Expected: exit 0, print inventory and ordered SQL preview, print `execution=false`, and mutate no table or object.

Run a wrong-name proof:

```bash
CUBE_WEB_ENV=development PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 cube_web/scripts/reset_partition_domain.py --database-name definitely-not-the-connected-database --dangerously-reset-partition-domain --execute
```

Expected: nonzero exit before the first DDL statement, with an error naming both supplied and actual database names.

- [ ] **Step 5: Create the guarded-reset slice commit**

```bash
git add cube_web/scripts/reset_partition_domain.py cube_web/cube_web/services/partition_domain_schema.py cube_web/tests/test_reset_partition_domain.py
git commit -m "feat(partition): guard domain reset"
```

Expected: one Opus worktree commit; no live reset is performed by the commit step.

---

### Task 5: Persist Deterministic Output Versions Atomically

**Owner:** Opus transaction/concurrency integrator

**Files:**
- Create: `cube_web/cube_web/services/partition_domain_store.py`
- Create: `cube_web/tests/test_partition_domain_store.py`
- Modify: `cube_web/cube_web/services/partition_job_store.py`

**Interfaces:**
- Consumes: frozen contracts, DDL, active `partition_job_attempts`, deterministic IDs.
- Produces: exact store classes/accessors/methods frozen above and `OpenGaussPartitionDomainStore.transaction()`.

- [ ] **Step 1: Write in-memory and SQL transaction behavior tests**

Create tests proving idempotency, mixed levels, reset quality fields, unique outbox, rollback, cancellation, unknown-commit recovery, and exact protocol/in-memory/OpenGauss read parity. The test module imports `inspect`, `UTC`, `datetime`, `pytest`, and all three store classes before these cases:

```python
def test_complete_output_switches_pointer_resets_quality_and_emits_once(store, request, dataset_result) -> None:
    version = store.start_output(request, request.datasets[0], dataset_result.task_id)
    assert version == dataset_result.output_version
    store.seed_quality_state(dataset_result.dataset_id, current_run="00000000-0000-0000-0000-000000000001", sequence=8, errors=4, warnings=2)
    first = store.complete_output(dataset_result)
    second = store.complete_output(dataset_result)
    dataset = store.get_dataset(dataset_result.dataset_id)
    assert first == second
    assert dataset["current_output_version"] == dataset_result.output_version
    assert dataset["partition_status"] == "completed"
    assert dataset["partition_completed_at"] is not None
    assert dataset["quality_status"] == "pending"
    assert dataset["current_quality_run_id"] is None
    assert dataset["quality_sequence"] == 8
    assert dataset["quality_error_count"] == 0
    assert dataset["quality_warning_count"] == 0
    assert len(store.claim_outbox("test", limit=10)) == 1


def test_result_cell_level_and_topology_property_are_persisted(store, dataset_result) -> None:
    dataset_result.grid_cells[0]["grid_level"] = 4
    dataset_result.grid_cells[0]["topology_code"] = "mgrs-topo-v1:utm-50n:4:50QKK12341234"
    store.complete_output(dataset_result)
    row = store.list_grid_cells(dataset_result.dataset_id, limit=20, offset=0, sort_by="output_id", sort_order="asc")[0]
    assert row["grid_level"] == 4
    assert row["topology_code"].startswith("mgrs-topo-v1:")
    assert "level" not in row
    assert "metadata" not in row


def test_failed_detail_insert_keeps_old_pointer_and_no_completion_event(store, completed_old_result, failing_new_result) -> None:
    store.complete_output(completed_old_result)
    store.fail_on_output_id = failing_new_result.indexes[0]["output_id"]
    with pytest.raises(RuntimeError, match="injected detail failure"):
        store.complete_output(failing_new_result)
    assert store.resolve_output_version(failing_new_result.dataset_id) == completed_old_result.output_version
    assert all(event["output_version"] != failing_new_result.output_version for event in store.outbox_rows())


DATASET_FILTER_CASES = (
    {"keyword": "Dataset A"}, {"data_type": "optical"}, {"product_type": "L2A"}, {"batch_id": "batch-01"},
    {"grid_type": "geohash"}, {"partition_status": "completed"}, {"quality_status": "pass"},
    {"publish_status": "unpublished"}, {"publish_status": "publishing"}, {"publish_status": "active"},
    {"publish_status": "withdrawing"}, {"publish_status": "failed"}, {"publish_status": "withdrawn"},
    {"time_start": datetime(2026, 7, 1, tzinfo=UTC)}, {"time_end": datetime(2026, 7, 31, tzinfo=UTC)},
)
DATASET_SORTS = ("updated_at", "created_at", "dataset_code", "partition_completed_at", "quality_status")


@pytest.mark.parametrize("changed", DATASET_FILTER_CASES)
@pytest.mark.parametrize("sort_by", DATASET_SORTS)
@pytest.mark.parametrize("sort_order", ("asc", "desc"))
def test_dataset_list_count_all_filters_sorts_and_pages_have_store_parity(store_pair, changed, sort_by, sort_order) -> None:
    memory, open_gauss = store_pair.seed_identical_datasets_and_publication_history()
    filters = dict(
        keyword=None, data_type=None, product_type=None, batch_id=None, grid_type=None,
        partition_status=None, quality_status=None, publish_status=None, time_start=None, time_end=None,
    )
    filters.update(changed)
    page = dict(limit=2, offset=1, sort_by=sort_by, sort_order=sort_order)
    assert memory.list_datasets(**filters, **page) == open_gauss.list_datasets(**filters, **page)
    assert memory.count_datasets(**filters) == open_gauss.count_datasets(**filters)


def test_dataset_publish_status_is_unpublished_only_without_history(store) -> None:
    dataset = store.seed_dataset("dataset-none")
    assert store.get_dataset(dataset["dataset_id"])["publish_status"] == "unpublished"
    store.seed_publication(dataset["dataset_id"], status="withdrawn", requested_at="2026-07-01T00:00:00Z")
    assert store.get_dataset(dataset["dataset_id"])["publish_status"] == "withdrawn"


DETAIL_SORTS = {
    "assets": ("source_asset_id", "created_at"),
    "bands": ("display_order", "band_code"),
    "tiles": ("created_at", "output_id", "space_code", "grid_level"),
    "indexes": ("created_at", "output_id", "space_code", "grid_level"),
    "grid_cells": ("created_at", "output_id", "space_code", "grid_level"),
}


@pytest.mark.parametrize("noun", tuple(DETAIL_SORTS))
@pytest.mark.parametrize("sort_order", ("asc", "desc"))
def test_every_detail_child_page_count_sort_and_cleanup_read_has_memory_sql_parity(store_pair, noun, sort_order) -> None:
    memory, open_gauss = store_pair.seed_identical_complete_domain()
    for sort_by in DETAIL_SORTS[noun]:
        kwargs = dict(limit=2, offset=1, sort_by=sort_by, sort_order=sort_order)
        assert getattr(memory, f"list_{noun}")("dataset-a", "version-a", **kwargs) == getattr(open_gauss, f"list_{noun}")(
            "dataset-a", "version-a", **kwargs,
        )
    assert getattr(memory, f"count_{noun}")("dataset-a", "version-a") == getattr(open_gauss, f"count_{noun}")(
        "dataset-a", "version-a",
    )
    for sort_by in ("requested_at", "activated_at", "status"):
        kwargs = dict(limit=2, offset=1, sort_by=sort_by, sort_order=sort_order)
        assert memory.list_publications("dataset-a", **kwargs) == open_gauss.list_publications("dataset-a", **kwargs)
    assert memory.count_publications("dataset-a") == open_gauss.count_publications("dataset-a")
    assert memory.output_has_publication_reference("dataset-a", "version-a") is True
    assert open_gauss.output_has_publication_reference("dataset-a", "version-a") is True
    assert memory.get_output_cleanup_state("dataset-a", "version-a") == open_gauss.get_output_cleanup_state("dataset-a", "version-a")


def test_protocol_inmemory_and_opengauss_read_signatures_are_identical() -> None:
    names = (
        "get_dataset", "list_datasets", "count_datasets", "get_output_version",
        "list_assets", "count_assets", "list_bands", "count_bands", "list_tiles", "count_tiles",
        "list_indexes", "count_indexes", "list_grid_cells", "count_grid_cells",
        "list_publications", "count_publications", "output_has_publication_reference", "get_output_cleanup_state",
    )
    for name in names:
        protocol_signature = inspect.signature(getattr(PartitionDomainStore, name))
        assert inspect.signature(getattr(InMemoryPartitionDomainStore, name)) == protocol_signature
        assert inspect.signature(getattr(OpenGaussPartitionDomainStore, name)) == protocol_signature


@pytest.mark.parametrize(("limit", "offset"), [(0, 0), (201, 0), (20, -1)])
def test_all_paginated_reads_reject_invalid_bounds_before_sql(store_pair, limit, offset) -> None:
    memory, open_gauss = store_pair.seed_identical_complete_domain()
    for store in (memory, open_gauss):
        with pytest.raises(ValueError):
            store.list_assets("dataset-a", limit=limit, offset=offset, sort_by="source_asset_id", sort_order="asc")
    assert open_gauss.recorded_sql == []
```

The parity fixture must include multiple pages, tied sort values, every publication lifecycle status, a later publication that supersedes an earlier status for derivation, a dataset without publication history, a foreign dataset/output pair, and an immutable historical output. Assert invalid sort names fail before SQL, counts use the identical filter predicate without `LIMIT/OFFSET`, and child/publication reads never return a foreign dataset row.

Use a recording fake psycopg connection to assert one `BEGIN`/commit boundary and SQL ordering; simulate `connection.commit()` raising an operational error after server commit, then assert the retry first queries version status/current pointer and returns success without duplicate inserts.

- [ ] **Step 2: Run store tests and prove the store is missing**

Run:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_web/tests/test_partition_domain_store.py -vv
```

Expected: FAIL during collection because `partition_domain_store.py` does not exist.

- [ ] **Step 3: Implement in-memory parity and the exact OpenGauss transaction order**

Implement `start_output()` as an idempotent insert keyed by `(dataset_id, output_version)` and reject a collision with a different task or request identity. Implement `complete_output()` in this exact order inside one `transaction()`:

```text
1. SELECT/INSERT then SELECT partition_datasets FOR UPDATE.
2. SELECT partition_job_attempts FOR UPDATE; require matching task/dataset scope and status running, not cancelled/cancel_requested.
3. SELECT partition_output_versions FOR UPDATE; require staging or prove the same version is already completed/current.
4. Upsert immutable dataset assets and exact normalized bands; reject identity-changing conflict.
5. Insert grid cells, indexes, and tiles by deterministic output_id; ON CONFLICT may accept only byte-equivalent rows.
6. Count rows and compare with result counts; validate all source-asset/band and output-version FKs.
7. UPDATE output version to completed with counts/completed_at.
8. UPDATE old completed current version to superseded only after the new version is complete.
9. UPDATE dataset current pointer, completed status/time, quality pending/current run NULL/error count 0/warning count 0; do not change quality_sequence.
10. INSERT output-version.completed outbox with ON CONFLICT(dataset_id, output_version, event_type) DO NOTHING.
11. Commit.
```

Outbox payload is exact and sufficient for M3:

```json
{
  "schema_version": "2026-07-14-m2-v1",
  "event_type": "output-version.completed",
  "dataset_id": "dataset-a",
  "output_version": "2d1300edc342d76f9647a0be1ce31018",
  "task_id": "task-a",
  "grid_type": "geohash",
  "requested_grid_level": 7,
  "partition_method": "logical",
  "counts": {"tiles": 1, "indexes": 1, "grid_cells": 1}
}
```

On ambiguous commit failure, release the broken connection, open a fresh transaction, query `(version.status, dataset.current_output_version, outbox count)`, and return success only when all three prove completion. Otherwise raise a structured `partition_commit_unknown` error; never blindly replay pointer switching.

Implement every frozen read method before declaring this task complete. OpenGauss builds one parameterized dataset predicate reused by list/count; derives the latest publication with a `LATERAL`/ranked subquery ordered by `(requested_at DESC, publication_id DESC)`; maps validated sort symbols to fixed SQL fragments; and appends stable primary-key ordering. In-memory code mirrors filters, latest-publication derivation, sorting, offset, and limit. `get_output_cleanup_state` returns the owned version status/object prefix/completed_at/failed_at plus current-pointer equality in one read. `output_has_publication_reference` returns true for any publication history status, including `failed` and `withdrawn`, because cleanup cannot delete immutable referenced output history.

`claim_outbox()` uses `FOR UPDATE SKIP LOCKED`, increments `attempt_count`, and records claim metadata. It does not allocate quality runs. `acknowledge_outbox()` marks delivered; `retry_outbox()` returns processing to pending with parameterized `available_at`.

- [ ] **Step 4: Run store and scheduling tests**

Run:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_web/tests/test_partition_domain_store.py cube_web/tests/test_app.py -k 'domain_store or attempt or cancel or partition' -vv
```

Expected: PASS; cancellation cannot switch the pointer, detail failure leaves the old version current, repeat completion is idempotent, exactly one outbox event exists, and every dataset/detail/publication/cleanup read has pagination/filter/order/count parity between in-memory and OpenGauss.

- [ ] **Step 5: Create the atomic-store slice commit**

```bash
git add cube_web/cube_web/services/partition_domain_store.py cube_web/cube_web/services/partition_job_store.py cube_web/tests/test_partition_domain_store.py
git commit -m "feat(partition): commit output versions atomically"
```

Expected: one Opus worktree commit containing store behavior and its tests only.

---

### Task 6: Version Entity Objects and Guard Orphan Cleanup

**Owner:** Sonnet object-lifecycle worker

**Files:**
- Create: `cube_web/cube_web/services/partition_object_store.py`
- Create: `cube_web/tests/test_partition_object_store.py`
- Modify: `cube_split/cube_split/jobs/entity_partition_job.py`

**Interfaces:**
- Consumes: `dataset_id`, `output_version`, configured bucket, entity tile bytes/checksum; current pointer/publication references from `PartitionDomainStore`.
- Produces: `version_prefix`, `put_entity_tile`, `verify_version_manifest`, and `cleanup_unreferenced_version`.

- [ ] **Step 1: Write immutable-prefix and cleanup guard tests**

```python
from datetime import UTC, datetime, timedelta

import pytest

from cube_web.services.partition_object_store import PartitionObjectStore


def test_entity_tile_uses_immutable_dataset_version_prefix(fake_minio) -> None:
    objects = PartitionObjectStore(fake_minio, bucket="cube")
    record = objects.put_entity_tile("dataset-a", "version-a", "tile-1.tif", b"tile")
    assert record["tile_uri"] == "s3://cube/partition/dataset-a/versions/version-a/tiles/tile-1.tif"
    assert not any("current" in key for key in fake_minio.keys())


@pytest.mark.parametrize("guard", ["current", "publication_referenced", "young", "wrong_prefix"])
def test_cleanup_refuses_any_unsafe_version(fake_minio, domain_store, guard: str) -> None:
    objects = PartitionObjectStore(fake_minio, bucket="cube")
    domain_store.seed_cleanup_guard("dataset-a", "version-a", guard)
    with pytest.raises(RuntimeError, match=guard):
        objects.cleanup_unreferenced_version(
            domain_store, "dataset-a", "version-a",
            older_than=datetime.now(UTC) - timedelta(hours=24),
        )
    assert fake_minio.keys()


def test_cleanup_is_idempotent_after_all_guards_pass(fake_minio, domain_store) -> None:
    objects = PartitionObjectStore(fake_minio, bucket="cube")
    objects.cleanup_unreferenced_version(domain_store, "dataset-a", "version-a", older_than=datetime.now(UTC))
    objects.cleanup_unreferenced_version(domain_store, "dataset-a", "version-a", older_than=datetime.now(UTC))
    assert fake_minio.keys() == []
```

- [ ] **Step 2: Run tests and prove the lifecycle service is absent**

Run:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_web/tests/test_partition_object_store.py -vv
```

Expected: FAIL during collection because `partition_object_store.py` does not exist.

- [ ] **Step 3: Implement immutable uploads, manifest validation, and all four cleanup guards**

Use this prefix constructor and reject traversal:

```python
def version_prefix(dataset_id: str, output_version: str) -> str:
    for value in (dataset_id, output_version):
        if not value or "/" in value or value in {".", ".."}:
            raise ValueError("dataset_id and output_version must be non-empty path segments")
    return f"partition/{dataset_id}/versions/{output_version}/"
```

Entity uploads use deterministic object names beneath `tiles/`, write checksums, and verify an existing key before reuse. The manifest is database data returned with `PartitionDatasetResult`; no mutable MinIO manifest pointer is created.

Cleanup must call `get_output_cleanup_state(dataset_id, output_version)` and `output_has_publication_reference(dataset_id, output_version)` and refuse when: (1) dataset current pointer equals the version, (2) any publication references it, (3) version completion/failure time is newer than retention cutoff, or (4) any manifest key does not begin with the exact expected prefix. Delete only the verified manifest keys, tolerate already-missing keys, and mark cleanup complete only after listing confirms no expected object remains.

- [ ] **Step 4: Run lifecycle and entity tests**

Run:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_web/tests/test_partition_object_store.py cube_split/tests/test_entity_partition_job.py -vv
```

Expected: PASS; uploads use immutable version prefixes and every unsafe cleanup case retains objects.

- [ ] **Step 5: Create the object-lifecycle slice commit**

```bash
git add cube_web/cube_web/services/partition_object_store.py cube_web/tests/test_partition_object_store.py cube_split/cube_split/jobs/entity_partition_job.py
git commit -m "feat(partition): version entity objects"
```

Expected: one Sonnet worktree commit with no workflow or DDL edits.

---

### Task 7: Return Normalized Logical and Entity Dataset Results

**Owner:** Sonnet Ray worker

**Files:**
- Modify: `cube_split/cube_split/jobs/ray_logical_partition_job.py`
- Modify: `cube_split/cube_split/jobs/entity_partition_job.py`
- Modify: `cube_split/cube_split/jobs/ray_partition_core.py`
- Modify: `cube_split/tests/test_ray_logical_partition_job.py`
- Modify: `cube_split/tests/test_entity_partition_job.py`
- Modify: `cube_split/tests/test_ray_partition_core.py`

**Interfaces:**
- Consumes: M1 `GridAddress` from `cube_encoder/grid_core/app/models/grid_address.py` and `GridCell` from `cube_encoder/grid_core/app/models/grid_cell.py`; `GridCell` exposes direct `.grid_type`, `.grid_level`, `.space_code`, `.topology_code`, `.bbox`, and `.geometry` properties. `CubeEncoderSDK.cover()` returns those cells. This task also consumes `OutputIdentity` and the immutable object prefix.
- Produces: dictionaries valid as `PartitionDatasetResult`, with deterministic output IDs and exact tile/index/grid-cell row keys.

- [ ] **Step 1: Write normalized result-shape tests using real M1 properties**

```python
def test_logical_result_uses_source_cog_window_and_cell_properties(run_logical, grid_cell) -> None:
    grid_cell.grid_level = 4
    grid_cell.topology_code = "mgrs-topo-v1:utm-50n:4:50QKK12341234"
    result = run_logical(grid_cell=grid_cell, requested_grid_level=5)
    assert result["partition_method"] == "logical"
    assert result["requested_grid_level"] == 5
    assert result["tiles"][0]["tile_kind"] == "logical_reference"
    assert result["tiles"][0]["tile_uri"].startswith("s3://cube/loader/")
    assert result["grid_cells"][0]["grid_level"] == 4
    assert result["grid_cells"][0]["topology_code"] == grid_cell.topology_code
    assert set(result["indexes"][0]) >= {
        "output_id", "source_asset_id", "band_code", "grid_type", "grid_level", "space_code",
        "topology_code", "time_bucket", "window_col_off", "window_row_off", "window_width", "window_height", "value_ref_uri",
    }


def test_entity_result_writes_versioned_tile_and_has_no_logical_window(run_entity) -> None:
    result = run_entity(dataset_id="dataset-a", output_version="version-a", requested_grid_level=3)
    tile = result["tiles"][0]
    index = result["indexes"][0]
    assert tile["tile_kind"] == "entity_file"
    assert tile["tile_uri"].startswith("s3://cube/partition/dataset-a/versions/version-a/tiles/")
    assert all(index[key] is None for key in ("window_col_off", "window_row_off", "window_width", "window_height"))
    assert index["grid_level"] == result["grid_cells"][0]["grid_level"]


def test_repeat_runs_keep_identical_output_ids_and_entity_keys(run_entity) -> None:
    first = run_entity(dataset_id="dataset-a", output_version="version-a", requested_grid_level=3)
    second = run_entity(dataset_id="dataset-a", output_version="version-a", requested_grid_level=3)
    for collection in ("tiles", "indexes", "grid_cells"):
        assert [row["output_id"] for row in first[collection]] == [row["output_id"] for row in second[collection]]
    assert [row["tile_uri"] for row in first["tiles"]] == [row["tile_uri"] for row in second["tiles"]]
```

- [ ] **Step 2: Run narrow tests and verify old shapes fail**

Run:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_split/tests/test_ray_logical_partition_job.py cube_split/tests/test_entity_partition_job.py cube_split/tests/test_ray_partition_core.py -k 'result or versioned or cell_properties or deterministic' -vv
```

Expected: FAIL because current results use legacy keys/levels and do not return `PartitionDatasetResult`-compatible rows.

- [ ] **Step 3: Normalize both job paths without copying grid logic**

For every M1 cell, read properties directly:

```python
cell_level = cell.grid_level
topology_code = cell.topology_code
identity = OutputIdentity(
    dataset_id=dataset_id,
    output_version=output_version,
    source_asset_id=asset.source_asset_id,
    band_code=band.band_code,
    grid_type=grid_type,
    grid_level=cell_level,
    space_code=cell.space_code,
    topology_code=topology_code,
    time_bucket=time_bucket,
    window_identity=window_identity,
)
output_id = make_output_id(identity)
```

Do not access `cell.level` or `cell.metadata["topology_code"]`. Logical rows reference the loader `cog_uri` and have a complete Rasterio window. Entity rows reference the immutable entity tile and have all four window columns null. Both paths return dataset/task/version identity, requested level, derived method, object prefix, and tuples of rows; conversion timings remain absent.

- [ ] **Step 4: Run job tests**

Run:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_split/tests/test_ray_partition_core.py cube_split/tests/test_ray_logical_partition_job.py cube_split/tests/test_entity_partition_job.py -vv
```

Expected: PASS; mixed result levels survive, MGRS topology comes from the property, and repeat output IDs/object keys match.

- [ ] **Step 5: Create the normalized-result slice commit**

```bash
git add cube_split/cube_split/jobs/ray_partition_core.py cube_split/cube_split/jobs/ray_logical_partition_job.py cube_split/cube_split/jobs/entity_partition_job.py cube_split/tests/test_ray_partition_core.py cube_split/tests/test_ray_logical_partition_job.py cube_split/tests/test_entity_partition_job.py
git commit -m "feat(partition): return versioned dataset results"
```

Expected: one Sonnet worktree commit based on the Task 2 Ray slice.

---

### Task 8: Fan Out by Dataset and Isolate Partial Failure

**Owner:** Opus workflow integrator

**Files:**
- Modify: `cube_web/cube_web/services/partition_workflow.py`
- Modify: `cube_web/cube_web/services/partition_job_store.py`
- Modify: `cube_web/cube_web/services/partition_runners.py`
- Create: `cube_web/tests/test_partition_dataset_workflow.py`
- Modify: `cube_web/tests/test_app.py`

**Interfaces:**
- Consumes: `group_datasets`, `make_output_version`, `PartitionDomainStore.start_output/complete_output/fail_output`, normalized Ray results.
- Produces: one batch task result with `datasets: list[{dataset_id, output_version, status, counts?, error?}]`, dataset-scoped attempts, and partial-failure status.

- [ ] **Step 1: Write two-dataset partial-failure and cancellation-race tests**

```python
def test_batch_commits_successful_dataset_when_sibling_fails(workflow, strict_request, domain_store) -> None:
    strict_request = strict_request.with_two_datasets("dataset-ok", "dataset-fail")
    workflow.runner.fail_dataset("dataset-fail", RuntimeError("unreadable loader COG"))
    result = workflow.run(task_id="task-batch", request=strict_request)
    assert result == {
        "batch_id": strict_request.batch_id,
        "status": "partial_failure",
        "datasets": [
            {"dataset_id": "dataset-ok", "output_version": make_output_version("dataset-ok", "task-batch"), "status": "completed",
             "counts": {"tiles": 1, "indexes": 1, "grid_cells": 1}},
            {"dataset_id": "dataset-fail", "output_version": make_output_version("dataset-fail", "task-batch"), "status": "failed",
             "error": {"code": "partition_execution_failed", "message": "unreadable loader COG"}},
        ],
    }
    assert domain_store.resolve_output_version("dataset-ok") == make_output_version("dataset-ok", "task-batch")
    with pytest.raises(KeyError):
        domain_store.resolve_output_version("dataset-fail")


def test_cancel_after_ray_before_commit_cannot_switch_pointer(workflow, strict_request, job_store, domain_store) -> None:
    old = domain_store.seed_completed_version("dataset-ok", "old-version")
    workflow.after_ray = lambda: job_store.request_cancel("task-batch")
    result = workflow.run(task_id="task-batch", request=strict_request)
    assert result["datasets"][0]["status"] == "cancelled"
    assert domain_store.resolve_output_version("dataset-ok") == old
    assert domain_store.outbox_rows() == []


def test_duplicate_dataset_is_rejected_before_attempt_creation(workflow, strict_request, job_store) -> None:
    duplicate = strict_request.model_copy(update={"datasets": (strict_request.datasets[0], strict_request.datasets[0])})
    with pytest.raises(ValueError, match="duplicate dataset_id"):
        workflow.run(task_id="task-duplicate", request=duplicate)
    assert job_store.list_attempts(task_id="task-duplicate") == []


def test_failed_output_does_not_mutate_sibling_rows(workflow, strict_request, domain_store) -> None:
    request = strict_request.with_two_datasets("dataset-ok", "dataset-fail")
    workflow.runner.fail_dataset("dataset-fail", RuntimeError("unreadable loader COG"))
    workflow.run(task_id="task-isolation", request=request)
    assert domain_store.get_dataset("dataset-ok")["partition_status"] == "completed"
    assert domain_store.get_dataset("dataset-fail")["partition_status"] == "failed"
    assert {row["dataset_id"] for row in domain_store.list_indexes("dataset-ok", limit=100, offset=0, sort_by="output_id", sort_order="asc")} == {"dataset-ok"}
```

- [ ] **Step 2: Run workflow tests and prove batch-level coupling remains**

Run:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_web/tests/test_partition_dataset_workflow.py -vv
```

Expected: FAIL because the new dataset-scoped workflow test module/imports do not exist and the current workflow runs a batch as one result.

- [ ] **Step 3: Implement deterministic dataset fan-out and per-dataset commits**

At synchronous acceptance, validate/group the full request before writing attempts. For each input in request order:

```python
for dataset_id, dataset in group_datasets(request).items():
    output_version = make_output_version(dataset_id, task_id)
    domain_store.start_output(request, dataset, task_id)
    try:
        result = runner.run_dataset(
            dataset=dataset,
            task_id=task_id,
            output_version=output_version,
            grid_type=request.grid_type,
            requested_grid_level=request.requested_grid_level,
            cover_mode=request.cover_mode,
        )
        assert_not_cancelled(task_id)
        committed = domain_store.complete_output(PartitionDatasetResult.model_validate(result))
    except PartitionCancelledError:
        domain_store.fail_output(dataset_id, output_version, error_code="partition_cancelled", error_message="Partition task cancelled")
        append_cancelled_result()
    except Exception as exc:
        domain_store.fail_output(dataset_id, output_version, error_code="partition_execution_failed", error_message=safe_message(exc))
        append_failed_result()
```

Do not wrap sibling datasets in one database transaction. Final batch status is `completed` when all complete, `failed` when none complete, `partial_failure` when completed and failed/cancelled coexist, and `cancelled` when all are cancelled. Dataset results never contain credentials, local cache paths, or conversion timings.

- [ ] **Step 4: Run workflow, route, and store tests**

Run:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_web/tests/test_partition_dataset_workflow.py cube_web/tests/test_partition_domain_store.py cube_web/tests/test_app.py -k 'partition or dataset or cancel or partial' -vv
```

Expected: PASS; a failed sibling does not roll back a completed dataset, cancellation cannot switch current output, and results are dataset-scoped.

- [ ] **Step 5: Create the orchestration slice commit**

```bash
git add cube_web/cube_web/services/partition_workflow.py cube_web/cube_web/services/partition_job_store.py cube_web/cube_web/services/partition_runners.py cube_web/tests/test_partition_dataset_workflow.py cube_web/tests/test_app.py
git commit -m "feat(partition): isolate outputs by dataset"
```

Expected: one Opus integration commit based on Tasks 1, 3, 5, 6, and 7.

---

### Task 9: Freeze Schema Version and Milestone 3 Handoff

**Owner:** Opus schema/store integrator

**Files:**
- Modify: `cube_web/cube_web/services/partition_domain_schema.py`
- Modify: `cube_web/cube_web/services/partition_domain_store.py`
- Modify: `cube_web/tests/test_partition_domain_schema.py`
- Modify: `cube_web/tests/test_partition_domain_store.py`

**Interfaces:**
- Consumes: schema version row and frozen M3 store surface.
- Produces: fail-closed `assert_schema_version(connection)` and stable M3 transaction/outbox/quality-run primitives.

- [ ] **Step 1: Write mismatch and M3-boundary tests**

```python
def test_schema_version_mismatch_refuses_writes_and_names_reset(store) -> None:
    store.connection.schema_version = "2026-07-13-old"
    with pytest.raises(RuntimeError, match=r"2026-07-14-m2-v1.*reset_partition_domain.py"):
        store.start_output(store.request, store.request.datasets[0], "task-a")


def test_m2_store_exposes_transaction_and_outbox_but_not_quality_mutation(store) -> None:
    assert callable(store.transaction)
    assert callable(store.claim_outbox)
    assert callable(store.acknowledge_outbox)
    assert callable(store.retry_outbox)
    for forbidden in ("allocate_quality_run", "complete_quality_run", "publish", "approve_warning"):
        assert not hasattr(store, forbidden)


def test_m3_frozen_read_surface_is_complete(store) -> None:
    required = {
        "get_dataset", "list_datasets", "count_datasets", "get_output_version",
        "list_assets", "count_assets", "list_bands", "count_bands", "list_tiles", "count_tiles",
        "list_indexes", "count_indexes", "list_grid_cells", "count_grid_cells",
        "list_publications", "count_publications", "output_has_publication_reference", "get_output_cleanup_state",
    }
    assert all(callable(getattr(store, name)) for name in required)
```

Extend `test_domain_schema_contains_exact_tables_constraints_and_quality_handoff` with these exact assertions (and keep the full-column test from Task 3):

```python
assert "trigger_event_id uuid null unique" in sql
assert "foreign key (trigger_event_id)" not in sql
assert "unique (dataset_id, output_version, quality_run_id)" in sql
assert "status in ('publishing','active','withdrawing','failed','withdrawn')" in sql
assert "desired_action in ('activate','withdraw')" in sql
assert "check ((claimed_at is null) = (claimed_by is null))" in sql
assert "uq_partition_publication_live_snapshot" in sql
assert "where status in ('publishing','active','withdrawing')" in sql
assert "on partition_publications(status, available_at, claimed_at, created_at)" in sql
assert "on partition_publications(dataset_id, requested_at desc, publication_id desc)" in sql
publication_statement = next(item.lower() for item in schema_statements() if "create table partition_publications" in item.lower())
assert "'published'" not in publication_statement
assert "'unpublished'" not in publication_statement
```

The real catalog test queries `pg_constraint`, `pg_attribute`, `pg_index`, and `pg_get_expr` after bootstrap and compares exact normalized rows for the run composite unique/FKs, publication checks, claim/latest/partial indexes, and quality error page/filter indexes. A generated-SQL-only substring test is insufficient.

- [ ] **Step 2: Run handoff tests and prove writes do not fail closed yet**

Run:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_web/tests/test_partition_domain_schema.py cube_web/tests/test_partition_domain_store.py -k 'schema_version or m2_store or trigger_event' -vv
```

Expected: FAIL because schema mismatch is not yet checked at every write boundary or forbidden quality mutation methods remain.

- [ ] **Step 3: Add fail-closed schema assertion and preserve the M3 boundary**

Implement:

```python
def assert_schema_version(connection: psycopg.Connection[Any]) -> None:
    row = connection.execute(
        "SELECT schema_version FROM partition_domain_schema_version WHERE singleton = TRUE"
    ).fetchone()
    actual = None if row is None else str(row[0])
    if actual != PARTITION_DOMAIN_SCHEMA_VERSION:
        raise RuntimeError(
            f"partition domain schema version {actual!r} does not match {PARTITION_DOMAIN_SCHEMA_VERSION!r}; "
            "run cube_web/scripts/reset_partition_domain.py with the required development guards"
        )
```

Call it before all store mutations, not on read-only list calls. Keep `trigger_event_id UUID NULL UNIQUE` without outbox FK. Keep `trigger`, `requested_by`, `attempt_count`, `available_at`, `claimed_at`, `claimed_by`, and `last_error` plus the `(status, available_at, claimed_at, created_at)` index. Do not allocate a quality run while acknowledging an event in M2; M3 must allocate pending run idempotently in its own transaction and acknowledge only after allocation succeeds.

- [ ] **Step 4: Run schema/store handoff tests**

Run:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_web/tests/test_partition_domain_schema.py cube_web/tests/test_partition_domain_store.py -vv
```

Expected: PASS; mismatches refuse writes with the explicit reset command; M2 exposes no quality/publication business mutation SQL; and the complete paginated read/cleanup surface plus exact structural DDL is frozen for M3.

- [ ] **Step 5: Create the interface-freeze slice commit**

```bash
git add cube_web/cube_web/services/partition_domain_schema.py cube_web/cube_web/services/partition_domain_store.py cube_web/tests/test_partition_domain_schema.py cube_web/tests/test_partition_domain_store.py
git commit -m "feat(partition): freeze M3 domain interfaces"
```

Expected: one Opus worktree commit that changes no API route or frontend file.

---

### Task 10: Prove OpenGauss, MinIO, and Ray End to End

**Owner:** Sonnet real-gate worker; Opus runs and signs off

**Files:**
- Create: `cube_web/tests/test_partition_domain_real.py`
- Create: `cube_web/scripts/run_m2_partition_domain_gate.py`
- Create: `cube_web/docs/M2_PARTITION_DOMAIN_GATE.md`
- Modify: `pytest.ini`

**Interfaces:**
- Consumes: real environment variables, strict loader payload, reset/bootstrap, Ray workflow, OpenGauss store, MinIO lifecycle.
- Produces: pytest marker `m2_real` and a non-skipping gate script with machine-readable scenario summary.

- [ ] **Step 1: Write a non-skipping real test and strict preflight**

Require these exact variables:

```text
CUBE_WEB_POSTGRES_DSN
CUBE_WEB_M2_DATABASE_NAME
CUBE_WEB_MINIO_ENDPOINT
CUBE_WEB_MINIO_ACCESS_KEY
CUBE_WEB_MINIO_SECRET_KEY
CUBE_WEB_MINIO_BUCKET
RAY_ADDRESS
CUBE_WEB_M2_GEOHASH_COG_URI
CUBE_WEB_M2_ISEA4H_COG_URI
```

At module setup, collect missing names and `pytest.fail("M2 real gate missing required environment: " + ", ".join(missing))`; never call `pytest.skip`. Preflight must connect and `SELECT current_database()`, compare the exact database name, stat both COG objects, open each through the same Ray worker cache/Rasterio path, connect to Ray and run a remote `ray.get`, and write/read/delete a probe only beneath `partition/_m2_gate_probe/{uuid4().hex}`. The module carries `pytestmark = pytest.mark.m2_real`; add `test_m2_real_marker_registered_by_root_config(pytestconfig)` asserting `m2_real` appears in `pytestconfig.getini("markers")`, so running from the repository root with `--strict-markers` proves the root configuration owns the marker.

Implement these real scenarios as separate tests:

```text
test_real_geohash_logical_dataset
  strict loader COG -> Ray logical windows -> atomic version/current pointer -> one outbox event; no output source upload.

test_real_isea4h_entity_dataset
  strict loader COG -> Ray entity tile -> immutable MinIO version prefix -> DB tile/checksum/current pointer.

test_real_two_dataset_partial_failure
  valid Geohash dataset plus deliberately missing COG; valid dataset completes, sibling fails, old/current state remains isolated.

test_real_atomic_rollback_keeps_old_pointer
  commit v1, inject a detail constraint failure for v2, verify v1 remains current and no v2 completion event exists.

test_real_unknown_commit_is_idempotent
  inject post-server-commit client error, verify recovery query returns completed v2 and exactly one event/one copy of every output row.

test_real_orphan_cleanup_guards_and_idempotency
  current and publication-referenced versions refuse cleanup; expired unreferenced failed version is deleted twice without error.

test_real_catalog_matches_m3_handoff
  query actual OpenGauss catalogs and assert every quality result/error/approval/publication column, composite FK/unique,
  exact publication status/action checks, withdrawing lease/reconciliation checks, and claim/partial/latest/page/filter indexes.
```

- [ ] **Step 2: Run the marker before implementation and verify a hard failure**

Run:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_web/tests/test_partition_domain_real.py -m m2_real --strict-markers -vv -s
```

Expected: FAIL during collection because the real test file/gate does not exist. If the file exists but required infrastructure is unavailable, expected remains FAIL, never SKIP or XFAIL.

- [ ] **Step 3: Implement the gate script, tests, marker, and operator instructions**

Register the marker in root `pytest.ini`, because root `python3.11 -m pytest` loads that configuration and does not load `cube_web/pyproject.toml`:

```ini
[pytest]
testpaths =
    cube_encoder/tests
    cube_split/tests
    cube_web/tests
markers =
    e2e: tests that require external Ray, MinIO, and PostgreSQL services
    m2_real: requires destructive isolated OpenGauss test database plus real MinIO and Ray; never skipped
```

`run_m2_partition_domain_gate.py` performs preflight, calls guarded reset with `--execute`, runs only the seven real scenarios in a subprocess, queries final invariants, and emits JSON:

```json
{
  "gate": "m2-partition-domain-real",
  "status": "passed",
  "scenarios": {
    "geohash_logical": "passed",
    "isea4h_entity": "passed",
    "partial_failure": "passed",
    "atomic_rollback": "passed",
    "unknown_commit": "passed",
    "cleanup": "passed",
    "catalog_handoff": "passed"
  },
  "skipped": 0
}
```

The documentation file contains the exact required variables, destructive isolated-database warning, preview command, execute command, expected JSON, object prefix used, and cleanup query. It must not include real credentials.

- [ ] **Step 4: Run the guarded reset and both real gate entry points**

Preview:

```bash
CUBE_WEB_ENV=development PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 cube_web/scripts/reset_partition_domain.py --database-name "$CUBE_WEB_M2_DATABASE_NAME" --dangerously-reset-partition-domain
```

Expected: exit 0, `execution=false`, exact known-object inventory, no unknown objects, and no mutation.

Execute isolated reset/bootstrap:

```bash
CUBE_WEB_ENV=development PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 cube_web/scripts/reset_partition_domain.py --database-name "$CUBE_WEB_M2_DATABASE_NAME" --dangerously-reset-partition-domain --execute
```

Expected: exit 0, `execution=true`, schema version `2026-07-14-m2-v1`, all three scheduling and fourteen domain tables present, no MinIO deletion.

Run pytest directly:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_web/tests/test_partition_domain_real.py -m m2_real --strict-markers -vv -s
```

Expected: 8 passed (seven scenarios plus root marker-registration proof), 0 failed, 0 skipped, 0 xfailed.

Run the operator gate:

```bash
CUBE_WEB_ENV=development PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 cube_web/scripts/run_m2_partition_domain_gate.py --database-name "$CUBE_WEB_M2_DATABASE_NAME" --dangerously-reset-partition-domain
```

Expected: exit 0 and final JSON exactly reports all seven named scenarios `passed` with `skipped: 0`. Any unavailable service, unreadable COG, skip, warning-only substitution, or fake backend blocks Milestone 2.

- [ ] **Step 5: Create the real-gate slice commit**

```bash
git add cube_web/tests/test_partition_domain_real.py cube_web/scripts/run_m2_partition_domain_gate.py cube_web/docs/M2_PARTITION_DOMAIN_GATE.md pytest.ini
git commit -m "test(partition): add real services milestone gate"
```

Expected: one Sonnet worktree commit after a real run; Opus records the command output and independently reruns it before final integration.

---

### Task 11: Run Removal, Regression, Four-Level Review, and Create One M2 Commit

**Owner:** Main Opus integrator; independent Sonnet/Opus reviewer; adversarial validation agent

**Files:**
- Verify all files listed in Tasks 1–10.
- Do not modify frontend, M3 API/quality/publication behavior, normative spec, or unrelated user files.

**Interfaces:**
- Consumes: all worker slice commits in the frozen merge order.
- Produces: one coherent local Milestone 2 commit and recorded four-level gate evidence.

- [ ] **Step 1: Integrate in dependency order and inspect scope**

Integrate contract, DDL/store, Ray cleanup/results, object lifecycle, workflow, handoff, and real-gate worker commits in the merge order stated above. For each slice, read `worker_commit_sha` from that slice's Level 1 evidence, require it matches `^[0-9a-f]{40}$`, verify `git cat-file -e "$worker_commit_sha^{commit}"`, then run `git cherry-pick --no-commit "$worker_commit_sha"` so the main integration worktree accumulates one staged/unstaged M2 change rather than intermediate mainline commits. Never type a guessed branch/SHA or leave an angle-bracket token in an executed command. Resolve a shared-file conflict only in the Opus integration worktree and rerun both affected owners' narrow tests. Before testing, run:

```bash
git status --short
git diff --stat
git diff --name-status
git diff --cached --stat
git diff --cached --name-status
```

Expected: the four diff commands inspect the complete uncommitted integration scope (not an unrelated prior commit range); only M2 implementation/test/gate files are modified or staged; `cube_web/docs/PARTITION_DATASET_QUALITY_REFACTOR_PLAN.md`, M1/M3/M4/M5 files, frontend files, credentials, and `.cube_web.env` are absent.

- [ ] **Step 2: Run static boundary and legacy removal checks**

Run:

```bash
rg -n 'convert_assets_to_cog|convert_asset_to_cog|upload_source_assets_to_minio|cog_workers|cog_compress|cog_predictor|cog_conversion_seconds|cog_upload_seconds|converted_cog_uri|import h3|from h3' cube_split/cube_split cube_web/cube_web/services cube_web/cube_web/schemas.py
```

Expected: exit 1, no output.

Run:

```bash
rg -n 'PartitionDemoRequest|selected_assets|selected_observations|source_uri|polarization|"band"\s*:|"grid_level"\s*:' cube_web/cube_web/services/partition_contracts.py cube_web/cube_web/schemas.py cube_web/cube_web/services/partition_workflow.py
```

Expected: exit 1, no output. The legitimate exact field name `bands` remains in `DatasetInput`; this check must not reject it.

Run:

```bash
rg -n '\.level\b|metadata\s*\[\s*["'"']topology_code["'"']\s*\]' cube_split/cube_split/jobs cube_web/cube_web/services
```

Expected: exit 1, no output. M2 result code reads `GridCell.grid_level` and `GridCell.topology_code`.

Run:

```bash
rg -n 's2|tile_matrix|plane_grid|h3' cube_web/cube_web/services/partition_contracts.py cube_web/cube_web/services/partition_domain_schema.py cube_web/cube_web/services/partition_workflow.py cube_split/cube_split/jobs/ray_logical_partition_job.py cube_split/cube_split/jobs/entity_partition_job.py cube_split/pyproject.toml
```

Expected: exit 1, no output. M2 removes the deferred `cube_split` H3 declaration together with old split selector/partition imports; no M2 production path or runtime dependency may reference old grids.

- [ ] **Step 3: Run complete automated M2 and repository gates**

Run focused M2 tests:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_split/tests/test_production_imports.py cube_web/tests/test_partition_contracts.py cube_web/tests/test_partition_domain_schema.py cube_web/tests/test_reset_partition_domain.py cube_web/tests/test_partition_domain_store.py cube_web/tests/test_partition_object_store.py cube_web/tests/test_partition_dataset_workflow.py cube_split/tests/test_ray_partition_core.py cube_split/tests/test_ray_logical_partition_job.py cube_split/tests/test_entity_partition_job.py -vv
```

Expected: PASS, 0 failed, 0 skipped.

Run cross-package regression:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_encoder/tests cube_split/tests
```

Expected: PASS, 0 failed.

Run Web regression:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_web/tests -m 'not m2_real'
```

Expected: PASS, 0 failed; only the explicitly excluded real marker is deselected, not skipped.

Run lint:

```bash
python3.11 -m ruff check cube_encoder cube_split cube_web
```

Expected: exit 0, `All checks passed!`.

Run type checking:

```bash
python3.11 -m mypy cube_encoder/grid_core cube_split/cube_split cube_web/cube_web
```

Expected: exit 0 and `Success: no issues found`.

Run the real pytest gate again:

```bash
PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 -m pytest cube_web/tests/test_partition_domain_real.py -m m2_real --strict-markers -vv -s
```

Expected: 8 passed (seven scenarios plus root marker-registration proof), 0 failed, 0 skipped, 0 xfailed.

Run the real operator gate again:

```bash
CUBE_WEB_ENV=development PYTHONPATH=cube_encoder:cube_split:cube_web python3.11 cube_web/scripts/run_m2_partition_domain_gate.py --database-name "$CUBE_WEB_M2_DATABASE_NAME" --dangerously-reset-partition-domain
```

Expected: exit 0; seven scenarios passed and `skipped: 0`.

- [ ] **Step 4: Complete all four review levels with blocking criteria**

**Level 1 — implementer self-test:** each owner supplies commit SHA, exact narrow command, pass count, and static-check result. Expected: every owned slice passes; missing evidence blocks integration.

**Level 2 — independent code review:** an independent Sonnet or Opus reviews strictness, transaction ordering, SQL constraints, object cleanup, and deletion scope. Expected: no unresolved high-severity finding; every accepted medium finding has a test and fix commit.

**Level 3 — adversarial validation:** a fresh agent attempts a missing/malformed/mismatched M1 ledger predecessor hash, documentation SHA substitution, Geohash/MGRS/ISEA4H level underflow/overflow, carbon observations aliases, missing full assets/bands, `grid_level_mode`, duplicate datasets, mismatched methods, malformed/foreign-bucket COGs, mixed M1 cell levels, topology-property loss, cancellation between Ray and commit, detail constraint failure, unknown commit, duplicate outbox delivery, unsafe reset object, wrong database name, current/publication-referenced/young/wrong-prefix cleanup, every publication lifecycle and stale `withdrawing` lease, an undeclared-but-imported H3 path, and one failed sibling. Expected: every attack is rejected or produces the specified isolated state; no pointer corruption, cross-dataset row, duplicate event, unsafe deletion, or skipped real test.

**Level 4 — final Opus gate:** main Opus personally runs every command in Step 3 and inspects live OpenGauss catalog constraints/indexes, paginated read/count parity, publication lifecycle/lease rows, counts/current pointers/outbox uniqueness plus MinIO prefixes. Expected: all automated and real gates pass and all review findings are closed. Any test failure, high-severity review finding, unavailable real dependency, skipped real test, schema mismatch, unknown reset object, or unrelated diff blocks the commit.

- [ ] **Step 5: Collapse implementation slices and create one coherent local M2 milestone commit**

After preserving review evidence outside Git commit content, keep the mainline parent at the gated M1 commit because every worker slice was applied with `git cherry-pick --no-commit`. Unstage everything, stage only M2 files, and verify the staged scope:

```bash
git reset
git add cube_web/cube_web/services/partition_contracts.py cube_web/cube_web/schemas.py cube_web/cube_web/app.py cube_web/cube_web/routes/partition.py cube_web/cube_web/routes/partition_adapters.py cube_web/cube_web/services/config_store.py cube_web/cube_web/services/partition_defaults.py cube_web/cube_web/services/partition_loaded_schemas.py cube_web/cube_web/services/partition_domain_schema.py cube_web/cube_web/services/partition_domain_store.py cube_web/cube_web/services/partition_object_store.py cube_web/cube_web/services/partition_workflow.py cube_web/cube_web/services/partition_job_store.py cube_web/cube_web/services/partition_runners.py cube_web/scripts/reset_partition_domain.py cube_web/scripts/run_m2_partition_domain_gate.py cube_web/tests/test_partition_contracts.py cube_web/tests/test_partition_domain_schema.py cube_web/tests/test_reset_partition_domain.py cube_web/tests/test_partition_domain_store.py cube_web/tests/test_partition_object_store.py cube_web/tests/test_partition_dataset_workflow.py cube_web/tests/test_partition_domain_real.py cube_web/tests/test_app.py cube_web/docs/M2_PARTITION_DOMAIN_GATE.md pytest.ini cube_split/pyproject.toml cube_split/tests/test_production_imports.py cube_split/cube_split/jobs/ray_partition_core.py cube_split/cube_split/jobs/ray_logical_partition_job.py cube_split/cube_split/jobs/entity_partition_job.py cube_split/tests/test_ray_partition_core.py cube_split/tests/test_ray_logical_partition_job.py cube_split/tests/test_entity_partition_job.py
git diff --cached --name-only
git diff --cached --check
git commit -m "feat(partition): complete dataset domain milestone"
```

Expected: `git diff --cached --check` exits 0; staged names are only the listed M2 files; exactly one new local implementation commit is created above M1. Do not push.

- [ ] **Step 6: Verify final commit and hand off to Milestone 3**

Run:

```bash
git status --short
git log -2 --oneline
```

Expected: clean working tree except any pre-existing user-owned untracked files; latest commit is `feat(partition): complete dataset domain milestone`; previous milestone commit hash equals the M2 row `predecessor_integration_hash`. After L1–L4/review are all `PASSED`, write the new 40-hex M2 commit hash to the M2 row `integration_hash`, set M2 `status=PASSED`, persist/re-read the row, and verify its predecessor value is unchanged. Hand M3 the exact complete paginated store surface, DDL version `2026-07-14-m2-v1`, publication lifecycle/lease constraints, outbox payload, and rule that M3 allocates/completes quality runs through `transaction()` before acknowledging events. M3 kickoff remains blocked if any list/count/detail/publication-reference/cleanup method, catalog assertion, or in-memory/OpenGauss parity test is absent.

---

## Final Acceptance Matrix

| Requirement | Proof |
|---|---|
| Executable M1 predecessor gate uses ledger implementation hash, never a documentation SHA | Global constraint and Task 11 L1–L4 review evidence |
| Exact normalized loader contract, including carbon COG `DatasetInput` and legitimate dataset-level `bands` | Task 1 validation and 422 tests |
| Frozen `POST /v1/partition/{data_type}/tasks/run` full-body contract, no `grid_level_mode` | Task 1 route/scheduler tests |
| Exported M1 level validator with Geohash `1..12`, MGRS `0..5`, ISEA4H `0..15`; result-cell properties | Tasks 1, 5, 7 static and behavior tests |
| No COG conversion/reprojection/source upload/zero timings and no split H3 runtime dependency | Task 2 import/dependency tests and Task 11 no-hit scans |
| One batch, independently committed datasets | Task 8 partial-failure test and real scenario |
| Immutable MinIO entity objects; OpenGauss-only current pointer | Task 6 tests and real ISEA4H scenario |
| Atomic detail/version/pointer/status/quality reset/outbox | Task 5 rollback/idempotency tests and real scenarios |
| Exact complete paginated dataset/detail/publication/cleanup read surface with in-memory/OpenGauss parity | Tasks 5 and 9 interface/parity tests |
| Complete quality/error/approval/publication DDL, lease reconciliation, exact statuses, nullable unique trigger event without outbox FK | Tasks 3, 9, and 10 SQL/catalog tests |
| M2 does not allocate/complete quality runs | Task 9 forbidden-method boundary test |
| Controlled full scheduling/result reset with unknown-object refusal | Task 4 tests and real preview/execute proof |
| Real OpenGauss, MinIO, Ray, Geohash, ISEA4H, exact M3 handoff catalog | Task 10 seven non-skipping scenarios |
| Four-level evidence and one local milestone commit | Task 11 review and commit steps |

Plan implementation is complete only when the M2 ledger row records the exact gated M1 implementation `integration_hash` as `predecessor_integration_hash` and every expected result above is observed; a fake-only result, skipped real test, unresolved review finding, or uncommitted interface divergence means Milestone 2 remains blocked.
