"""Integrity checks for the committed offline DGGRID ISEA4H fixtures."""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

from grid_core.app.engines.isea4h.addressing import cell_count

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "isea4h"
EXPECTED_DIGESTS = {
    "children_res0.jsonl": "90e8304c91bd3cf0f89f931fe625ce889d24677b96ebc0ab3bb275c1a1725f3e",
    "children_res1.jsonl": "c85062e143b9e653d5eef1f5b787dfc2d21a3e2a6c8612fc3a4dc05769ac3f43",
    "children_res2.jsonl": "68f3c1a54ee11038d45a4cee0be82106aa3aacb20f8fee41e38bfb3532de7bda",
    "children_res3.jsonl": "c432d5741e5871bd40a41f66bb2b93cc5ffcdfc0b2e4838d7ff9b34bc2ba6a9e",
    "neighbors_res0.jsonl": "6fe9adda845b5eec89562256b562db1642177ee1ef1a0aad2d8ac376a2e825e3",
    "neighbors_res1.jsonl": "18f54b895ba41b3b4cfcbba780198d021c98bb2e714758740fadb1bf58933447",
    "neighbors_res2.jsonl": "36318da92501b4d5481d6969b1f2f5ac2f30596437ef0e409c1982b2ab0d6a83",
    "neighbors_res3.jsonl": "bd3bc783dee733c092b786b08c8f0027d5cea6b758de665c752508e80420fa2f",
    "neighbors_res4.jsonl": "b061ca42fc106930a4d9c3874ff8d955f5d70d146e225662f740b83265ef99b8",
    "vectors_res0.jsonl": "51b0e0f453d4c46e2fe1e72f42d91372f138cc4f9ce64f3d692c92e093471071",
    "vectors_res1.jsonl": "27f8815e39fa3cadbd74307789bfa8fefd93b8ff26120182af5931d976a99393",
    "vectors_res2.jsonl": "988f31cde3ce80d858212a7d1be79dff7ac949193071b6eeae3f8a563c594d5b",
    "vectors_res3.jsonl": "eece5502f7463d190c489130d8ed1904183a8a25dbdff3ce60fc21782a1006a2",
    "vectors_res4.jsonl": "2f20d2fe4949f27b3fc12de25f5dbfdc16a1a03230aaf9fac37edeba6201cdc3",
    "vectors_res5.jsonl": "d854a06dac003df89475f476af816339f38c2f7823715c3f58fca117ef481d79",
    "vectors_res6.jsonl": "460f7b939a28790aad125303d07f9894d8c9a3a81fa3976d5b35163dc3742cb3",
}


def _rows(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_fixture_file_set_and_digests_are_frozen() -> None:
    assert {path.name for path in FIXTURE_DIR.glob("*.jsonl")} == set(EXPECTED_DIGESTS)
    for name, expected in EXPECTED_DIGESTS.items():
        assert hashlib.sha256((FIXTURE_DIR / name).read_bytes()).hexdigest() == expected


def test_exhaustive_vector_counts_and_sequence_numbers() -> None:
    total = 0
    for resolution in range(7):
        rows = _rows(FIXTURE_DIR / f"vectors_res{resolution}.jsonl")
        assert len(rows) == cell_count(resolution)
        assert [int(row["seqnum"]) for row in rows] == list(range(1, cell_count(resolution) + 1))
        assert all(row["res"] == resolution for row in rows)
        assert all(math.isfinite(row["center_lon"]) and math.isfinite(row["center_lat"]) for row in rows)
        total += len(rows)
    assert total == 54_624


def test_topology_fixture_rows_cover_each_source_cell_once() -> None:
    for kind, max_resolution in (("neighbors", 4), ("children", 3)):
        for resolution in range(max_resolution + 1):
            rows = _rows(FIXTURE_DIR / f"{kind}_res{resolution}.jsonl")
            assert [int(row["seqnum"]) for row in rows] == list(range(1, cell_count(resolution) + 1))
            assert all(row["res"] == resolution for row in rows)
            assert all(row[kind] == sorted(set(row[kind]), key=int) for row in rows)
