"""Pinned demographic admission; synthetic fixtures never touch published references."""

import hashlib
import json
from pathlib import Path
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from fnb_platform.analytics import warehouse as wh


def publication(folder, **changes):
    row = dict(
        ward_code="test-ward",
        ward_name="Synthetic Area",
        city_code="79",
        legal_code_status="CODE_AND_NAME_AGREE",
        area_km2=2.0,
        population=4000,
        reference_year=2025,
        source_version="fixture-only",
        source_url="synthetic://reference",
        source_record_url="synthetic://ward",
        population_basis="2025_REORGANIZATION_PROPOSAL_NOT_CENSUS",
        area_basis="PROVIDER_REORGANIZATION_REFERENCE_NOT_INDEPENDENTLY_VERIFIED",
        data_quality_status="WARNING",
        provenance_json="{}",
    )
    row.update(changes)
    signature = {
        "publication_kind": "AREA_DEMOGRAPHICS_SILVER",
        "fixture_only": True,
        "reference": "synthetic",
    }
    rid = hashlib.sha256(wh.dump(signature).encode()).hexdigest()[:24]
    release = folder / "releases" / rid
    release.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist([row]), release / "area_demographics.parquet")
    for name in ("summary.json", "dq_summary.json"):
        (release / name).write_text("{}")
    manifest = dict(
        publication_kind="AREA_DEMOGRAPHICS_SILVER",
        release_id=rid,
        signature=signature,
        artifacts={p.name: {"sha256": wh.sha(p)} for p in release.iterdir()},
    )
    (release / "manifest.json").write_text(wh.dump(manifest))
    return release, wh.sha(release / "manifest.json")


def test_valid_pinned_reference_preserves_warning_and_provenance(tmp_path):
    path, digest = publication(tmp_path)
    row, pins = wh.demographic_input(path, digest, "test-ward")
    assert (row["area_km2"], row["population"], row["data_quality_status"]) == (
        2.0,
        4000,
        "WARNING",
    )
    assert len(pins) == 4 and pins[str(path / "manifest.json")] == digest


@pytest.mark.parametrize(
    "fault",
    [
        "missing",
        "corrupt",
        "wrong_pin",
        "pointer",
        "wrong_ward",
        "legal_conflict",
        "bad_basis",
        "negative_area",
        "duplicate",
        "numeric_schema",
    ],
)
def test_invalid_demographic_admission_rejected(tmp_path, fault):
    changes = (
        {"legal_code_status": "CONFLICT"}
        if fault == "legal_conflict"
        else (
            {"population_basis": "CENSUS"}
            if fault == "bad_basis"
            else {"area_km2": -1.0} if fault == "negative_area" else {}
        )
    )
    if fault == "numeric_schema":
        changes["population"] = "not-a-number"
    path, digest = publication(tmp_path, **changes)
    ward = "test-ward"
    if fault == "missing":
        (path / "area_demographics.parquet").unlink()
    if fault == "corrupt":
        with (path / "area_demographics.parquet").open("ab") as stream:
            stream.write(b"CORRUPT_FIXTURE")
    if fault == "wrong_pin":
        digest = "0" * 64
    if fault == "pointer":
        path = path.parent / "latest"
    if fault == "wrong_ward":
        ward = "different-boundary"
    if fault == "duplicate":
        table = pq.read_table(path / "area_demographics.parquet")
        pq.write_table(
            pa.concat_tables([table, table]), path / "area_demographics.parquet"
        )
        manifest = json.loads((path / "manifest.json").read_text())
        manifest["artifacts"]["area_demographics.parquet"]["sha256"] = wh.sha(
            path / "area_demographics.parquet"
        )
        (path / "manifest.json").write_text(wh.dump(manifest))
        digest = wh.sha(path / "manifest.json")
    with pytest.raises(ValueError):
        wh.demographic_input(path, digest, ward)
