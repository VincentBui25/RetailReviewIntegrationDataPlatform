import json
from copy import deepcopy

import pytest
from shapely.geometry import Point

from crawl_experiment.core.place_catalog import CanonicalPlace
from crawl_experiment.core.place_categories import (
    get_primary_category, get_category_hierarchy, get_product_category_group,
    is_food_place, normalize_categories,
)
from crawl_experiment.storage import place_catalog as storage
from crawl_experiment.storage.place_catalog import build_overture_catalog


def record(category="vietnamese_restaurant", source_id="one", xy=(1, 1)):
    return {
        "id": source_id, "names": {"primary": "Example"},
        "geometry": Point(*xy).wkb, "confidence": 0.8,
        "addresses": [{"freeform": "Outside ward address text"}],
        "taxonomy": {"primary": category, "hierarchy": [
            "food_and_drink", "restaurant", category,
        ]},
    }


def ward():
    return {"type": "Feature", "properties": {
        "admin_area_id": "ward-1", "admin_area_name": "Current ward", "is_current": True,
        "valid_to": None,
    }, "geometry": {"type": "Polygon", "coordinates": [
        [[0, 0], [2, 0], [2, 2], [0, 2], [0, 0]],
        [[1.4, 1.4], [1.6, 1.4], [1.6, 1.6], [1.4, 1.6], [1.4, 1.4]],
    ]}}


def test_taxonomy_preservation_and_food_rule():
    raw = record()
    original = deepcopy(raw)
    assert is_food_place(raw)
    assert get_primary_category(raw) == "vietnamese_restaurant"
    assert get_category_hierarchy(raw) == raw["taxonomy"]["hierarchy"]
    assert normalize_categories(raw) == ["vietnamese_restaurant", "restaurant"]
    assert raw == original
    raw["taxonomy"]["hierarchy"] = ["shopping", "restaurant"]
    assert not is_food_place(raw)  # primary alone never includes a POI
    assert get_product_category_group(raw) is None
    assert not is_food_place({"taxonomy": None})
    assert get_primary_category({}) is None
    assert get_category_hierarchy({}) == []


@pytest.mark.parametrize("category,group", [
    ("vietnamese_restaurant", "restaurant"), ("seafood_restaurant", "restaurant"),
    ("coffee_shop", "cafe_coffee"), ("cafe", "cafe_coffee"),
    ("bakery", "bakery_dessert"), ("bubble_tea_shop", "beverage"),
    ("fast_food_restaurant", "fast_food"), ("bar", "bar"),
    ("future_unknown_food", "other_food"),
])
def test_explicit_groups(category, group):
    assert get_product_category_group(record(category)) == group


def test_no_invented_ancestors_or_duplicates():
    raw = record()
    raw["taxonomy"]["hierarchy"] = ["food_and_drink", "vietnamese_restaurant"]
    assert normalize_categories(raw) == ["vietnamese_restaurant"]
    raw["taxonomy"]["primary"] = None
    assert normalize_categories(raw) == ["vietnamese_restaurant"]
    assert get_product_category_group(raw) == "other_food"


def test_polygon_filter_not_address_and_raw_separation():
    inside = record()
    nonfood = record(source_id="nonfood")
    nonfood["taxonomy"]["hierarchy"] = ["shopping"]
    result = build_overture_catalog([
        inside, record(source_id="outside", xy=(3, 3)),
        record(source_id="hole", xy=(1.5, 1.5)), nonfood,
        record(source_id="boundary", xy=(0, 1)), inside,
    ], ward(), observed_at="2026-10-01T00:00:00Z")
    assert [p.source_place_id for p in result.places] == ["one", "boundary"]
    place = result.places[0]
    assert place.admin_area_id == "ward-1"
    assert place.category_source == "overture"
    assert place.address == "Outside ward address text"
    assert "taxonomy" not in place.to_dict()
    assert result.raw_payloads[place.place_key] == inside
    result.raw_payloads[place.place_key]["taxonomy"]["primary"] = "changed"
    assert inside["taxonomy"]["primary"] == "vietnamese_restaurant"


def test_serialization_reobservation_and_google_enrichment():
    first = build_overture_catalog([record()], ward(), observed_at="2026-09-30T00:00:00Z").places[0]
    enriched = first.with_google_enrichment(
        place_id="google-id", match_status="matched", match_score=0.9,
        categories=["Google-only category"],
    )
    restored = CanonicalPlace.from_dict(json.loads(json.dumps(enriched.to_dict())))
    assert restored == enriched
    second = build_overture_catalog([record()], ward(), observed_at="2026-10-01T00:00:00Z",
                                    existing={restored.place_key: restored}).places[0]
    assert second.first_seen_at == first.first_seen_at
    assert second.last_seen_at != first.last_seen_at
    assert second.google_categories == ["Google-only category"]
    assert second.primary_category == first.primary_category
    assert second.category_hierarchy == first.category_hierarchy


@pytest.mark.parametrize("geometry", [None, b"bad wkb", {"type": "Point", "coordinates": [999, 1]}])
def test_invalid_coordinate_exclusion(geometry):
    raw = record()
    raw["geometry"] = geometry
    assert build_overture_catalog([raw], ward(), observed_at="now").places == []


def test_historic_ward_rejected():
    area = ward()
    area["properties"]["is_current"] = False
    with pytest.raises(ValueError, match="current ward"):
        build_overture_catalog([], area, observed_at="now")


def test_multipolygon_and_crs():
    area = ward()
    area["geometry"] = {"type": "MultiPolygon", "coordinates": [area["geometry"]["coordinates"]]}
    assert len(build_overture_catalog([record()], area, observed_at="now").places) == 1
    area["crs"] = {"properties": {"name": "EPSG:3857"}}
    with pytest.raises(ValueError, match="WGS84"):
        build_overture_catalog([], area, observed_at="now")


def test_dedup_last_accepted_wins_before_normalization(monkeypatch):
    first, last = record(), record(category="coffee_shop")
    last["names"]["primary"] = "Last row"
    calls = []
    original = storage.normalize_categories
    monkeypatch.setattr(storage, "normalize_categories", lambda r: (calls.append(r["id"]), original(r))[1])
    catalog = build_overture_catalog([first, record(source_id="two"), last,
                                      record(source_id="one", xy=(3, 3))], ward(), observed_at="now")
    assert [p.source_place_id for p in catalog.places] == ["one", "two"]
    assert catalog.places[0].name == "Last row"
    assert calls == ["one", "two"]
    assert catalog.counts == dict(raw_bbox=3, food_filtered=3, inside_polygon=3,
                                  deduplicated=2, canonical_persisted=2)


def test_persisted_catalog_roundtrip_manifest_bbox_and_single_polygon_parse(monkeypatch, tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq
    source, boundary, output = tmp_path / "raw.parquet", tmp_path / "ward.json", tmp_path / "canonical.parquet"
    nonfood = record(source_id="shopping")
    nonfood["taxonomy"]["hierarchy"] = ["shopping"]
    records = [record(), record(source_id="hole", xy=(1.5, 1.5)), record(source_id="outside", xy=(3, 3)),
               record(), nonfood]
    pq.write_table(pa.Table.from_pylist(records), source)
    boundary.write_text(json.dumps(ward()))
    lineage = dict(source_release="test-release", client_version="test-client", retrieved_at="retrieved")
    calls = []
    original = storage.administrative_polygon
    monkeypatch.setattr(storage, "administrative_polygon", lambda area: (calls.append(True), original(area))[1])
    catalog, manifest = storage.prepare_catalog(source, boundary, output, lineage, observed_at="now")
    assert len(calls) == 1
    assert manifest["bbox"] == [0, 0, 2, 2]
    assert manifest["counts"] == dict(raw_bbox=4, food_filtered=3, inside_polygon=2,
                                       deduplicated=1, canonical_persisted=1)
    assert manifest["overture_release"] == "test-release"
    assert manifest["overturemaps_client_version"] == "test-client"
    monkeypatch.setattr(storage, "_build_catalog", lambda *a, **kw: pytest.fail("load must not rebuild"))
    monkeypatch.setattr(storage, "administrative_polygon", lambda *a: pytest.fail("load must not parse polygon"))
    assert storage.load_catalog(output).places == catalog.places


def test_manifest_deterministic_empty_catalog_and_enrichment_update(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq
    source, boundary, output = tmp_path / "raw.parquet", tmp_path / "ward.json", tmp_path / "canonical.parquet"
    pq.write_table(pa.Table.from_pylist([record()]), source)
    boundary.write_text(json.dumps(ward()))
    lineage = dict(source_release="known", client_version="client", retrieved_at="retrieved")
    first, manifest = storage.prepare_catalog(source, boundary, output, lineage, observed_at="first")
    again, same = storage.prepare_catalog(source, boundary, output, lineage, observed_at="first")
    assert manifest == same and again.places == first.places
    enriched = first.places[0].with_google_enrichment(place_id="google", match_status="RESOLVED",
                                                    match_score=1.0, categories=["enrichment"])
    storage.persist_catalog(storage.PlaceCatalog([enriched], {}, first.counts), output, manifest)
    updated, _ = storage.prepare_catalog(source, boundary, output, lineage, observed_at="second")
    assert updated.places[0].first_seen_at == "first"
    assert updated.places[0].last_seen_at == "second"
    assert updated.places[0].google_place_id == "google"
    empty = storage.build_overture_catalog([], ward(), observed_at="now")
    storage.persist_catalog(empty, output, manifest)
    assert storage.load_catalog(output).places == []


def test_query_reader_receives_polygon_bbox_and_decodes_once(monkeypatch, tmp_path):
    import pyarrow as pa
    boundary, output = tmp_path / "ward.json", tmp_path / "catalog.parquet"
    boundary.write_text(json.dumps(ward()))
    calls, decoded = [], []
    original = storage.from_wkb
    monkeypatch.setattr(storage, "from_wkb", lambda values, **kwargs: (decoded.append(len(values)), original(values, **kwargs))[1])
    def reader(**kwargs):
        calls.append(kwargs)
        return pa.Table.from_pylist([record()])
    catalog, manifest = storage.prepare_catalog(None, boundary, output, {"release": "fixed"},
                                                observed_at="now", reader=reader)
    assert calls == [{"bbox": (0.0, 0.0, 2.0, 2.0), "release": "fixed"}]
    assert decoded == [1] and len(catalog.places) == 1
    assert manifest["source_mode"] == "bbox_query"


def test_catalog_load_fails_closed_on_corrupt_or_missing_manifest(tmp_path):
    output = tmp_path / "catalog.parquet"
    catalog = build_overture_catalog([record()], ward(), observed_at="now")
    storage.persist_catalog(catalog, output, {})
    output.write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="checksum"):
        storage.load_catalog(output)
    storage.manifest_path(output).unlink()
    with pytest.raises(FileNotFoundError, match="prepare_place_catalog"):
        storage.load_catalog(output)
