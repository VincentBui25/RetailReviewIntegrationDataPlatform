# Canonical Place Catalog

## Purpose

The Canonical Place Catalog is the contract between Overture Places discovery and
downstream source-specific Google Entity Resolution. Discovery becomes stable
CanonicalPlace records, persisted once and reused by smoke tests, benchmarks,
Google Entity Resolution and Google Review Crawler workflows.

## Processing Flow

```text
Administrative Polygon
        |
        v
polygon.bounds
        |
        v
Overture bbox load/query
        |
        v
food_and_drink taxonomy filter
        |
        v
exact point-in-polygon
        |
        v
source-ID deduplication
        |
        v
normalization
        |
        v
canonical_places.parquet
        |
        v
Google Entity Resolution
```

## Processing Stages

| Stage | Input | Operation | Output |
|---|---|---|---|
| Boundary | Administrative Polygon | Validate WGS84/current ward; compute bbox | Query bbox |
| Discovery | Bbox | Load/query Overture Places | Candidate POIs |
| Taxonomy | Candidate POIs | Keep hierarchy containing `food_and_drink` | Food POIs |
| Spatial | Food POIs | Exact point-in-polygon | Ward POIs |
| Dedup | Accepted POIs | Exact source-ID dedup | Unique source records |
| Normalize | Unique records | Create CanonicalPlace fields | Typed canonical rows |
| Persist | Canonical rows | Parquet + manifest | Reusable Canonical Place Catalog |

Implementation: `scripts.prepare_place_catalog` calls `storage.place_catalog.prepare_catalog()`.
The Administrative Polygon is parsed once. Geometry is decoded once and coordinates
are reused; `polygon.covers(point)` includes boundary points and excludes polygon holes.
`build_overture_catalog()` remains a compatibility entrypoint to the same pipeline.

## Why BBox + Polygon

```text
bbox = coarse filter
polygon = exact administrative filter
```

Bbox reduces logical candidate scope but may include places outside the ward.
Point-in-polygon decides exact membership. In local cache mode the current
implementation still scans the full local snapshot before bbox filtering.

## Filtering Order

```text
bbox → taxonomy → PIP → dedup → normalization
```

Selective filters reduce work before normalization. In the current Arrow path,
bbox filtering precedes nested Python materialization; taxonomy/PIP are then
applied to those bbox records, not pushed into the Parquet reader. Deduplication
ensures each winning source record is normalized once.

## Current Khánh Hội Example

Example from cached Overture Places release `2026-09-23.1`, not global constants:

| Stage | Rows |
|---|---:|
| Full snapshot scanned | 378,036 |
| Within ward bbox | 4,354 |
| Food hierarchy | 1,159 |
| Inside polygon | 682 |
| Deduplicated/persisted | 682 |

## CanonicalPlace Contract

The following persisted fields match the current `core.place_catalog.CanonicalPlace`
dataclass. Query/matching inputs and sample-selection fields are identified explicitly.

| Field | Meaning |
|---|---|
| `place_key` | Internal identity (`overture:<source ID>`); used to derive the review-job identity. |
| `source` | Source catalog identifier; currently `overture`. |
| `source_place_id` | Stable Overture Places source ID. |
| `name` | Place name; Google Entity Resolution query and name-matching input. |
| `lat` | Latitude; Google Entity Resolution distance input. |
| `lng` | Longitude; Google Entity Resolution distance input. |
| `address` | Available freeform address; query and optional address-matching input. |
| `primary_category` | Primary Overture taxonomy category; selection input. |
| `categories` | Normalized supplied categories, primary first. |
| `category_hierarchy` | Supplied Overture taxonomy hierarchy. |
| `product_category_group` | Application group derived from Overture taxonomy; sample-selection input. |
| `category_source` | Classification provenance; currently `overture`. |
| `confidence` | Available source confidence; high-confidence sample-selection input. |
| `admin_area_id` | Administrative Polygon identity. |
| `admin_area_name` | Administrative Polygon name; Google Entity Resolution query input. |
| `first_seen_at` | First canonical observation timestamp. |
| `last_seen_at` | Latest catalog preparation observation timestamp. |
| `google_place_id` | Optional retained Google identity enrichment. |
| `google_match_status` | Optional retained Google Entity Resolution status enrichment. |
| `google_match_score` | Optional retained Google match-score enrichment. |
| `google_categories` | Optional retained Google categories; never replace Overture taxonomy. |

All 21 fields are persisted; loading does not reconstruct them from raw payloads.
Overture taxonomy stays canonical and Google categories remain enrichment.

## Artifact Layout

```text
data/place_catalog/khanh_hoi/
  canonical_places.parquet
  canonical_places.manifest.json
```

Parquet contains typed CanonicalPlace rows. The manifest contains lineage and
validation metadata. Each file is atomically finalized; the pair is not a
multi-file transaction. A checksum mismatch rejects mixed generations.

## Manifest Contract

Metadata currently produced by preparation:

| Keys | Meaning |
|---|---|
| `schema_version` | Canonical artifact schema version |
| `overture_release` | Actual Overture Places release used |
| `overturemaps_client_version` | Client version recorded in source lineage |
| `retrieved_at`, `generated_at` | Source retrieval and catalog preparation timestamps |
| `admin_area_id`, `admin_area_name` | Administrative Polygon identity and name |
| `boundary_source`, `boundary_osm_id`, `boundary_valid_from`, `boundary_sha256` | Available boundary provenance/version and geometry hash |
| `bbox` | `polygon.bounds`: west, south, east, north |
| `counts` | `raw_bbox`, `food_filtered`, `inside_polygon`, `deduplicated`, `canonical_persisted` |
| `artifact_sha256` | Parquet checksum |
| `dedup_policy` | Winner and row-order policy |
| `source_mode`, `source_path`, `source_url`, `attribution` | Access mode, cached path when applicable, source and attribution |

The manifest records the exact release used. It does not claim that the release
is the latest available Overture release. Unavailable optional lineage values may
be null; a source release is required by preparation.

The loader validates schema version, checksum, Parquet fields/types, canonical
row count and identity uniqueness. A complete required-field/type validator for
all lineage metadata is not implemented; manifest test coverage is partial.

## Rebuild Conditions

Rebuild when:

- Overture Places release changes.
- Administrative Polygon changes.
- Taxonomy rules change.
- Canonical schema changes.

Do not rebuild because a review crawl runs. Preparation uses source cache plus
Administrative Polygon and source-metadata sidecar; no existing generated catalog
is needed for a fresh build. Repreparation can retain existing first-seen timestamps
and Google enrichment. Missing/incompatible artifacts fail with preparation guidance.

## Deduplication

Exact source-ID deduplication is implemented. The audited Khánh Hội food records
inside the Administrative Polygon contain **0 duplicate source IDs**.

Current fallback behavior: the last accepted record wins and the first identity
occurrence determines row order. It is order-dependent if conflicting duplicate
records ever appear. For the current dataset no conflicting duplicate policy
affects output. Fuzzy geographic conflation is not implemented.

## Reproducibility

For the same ordered source cache, Administrative Polygon and source metadata,
row count is stable and canonical content is reproducible. The current audit
produced identical content hashes across two fresh generations after excluding
`first_seen_at`/`last_seen_at`; stable manifest fields also matched after excluding
the generation timestamp and timestamp-dependent artifact checksum.

Order-independent reproducibility is not guaranteed if future conflicting
duplicate IDs are introduced. Source reordering can also change row order even
when identities are unique.

## Downstream Contract

Downstream consumers load `canonical_places.parquet`. They must not re-run taxonomy
filtering, re-run point-in-polygon or re-read raw Overture Places payloads during
normal execution.

```text
canonical_places.parquet
→ load_catalog()
→ CanonicalPlace.from_dict()
→ smoke/benchmark selection
→ resolve_place(CanonicalPlace, driver)
```

Existing benchmark sample JSON is self-contained and can deserialize saved
CanonicalPlace rows without catalog access. New samples load the persisted
Canonical Place Catalog. `--catalog` selects the artifact; legacy `--raw-catalog`
is rejected with explicit preparation guidance.

## Coverage Limitation

Overture Places is the primary discovery source. Places that exist only on Google
and are absent from Overture are not guaranteed to enter the current pipeline.
This is an accepted MVP coverage limitation.

## Non-Goals

This stage does not:

- Crawl reviews.
- Perform Google grid discovery.
- Guarantee Google-only POI coverage.
- Perform incremental review ingestion.
- Perform fuzzy duplicate conflation beyond exact source-ID handling.
