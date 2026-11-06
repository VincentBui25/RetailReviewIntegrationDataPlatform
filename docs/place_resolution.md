# Google Entity Resolution

## Purpose

Overture Places and Google use different identity systems. Google Entity Resolution
maps one CanonicalPlace to an explicit Google identity without replacing its
canonical identity or taxonomy.

## Input

CanonicalPlace loaded from the [Canonical Place Catalog](place_catalog.md).
Query inputs are name, address and administrative-area name; coordinates provide
matching evidence. No raw Overture Places filtering occurs here.

## Resolution Flow

```text
CanonicalPlace
→ Google Maps text search (browser UI)
→ candidate selection / confirmed place entity
→ matching
→ resolution status
→ google_place_id (when exposed)
```

`sources/google_maps/place_resolution.py` uses the existing Navigator. A search
preview is not a confirmed `/maps/place/` entity. The resolver records identity
kind: `ChIJ...` is a Place ID; `0x...:0x...` is a Google data ID. The existing
`google_place_id` result field can hold either, distinguished by `google_id_kind`.

## Matching Signals

- Normalized name similarity.
- Optional address similarity.
- Geographic distance from CanonicalPlace coordinates to explicit entity coordinates.

The existing gate requires confirmed entity, explicit identity, distance <=100 m,
name similarity >=0.85 and match score >=0.85. Address similarity contributes when
available; postcode is not a separate signal. Coordinates come from `!3d/!4d`,
not the `@` viewport camera. No thresholds are changed by this documentation.

The resolved-entity bridge opens the verified URL and fails closed if identity changes.

## Statuses

| Status | Meaning / handling |
|---|---|
| RESOLVED | Matching gate satisfied; Google Review Crawler is eligible |
| AMBIGUOUS | Insufficient/conflicting evidence or existing duplicate-match guard; retain evidence, skip review crawling without retry |
| NOT_FOUND | No-results evidence; retain CanonicalPlace, skip review crawling without retry |
| ERROR | Exception recorded by the runner; existing retry policy handles transient/unexpected failure |

## Source Presence Behavior

- Overture Places record + Google match → RESOLVED → review crawl eligible.
- Overture Places record + confirmed no Google result → retain CanonicalPlace →
  NOT_FOUND → no review crawl until future Google Entity Resolution.
- Inconclusive matching → AMBIGUOUS, not automatically NOT_FOUND.
- Google-only place → may never enter the current MVP; see
  [Coverage Limitation](place_catalog.md#coverage-limitation).

Evidence persists in smoke `google_resolution.csv` and benchmark events/metrics.
There is no dedicated durable Google Entity Resolution repository. The benchmark
duplicate-Google-ID guard remains worker-local, not a global cross-worker registry.

## Google Places API usage

The requested Google Places Text Search (New) interface,
`POST /v1/places:searchText`, is **not integrated in the current resolver**.
No API request or minimal field mask exists in the current code to document.

Current Google Entity Resolution uses browser navigation and entity evidence for
identity resolution and selected metadata enrichment. Google Review Crawler
collects reviews from Google Maps; it does not use Places API for full review history.
