# Changelog

## Current milestone

- Established Overture-first canonical place discovery with persisted catalog
  reuse, separating geographic POI discovery from Google resolution.
- Added full, incremental, and reconciliation review modes. Incremental mode
  uses stable IDs and known-unchanged streaks; reconciliation revisits a recent
  window without treating incomplete coverage as deletion.
- Moved review durability to SQLite state plus a transactional outbox and made
  Parquet the append-only downstream mutation format.
- Added explicit FULL, LIMITED, and UNKNOWN access handling with deferred
  retry/backoff and separate browser/runtime failures.

## Earlier milestones

- Split the configured CLI from the reusable crawl runner and run-artifact
  reporting layer.
- Added deterministic review content hashing, idempotent reruns, and bounded
  persistence/integration validation.
- Kept network review extraction experimental while retaining DOM extraction as
  the production path.
