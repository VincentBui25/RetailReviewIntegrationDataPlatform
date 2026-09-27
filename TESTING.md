# Testing Guide

Tests are kept under `tests/`. Test fixtures, fake drivers, and mock elements should not be added to the production package.

## Test Coverage

The current milestone focuses on validating the crawl runner, observability, and persistence pipeline.

| Production area | Tests |
|---|---|
| Full-crawl runner and lifecycle | `test_full_crawl_runner.py` |
| Run artifacts, metadata, DLQ, and summaries | `test_observability.py` |
| SQLite state, idempotency, outbox, and Parquet persistence | `test_review_persistence.py` |
| End-to-end SQLite → outbox → Parquet → DuckDB flow | `test_persistence_e2e.py` |

These tests cover the main behaviors introduced or refactored in the current architecture.

## Testing Rules

- Unit and integration tests must not access Google Maps or the public network.
- Repository tests should use temporary SQLite databases or isolated temporary directories.
- Runner tests must use `tmp_path` or an equivalent temporary location and must not write to the real `data/` directory.
- Persistence tests should verify `INSERT`, `UNCHANGED`, and `UPDATE` behavior explicitly.
- Outbox recovery tests should verify that restart/replay does not duplicate finalized Parquet events.
- Tests should not depend on execution order, previous crawl runs, real browser state, or wall-clock timing.
- A production bug should have a focused regression test whenever practical.

## Test Levels

1. **Unit tests**  
   Validate individual behaviors such as state transitions, hashing, or persistence decisions.

2. **Component tests**  
   Validate interactions between related components such as repository state, transactional outbox, and Parquet writing.

3. **Runner tests**  
   Validate run-directory creation, lifecycle metadata, DLQ behavior, summaries, and resource cleanup using temporary directories.

4. **End-to-end persistence validation**  
   Validate the complete synthetic flow:

   ```text
   Review
     -> SQLite state
     -> transactional outbox
     -> Parquet
     -> DuckDB readback
   ```

   This validation uses synthetic reviews only and does not require a live browser or Google Maps access.

## Running Tests

Run focused tests first when modifying a specific component.

For the persistence and runner milestone:

```bash
python -m pytest -q \
  tests/test_full_crawl_runner.py \
  tests/test_observability.py \
  tests/test_review_persistence.py \
  tests/test_persistence_e2e.py
```

Then run the full suite:

```bash
python -m pytest -q
```

Compile-check the project:

```bash
python -m compileall crawl_experiment scripts tests
```

Check Git whitespace issues:

```bash
git diff --check
```

Do not record a fixed passing test count, run ID, or generated artifact path in this document because those values change as the project evolves.

## Live Validation

Live Google Maps crawling is intentionally excluded from the default automated test suite.

When live validation is needed, run it manually with bounded limits and dedicated artifacts. Long-running crawls should not be started or polled by automated test workflows.
