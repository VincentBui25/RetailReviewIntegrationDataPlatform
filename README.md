# Retail Review Integration Data Platform

The active crawler gathers Google Maps reviews through SeleniumBase UC and normalizes them into source-independent models. Crawlee handles request orchestration; it does not own the browser or Google Maps behavior.

See [ARCHITECTURE.md](ARCHITECTURE.md) for module boundaries, semantic statuses, and failure ownership.

Run bounded tests (no network or browser): `python -m pytest -q`.

Run a crawl manually: `python -m crawl_experiment --manifest config/known_coles_stores.json`.

Progress is persisted under `data/checkpoints` and reviews are upserted into `data/reviews.sqlite3`. Current networking is direct; no proxy or CAPTCHA bypass is implemented. Long crawls must be run outside Codex monitoring.
