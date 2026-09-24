# Meal vs Basket — Google Maps Review Crawler

This repository contains the production foundation for crawling Google Maps reviews for bounds of curated Coles stores.

## Current Architecture

The actively selected and successful review crawling approach uses **SeleniumBase UC** for bot-evasion and search-query navigation. The core pipeline steps are:

1. **Warm-up**: Navigate to `google.com` to establish a healthy, trusted session browser profile.
2. **Search Navigation**: Navigate directly via Google Maps search (`/maps/search/Store+Name+Address/`) to naturally trigger Google's backend place resolution.
3. **Reviews UI**: Programmatically open the "Reviews" tab for the resolved place.
4. **Scrolling and Extraction**: Rapidly scroll the review pane, extracting the internal `data-review-id`, author, rating, and text of each review in sight. 
5. **Normalization**: Extract stable `data-review-id`s to correctly deduplicate appended records and incrementally persist to local storage (SQLite/JSON lines).

This approach consistently validates extraction of >1000 reviews/store on healthy sessions without encountering CAPTCHAs, rate-limits, or requiring proxy rotation.

## Repository layout

```
crawl_experiment/          # Shared runtime code & helpers
  models.py, storage.py    # Baseline canonical identity & storage modules
  ...                      

experiments/               
  google_reviews_multistore_benchmark.py # Main entry point for multistore SeleniumBase crawler
  google_reviews_scraper_pro_benchmark/  # Upstream SeleniumBase foundations
    run_upstream.py
    upstream/              # Vendored dependencies (minimal retained modules)

tests/                     # Bounded unit/integration tests
config/                    # Store manifests (e.g. sydney_cbd.json)
docs/                      # Notes and retained technical docs
```

## Running the Crawler

Run the bounding tests (no network access required):
```powershell
python -m pytest tests/
```

Run the selected Multistore Benchmark Crawler directly:
```powershell
python experiments/google_reviews_multistore_benchmark.py
```
This serves as the primary entry point to exercise the SeleniumBase pipeline on the known `sydney_cbd.json` store config.

For a targeted single-store check using the upstream wrapper:
```powershell
python experiments/google_reviews_scraper_pro_benchmark/run_upstream.py
```

## Upstream attribution

The foundational search-navigation scraper uses vendored code from **google-reviews-scraper-pro** (MIT, © 2025 Google Reviews Scraper Pro). See `experiments/google_reviews_scraper_pro_benchmark/upstream/UPSTREAM.md` and `LICENSE` for details.
