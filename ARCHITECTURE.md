# Crawler architecture

## Goals and flow

Failures are isolated at the owning layer, partial data is preserved, browser implementations are replaceable, and future sources can reuse canonical models, storage, orchestration, and metrics.

```text
Manifest -> Crawlee BasicCrawler / RequestQueue / SessionPool
         -> BrowserFactory -> SeleniumBaseSession (UC)
         -> Navigator -> ReviewSurface -> Paginator -> Extractor
         -> ReviewRepository + CheckpointRepository + CrawlMetrics
```

## Module responsibilities

| Module | Owns | Explicitly does not own | Typical failure |
|---|---|---|---|
| `core/models.py`, `statuses.py` | canonical data and outcomes | DOM/browser/storage | invalid domain input |
| `browser/seleniumbase_session.py` | UC start, liveness, close, restart | Maps/selectors/storage | dead WebDriver |
| `browser/browser_factory.py` | session construction | source behavior | construction failure |
| `network/*` | direct identity/future proxy seam | rotation/source behavior | identity allocation |
| `google_maps/selectors.py` | Maps selectors | interaction policy | selector drift |
| `google_maps/navigator.py` | warm-up, search, place plausibility | reviews/parsing | LIMITED/navigation |
| `google_maps/review_surface.py` | Reviews tab, sort, pane | scrolling/parsing | surface/sort unavailable |
| `google_maps/paginator.py` | IDs, scrolling, idle/deadline | field parsing/storage | stall/timeout |
| `google_maps/extractor.py` | card to canonical Review | navigation/restart/write | malformed card |
| `google_maps/health.py` | page-state classification | retry/CAPTCHA solving | challenge/rate limit |
| `google_maps/crawler.py` | per-store flow | retry/concurrency | partial result assembly |
| `storage/*` | idempotent upsert and atomic checkpoints | DOM/browser | duplicate/interruption |
| `orchestration/crawlee_adapter.py` | queue, dedupe, concurrency, request/session lifecycle | WebDriver/Maps | exhausted request |
| `orchestration/retry_policy.py` | failure-aware action | state detection | bounded retry decision |
| `observability/metrics.py` | structured stage events | recovery | diagnostics |

## Failure ownership

| Failure | Owner | Result/action |
|---|---|---|
| WebDriver dies | browser session | `SESSION_FAILED`; fresh session, bounded |
| LIMITED | health/navigation | `LIMITED`; fresh session, bounded |
| Rate limit | health + retry policy | `RATE_LIMITED`; cooldown/retire |
| Challenge | health + retry policy | `CHALLENGE`; stop/retire, no bypass |
| Place resolution | navigator | `NAVIGATION_FAILED`; bounded retry |
| Reviews tab/sort drift | surface/selectors | surface or `SORT_FAILED` |
| Scroll stops | paginator | checkpoint; `PAGINATION_STALLED` |
| Safety deadline | paginator/coordinator | preserve data; `PARTIAL_TIMEOUT` |
| Review DOM drift | extractor/selectors | skip/metric; `EXTRACTION_DEGRADED` |
| Duplicate | repository | idempotent upsert |

## Crawlee, SeleniumBase, and session lifecycle

Crawlee 1.x `BasicCrawler` supplies its public request queue, unique keys, concurrency, statistics, retry, and session pool. Blocked-page bypass is disabled. A Crawlee session ID selects one network identity and one project-owned browser session. `BrowserFactory` creates SeleniumBase UC; Google Maps modules receive only its driver. Health describes the page, while `RetryPolicy` chooses retry, cooldown, partial preservation, or stop.

## Extension points

`DirectProxyProvider` returns no proxy. A future sticky provider can bind a proxy to `NetworkIdentity` without changing navigator, paginator, or extractor. A future source adds its own navigator, surface/paginator, and extractor while reusing `core`, `storage`, `orchestration`, and `observability`.

## Status model

`COMPLETE` reached the end. `PARTIAL_TIMEOUT` retains reviews at the deadline. `SESSION_FAILED`, `NAVIGATION_FAILED`, and `PLACE_RESOLUTION_FAILED` locate infrastructure/navigation failures. `LIMITED`, `RATE_LIMITED`, and `CHALLENGE` describe health. `REVIEW_SURFACE_UNAVAILABLE` and `SORT_FAILED` locate UI failures. `PAGINATION_STALLED` retains partial extraction. `EXTRACTION_DEGRADED` means cards failed individually. `ERROR` is unclassified.

## Operational rules

No CAPTCHA solving or access-control bypass is attempted. Reviews are written incrementally and checkpoints atomically. Long crawls run manually and produce checkpoint/progress artifacts; Codex does not poll them.
