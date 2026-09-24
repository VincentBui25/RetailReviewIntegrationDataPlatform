# Project Context

This repository implements the "Meal vs Basket" data engineering project.

Current milestone:
- We are currently testing data crawling only.
- Do NOT build the full pipeline yet.
- Start with one retailer and 5–10 stores.

## Crawl goals

The crawler must prove:

1. Store discovery works.
2. Stores are uniquely identified by:
   retailer_code + retailer_store_id
3. Reviews are deduplicated by source review ID.
4. Running the crawler twice must not create duplicate stores or reviews.
5. Failed crawls must be resumable.
6. Record rate-limit and anti-bot indicators.

## Crawl strategy

Preferred extraction order:

1. Official/public API
2. Network JSON/API responses
3. DOM extraction
4. Browser automation only when required

Use Crawlee + Playwright where browser interaction is necessary.

## Current test scope

Retailer:
- Coles only

Scope:
- 5–10 stores
- one small geographic area

Do not crawl all Sydney yet.

## Rate-limit testing

Start conservatively.

Log:
- HTTP status
- latency
- retry count
- 403 count
- 429 count
- CAPTCHA/block indicator
- records extracted

Do not attempt to bypass source restrictions.

## Idempotency

Store logical key:

retailer_code + retailer_store_id

Review logical key:

source_code + source_review_id

Database writes must be idempotent.

## Data handling

Raw responses should be retained for replay/debugging.

Do not overwrite historical raw payloads.

## Before modifying code

Read:
- docs/architecture.md
- docs/data-model.md
- docs/crawl-plan.md

## Testing requirements

Before finishing a task:
- run existing tests
- add tests for new crawler behavior
- verify rerunning the same input does not create duplicates